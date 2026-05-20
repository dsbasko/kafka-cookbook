# 09-02 — Push Notifications

A use case about delivering notifications to three external channels. One Kafka topic at the input, three recipient channels, each with its own retry pipeline and DLQ. This assembles four lectures at once — the outbox pattern ([Outbox pattern](../../../../04-reliability/04-03-outbox-pattern/i18n/en/README.md)) was left out, but retry/DLQ ([Retry and DLQ deep dive](../../../../04-reliability/04-04-retry-and-dlq/i18n/en/README.md)), CB and HMAC ([Delivery to external systems](../../../../04-reliability/04-05-external-delivery/i18n/en/README.md)), microservice guarantee with dedup ([Microservice communication](../../../01-microservices-comm/i18n/en/README.md)), and Protobuf ([Protobuf in Go](../../../../05-contracts/05-02-protobuf-in-go/i18n/en/README.md)) are all relevant here.

## What we're building

The scenario is simple. Some upstream (producer) sends a `Notification` to `notification-events`. The message has a `channel` field. Three valid values:

1. `firebase` — push to an Android app.
2. `apns` — push to an iOS app.
3. `webhook` — HTTP call to a third-party client URL.

The service then needs to reach the corresponding external recipient over HTTP, survive failures with retry and backoff, not crash entirely when one channel fails for an extended period, avoid duplicate delivery, and maintain a history per notification.

Internally it looks like this:

```
notification-events
       │
       ▼
 notification-router (consumer)
       │
       ├──► notification-firebase ──► firebase-sender ──► mock-firebase
       │           │
       │           ├──► notification-firebase-retry-30s
       │           ├──► notification-firebase-retry-5m
       │           └──► notification-firebase-dlq ──► firebase-dlq-consumer
       │
       ├──► notification-apns     ──► apns-sender     ──► mock-apns
       │           └── (same retry/dlq)
       │
       └──► notification-webhook  ──► webhook-sender  ──► mock-webhook
                   └── (same retry/dlq)
```

14 topics. That sounds like a lot. Each channel gets 4 (main + retry-30s + retry-5m + dlq), one shared entry topic, and one router-DLQ for records where `channel` is not set at all (proto3 default = `CHANNEL_UNSPECIFIED`) or has an unknown value. If there were 10 channels, only the topic list would grow, not the code.

## Why separate topics per channel

You could keep a single `notifications` topic and filter by `channel` in each sender. Don't. Reasons:

- channel consumer groups scale independently. If firebase is slow, apns shouldn't wait;
- retry delays can differ per channel. Five minutes is fine for a webhook; a push to a banking app should retry in thirty seconds;
- DLQ is easier to read when it's immediately clear: `notification-firebase-dlq` — the problem is in one channel, the others are alive;
- Kafka's topic limit is tens of thousands per broker. Ten extra topics won't matter.

The cost is one extra forward in the router. Acceptable.

## What the code shows

First, the router. A thin consumer on `notification-events`. Extracts `Notification` from the protobuf payload and forwards it byte-for-byte to the appropriate channel topic based on the `channel` field. No extra processing, no application logic.

```go
out = append(out, &kgo.Record{
    Topic:   dest,
    Key:     r.Key,
    Value:   r.Value,
    Headers: appendRouterHeaders(r.Headers, o.NodeID, n.GetChannel().String()),
})
```

`destinationFor` is a switch on the channel enum, nothing fancy. We append `router-node` and `channel` headers for tracing. Then `ProduceSync` with the accumulated batch, then `CommitRecords` on the input. The same at-least-once guarantee as in outbox: there's a window for duplicates between produce and commit, caught by dedup in the sender.

If a record has `channel=CHANNEL_UNSPECIFIED` (proto3 default from a broken producer) or an enum the router doesn't know, it goes to `notification-events-dlq` — a separate router-DLQ topic. Silently dropping it is not an option: with `proto3`, forgetting to set the field is easy, so we separate "routed correctly" from "no destination found" at the topic level, not by silence.

The key point here: no domain logic appeared. The router is dumb and fast. All the complexity lives in the senders.

### Sender — one per channel

Each channel sender is a single process with thin scaffolding (three channel variants, one sender each). It subscribes to its own main topic and two retry stages:

```go
stages := []Stage{
    {Topic: *mainTopic,    Delay: 0,             NextTopic: *retry30Topic},
    {Topic: *retry30Topic, Delay: *delay30,      NextTopic: *retry5mTopic},
    {Topic: *retry5mTopic, Delay: *delay5m,      NextTopic: ""},
}
```

One consumer group across all three topics. On retry stages, before processing we wait until `record.Timestamp + stage.Delay` has elapsed — a record written to retry-30s at 12:00:00 waits until 12:00:30. This blocks the poll loop, but intentionally: the pipeline should be transparent. Production does it more elegantly (a separate goroutine per retry topic, or `PauseFetchPartitions` — the latter covered in [Delivery to external systems](../../../../04-reliability/04-05-external-delivery/i18n/en/README.md)).

```go
if st.Delay > 0 {
    if err := waitUntilDue(ctx, r.Timestamp, st.Delay); err != nil {
        return err
    }
}
```

Then — a delivery attempt. Under the protection of the circuit breaker:

```go
result, err := s.cb.Execute(func() (deliveryResult, error) {
    return s.deliverWithRetries(ctx, &n)
})
```

Inside `deliverWithRetries` — standard backoff with jitter, up to `MaxAttempts` times within a single stage. The CB watches from outside: if N consecutive `Execute()` calls return an error, it transitions to Open and cuts further calls. Half of [Delivery to external systems](../../../../04-reliability/04-05-external-delivery/i18n/en/README.md) appears here unchanged — the pattern is universal.

### Where a record goes after failure

Three outcomes:

1. **Success** — write `notifications_log(status='delivered', ...)` plus `processed_events(consumer, notification_id)` in a single transaction. Commit the offset in Kafka.
2. **Permanent error** (4xx except 408/429, or invalid protobuf) — immediately to DLQ, skipping retry. Commit (safe to do so).
3. **Transient error** (5xx, 408, 429, network, timeout) and retries exhausted — forward to the stage's `nextTopic`. Commit.

```go
target := st.NextTopic
reason := "next-retry"
if permanent {
    target = s.opts.DLQTopic
    reason = "permanent"
} else if target == "" {
    target = s.opts.DLQTopic
    reason = "exhausted"
}
```

An empty `NextTopic` on the last stage signals "DLQ next". The forward headers carry: `retry.count`, `error.class`, `error.message`, `original.topic`, `previous.topic`, `forward.reason`. In the DLQ you can see the full route — what failed, where it failed, how many times.

### Dedup and effective-exactly-once

Between `cb.Execute()` (which may contain multiple HTTP attempts) and the write to Postgres there is a window. If the process crashes after a successful delivery but before committing the offset — on restart the sender reads the same message again. The receiver has already seen the notification by `Idempotency-Key` (the `notification_id`) — it won't duplicate. But our `notifications_log` table could:

```go
err = pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error {
    consumer := string(s.opts.Channel) + "-sender"
    tag, err := tx.Exec(ctx, dedupSQL, consumer, n.GetId())
    if err != nil { return fmt.Errorf("dedup: %w", err) }
    if tag.RowsAffected() == 0 {
        return nil // already processed — exit without INSERT into notifications_log
    }
    _, err = tx.Exec(ctx, insertHistorySQL, ...)
    return err
})
```

The gate and the business insert in one transaction — otherwise a crash between them would leave the gate saying "already processed" while the log never appeared. The same technique as in [Microservice communication](../../../01-microservices-comm/i18n/en/README.md).

### HMAC and external idempotency

Every HTTP request carries two key headers:

```go
req.Header.Set("Idempotency-Key", n.GetId())
req.Header.Set("X-Signature", sig) // hex(HMAC-SHA256(secret, body))
```

`Idempotency-Key` is the `notification_id`. The same id across all retries of one notification tells the receiver on the Firebase / APNs / webhook side: "I've seen this, not sending to the user again". In the mocks we only log it; in production there's real receiver logic.

`X-Signature` is HMAC-SHA256 of the body using a shared secret. The receiver verifies it — no one from the same IP can inject unauthorized pushes.

### DLQ as a separate consumer

The DLQ topic is a terminal. It receives records that exhausted all stages without delivery, or those that arrived as `permanent`. The sender does not write to the DLQ itself — that's a separate process running in `-mode=dlq` mode:

```go
case "dlq":
    err := RunDLQ(ctx, DLQOpts{
        NodeID:    *nodeID,
        Channel:   d.Channel,
        DLQTopic:  *dlqTopic,
        Group:     *dlqGroup,
        DSN:       dsn,
        FromStart: *fromStart,
    })
```

This process writes `notifications_log(status='dlq', last_error=..., attempts=...)` — a history entry indicating delivery failed. In a real system you'd also add a Slack alert, a Prometheus metric, an admin page for manual replay, and a thread in the on-call channel.

## Mock services

Three stdlib-only HTTP handlers. Same code, different port and name:

- `cmd/mock-firebase/main.go` on :8091
- `cmd/mock-apns/main.go` on :8092
- `cmd/mock-webhook/main.go` on :8093

Each accepts POST `/send`. Based on `FAIL_RATE_503` it returns 503, based on `FAIL_RATE_TIMEOUT` it hangs for N seconds, `/health` handles health checks, everything else returns 200. This is the pattern from [Delivery to external systems](../../../../04-reliability/04-05-external-delivery/i18n/en/README.md) replicated across three channels.

```go
case dice < fail503:
    w.Header().Set("Retry-After", "1")
    http.Error(w, ..., http.StatusServiceUnavailable)
case dice < fail503+failTimeout:
    select {
    case <-time.After(time.Duration(timeoutHangSec) * time.Second):
        w.WriteHeader(http.StatusGatewayTimeout)
    case <-r.Context().Done():
        return
    }
default:
    w.WriteHeader(http.StatusOK)
```

Each mock is a standalone Go module, built with a separate Dockerfile via `go mod init` on the fly. This is intentional: so the images don't pull half the course workspace.

For the integration test it's different. The mocks spin up directly inside the test via `httptest.NewServer` with an import of `internal/mockserver`. No Docker, on free ports. Same handler template, different environment.

## Running manually

The sandbox from the root `docker-compose.yml` is already running. Then:

```sh
make up                  # Postgres :15441 + three mocks in docker
make db-init             # notifications_log + processed_events tables
make topic-create-all    # 14 topics, P=6 RF=3
make run-router &        # forwards by channel
make run-firebase-sender &
make run-firebase-dlq &  # separate — writes notifications_log status='dlq'
# same for apns and webhook (run-apns-sender / run-apns-dlq / run-webhook-sender / run-webhook-dlq)
make seed                # 100 notifications into notification-events
make db-history          # see breakdown by channel/status
```

Under normal conditions everything lands in `delivered`. To observe retry and DLQ:

```sh
make chaos-fail-50       # restart mocks with FAIL_RATE_503=0.5
make seed
make db-history          # rows with status='dlq' will appear (if retries didn't help)
make mock-stats          # see how many 503s the mocks actually returned
```

The sender's default parameters are `delay-30s=30s`, `delay-5m=5m`. For an interactive demo that's too slow: the sender in retry-30s waits half a minute before retrying. Use the `-delay-30s` and `-delay-5m` flags — set them to, say, `2s` and `5s` to watch the pipeline turn in real time.

## Integration test

The most interesting part. File `test/integration_test.go` under the `integration` build tag. Run via `make test-integration`; requires Kafka and Postgres to be running.

What it does:

1. Starts three `httptest.Server` instances with `FAIL_RATE_503=0.7` (aggressive — so at least some records exhaust retries and land in DLQ).
2. Starts the router, one sender per channel in `deliver` mode and one in `dlq` mode — all seven as goroutines inside the test.
3. Sends 200 notifications to `notification-events` round-robin across channels.
4. Waits until `notifications_log.delivered + notifications_log.dlq == 200`. That is the "pipeline completed" criterion.
5. If `dlq > 0` — switches mocks to `FAIL_RATE_503=0`, waits for stabilization, then re-reads the DLQ topics and publishes records back to main with a new `notification_id` (`replay-*`). That is the DLQ replay.
6. Checks that after replay `delivered` increased by at least half of the replayed records.
7. Stops all nodes, checks that none crashed with an unhandled error.

```go
if lastSnap.dlq > 0 {
    fbCfg.set(0.0, 0.0, 5)   // liveMockHandler reads atomically,
    apnsCfg.set(0.0, 0.0, 5) // the change is visible on the next request
    whCfg.set(0.0, 0.0, 5)
    replayed, err := replayDLQ(root, bootstrap)
    threshold := baseline.delivered + replayed/2
    // wait until delivered ≥ threshold
}
```

There's a nuance here. `mockserver.Handler(cfg, stats)` freezes `cfg` in a closure, so the test wraps it in `liveMockHandler`: it holds a `mockConfig` with `atomic.Value` fields and reads them on every request. A fail-rate switch via `cfg.set(...)` takes effect on the next incoming request, no handler swap needed — `http.Server.Handler` is a plain field with no atomic guarantees, and a race with in-flight requests would be caught by `go test -race`.

200 notifications instead of the original 5000 — for speed on a dev machine. The logic is the same; the test runs in 12–15 seconds. For real load, change the `totalNotifications` constant.

## Where this fits in the course

The use case brings together:

- [Retry and DLQ deep dive](../../../../04-reliability/04-04-retry-and-dlq/i18n/en/README.md) — retry topics with delay and DLQ as a terminal
- [Delivery to external systems](../../../../04-reliability/04-05-external-delivery/i18n/en/README.md) — circuit breaker, HMAC, exponential backoff with jitter, mock-webhook pattern
- [Protobuf in Go](../../../../05-contracts/05-02-protobuf-in-go/i18n/en/README.md) — Protobuf as the wire format
- [Microservice communication](../../../01-microservices-comm/i18n/en/README.md) — at-least-once + dedup on the consumer via `processed_events`

What is deliberately absent:

- Schema Registry. Here it's byte-for-byte Protobuf without `magic byte` + `schema_id`. A separate lecture ([Schema Registry](../../../../05-contracts/05-03-schema-registry/i18n/en/README.md)) shows how to add SR — the pattern is orthogonal and plugs into this use case without logic changes.
- [Outbox pattern](../../../../04-reliability/04-03-outbox-pattern/i18n/en/README.md). The outgoing message to `notification-events` is written directly — we don't model the write side with a database. If we did, we'd add an `outbox` table and publisher, but that would extend the example without teaching anything new.
- gRPC API. This use case is about async delivery. The gRPC front is shown in [Microservice communication](../../../01-microservices-comm/i18n/en/README.md) and [Hybrid gRPC + Kafka](../../../../06-communication-patterns/06-04-hybrid-grpc-and-kafka/i18n/en/README.md).
- Real APNs / Firebase credentials. Cert-flow for APNs and FCM HTTP v1 token-exchange are a separate story, out of scope for this course. Channel architecture is demonstrated on mocks.

## Things to try

Once the pipeline is running, there are several good experiments:

- run `make chaos-fail-50` with aggressive `FAIL_RATE_503=0.9` and observe how the sender's CB throttles during a prolonged failure. Logs show `CB ...: closed → open`.
- stop `firebase-sender` mid-load. After restart the sender resumes from the same offset — no records lost. Postgres has no duplicates — the dedup gate holds.
- run two senders for the same channel with different `-node-id`. They join one consumer group and split the partitions in half. Scale — no code changes.
- manually merge retry-30s and retry-5m into one topic with a 1-minute delay and observe the behavior change. Hint: in `Stage{}` this changes in one place.

## Files

```
.
├── README.md                          # this file
├── Makefile                           # all commands
├── docker-compose.override.yml        # Postgres :15441 + 3 mocks
├── db/init.sql                        # notifications_log + processed_events
├── proto/notifications/v1/            # Notification + Channel enum
├── gen/                               # generated Go code
├── buf.yaml / buf.gen.yaml            # buf config
├── cmd/
│   ├── notification-router/           # consumer on notification-events → channels
│   ├── firebase-sender/               # thin wrapper over sender.Main
│   ├── apns-sender/
│   ├── webhook-sender/
│   ├── mock-firebase/                 # HTTP mock + Dockerfile, stdlib-only
│   ├── mock-apns/
│   ├── mock-webhook/
│   └── seed-tool/                     # make seed
├── internal/
│   ├── router/router.go               # router logic
│   ├── sender/sender.go               # retry + CB + HMAC + DB (shared code for all channels)
│   ├── sender/cmdmain.go              # CmdDefaults + flags for cmd wrappers
│   └── mockserver/server.go           # handler factory for the test
└── test/integration_test.go           # end-to-end test with DLQ replay
```
