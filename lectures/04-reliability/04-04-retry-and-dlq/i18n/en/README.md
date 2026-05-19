# 04-04 — Retry & DLQ Deep Dive

The [Error handling](../../../../03-consumer/03-04-error-handling/i18n/en/README.md) lecture already covered error handling on the consumer side: in-place retry for transient errors and DLQ for everything that failed. That works at the scale of a single consumer loop. Here we go further. Several retry topics with delays appear, the DLQ gets its own lifecycle, and there's a separate CLI for replaying messages.

Why complicate things? Let's break it down.

## Why in-place retry stops being enough

The main problem with in-place retry is that it sits inside the poll loop. While you're trying five times to reach a broken downstream, the consumer doesn't call poll. In franz-go v1.21.0 the heartbeat loop runs independently of handler work, so a long backoff alone won't kick you out of the group — the coordinator considers the client alive as long as the network heartbeat keeps flowing. The failure mode hits during a rebalance (a new member joined, the leader changed, a broker went down): if the handler is sitting in backoff at that moment, it has only `RebalanceTimeout` (`rebalance.timeout.ms`, default 60 seconds in franz-go v1.21.0) to wrap up and rejoin. Miss that window and the coordinator kicks the client; the partition moves to another member, which picks up the same offset with the same error. In the Java client the mechanics are stricter: `max.poll.interval.ms` (default 5 minutes) is enforced between `poll()` calls and exceeding it kicks the consumer immediately, with no dependency on a rebalance.

That's the first argument. The second — head-of-line blocking. You have 1000 messages in one partition, one of them is broken. It takes thirty seconds. All 999 behind it wait. That's a hot-line caused by a single garbage record.

The third is about pause duration. If the downstream is down, there's no point hitting it more often than it wakes up. A minute, five minutes, an hour. Sleeping that long inside the poll loop is impossible for reason number one. Working in parallel won't work either — ordering breaks, offset can't be committed while a record is "hanging" (see [Concurrency and lag](../../../../03-consumer/03-05-concurrency-and-lag/i18n/en/README.md)).

Conclusion. If retries need to be "in 30 seconds / 5 minutes / an hour" rather than "right now again" — you need a different mechanism. One that doesn't block the main consumer.

## The retry topic idea

The solution is simple and clear. Create a separate topic for each wait interval:

- `payments` — main;
- `payments-retry-30s` — failed in main, forwarded here;
- `payments-retry-5m` — failed in `retry-30s`, forwarded here;
- `payments-retry-1h` — last chance;
- `payments-dlq` — final stop.

One consumer listens to all four topics (main and three retry). When a record arrives from main and handle fails — we pack it with additional headers and send it to `retry-30s`. From there it sits as a regular Kafka message. The same consumer will eventually read it. And here's the trick: before processing, we check the `record.Timestamp` and wait until the required interval has passed. If the record arrived a second ago and we need to wait thirty — we sleep for 29 seconds. Then handle again. Success — commit and move on. Failure — `retry-5m`. The scenario repeats at each stage.

We get what we wanted:

- retries don't block the main flow. Main partitions are always processed at the same rate as without errors;
- real wait intervals between attempts, not "however fast the poll loop gets to it";
- the movement history through the pipeline is visible in headers (`error.message`, `previous.topic`, `retry.count`) — an operator can reconstruct an incident from DLQ message headers without digging through logs.

Downside: I'm still blocking the poll loop on retry topics while "resting" a record. At lecture-level load that's fine. At production load you do it differently — a separate consumer per retry topic, or `PauseFetchPartitions` plus a deferred `ResumeFetchPartitions` (that's covered in [Delivery to external systems](../../../04-05-external-delivery/i18n/en/README.md)). For understanding the pattern, the escalation itself is what matters; the rest is implementation detail.

## Headers as a protocol

Each stage of the pipeline leaves a trace. The convention in this lecture:

| Header | Set by | Meaning |
| --- | --- | --- |
| `error.class` | each stage | `permanent` or `transient` (latest classification) |
| `error.message` | each stage | error string |
| `error.timestamp` | each stage | when it failed (RFC3339Nano UTC) |
| `retry.count` | each stage | escalation counter (0 → 1 → 2 → 3 → DLQ) |
| `previous.topic` | each stage | where it came from (for DLQ this is the last retry stage) |
| `original.topic` | first escalation | where the record was born (never changes) |
| `original.partition` / `original.offset` | first escalation | coordinates of first appearance |

The convention is deliberately conservative. Headers are byte pairs — nothing self-validating. We decide what to put there and how. If the field choices are clear, a DLQ can be analyzed without access to the processor code: open headers, read `error.class` and `retry.count`, and you already see the picture.

`previous.topic` is separately useful for replay. When an operator catches a DLQ incident and wants to know which stage finally gave up — `previous.topic` answers that. `original.topic` serves a different purpose: to understand where this payload "lives." After replaying from DLQ back to main, `original.topic` stays unchanged — we don't overwrite it during replay. That gives you a stable "birthplace" identifier for the record, useful for tracing.

## What our processor shows

The key part is the stage table. I defined them explicitly because this is the lecture's contract:

```go
stages := []stage{
    {topic: *mainTopic, delay: 0, nextTopic: *retry30},
    {topic: *retry30, delay: *delay30s, nextTopic: *retry5m},
    {topic: *retry5m, delay: *delay5m, nextTopic: *retry1h},
    {topic: *retry1h, delay: *delay1h, nextTopic: ""},
}
```

The empty `nextTopic` on the last retry stage is a "nothing left to escalate" flag. `forwardOrDLQ` sees the empty string and pushes the record to the DLQ with `reason=exhausted`. If we passed `*dlq` directly here, the log would read `reason=next-retry`, and the three cases (`next-retry` / `permanent` / `exhausted`) would collapse into two.

One consumer in group `lecture-04-04-processor` subscribes to all four topics. Before `handle()` we check the stage's `delay` and, if it's positive, wait until `record.Timestamp + delay`. This is the heart of the retry mechanic:

```go
func waitUntilDue(ctx context.Context, recordTs time.Time, delay time.Duration) error {
    due := recordTs.Add(delay)
    wait := time.Until(due)
    if wait <= 0 {
        return nil
    }
    fmt.Printf("WAIT  due=%s (через %s)\n", due.UTC().Format(time.RFC3339), wait.Truncate(time.Second))
    select {
    case <-ctx.Done():
        return ctx.Err()
    case <-time.After(wait):
        return nil
    }
}
```

Then — the decision of where to send a failed record. Three cases, each with its own target:

```go
target := st.nextTopic
reason := "next-retry"
if isPermanent(cause) {
    target = dlqTopic
    reason = "permanent"
} else if target == "" {
    target = dlqTopic
    reason = "exhausted"
}
```

`permanent` — straight to DLQ, skipping retry stages. Broken JSON (poison pill) or domain validation rejection — retrying is pointless, even in an hour. `exhausted` — this is transient, but we're already at `retry-1h` and `nextTopic` is empty. Anything not healed within an hour is considered hopeless.

Headers are assembled in `forwardWithHeaders`. The subtle point — `original.*` is set only on the first escalation:

```go
if _, ok := idx["original.topic"]; !ok {
    headers = appendOrReplace(headers, "original.topic", r.Topic)
    headers = appendOrReplace(headers, "original.partition", strconv.Itoa(int(r.Partition)))
    headers = appendOrReplace(headers, "original.offset", strconv.FormatInt(r.Offset, 10))
}
headers = appendOrReplace(headers, "previous.topic", r.Topic)
headers = appendOrReplace(headers, "retry.count", strconv.Itoa(nextRetries))
```

`appendOrReplace` matters: error headers are overwritten at each stage (we need the last error, not the first), while `original.*` is written once and held.

## DLQ as a terminal

When a record reaches the DLQ — that's the end of the automated pipeline. A separate handler reads it and in the general case does not return it to the main flow. The DLQ handler has three goals:

1. Record the incident in durable storage (DB, append-only log, S3) — so it can be reviewed a week later.
2. Fire an alert — someone alive needs to know a message died.
3. Don't block DLQ partitions with infinite processing — the DLQ must drain quickly, otherwise lag grows and you lose visibility.

In this lecture, `cmd/dlq-processor` does the first and second. The alert is mocked to stdout (in production this is a webhook to Slack or PagerDuty). Storage is an append-only JSON file `/tmp/lecture-04-04-incidents.jsonl`. The lecture plan calls for a Postgres table — the pattern is identical, the file was chosen to avoid pulling in another docker-compose. In production — swap `os.OpenFile` for `pgxpool.Exec(INSERT ...)`, and that's it.

Incident record structure:

```go
type incident struct {
    DLQTopic         string `json:"dlq_topic"`
    DLQPartition     int32  `json:"dlq_partition"`
    DLQOffset        int64  `json:"dlq_offset"`
    Key              string `json:"key,omitempty"`
    OriginalTopic    string `json:"original_topic,omitempty"`
    OriginalPart     string `json:"original_partition,omitempty"`
    OriginalOffset   string `json:"original_offset,omitempty"`
    PreviousTopic    string `json:"previous_topic,omitempty"`
    RetryCount       string `json:"retry_count,omitempty"`
    ErrorClass       string `json:"error_class,omitempty"`
    ErrorMessage     string `json:"error_message,omitempty"`
    ErrorTimestamp   string `json:"error_timestamp,omitempty"`
    DLQRecordTime    string `json:"dlq_record_time"`
    PayloadByteCount int    `json:"payload_bytes"`
}
```

There's intentionally no `payload` field. The idea — the incident log should be lightweight and indexable (by `error_class`, by `original_topic`). To inspect a payload — that's a separate operation via `kafka-console-consumer` or a dump via `replay-cli --dry-run`. Copying payloads into the incident log is a path to a terabyte of fat JSONs through which you'll never find a single incident you need.

The stdout alert is simple:

```
[ALERT] #3  dlq=payments-dlq p=1 off=2 key=k-7
        original=payments/0/14 previous=payments-retry-1h retries=3
        class=transient message="exhausted retries: transient downstream blip on payment id=\"k-7\""
        payload=42 bytes
```

This is enough to understand: record `k-7` came from main `payments`, passed through all three retry stages, failed as transient at each one, and finally gave up after an hour of waiting. In a real alert channel the formatting differs; the fields are the same.

## Replay

DLQ is the end of automation, not a verdict. Some incidents make sense to replay after fixing the downstream. Take `transient`: the downstream was fixed within the hour, and now `payments-dlq` has 200 records that could have gone through if resubmitted.

`cmd/replay-cli` handles this. Key flags:

- `-from-topic` — where to read from, default `payments-dlq`;
- `-to-topic` — where to resend, default main `payments`;
- `-since` — time filter on DLQ record time (take everything newer than `now() - since`);
- `-error-class` — optional filter by header; typical case — `transient`;
- `-dry-run` — count matches without publishing anything.

Repacking into a new record:

```go
func replayRecord(r *kgo.Record, toTopic string) *kgo.Record {
    headers := append([]kgo.RecordHeader(nil), r.Headers...)
    headers = setHeader(headers, "retry.count", "0")
    headers = setHeader(headers, "replay.from-dlq", r.Topic+"/"+strconv.Itoa(int(r.Partition))+"/"+strconv.FormatInt(r.Offset, 10))
    headers = setHeader(headers, "replay.timestamp", time.Now().UTC().Format(time.RFC3339Nano))
    return &kgo.Record{
        Topic:   toTopic,
        Key:     r.Key,
        Value:   r.Value,
        Headers: headers,
    }
}
```

What matters here:

- `retry.count` is reset to zero. The new pipeline starts fresh — otherwise a DLQ replay would immediately hit the exhausted-retry counter from the previous session and fly back into the DLQ.
- `replay.from-dlq` — the coordinates of the original record in the DLQ. If we fail again after replay, the new DLQ incident has this header showing that the current run is already the second.
- Payload and key are untouched. This matters: in systems where the consumer builds dedup by business key from the payload, replay must not break idempotency.

What's intentionally left out. Replay does not deduplicate. Run `make replay` twice in a row — it sends twice. The consumer must handle that protection (see [Outbox pattern](../../../04-03-outbox-pattern/i18n/en/README.md) on idempotency with a dedup table). The alternative — storing IDs of completed replays on the CLI side — gives you a stateful CLI, which is a separate story.

## Metrics to watch

Pipeline observability is built on four numbers. Each has a meaningful target:

- End-offset of main `payments`. Grows proportionally to load. You can attach a "throughput dropped" alert to it.
- End-offset of each retry topic. On a healthy system these should be low and growing slowly. A sudden spike signals "downstream degraded." Ideally all three retry topics stay near zero.
- End-offset of the DLQ. Any non-zero growth — alert. In production this is usually `rate(messages_in_dlq_total[5m]) > 0` in Prometheus.
- Consumer lag for the processor group. The [Concurrency and lag](../../../../03-consumer/03-05-concurrency-and-lag/i18n/en/README.md) lecture showed `kadm.Lag` — each stage has its own lag, and if main looks fine but `retry-30s` lag is huge, you're drowning in retries.

There's a separate meta-metric for the DLQ — `error.class` distribution. Extract it from the incident log in one line: `jq -r '.error_class' /tmp/lecture-04-04-incidents.jsonl | sort | uniq -c`. If 90% of incidents are `transient`, the retry pipeline is likely too short: you need one more level with a longer delay, or a scheduled replay.

## Demo

The sandbox from the repository root must be running (`docker compose up -d` in the root). Then from the lecture directory:

```sh
make topic-create-pipeline
make seed-with-failures SEED_MESSAGES=20
```

`payments` has 20 mock messages. Some with `mode=ok` (passed on the first attempt), some `transient` (always fail, will move forward at each stage), some `permanent` (broken JSON or explicit reject — straight to DLQ).

Start the processor with fast delays so the pipeline runs in half a minute rather than an hour:

```sh
make run-processor-fast
```

The output shows how records travel. Something like:

```
OK    [payments] p=0 off=3 key=k-3
FAIL  [payments] p=2 off=4 key=k-5 reason=next-retry err=transient ... → payments-retry-30s
FAIL  [payments] p=1 off=2 key=k-7 reason=permanent err=invalid json: ... → payments-dlq
WAIT  due=2026-05-01T12:30:15Z (через 1s)
FAIL  [payments-retry-30s] p=0 off=0 key=k-5 reason=next-retry err=transient ... → payments-retry-5m
```

Once the processor has pumped through all 4 topics and sits on "no new messages" — Ctrl+C. In a second terminal:

```sh
make run-dlq
```

The DLQ processor reads `payments-dlq`, prints ALERT, and writes JSON lines to `/tmp/lecture-04-04-incidents.jsonl`. Verify:

```sh
make dlq-count
cat /tmp/lecture-04-04-incidents.jsonl | jq -r '[.error_class, .original_topic, .key] | @tsv'
```

In the DLQ — all `permanent` (immediately) plus all `transient` (after exhausting three retry stages).

Now replay. Say we fixed the downstream and want to send all `transient` from the last hour back to the main topic:

```sh
make replay REPLAY_CLASS=transient REPLAY_SINCE=1h
```

The CLI reads `payments-dlq`, filters by `error.class=transient`, packs with reset `retry.count`, and sends to `payments`. The same payloads appear in the main topic again — `payment.k-5`, `payment.k-9`. Running the processor again sends them through the pipeline from scratch. On lecture mocks they'll fail again (mocks don't heal), but in the processor log the new retry messages will have the `replay.from-dlq` header pointing to the original DLQ offset. From that, an operator knows: this run is already the second; the record's first life ended in the DLQ.

`make replay-dry` does the same without `ProduceSync` — useful to confirm the filter captures what you expect before sending real traffic.

## Pattern boundaries

A few limits that are easy to miss.

A retry topic pipeline alone doesn't make delivery guaranteed — it's the same at-least-once that was in [Error handling](../../../../03-consumer/03-04-error-handling/i18n/en/README.md). The exact same pitfall of "failed between produce and commit" applies here too. If the processor crashed between "did ProduceSync to `retry-5m`" and "did CommitRecords for `retry-30s`" — on restart `retry-30s` will deliver that record again, and it will land in `retry-5m` a second time. Duplicate in `retry-5m`. Handler idempotency is the only protection.

Long waits in `retry-1h` (one hour) on a single partition block all other records in that same partition. This is a subtle point. One way around it — partitioning by business key: if `key=k-5` is stuck for an hour, other keys sit in other partitions and are processed as normal. But if all retry messages go to one partition (for example, the key is `user_id` and one user has 100 messages at once) — the pipeline stalls. Solutions: reduce the `retry-1h` delay, parallelize via a worker pool with per-key affinity (see [Concurrency and lag](../../../../03-consumer/03-05-concurrency-and-lag/i18n/en/README.md)), or give the retry pipeline more partitions than the main topic.

Replay is a manual operation, and that's intentional. Automatic replay from DLQ back to main without understanding the incident cause is a path to an infinite loop. If the fix wasn't deployed or the cause was in the payload itself rather than the downstream — the record will fail again by the same scenario, and the DLQ will start growing. That's why replay is initiated by a person or a routine that verified the cause was resolved.

Last point. The retry pipeline doesn't work for cases where ordering matters above all else. When payment `k-5` went to `retry-30s` while payment `k-6` (same key, later message) went through the main path — you've broken per-key order. If the business logic tolerates inversions, that's fine. If they're strictly forbidden — you need a different architecture, such as parking the whole partition via `PauseFetchPartitions` until the downstream recovers ([Delivery to external systems](../../../04-05-external-delivery/i18n/en/README.md)).

## Full run

```sh
make topic-create-pipeline
make seed-with-failures SEED_MESSAGES=50

# terminal 1
make run-processor-fast

# terminal 2 (once the processor finishes)
make run-dlq

# terminal 3
make dlq-count
make replay REPLAY_CLASS=transient REPLAY_SINCE=24h

make clean       # tear down groups, topics, and the incident log
```

Useful sanity checks: `make main-count` (total records in the main topic including replays), `make dlq-count` (how many died), `wc -l /tmp/lecture-04-04-incidents.jsonl` (how many alerts were generated — should match the DLQ count).
