# 04-05 — External Delivery

In [Retry and DLQ deep dive](../../../04-04-retry-and-dlq/i18n/en/README.md) we covered retries and DLQ inside Kafka — where the failure source was our own processing logic or a neighboring service we also write to via Kafka. This is a different scenario. A notification arrives from a topic and must be delivered to an external HTTP receiver: a partner webhook, a push provider, a third-party platform. The task sounds simple. In practice, this is exactly where end-to-end guarantees most often fall apart, because the external downstream lives by its own rules — its own rate limit, its own timeouts, its own security policies, and its own failure schedule.

## What makes "external delivery" different

First — you have no ownership over the receiver. When you write to your own Kafka, problems are visible: a broker is down, a producer is misbehaving, retention is expiring. Here it's someone else's stack. Their app server is down. Their nginx returns 502. Their rate-limit window ran out and they're sending 429 on every request. The naive path — blind retries — ends with us finishing off an already-down downstream.

Second — HTTP has a different error model. Network timeouts and drops are one thing. 5xx from the server is another (it's alive but can't cope). 4xx is a third category (it's alive, you're wrong). Treating all of this as "failed" indiscriminately is wrong, because retrying 4xx is pointless — the receiver already said our payload is bad. You can knock for six months and it won't get better.

Third — the attack surface. A webhook accepts requests from anywhere. To prevent the receiver from getting someone else's notifications (and to prevent us from consuming someone else's), we introduce signatures. To prevent the receiver from repeating the same operation on retry — we introduce idempotency. This isn't "extra sugar"; it's basic integration hygiene.

Fourth — backpressure. If the receiver is down and messages are flowing into Kafka at thousands per second — we must not make thousands of attempts per second. We need to slow down. And this is where `PauseFetchPartitions` becomes useful for the first time.

Below, each of the four in order.

## Exponential backoff with jitter

The standard retry pattern: first attempt — almost immediately, second — after 200 ms, third — 400, fourth — 800. Double until you hit the ceiling (say, 5 seconds). Why double — to avoid hammering an alive-but-slow downstream. If it can't respond within 200 ms, give it a second.

But pure doubling creates another problem. Imagine a thousand of our instances all seeing the same failure at the same time. They all start retrying at 200 ms. Then 400. Then 800. The result is a synchronized herd — it hits the downstream in waves. The cure is jitter. Instead of "sleep exactly backoff" — "sleep a random duration in the range `[0, backoff]`" (this is full-jitter per AWS). The herd scatters, load spreads out over time.

In our courier it looks like this:

```go
backoff := c.initialBackoff
for attempt := 1; attempt <= c.maxAttempts; attempt++ {
    status, err := c.send(ctx, r)
    if err == nil {
        return ..., nil
    }
    if errors.Is(err, errPermanent) {
        return ..., err
    }
    sleep := time.Duration(rand.Int64N(int64(backoff)))
    select {
    case <-ctx.Done():
        return ..., ctx.Err()
    case <-time.After(sleep):
    }
    backoff *= 2
    if backoff > c.maxBackoff {
        backoff = c.maxBackoff
    }
}
```

One detail matters here — we use `rand.Int64N(int64(backoff))`, not `time.Sleep(backoff)`. No fixed interval. Each attempt sleeps a random duration within the current backoff window. Run this across a thousand parallel instances and you'll see a uniform distribution of pauses — no waves.

## Circuit breaker

Retries solve "knock sensibly for one message." They don't solve "stop knocking entirely when downstream is down." If we have 100 messages in the queue and each of them goes through four retry attempts — that's four hundred useless requests to a dead server. 399 of them are wasted by design.

This is where the circuit breaker comes in. The idea is old — it comes from electrical engineering. If the circuit accumulates too many consecutive failures, open the contact. From then on, everything that hits `Execute()` immediately bounces with `ErrOpenState`, no network call made. After some time (cooldown) it switches to Half-Open and lets through one probe attempt. If it succeeds — close, everything flows again. If not — back to Open for another cooldown.

Three states: Closed, Open, Half-Open. Four valid transitions between them:

1. Closed → Open. Consecutive failures reach the threshold — open. Default is five consecutive.
2. Open → Half-Open. Cooldown elapsed (`Timeout` in Settings). Try one probe.
3. Half-Open → Closed. Probe succeeded — close.
4. Half-Open → Open. Probe failed — back to cooldown.

We use `sony/gobreaker/v2` because it's trivial, has no dependencies, and supports generics. The Settings that matter:

```go
c.cb = gobreaker.NewCircuitBreaker[deliveryResult](gobreaker.Settings{
    Name:        "courier-webhook",
    MaxRequests: 1,                    // in Half-Open — exactly one probe
    Timeout:     *cbOpenTimeout,       // how long it stays in Open before Half-Open
    ReadyToTrip: func(counts gobreaker.Counts) bool {
        return counts.ConsecutiveFailures >= uint32(*cbConsecutive)
    },
    OnStateChange: c.onStateChange,
})
```

`MaxRequests=1` — strictly one probe. Allowing ten would send a batch of simultaneous requests to a not-yet-warmed server in Half-Open. That's bad. One request — one signal: working or not.

`ReadyToTrip` here uses consecutive failures. You can use ratio (e.g., 50% of 20 requests), but for this lesson consecutive is clearer. Our runs are steady, failures come in sequence — five consecutive = open.

The CB wraps not each individual retry but the entire delivery with its internal retries:

```go
func (c *courier) deliver(ctx context.Context, r *kgo.Record) (deliveryResult, error) {
    return c.cb.Execute(func() (deliveryResult, error) {
        return c.deliverWithRetries(ctx, r)
    })
}
```

One Execute = one event for the CB. How many retry iterations happened inside is irrelevant to it. This is convenient — the CB has its own metric of "messages that failed entirely." If we counted individual HTTP failures, the CB would open on one difficult message that needed three retries before succeeding.

## Backpressure in Kafka via PauseFetchPartitions

The CB by itself does not stop the consumer. It only cuts HTTP calls. What does our poll loop do while the CB is Open? It keeps fetching new messages from Kafka, running them through the CB, and catching `ErrOpenState`. This is pointless activity — the fetch buffer grows, messages accumulate, we "process" them (bouncing instantly), don't commit, then the next iteration picks them up again. A race.

The fix — tell the Kafka client at its level "don't pull new fetches for now." In franz-go this is `cl.PauseFetchPartitions(...)` or `cl.PauseFetchTopics(...)`. After the call, PollFetches returns empty for those topics until `ResumeFetchTopics` is called (`pkg/kgo/consumer.go:655` — topic pause is independent from individual `PauseFetchPartitions`). No new FetchRequests go to the broker, already-buffered records for paused topics are filtered out at `takeBuffered(paused)` (`pkg/kgo/consumer.go:542`), nothing in-flight grows. The heartbeat loop keeps running as usual — the partitions stay assigned to us, no rebalance is triggered.

There is one nuance — pausing exactly on every CB Open transition is risky. The CB can flap: 5 failures in a row, opens, checks a probe after 15 seconds, probe passes, closes, 200 ms later another 5 in a row, opens again. Pause/Resume is purely client-side state — the group coordinator knows nothing about it (`pkg/kgo/consumer.go:655` — no RPC, just an internal atomic flag). So the "noise" here is not network traffic but a messy log and a sawtooth lag chart — fetches stop and start in a jagged pattern. To avoid that sawtooth, we set a threshold: pause only if the CB has been Open longer than `pause-after`.

Here's how it's wired together. First, the CB callback:

```go
func (c *courier) onStateChange(name string, from, to gobreaker.State) {
    c.logger.Warn("CB state change", "name", name, "from", from, "to", to)
    switch to {
    case gobreaker.StateOpen:
        c.openSince.Store(time.Now().UnixNano())
    case gobreaker.StateHalfOpen, gobreaker.StateClosed:
        c.openSince.Store(0)
        if c.paused.Swap(false) {
            c.cl.ResumeFetchTopics(c.topics...)
            c.logger.Info("partitions resumed", "topics", c.topics)
        }
    }
}
```

Here we only record the time of transition to Open and resume if we're leaving it. The actual pause is triggered from the poll loop:

```go
func (c *courier) maybePauseOnLongOpen() {
    since := c.openSince.Load()
    if since == 0 {
        return
    }
    if time.Since(time.Unix(0, since)) < c.pauseAfter {
        return
    }
    if c.paused.CompareAndSwap(false, true) {
        c.cl.PauseFetchTopics(c.topics...)
        c.logger.Warn("partitions paused — CB stayed Open too long", ...)
    }
}
```

`maybePauseOnLongOpen` is called at the start of every main loop iteration before PollFetches. If the CB has been Open too long — pause; the next PollFetches returns empty, we don't pressure the downstream. When the CB transitions to Half-Open and then Closed, the callback calls ResumeFetchTopics — fetches resume normally.

The result is two control loops: HTTP level (CB catches most of the junk calls) and Kafka level (PauseFetchPartitions silences the source if the CB doesn't recover). They don't duplicate each other — they operate on different time horizons. CB — seconds. Kafka pause — tens of seconds and beyond.

## HMAC and Idempotency-Key

The receiver doesn't trust us by default. It sits on a public `:8090` and anyone can POST to it. To distinguish us from noise, we add a signature. Take a shared secret (`HMAC_SECRET`), compute HMAC-SHA256 over the request body, put it in the `X-Signature` header. The receiver does the same with its copy of the secret and compares. If they match — it's us. If not — rejected.

```go
mac := hmac.New(sha256.New, c.hmacKey)
mac.Write(body)
signature := hex.EncodeToString(mac.Sum(nil))
req.Header.Set("X-Signature", signature)
```

One note — the secret is shared. This is symmetric, no PKI. Fine for integration with a single partner. If there are many partners and you don't want to hand them all one secret — use asymmetric signatures instead of HMAC (Ed25519 or RS256 in JWS style). But that's significantly more expensive, and for a lesson on delivery — overkill.

Idempotency-Key is a separate concern. The receiver gets our request, executes it successfully, tries to respond — and our http-timeout has already expired. We see a failure and retry. The receiver gets a second identical request. Ideally it should recognize this as a duplicate and not repeat the operation. To enable this, we put a stable identifier in the header:

```go
idem := fmt.Sprintf("%s:%d:%d", r.Topic, r.Partition, r.Offset)
req.Header.Set("Idempotency-Key", idem)
```

The key from `topic:partition:offset` is stable forever. The same record on retry produces the same key. The receiver maintains a table of processed keys with a TTL (a day, a week — depends on the business). Sees a duplicate — returns the same response without touching the backend.

In our mock-webhook we only log the `Idempotency-Key` and don't perform deduplication — this lesson is not about receivers. But in reality this is the most critical part of end-to-end exactly-once with an external downstream. Without it, retries become duplicate orders.

## HTTP status code classification

Short but important. Not all HTTP failures are equally retriable:

- 2xx — success, commit the offset.
- 4xx (except 408 and 429) — the receiver said "this is a bad request." No amount of retries will improve it. This is **permanent** for us. Don't retry; surface it externally with a distinct marker — in our case, commit it (the record is "lost," but we at least don't block the partition).
- 408 (Request Timeout), 429 (Too Many Requests) — technically 4xx, but semantically "try later." Retriable.
- 5xx — server failed to cope. Retriable.
- network error / timeout — anything from the transport layer (`net.Error`, connection drop, http.Client timeout). Retriable.

In code:

```go
switch {
case resp.StatusCode >= 200 && resp.StatusCode < 300:
    return resp.StatusCode, nil
case resp.StatusCode == http.StatusRequestTimeout,
    resp.StatusCode == http.StatusTooManyRequests,
    resp.StatusCode >= 500:
    return resp.StatusCode, fmt.Errorf("retriable status %d", resp.StatusCode)
default:
    return resp.StatusCode, fmt.Errorf("status %d: %w", resp.StatusCode, errPermanent)
}
```

Internal rule: anything we mark `errPermanent`, run() recognizes via `errors.Is` and commits calmly — even without delivery. A garbage message must not block a partition forever. If losing it is unacceptable — you need a retry pipeline and DLQ from [Retry and DLQ deep dive](../../../04-04-retry-and-dlq/i18n/en/README.md) alongside, so permanent messages at least land in a DLQ incident log. In this lesson we simplified it to commit-without-delivery to keep the topic focused.

## What the mock-webhook does

`cmd/mock-webhook/main.go` — a plain HTTP server on the standard library, no dependencies. Accepts POST `/deliver`. On a dice roll decides: 200, 503, or "hang" (timeout simulation). Failure rates — `FAIL_RATE_503` and `FAIL_RATE_TIMEOUT`. Also exposes `/health` and `/stats` — health for Docker healthchecks, stats for viewing current counters.

The most useful part for debugging — `/stats`:

```go
mux.HandleFunc("/stats", func(w http.ResponseWriter, _ *http.Request) {
    s := stats.snapshot()
    fmt.Fprintf(w, `{"total":%d,"ok":%d,"fail_503":%d,"fail_timeout":%d}\n`, ...)
})
```

After running `make seed` + courier you can see: how many requests we actually sent, how many hit 503, how many hung on timeout. If you set `FAIL_RATE_503=0.5` and `/stats` shows `total=120 ok=60 fail_503=60` — everything is right.

The mock does not validate HMAC and does not deduplicate by Idempotency-Key. It only logs both headers. That's enough to visually confirm that the courier sets them. In a real receiver, signature validation is mandatory; deduplication is strongly recommended.

## How to run it

The sandbox must be running from the repository root (`docker compose up -d`). Then, from the lesson directory:

```sh
make topic-create        # create the notifications topic
make up-mock             # mock-webhook on :8090, no failures
make seed                # 100 notifications into notifications

# in another terminal:
make run-courier         # courier subscribes, delivers, prints logs
```

The failure scenario is the most interesting part:

```sh
# 50% of requests fail with 503
make chaos-fail-50

# courier logs will show retries, then CB state change → Open,
# then partitions paused — CB stayed Open too long.
# after cb-open-timeout → Half-Open → Closed → partitions resumed.

make chaos-clear         # restore mock to normal mode
```

To play with CB sensitivity:

```sh
make run-courier CB_TRIP_AFTER=3 PAUSE_AFTER=3s CB_OPEN_TIMEOUT=5s
```

This opens the CB after three consecutive failures, partition pause kicks in after 3 seconds of Open, and cooldown is 5 seconds. For a demo this is far more visible than the defaults of 5/10/15.

## Key takeaways

External delivery is the last link in reliability. Inside Kafka we can guarantee exactly-once on our side. At the boundary with an external downstream that guarantee becomes at-least-once. To keep that at-least-once from breaking the receiver — you need an idempotent receiver via Idempotency-Key. Without it, every retry becomes a duplicate order / notification / charge.

Retries must use backoff and jitter — otherwise we finish off an already-down downstream. On top of retries — a circuit breaker, which protects against useless calls in bulk. On top of the CB — a Kafka-level pause, which prevents the fetch buffer from growing while the CB stays Open. All of this is wired together through the `OnStateChange` callback — it's the bridge between the CB and `PauseFetchTopics`.

This lesson closes module 04 (reliability). Next is module 05 on contracts: Protobuf, Schema Registry, schema evolution. Not "how to deliver" — but "what exactly we deliver and how to change the format without breaking consumers."
