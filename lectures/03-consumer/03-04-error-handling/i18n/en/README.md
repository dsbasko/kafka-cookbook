# 03-04 — Error Handling

In the previous lesson we put dedup on the DB side and closed the question of duplicates. Every message from Kafka hit `messages` exactly once; duplicates were swallowed silently. What we didn't show there — `handle` always returned `nil`. As if the business handler never fails. On a real system that's an illusion.

It fails regularly. The network blinked — TCP timeout to downstream. The payment gateway went down for 5 minutes — 503. The analytics pipeline hit rate-limit — 429. A payload arrives where the `amount` field is a string `"$100"` but you expect a float — JSON unmarshal fails. Your consumer is now stuck on a record that won't process. What do you do?

## Four strategies

There's no single right answer — pick a strategy for the error class.

1. **Skip** — log the error or record a metric, skip the message, commit the offset. Done, move on. The message is lost for business logic, but the pipeline keeps running. Suitable for "non-critical, happens" — button clicks, low-priority analytics tracking.
2. **Retry in-place** — try the same record again right there, in the same worker loop, with backoff. One, two, three times. If it succeeds — commit and move on. If it burns out — fall through to one of the strategies below. Suitable for short failures: TCP blink, momentary rate-limit.
3. **Retry-topic** — send the message to a separate topic `*-retry-30s`, whose consumer waits until timestamp+30s, then processes. If that fails — next retry topic with a larger window (`*-retry-5m`, `*-retry-1h`). This keeps the main consumer fast while routing broken records to separate "shelves" with increasing delays. That's [Retry and DLQ deep dive](../../../../04-reliability/04-04-retry-and-dlq/i18n/en/README.md) territory — here we only show the idea.
4. **DLQ** — dead-letter queue. A separate topic `*-dlq` that receives the message along with diagnostics (what failed, at which offset, how many attempts). From there — separate handling: alert, incident review, replay.

In practice these strategies are usually combined. Transient error — retry in-place; didn't help — retry-topic with a delay; still failing after several runs — DLQ. "Keep retrying forever" doesn't exist: if processing hasn't succeeded after a reasonable number of attempts, the problem is either in the data or the downstream, and sitting in the poll loop is pointless.

## Classifying the error is half the job

Before choosing a strategy, understand the error class. There are two:

- **transient** — something temporary; worth waiting and retrying. Network failures, HTTP timeouts, rate-limits, brief downstream unavailability.
- **permanent** — something irreversible; retrying won't change anything. Malformed JSON, schema mismatch, business rule rejected the request (payment blocked by compliance — retrying the same payment won't go through), non-existent key in the DB.

The boundary isn't always clear. The same 500 from downstream can be transient (admin restarts it — back up) or permanent (bug in the downstream service, fixed next week). That's where engineering judgment comes in: after N attempts, consider it permanent. There's no magic constant.

In our code, classification is done through the Go error type. There's a custom `permError` type; everything else is transient:

```go
type permError struct{ msg string }

func (e *permError) Error() string { return e.msg }

func permErrorf(format string, args ...any) error {
    return &permError{msg: fmt.Sprintf(format, args...)}
}

func isPermanent(err error) bool {
    var p *permError
    return errors.As(err, &p)
}
```

`errors.As` works well with wrapped errors — if somewhere in the pipeline someone wraps a `permError` with `fmt.Errorf("...: %w", err)`, the check will still find it.

## Poison-pill problem

A special case of a permanent error — the poison pill. This is a message that **permanently** breaks a naive consumer: every poll returns it, processing fails, offset is not committed, the next poll returns the same message, fails again. Pipeline is stuck. Lag grows. Alert fires.

Most common poison pills:

- incomplete or malformed JSON (especially when producer and consumer were maintained by different teams and the producer rolled a breaking change without coordination);
- a message written against a different schema (e.g., version v3 while the consumer expects v1, and the unmarshal produces something the downstream handler doesn't expect);
- an invalid enum value (consumer switches on it, falls into default, which panics);
- a payload where a field is `null` but the code does `pmt.Items[0]`.

The fix — **detect and route aside**. Don't loop on this record, don't panic on an uncaught panic, but don't silently swallow it in a goroutine that does `recover` and continues either. The right move — catch it, pack it into DLQ with a note on what exactly failed, commit the offset, and keep reading.

In our `handle` this is the first check after a record arrives:

```go
var p payment
if err := json.Unmarshal(r.Value, &p); err != nil {
    return permErrorf("invalid json: %v", err)
}
```

If the payload doesn't parse — it's permanent immediately. No backoff will fix "this string with the wrong quote." Route it to DLQ and move on.

## What our code does

The lesson has two binaries. They run in parallel but watch different topics.

`cmd/multi-strategy/main.go` — the main processor. Reads `payments`, decides what to do with each error:

- mode=ok → processing succeeds → commit after batch;
- mode=transient → in-place retry up to `max-retries` times with exponential backoff; if it "heals" in that time (that's how the mock works — after two attempts the mock starts returning nil) → commit; if not → DLQ as exhausted retries;
- mode=permanent → DLQ immediately;
- malformed JSON → DLQ immediately as poison-pill.

The retry loop itself — no fancy queues, just a `for` with backoff:

```go
for attempt := 1; attempt <= o.maxRetries; attempt++ {
    backoff := o.baseBackoff * (1 << (attempt - 1))
    select {
    case <-ctx.Done():
        return ctx.Err()
    case <-time.After(backoff):
    }

    err = handle(r, attempts, key)
    if err == nil {
        // ok, keep going
        return nil
    }
    if isPermanent(err) {
        // turned permanent during retry — to DLQ
        return forwardToDLQ(ctx, cl, o.dlqTopic, r, err, usedAttempts)
    }
}
```

`baseBackoff * (1 << (attempt - 1))` is `200ms * 2^(attempt-1)` by default: 200ms, 400ms, 800ms. For three attempts the total retry window per record is about 1.4 seconds, then either healed or to DLQ.

When a message goes to DLQ, we attach headers with diagnostics:

```go
headers = append(headers,
    kgo.RecordHeader{Key: "error.class", Value: []byte(errClass(cause))},
    kgo.RecordHeader{Key: "error.message", Value: []byte(cause.Error())},
    kgo.RecordHeader{Key: "original.topic", Value: []byte(r.Topic)},
    kgo.RecordHeader{Key: "original.partition", Value: []byte(strconv.Itoa(int(r.Partition)))},
    kgo.RecordHeader{Key: "original.offset", Value: []byte(strconv.FormatInt(r.Offset, 10))},
    kgo.RecordHeader{Key: "retry.count", Value: []byte(strconv.Itoa(attempts))},
    kgo.RecordHeader{Key: "dlq.timestamp", Value: []byte(time.Now().UTC().Format(time.RFC3339Nano))},
)
```

These headers are the only bridge from the error to the person who will later triage the DLQ. Without them the DLQ contains a bare payload with no indication of what's wrong with it.

`cmd/dlq-reader/main.go` — a separate process that sits on `payments-dlq` and pretty-prints each record with its headers. In a real system this would be an alerter, a Jira ticket, a metric, an incident-ID index. Here it's a demo stdout: you can see that different error classes arrived in the DLQ and each preserved its context.

## Partition pause / resume — a separate tool

Sometimes in-place retry doesn't fit and retry-topic seems like overkill. For example, a downstream API is down for five minutes. A long retry loop with exponential backoff won't kick the consumer out of the group on its own: franz-go heartbeats independently from processing, so the coordinator considers the client alive as long as its network heartbeat loop is healthy. The problem hits during a rebalance (new member joined, leader changed, broker went down). If at that moment the handler is stuck in backoff, it only has `RebalanceTimeout` (`rebalance.timeout.ms`, default 60 seconds in franz-go v1.21.0) to wind down the work, commit the offset, and rejoin. If it doesn't make it — the coordinator kicks the client, the partition migrates to another member, which picks up the same work and also fails. Ping-pong at the group level. Bad.

`franz-go` has `cl.PauseFetchPartitions` and `cl.ResumeFetchPartitions`. This is a different mechanism: the partition stays **assigned** to the consumer (heartbeats keep going, the group considers it alive), but `PollFetches` stops returning new records from that partition. You can pause the partition, run an HTTP health check for downstream in the background, and resume when it's back up.

This lesson's code doesn't use it for simplicity, but you should know it exists. We'll come back to pause/resume in [Delivery to external systems](../../../../04-reliability/04-05-external-delivery/i18n/en/README.md), where a circuit breaker maps naturally onto this pair of calls: CB transitions to open → pause partitions → CB returns to half-open → resume.

## Duplicates in DLQ are normal

A subtle point here. After writing to DLQ we commit the offset of the main topic. Between these two actions there is a micro-window in which the process can crash.

```
ProduceSync(DLQ)     ✓  ← record is already in DLQ
[crash here]
CommitRecords         ✗  ← committed offset didn't advance
```

On restart the main topic delivers the same record again, it takes the same path and lands in DLQ a second time. DLQ has no duplicate protection — that's fine, but the DLQ handler must account for it. Either dedup by `(original.topic, original.partition, original.offset)` (which we put in headers), or simply accept that DLQ incidents sometimes come as duplicates.

This is solved by the same transactional outbox or Kafka transactions ([Transactions and EOS](../../../../04-reliability/04-01-transactions-and-eos/i18n/en/README.md)) — but only if DLQ production and the main consumer commit go through a single Kafka transaction. That's more complex, and not every case warrants those guarantees.

## Tradeoffs

Every error handling approach is a tradeoff. Here they are.

In-place retry **blocks the poll loop**. While backoff is spinning on one record, all other partitions of this consumer are also stalled. Long retries lag everything. Protection — keep the retry window short (a few seconds); if you need longer, move to retry-topic.

DLQ **hides errors**. If there's no alert on DLQ and nobody checks it, after a week 50,000 records are sitting there and nobody knows. DLQ without operational tooling is "lost and forgotten." An alert on DLQ topic lag growth is mandatory.

Permanent classification **can be wrong**. If the processor was too quick to mark something as permanent and sent it to DLQ — restoration is manual: a replay CLI reads DLQ and resends to the main topic ([Retry and DLQ deep dive](../../../../04-reliability/04-04-retry-and-dlq/i18n/en/README.md)). So permanent = "I'm sure a retry won't help", not "I'm tired of retrying".

DLQ headers are **not a default contract**. Different teams have different naming conventions. Here we use `error.class` / `error.message` / `original.*` / `retry.count` / `dlq.timestamp`. Confluent's connector framework uses `__error.class.name` and company. With multiple teams this needs to be agreed upfront, otherwise one team writes and another reads — and finds nothing.

## Running it manually

First the topics and seed:

```sh
make topic-create-all      # payments + payments-dlq
make seed                  # 30 messages: 50% ok, 30% transient, 20% permanent
```

`seed` pushes three categories of messages randomly according to the specified percentages (`SEED_MESSAGES`, `TRANSIENT_PCT`, `PERMANENT_PCT` — Makefile variables). Within the permanent share, half are invalid JSON (poison-pill) and half are valid JSON with `mode=permanent`.

Then the processor:

```sh
make run-processor
```

Output will show lines `OK`, `RETRY`, `PERM`, `EXH` — per record you can see what happened. After each batch — counters like `ok=N retried=N dlq-perm=N dlq-exh=N`.

In parallel (or after Ctrl+C) — the DLQ reader:

```sh
make run-dlq-reader
```

Shows each record from DLQ with headers. You can see that permanent records have `error.class=permanent`, malformed JSON also has permanent but `error.message` mentions unmarshal. Exhausted retries (if any appear with low `max-retries`) have `error.class=transient`, and `error.message` will say `exhausted retries: ...`.

Total count in DLQ:

```sh
make dlq-count             # total across all partitions of payments-dlq
```

Cleanup:

```sh
make clean                 # delete committed offsets and both topics
```

## Things to try

- set `TRANSIENT_PCT=80` — most records will spin through retry but still "heal" after 2 attempts (hardcoded in the mock, see constant `transientFails`); result — 80% OK with retry, 20% in DLQ;
- set `MAX_RETRIES=1` with the same `TRANSIENT_PCT=80` — most transient records don't get the chance to "heal" in one attempt and land in DLQ as exhausted; headers will show `error.class=transient`, `error.message: exhausted retries: ...`;
- set `BASE_BACKOFF=2s` and watch processing slow down: one transient record's retry cycle now takes about 14 seconds (`2s + 4s + 8s`); you can see the poll loop blocking;
- run the processor in two instances with the same `GROUP` — partitions split between them, retries run in parallel across partitions, but **within a partition** processing is still sequential (that's about concurrency — lesson [Concurrency and lag](../../../03-05-concurrency-and-lag/i18n/en/README.md));
- run `make seed PERMANENT_PCT=100` — all records go to DLQ, the processor "works" stably (everything commits), but real business impact is zero; this is DLQ-flood — an alert should fire on DLQ lag growth.

## What's next

Here we learned to separate transient from permanent and route them down different paths. But the retry loop is still inside the poll loop — long backoffs are impossible here. The [Retry and DLQ deep dive](../../../../04-reliability/04-04-retry-and-dlq/i18n/en/README.md) lesson untangles this through a chain of retry topics with increasing delays: `*-retry-30s` → `*-retry-5m` → `*-retry-1h` → `*-dlq`. Same principle, different dispatcher.

[Concurrency and lag](../../../03-05-concurrency-and-lag/i18n/en/README.md) — about concurrency. If retry on one record blocks all partitions — maybe process partitions in parallel? Or per-key worker pool? Covers lag, ordering, and tradeoffs between throughput and ordering guarantees.

And in [Delivery to external systems](../../../../04-reliability/04-05-external-delivery/i18n/en/README.md) `PauseFetchPartitions` will appear. Here we only mentioned it; there you'll see a full circuit breaker driving that switch.
