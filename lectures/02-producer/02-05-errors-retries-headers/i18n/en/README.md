# 02-05 — Errors, Retries & Headers

This is the last lecture in the producer module. We close three topics left hanging from previous ones: what to do with a write error, how retries work, and why each record has a separate headers slot that is not part of the payload.

Errors differ. Some the client must retry itself; others it must surface upstream and leave alone. This is the retriable vs. non-retriable split. At the Go code level it looks like "ProduceSync returned an error" — but what's inside that error and what to do with it depends on the error code that came back from the broker.

## Retriable vs. non-retriable — the formal difference

In Kafka every protocol error has a 16-bit code. Some codes are marked retriable, others non-retriable. This field is hard-wired in the Kafka specification, not in the client.

The logic is straightforward. Retriable means "try again, it will likely go through." The partition leader is being re-elected, the ISR count is temporarily below the required threshold, the controller is migrating — all transient, resolves in a second or two. The broker openly says "I can't right now, retry."

Non-retriable means "this won't be fixed by another attempt, sort it out." The message is larger than the broker will accept. The topic doesn't exist and auto-creation is off. Authorization failed. The record body is broken at the CRC level. There's no point sending the same bytes again and hoping for a different result — the request has exactly that format and will have exactly that format on the next attempt.

In `franz-go` the check is simply `kerr.IsRetriable(err)`:

```go
func IsRetriable(err error) bool {
    var kerr *Error
    return errors.As(err, &kerr) && kerr.Retriable
}
```

Every known error has a `Retriable` field. For example:

- `MESSAGE_TOO_LARGE` — `Retriable=false`. The request exceeds `max.message.bytes`. Send it a thousand times — the broker rejects it every time.
- `NOT_ENOUGH_REPLICAS` — `Retriable=true`. ISR dropped below `min.insync.replicas`. Wait, the ISR recovers, the retry succeeds.
- `LEADER_NOT_AVAILABLE` — `Retriable=true`. The partition leader is moving; metadata updates in a second.
- `TOPIC_AUTHORIZATION_FAILED` — `Retriable=false`. The client lacks permissions. A hundred retries won't change that.

The client sees the code, reads the `Retriable` flag, and decides: either put the request in the retry queue or surface the error upstream via the future.

## What franz-go does with a retriable error

When a Produce request comes back with a retriable error, the client does **not** immediately return `ProduceSync(...) → err`. It holds the record in an internal queue and retries on its own. The retry count is capped by `RecordRetries` (default `math.MaxInt64`, meaning "until time runs out"). The time budget is capped by `RecordDeliveryTimeout`.

`RecordDeliveryTimeout` is the total budget for delivering **one** record. It covers everything: the first attempt, the wait before a retry, the second attempt, metadata refreshes — all of it. By default it is unset (= ∞), and then the effective limit is only `RequestTimeoutOverhead` plus `RecordRetries`. In practice, set an explicit ceiling — 30 seconds, a minute — otherwise a retrying record can sit in the buffer for several minutes, consuming `MaxBufferedRecords`.

The relationship with `RequestTimeoutOverhead` matters. That parameter adds to the time the broker itself waits before responding. It does not limit total record delivery; it limits **one attempt**. To make the client quickly detect "broker not responding" and move on to a retry — lower `RequestTimeoutOverhead`. To give a record more total time — raise `RecordDeliveryTimeout`. These are different knobs for different jobs.

When the delivery timeout is exhausted, `franz-go` returns not a bare `kerr.NotEnoughReplicas` but its own `kgo.ErrRecordTimeout`, with the last observed retriable cause wrapped inside via `%w`. On the application side the check therefore goes through two levels:

```go
if errors.Is(pErr, kgo.ErrRecordTimeout) {
    // handle the fact that delivery did not go through
}
if errors.Is(pErr, kerr.NotEnoughReplicas) {
    // specific case — the last cause was NER
}
```

Any non-retriable error is returned **immediately** after the first failed attempt. No waiting, no retries. `ProduceSync` completes at that point and the application gets the error via `out.FirstErr()`.

## What the code shows

There is one binary — `cmd/error-classes` — with two modes selected by the `-mode` flag. This is a rare case where two almost-different scenarios are more logically kept in one file than split into two: the shared infrastructure (admin client, ensure-topic, client creation) is identical; only the topic config and the expectation differ.

**Non-retriable.** Create a topic with `max.message.bytes=1024`, write a 4 KB record of random bytes. Expect `MESSAGE_TOO_LARGE` instantly:

```go
if err := ensureTopicWithMaxBytes(ctx, admin, topic, "1024"); err != nil {
    return fmt.Errorf("ensure topic %s: %w", topic, err)
}

cl, err := kafka.NewClient(
    kgo.DefaultProduceTopic(topic),
    kgo.ProducerBatchMaxBytes(1<<20),
    kgo.MaxBufferedRecords(10),
    kgo.ProducerBatchCompression(kgo.NoCompression()),
)
```

`ProducerBatchCompression(kgo.NoCompression())` is required. The `franz-go` default is Snappy. Without explicitly disabling it, a "dense" payload (cycling `a`–`z`) compresses to ~300 bytes, fits under the limit, and the broker accepts it. With `NoCompression` and truly random bytes the on-wire size matches the record length, and the broker fires as expected.

After `ProduceSync` inspect:

```go
out := cl.ProduceSync(rpcCtx, rec)
pErr := out.FirstErr()
switch {
case errors.Is(pErr, kerr.MessageTooLarge):
    // expected scenario — non-retriable
}
```

The output shows the entire `ProduceSync` cycle took tens of milliseconds — one round-trip, no retries. The error itself includes exactly how many bytes arrived (`uncompressed_bytes=4114, compressed_bytes=4114`).

**Retriable.** Create a topic with `min.insync.replicas=3` at RF=3, write with `acks=all`. Before the run the operator executes `make kill-broker` (stops `kafka-2`), and the ISR drops to 2. Every attempt comes back with `NOT_ENOUGH_REPLICAS` — the client retries until `RecordDeliveryTimeout` expires.

To see exactly how retries proceed, enable the built-in `franz-go` logger at debug level:

```go
opts := []kgo.Opt{
    kgo.DefaultProduceTopic(topic),
    kgo.RecordDeliveryTimeout(deliveryTimeout),
    kgo.RequiredAcks(kgo.AllISRAcks()),
}
if debug {
    opts = append(opts, kgo.WithLogger(kgo.BasicLogger(os.Stderr, kgo.LogLevelDebug, nil)))
}
```

The debug log shows the full chain: first Produce → broker responded `NOT_ENOUGH_REPLICAS` → client writes `rewinding produce sequence to resend pending batches` → new request → NER again → … → until the delivery timeout hits.

Once the timer expires, `out.FirstErr()` returns `kgo.ErrRecordTimeout`. Inside (via `errors.Unwrap`) is `NOT_ENOUGH_REPLICAS` — the last cause that blocked delivery.

Restore the broker (`make restore-broker`) — ISR returns to 3, the run completes in a few hundred milliseconds, no errors.

## Headers — a separate slot, not part of the payload

Now for headers. Every record in Kafka, starting from protocol 0.11, has a dedicated headers section — an array of `(key string, value []byte)` pairs. The broker stores them, passes them to consumers, and does not interpret them. They are your data, and Kafka's position on them is "none of my business, I'll store them as-is."

Why do they exist? Headers are the place for **infrastructure** metadata that the infrastructure needs, not the consumer's business logic. The standard set:

- `traceparent` (or `b3`) — distributed tracing context. The producer injects its current trace; the consumer reads it and continues the span. Everything works at the infrastructure level; the business handler doesn't even know tracing exists.
- `correlation-id` — request ID for correlating logs across services. Useful when OpenTelemetry tracing is not configured.
- `message-type` — event version and type (`order.created.v1`, for example). The consumer uses it to select a schema/decoder without looking inside the payload.
- `source-service` — who produced the event. Useful for auditing, DLQ filtering, multi-service debugging.
- `timestamp` (when needed — the built-in one is usually enough), `idempotency-key`, `tenant-id` — the list goes on.

Why **not** put this in the payload? Several reasons.

First, the payload is your business contract — the schema. It carries the fields of the event itself: `order_id`, `amount`, `currency`. Putting `traceparent` there means it lands in the schema, lands in Schema Registry, and cannot change without a formal schema revision. A tracing context is completely orthogonal to the event — it belongs to the transport, not to the meaning.

Second, headers are accessible without parsing the payload. This matters for routers, filters, and DLQ handlers that do not want to know the event schema and simply make decisions based on type, correlation, or source. A distributed tracing collector also should not parse every event's Protobuf or Avro just to extract a trace ID.

Third, headers are a standard. OpenTelemetry, W3C Trace Context, CloudEvents — all of them formalize propagation through headers. Move to the payload and you need a custom parser for every case.

## What the headers-demo does

The binary `cmd/headers-demo` is a compact round-trip: a producer writes 5 records with a set of headers; a consumer immediately reads them and prints the headers alongside the payload. By default both run in the same process via `-mode=roundtrip`; for two terminals there are separate `-mode=producer` and `-mode=consumer` modes.

The record structure inside the producer:

```go
rec := &kgo.Record{
    Key:   []byte(fmt.Sprintf("order-%d", i+1)),
    Value: []byte(fmt.Sprintf(`{"id":"order-%d","status":"created"}`, i+1)),
    Headers: []kgo.RecordHeader{
        {Key: "traceparent", Value: []byte(trace)},
        {Key: "correlation-id", Value: []byte(correlationID)},
        {Key: "message-type", Value: []byte(msgType)},
        {Key: "source-service", Value: []byte(service)},
    },
}
```

Headers are a plain slice; the key is a string, the value is bytes. No restrictions on keys: the same key can appear multiple times (permitted by the protocol; useful for multi-value headers). Values are arbitrary — a UTF-8 string, binary, JSON, anything. The one practical rule: the combined length of headers — keys plus values — counts toward `max.message.bytes`. Don't put megabytes in there.

On the consumer side every field arrives exactly as it was:

```go
fetches.EachRecord(func(r *kgo.Record) {
    fmt.Fprintf(tw, "  %d\t%d\t%s\t%s\t%s\n",
        r.Partition, r.Offset,
        string(r.Key),
        formatHeaders(r.Headers),
        string(r.Value),
    )
})
```

Two things are visible. Headers are `[]kgo.RecordHeader`, the same type as in the producer; their order is preserved. The broker does nothing with them: what went in comes out, byte for byte.

`traceparent` is constructed in W3C format: `00-<trace-id 32 hex>-<span-id 16 hex>-01`. No real spans are opened here — in production code the current trace from OpenTelemetry would be injected. Here it is random data, just to show that values differ across records.

## Takeaways

- Kafka protocol errors split into retriable (transient, worth retrying) and non-retriable (retrying is pointless). The split is in the spec, not in the client.
- `franz-go` retries retriable errors automatically. The limits are `RecordRetries` and `RecordDeliveryTimeout`. By default retries are effectively unlimited, so without an explicit delivery timeout a record can sit in the buffer for a very long time.
- `RecordDeliveryTimeout` is the total budget for delivering one record; `RequestTimeoutOverhead` is an addend for a single round-trip. They are different knobs.
- When the delivery timeout is exhausted you get `kgo.ErrRecordTimeout` wrapping the last cause. Check with `errors.Is(err, kgo.ErrRecordTimeout)` or drill in with `errors.Is(err, kerr.NotEnoughReplicas)` and similar.
- Non-retriable errors fail immediately. `ProduceSync` returns the error without retries; the timing shows it: single-digit to tens of milliseconds.
- Headers are a separate record slot. Put everything infrastructural there: tracing context, `correlation-id`, `message-type`, `source-service`, `idempotency-key`, and so on. The broker does not touch them.
- The payload carries only the event's business fields — those described in its schema. Headers are for everything else that the transport and infrastructure need.

Module 03 switches to the consumer side: consumer groups, rebalancing, processing guarantees. Headers will be needed in every other lecture there — especially in [Error handling](../../../../03-consumer/03-04-error-handling/i18n/en/README.md), where DLQ messages fundamentally require `error.message`, `error.class`, and `original.offset` in headers, not in the payload.

## Running

The sandbox must be running from the repository root (`docker compose up -d`).

Non-retriable scenario:

```sh
make run-errors
```

Creates topic `lecture-02-05-non-retriable` with `max.message.bytes=1024`, writes 4 KB of random bytes, catches `MESSAGE_TOO_LARGE`.

Retriable scenario:

```sh
make kill-broker      # stops kafka-2, ISR drops to 2
make run-errors-retriable
make restore-broker   # starts kafka-2, ISR returns to 3
```

In default mode the client runs with the debug logger and every attempt is visible: Produce → `NOT_ENOUGH_REPLICAS` → rewind → again. After 20 seconds (delivery timeout) `kgo.ErrRecordTimeout` arrives.

Headers demo:

```sh
make run-headers
```

One process in roundtrip mode: writes 5 records with `traceparent`/`correlation-id`/`message-type`/`source-service`, immediately reads them in a consumer group, and prints them in a table.

Re-running clean is also useful, especially if anything is left from previous runs:

```sh
make topic-delete
```
