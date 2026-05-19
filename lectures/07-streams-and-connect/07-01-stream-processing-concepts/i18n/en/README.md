# 07-01 — Stream Processing Concepts

Up to this point, we treated Kafka as a transport: write messages, read messages, process one at a time. Most real-world workloads are exactly that. A consumer reads a record, does something with it, writes the result somewhere else, commits the offset, moves on.

But there is a second large class of workloads — where we want to **compute something over the stream**. Average value per minute. Top-10 users per hour. How many times a card was tapped in the last 5 seconds. Offsets alone are not enough here.

That is stream processing. This lesson is an introduction — we'll get into the code in [Stream processing in Go (franz-go + Pebble)](../../../07-02-stream-processing-in-go/i18n/en/README.md). Here — the ideas. What event-time is, how it differs from processing-time, what windowing is, why it is almost always event-time-based, what "late events" means, and why watermark is needed. Plus short notes on KStream/KTable, repartitioning, and stateful operations — vocabulary you need before [Stream processing in Go (franz-go + Pebble)](../../../07-02-stream-processing-in-go/i18n/en/README.md) makes sense.

## Two timestamps

Every event carries multiple timestamps, and confusing them is the primary source of bugs in stream processing.

`Event-time` — when the event happened in the real world. The user pressed a button. A card was swiped at a terminal. A sensor recorded a temperature reading. This is the "correct" time for analytics and business.

`Processing-time` — when our stream process picked up the message. That is, `time.Now()` inside the consumer at the moment we see the record.

`Ingestion-time` — when the message landed in the Kafka log. That is `record.Timestamp`, which the broker either takes from the producer (CreateTime — the default), or stamps itself at append time (LogAppendTime — configured via `message.timestamp.type`).

The gap between them can be enormous. A store terminal was offline for three hours. Then it connected to 4G and flushed a batch of accumulated transactions. Event-time for those transactions is three hours ago. Ingestion-time is now. Processing-time is a second after ingestion (when our consumer wakes up).

When we say "sales for the minute from 14:00 to 14:01", which timestamp do we mean? If it's the business figure — event-time. If it's "how much work hit our cluster during that minute" — processing-time. Mix them up and you get unexplained spikes on dashboards at the worst possible moment.

## Windowing

A stream is infinite. To compute "an average over a period", you need to slice it into finite chunks. Those chunks are called windows. There are several types.

`Tumbling` — fixed size, no overlap. For example, 1-minute windows: 14:00–14:01, 14:01–14:02, 14:02–14:03, and so on. Each event falls into exactly one window. This is what we use in the code below.

`Hopping` — fixed size plus a step. A 5-minute window with a 1-minute step: 14:00–14:05, 14:01–14:06, 14:02–14:07, and so on. Windows overlap; an event falls into several of them. Useful for moving averages.

`Sliding` — a window "around each event". Size is fixed; the anchor is the arrival time of the record. Used less often because it is more expensive to compute.

`Session` — an activity-based window. Opens on the first event, closes when no new events arrive for longer than the gap (for example, 30 minutes). Each window has its own size. This is how user web sessions are typically computed.

In our code — the simplest tumbling windows of 1 minute, computed **two ways simultaneously**: by event-time and by processing-time. The goal is to see with your own eyes that the same events land in different minute buckets.

## Watermark and late events

Since we aggregate by event-time, an uncomfortable question arises. When do you **close** a window? The 14:00–14:01 event-time window can receive new events at 14:02 (just arriving from offline) or a day later (after full reconnection). Wait for "everyone late" and the window never closes.

The solution is a watermark. It is a monotonically increasing estimate of the form "I am confident no more events with event-time below W will arrive". When the watermark passes the end of a window, that window is emitted as complete. Events that arrive after — late events. They are handled in different ways. Drop them. Put them in a side topic for post-processing. Retroactively update an already-emitted window — if the downstream can handle it.

The simplest watermark strategy is `max(event-time of seen records) - tolerance`. Tolerance — for example, 1 minute. Meaning: if the freshest event we have seen was at 14:30, windows up to 14:29 can be closed. Records with event-time before 14:29 that arrive after this point — late.

A subtle point. The watermark is **per-partition**, not global. If we have 6 partitions and one of them stalls (no new records), the topic-wide watermark is stuck — waiting for the slow one. Same logic as HWM in replication: don't advance until the slowest side catches up.

Our code has no watermark — we just accumulate windows in memory indefinitely and print them. This is a simplification for the lesson. In [Stream processing in Go (franz-go + Pebble)](../../../07-02-stream-processing-in-go/i18n/en/README.md) there is no watermark either — word-count does not need one. Fully-fledged systems (Flink, Beam, Kafka Streams 3.x+) compute watermarks automatically.

## KStream vs KTable

Two terms from the Kafka Streams world, worth knowing even if Go has no native Streams.

`KStream` — a stream of events. Each record is an independent event. Duplicate keys are normal (the same user can buy five times — that is five events). History matters. Semantics — append.

`KTable` — a **stateful snapshot**, a compacted topic projected onto "current state". A record with the same key overwrites the previous one. Semantics — upsert. A tombstone (value=nil) deletes the key.

The difference in plain terms. KStream `purchases` — all purchases across all time. KTable `user-balance` — the user's current balance at this moment. The key is the same (user_id); the meaning is completely different.

Under the hood, KTable is typically a compacted topic plus a local state store (RocksDB or Pebble) that records are projected into. On restart, the store is rebuilt from the compacted topic. In [Stream processing in Go (franz-go + Pebble)](../../../07-02-stream-processing-in-go/i18n/en/README.md) we reproduce this pattern manually: Pebble + changelog topic for word-count.

## Repartitioning

Suppose we read `orders` (key=order_id) and want to group by user_id to compute total orders per user. The problem is that all records for a single user are spread across all topic partitions — because they were hashed by order_id. You cannot aggregate counts across partitions locally: different workers see different subsets.

The solution is repartitioning. Step 1: read `orders`, repack into `orders-by-user` with key=user_id. Step 2: read `orders-by-user` as a stream already grouped by the desired key. Now all records for one user land in a single partition — you can keep local state and compute correctly.

Repartitioning is not cheap. Extra serialization, an extra topic, an extra network round-trip. In Kafka Streams it happens implicitly on `groupBy` operations (when the key changes) — which is why the library API often carries a warning "may trigger repartitioning". When building a stream on franz-go yourself, repartitioning is manual: the producer writes to the repartition topic, the consumer reads from it.

## Stateful operations

Stateless — where processing one record does not depend on others. `map`, `filter`, `flatMap`. Transform, drop, multiply. No state needed; restart is lossless.

Stateful — where you need to remember something between records. `count`, `sum`, `min/max`, `aggregate`, `join`. A state store is required: either on disk (Pebble/RocksDB), or in memory with a changelog topic for durability. Otherwise, any restart means losing accumulated state. In our aggregator, state is two in-memory maps; `kill -9` resets them. Fine for a lesson; not for production.

In [Stream processing in Go (franz-go + Pebble)](../../../07-02-stream-processing-in-go/i18n/en/README.md) we add Pebble and a changelog topic — a working model of stateful processing with state recovery after restart. Without this layer, any stream analytics resets on the first failure.

## What the code shows

One binary, two roles.

The producer (`-role=events`) ticks every `rate` (50ms by default) and writes one event. Each event has a synthetic event-time that **lags** behind wall-clock by a random amount. A normal event: lag of 0 to 60 seconds. With probability `late-prob` (10% by default) the lag jumps to 90–240 seconds: simulating a terminal that flushed a batch from offline.

The event-time construction looks exactly like this:

```go
lag := time.Duration(rng.Int63n(int64(o.eventLagMax) + 1))
late := false
if rng.Float64() < o.lateProb && o.lateLagMax > o.lateLagMin {
    lag = o.lateLagMin + time.Duration(rng.Int63n(int64(o.lateLagMax-o.lateLagMin)+1))
    late = true
}
eventTime := now.Add(-lag)
```

Then event-time is placed in the Kafka header `event-time` as 8 bytes of `unix-nano` big-endian (the header format at the course level is our own, not a standard):

```go
headers := []kgo.RecordHeader{
    {Key: "event-time", Value: encodeUnixNano(eventTime)},
}
if late {
    headers = append(headers, kgo.RecordHeader{Key: "late", Value: []byte("1")})
}
```

The producer writes via `ProduceSync` with key `u-XX` (50 different users by default) — the partitioner distributes them across 3 partitions deterministically.

The aggregator (`-role=aggregator`) subscribes to the topic in group `lecture-07-01-aggregator`, starting with `AtEnd()` (old events from previous test runs are not interesting). For each record it computes two timestamps:

```go
processingTime := time.Now()
eventTime := processingTime
for _, h := range rec.Headers {
    switch h.Key {
    case "event-time":
        if t, ok := decodeUnixNano(h.Value); ok {
            eventTime = t
        }
    case "late":
        late = string(h.Value) == "1"
    }
}
agg.add(eventTime, processingTime, late, missing)
```

Inside `add` — the same record increments counters in two maps: `byEventTime[eventTime.Truncate(window)]++` and `byProcessing[processingTime.Truncate(window)]++`. Truncating is the standard trick for tumbling windows: 14:23:47 with a window size of 1m gives key `14:23:00`.

Every `print` interval (5s by default) a background goroutine calls `snapshot` and prints a table:

```
[15:42:11]  total=512  late=48  no-header=0
WINDOW  BY EVENT-TIME  BY PROCESSING-TIME  DIFF
15:38   3              0                   +3
15:39   12             0                   +12
15:40   38             0                   +38
15:41   67             100                 -33
15:42   13             412                 -399
---
```

What you see here. In the `BY PROCESSING-TIME` column, everything is in the current minute — we are "now", so processing falls into one or two of the latest windows. In `BY EVENT-TIME`, the distribution is smeared back 4–5 minutes: events arriving now but with event-time from 15:38 or 15:39 land in their actual minutes. `DIFF` shows how far the same event landed "off" from the processing-time perspective. Late events (5–10% by default) drifted even further back — they make up the noise in the 15:38–15:39 windows that are already "closed" in processing-time.

If we had a watermark with 1-minute tolerance, the 15:38 window would have closed around 15:39:30 — and records arriving at 15:42 would be late events. You could drop them, send them to a side output, or retroactively update the emitted value. Every choice is a trade-off. For dashboard metrics, typically drop; for financial reports — update; for analytics — send to a late-topic for post-processing.

## Running

The sandbox must be running (`docker compose up -d` from the root).

Create the topic once:

```sh
make topic-create
```

In one terminal — the producer:

```sh
make run-events
```

In another — the aggregator:

```sh
make run-aggregator
```

Every 5 seconds the aggregator prints the windows table. Wait a minute or two until several windows are filled — the gap between event-time and processing-time will become obvious.

Tune the parameters:

```sh
make run-events RATE=10ms LATE_PROB=0.30        # faster and with a larger late tail
make run-aggregator WINDOW=30s PRINT=2s         # 30-second windows, print every 2 seconds
```

Clean up after the lesson:

```sh
make topic-delete
```

## Takeaways

A few things to take away from this.

1. **Event-time is your source of truth for analytics.** Processing-time is smooth and convenient, but it describes your system, not reality. If the business counts "revenue per minute", compute by event-time. If SRE measures "cluster load" — processing-time.
2. **Tumbling windows are the simplest and correct default.** No overlap. Clear semantics. Each event in exactly one window. Use hopping/sliding/session only when tumbling genuinely does not fit.
3. **Late events are normal.** A gap of tens of seconds appears instantly with any network instability. Minutes — with offline devices. Hours and days — with retries from a dead-letter queue. Every stream system must explicitly answer "what do we do with late events".
4. **Watermark is not an exact science.** It is a heuristic. Too short — the window closes early, you lose data. Too long — the window emits late, the dashboard lags. Tune it for your traffic profile.

In [Stream processing in Go (franz-go + Pebble)](../../../07-02-stream-processing-in-go/i18n/en/README.md) we take this and apply it to a stream that actually stores state — word-count on franz-go + Pebble + changelog topic. After restart, state is recovered from the changelog and accumulated counts are not lost. That is already close to real stream processing.
