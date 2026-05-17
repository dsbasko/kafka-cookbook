# 01-04 - Offsets and Retention

Last night at Brew the `notification-service` process died. The on-call engineer brought it back in a minute, but it had been down for three hours. During that time about twenty thousand `OrderPlaced` and as many `PaymentReceived` events landed in `brew.orders.v1`. Question for the on-call: what should the service read now? Catch up on everything it missed? Only new stuff? And where does Kafka even remember how far it had read before the crash?

This lecture answers both questions. The first is about offset: every message in a partition has its own number, and the consumer keeps a bookmark "I was here." The second is about retention: Kafka deletes old messages on a timer by itself, and the bookmark may end up in a section of the log that is no longer on disk.

## Offset - just a record number

A partition is an ordered append-only log (see [Topics and partitions](../../../01-02-topics-and-partitions/i18n/en/README.md)). Records in that log are numbered sequentially: 0, 1, 2, 3, and so on. That number is the offset. The broker assigns it on write: the producer writes `OrderPlaced`, the broker responds "received, partition=2 offset=17". The pair `(partition, offset)` then identifies the message uniquely and permanently.

Analogy for a backend engineer with PostgreSQL experience. An offset is similar to a `ctid` or a monotonic `id` of a row, only without UPDATEs: a record with offset=10 always came before the record with offset=11, and that fact stays true forever. This is the basic ordering guarantee within a partition.

A few properties worth stating upfront:

- An offset lives in **a single partition**. Offsets are independent across partitions. Partition-0 at offset=42 and partition-1 at offset=42 are unrelated records.
- The broker assigns the offset, not the client. The client cannot ask "please write this under offset=100" - the broker decides the next number.
- Offsets grow monotonically. There are no holes in the numbering (technically, holes are possible for an idempotent producer after retries, but for the course model we treat that as a detail).
- The offset survives a broker restart. The number lives on disk next to the message, not in RAM. A Kafka restart does not renumber anything.

A partition at any moment has two boundaries. **Earliest** is the offset of the oldest live message, **latest** is the offset that the next write will receive (one more than the offset of the most recent stored message). On an empty partition earliest=latest=0. When `order-service` writes to the topic, latest grows. When retention sweeps old segments, earliest grows. The log "flows" - filled from the top, draining from the bottom.

## LEO, HWM, and leader epoch - under the microscope

This is where confusion starts. Worth sorting out once.

`Log End Offset (LEO)` is the position where the partition **leader** will write the next message. The offset of the "next record" that doesn't exist yet. The leader and each follower have their own LEO; the follower's usually lags slightly because the follower pulls data asynchronously (for roles, see [Replication and ISR](../../../01-03-replication-and-isr/i18n/en/README.md)).

`High Watermark (HWM)` is the offset up to which a consumer is allowed to **read**. HWM equals the minimum LEO across replicas in the ISR. The idea is simple: until a message has been picked up by all ISR replicas, nobody should see it. Otherwise, after a failover, the new leader would not remember something a consumer had already read - readable history that vanishes after the switch. Kafka cannot allow that.

Between the leader's LEO and the HWM there is a gap - records the leader already accepted but the ISR has not caught up to. They physically sit in the log, but are invisible to the consumer.

`Leader Epoch` is a counter that ticks on every leader change. It is needed to correctly truncate follower logs after a switch - a rare invariant that fixes complex failover bugs. Knowing it exists is enough; we will not dig into it in this course.

In our code, `kadm.ListEndOffsets` returns an offset equivalent to the HWM (for an in-sync client, that's the leader's LEO bounded by the ISR - Kafka does not expose records that are not yet committed).

```
partition: brew.orders.v1-0

  earliest                                       latest = HWM
     │                                              │
     ▼                                              ▼
   ┌──────────────────────────────────────────────┐
   │ msg msg msg msg msg msg msg msg msg msg msg  │
   └──────────────────────────────────────────────┘
   offset:  17  18  19  20  21  22  23  24  25  26  27 ◄── next OrderPlaced lands here

   retained = latest - earliest = 27 - 17 = 10

   old segments (offsets 0..16) already deleted by retention
```

## The consumer's bookmark - committed offset

Brokers assign offsets on write. But who remembers how far `notification-service` had read? The service itself remembers in memory while it is alive. But if the process dies and is brought back three hours later, memory is empty. An external bookmark mechanism is needed.

That mechanism is called the **committed offset**. The consumer periodically tells Kafka: "group `notification-service`, topic `brew.orders.v1`, partition=0 - I processed records 0..41, next time start from 42." That is a commit.

Where does Kafka store these bookmarks? In a system topic called `__consumer_offsets`. Inside the sandbox it has 50 partitions (`offsets.topic.num.partitions=50` by default), the replication factor matches `KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR` (3 on the Brew sandbox). Each record in `__consumer_offsets` is a regular Kafka message with key `(group, topic, partition)` and value "committed offset, metadata."

Important. **The committed offset is a pointer to the next record to read**, not to the last processed record. "Committed=42" means "I processed 0..41, read on starting from 42." A trivial detail, but people trip over it: write `lastProcessed` instead of `lastProcessed+1` and you'll re-read the same message every time.

When `notification-service` comes back, it goes to `__consumer_offsets`, finds the entry for its group and partition 0, reads the committed offset (say, 4781) and starts fetching from 4781. All 20,000 `OrderPlaced` events that arrived during the three-hour downtime get caught up in order - Kafka acts like a catch-up read queue.

On the **first** start of a group there is no committed offset. What to do? That is decided by `auto.offset.reset`. The value `latest` means start from the end - the group sees only new events and skips everything that happened before launch. The value `earliest` means start from the beginning - the group rereads the log from offset=0, useful for analytics or state recovery. The value `none` fails with an error - useful in systems where "forgetting" a position must be visible and requires manual triage.

For Brew's `notification-service`, the setting is `latest`: on first launch you don't want to hammer customers with push notifications about orders from three weeks ago. For `analytics-service`, which recomputes aggregates, it's `earliest`. Details about the `__consumer_offsets` topic itself (record format, how Kafka picks the bookmark partition by `hash(group)`) are covered in [Offset commits](../../../../03-consumer/03-02-offset-commits/i18n/en/README.md). Here we fix the model: consumer position lives separately from data, in a system compacted topic.

## Retention - two axes on which the log ages

The parameter that answers "how long do messages live in Kafka." There are two.

`retention.ms` - by time. A log segment whose last record is older than `retention.ms` milliseconds is considered stale and deleted entirely. Default - 7 days (`604800000`).

`retention.bytes` - by size. When the total size of a partition on disk exceeds `retention.bytes`, the oldest segments are deleted until the size returns to bounds. Default `-1`, meaning no size limit.

The parameters are **not mutually exclusive**. A segment is deleted if it hits either limit. On critical Brew topics both are set: time for the "consumers have N days to catch up" guarantee, size so that a random traffic spike does not eat the disk. For `brew.orders.v1` it looks like this:

```
retention.ms     = 2592000000   # 30 days
retention.bytes  = 53687091200  # 50 GiB per partition
cleanup.policy   = delete
segment.ms       = 86400000     # 1 day
```

Right here is the most common beginner mistake. "We have retention.ms=86400000, so messages live exactly one day." No. They live **at least** one day and **at most** one day plus the duration of the active segment. The active segment (the one currently being written) is never deleted. Retention looks at the timestamp of the **last** record in a segment, not at each message individually. A message that landed at the very start of a segment's life will survive for `retention.ms + segment.ms` - until the segment closes, ages out, and gets swept.

One more thing. Cleanup is deferred. The broker runs the retention checker once every `log.retention.check.interval.ms` (default 5 minutes). On the Brew sandbox this is the default value - so in the demo below `earliest` will jump in discrete steps every few minutes, not smoothly.

## Retention across Brew topics

Each Brew topic has its own retention tuned to its use profile:

- `brew.orders.v1` - 30 days. Analysts build month-long funnels, order-state recovery after a bug requires replay.
- `brew.payments.v1` - 30 days. Mirrors orders, financial audit requests monthly samples.
- `brew.kitchen.v1` - 7 days. Operational kitchen events, nobody looks at them after a week.
- `brew.delivery.v1` - 7 days. Courier tracking lives briefly, after delivery the record becomes useless.

What if accounting requires keeping financial events for 7 years for compliance? Kafka is not the right tool for that. Long-term storage in Brew is offloaded by a nightly job from `brew.payments.v1` into S3 (or into a data lake - depending on infrastructure). In this scenario Kafka is a fast month-long buffer, S3 is the archival store for years. Nobody tries to make Kafka hold seven years of data: it's expensive, inefficient (S3 is roughly 50x cheaper per gigabyte), and not what Kafka is designed for.

## cleanup.policy - delete and compact

Since we touched on `__consumer_offsets`, two words about the cleanup parameter itself. A topic has a `cleanup.policy` that controls **how** Kafka cleans up old data. Four behaviors are available:

- `delete` - standard behavior, default. Old segments are deleted according to retention.ms / retention.bytes. This is for ordinary event topics: `brew.orders.v1`, `brew.payments.v1`, `brew.kitchen.v1`, `brew.delivery.v1` - all on `delete`.
- `compact` - log compaction. No whole segments are deleted. **Old versions of each key** are removed - the most recent record with key `K` survives until a new one with the same key appears. This is for state topics: latest customer profile, latest config, latest committed offset of a group.
- `delete,compact` - hybrid. Segments are compacted by key, then anything older than retention is dropped entirely. Useful when both a snapshot and a time bound are needed.
- Unset. The topic inherits the broker default (`log.cleanup.policy`, usually `delete`). On the Brew sandbox this is what happens - topics do not declare `cleanup.policy` explicitly.

The `__consumer_offsets` topic uses `compact` exactly for the reason we mentioned: there are millions of closing offsets, but only the latest position of a group matters. Compaction is treated in depth in [Retention and compaction](../../../../08-operations/08-02-retention-and-compaction/i18n/en/README.md), that's where it belongs. Here it's enough to know that there are several options and that `__consumer_offsets` uses `compact`.

## What load-and-watch shows

`cmd/load-and-watch/main.go` builds a small retention sandbox on top of `brew.orders.v1`. It creates the topic with `partitions=3`, `rf=3`, **`retention.ms=60000`** (one minute), **`segment.ms=10000`** (ten seconds). Those are demo numbers: in production at Brew, as we saw above, `brew.orders.v1` has retention of 30 days. But to see old segments disappear in five minutes, retention is dialed down to a minute.

Idempotent. If the topic already exists, the config is updated via `AlterTopicConfigs` - it does not crash and does not get stuck with stale retention. Then it writes 100 "orders" via `ProduceSync` with keys `order-0..order-99` and payloads like `OrderPlaced order_id=order-N` - emulating the Friday promo order surge (for hash partitioning, see [Keys and partitioning](../../../../02-producer/02-01-keys-and-partitioning/i18n/en/README.md)).

Topic configs are passed directly to `CreateTopic` as the fourth argument - a `name → *string` map:

```go
configs := map[string]*string{
    "retention.ms":   kadm.StringPtr(strconv.FormatInt(o.retention.Milliseconds(), 10)),
    "segment.ms":     kadm.StringPtr(strconv.FormatInt(o.segment.Milliseconds(), 10)),
    "cleanup.policy": kadm.StringPtr("delete"),
}

resp, err := admin.CreateTopic(rpcCtx, o.partitions, o.rf, configs, o.topic)
```

After writing 100 messages, a 10-second ticker starts. On each tick it:

1. Writes one heartbeat message `hb-N`. Why - below.
2. Calls `kadm.ListStartOffsets` (earliest = log start offset).
3. Calls `kadm.ListEndOffsets` (latest = HWM).
4. Prints a table: PARTITION / EARLIEST / LATEST / RETAINED, plus TOTAL.

In code, two back-to-back requests - both return a `(topic, partition) → offset` map:

```go
starts, err := admin.ListStartOffsets(rpcCtx, topic) // earliest = log start
ends,   err := admin.ListEndOffsets(rpcCtx, topic)   // latest   = HWM

starts.Each(func(o kadm.ListedOffset) {
    rows = append(rows, row{partition: o.Partition, earliest: o.Offset})
})
for i := range rows {
    if eo, ok := ends.Lookup(topic, rows[i].partition); ok && eo.Err == nil {
        rows[i].latest = eo.Offset
    }
}
// retained := latest - earliest - how many messages are live right now
```

Heartbeats are not decoration here. A segment closes based on `segment.ms` from the moment of the **last** write into it, and the active segment is never deleted. Without heartbeats, after the initial 100 messages the active segment would live forever - retention would remove nothing because the entire log would sit in one unclosed segment. A heartbeat every 10 seconds rolls the current segment: it closes per `segment.ms`, a new one opens in its place, and the closed one can now be picked up by retention.

What you will see when you run it:

```
[16:42:11]  heartbeats=0
PARTITION  EARLIEST  LATEST  RETAINED
0          0         34      34
1          0         33      33
2          0         33      33
TOTAL      0         100     100
---
```

Start. All 100 messages present. EARLIEST is 0 everywhere.

```
[16:43:21]  heartbeats=7
PARTITION  EARLIEST  LATEST  RETAINED
0          0         36      36
1          0         35      35
2          0         36      36
TOTAL      0         107     107
---
```

After a minute, LATEST grew (heartbeats added), EARLIEST still 0. Old segments are stale already, but the retention checker has not run yet.

After a few minutes (5-7, on the sandbox with the default `log.retention.check.interval.ms=300000`):

```
[16:48:31]  heartbeats=37
PARTITION  EARLIEST  LATEST  RETAINED
0          34        66      32
1          33        65      32
2          33        66      33
TOTAL      100       197     97
---
```

Here is the interesting part. EARLIEST on each partition jumped from 0 to 33-34. The retention checker ran, found segments whose max timestamp was older than 60s, and deleted them entirely. The original 100 records went with them - they're no longer readable by anyone. RETAINED shows "how many messages are currently in the log" - about 32 per partition (the recent heartbeats).

Leave the program running and the picture keeps drifting right. EARLIEST chases LATEST with a lag of `retention.ms + segment.ms + log.retention.check.interval.ms` - roughly 6-7 minutes.

This scenario is a tiny model of what would have happened to `notification-service` after the three-hour downtime, had `brew.orders.v1` retention been shorter than three hours. The service would come back, fetch its committed offset from `__consumer_offsets`, get, say, 5000. It would ask the broker for records starting at 5000 - and get `OFFSET_OUT_OF_RANGE`, because retention had already swept that range. Subsequent behavior depends on `auto.offset.reset`: `latest` skips the gap and continues from the end, `earliest` starts from current earliest (not from 5000), `none` fails. Brew has 30-day retention on orders specifically so that this does not happen during typical incidents.

## Running

The sandbox must be up (`docker compose up -d` from the root).

```sh
make run
```

In a second terminal, useful to compare against the CLI in parallel:

```sh
make topic-describe
```

You get `kafka-topics.sh --describe` (RF, partitions, leader/replicas/ISR - the picture from [Topics and partitions](../../../01-02-topics-and-partitions/i18n/en/README.md) and [Replication and ISR](../../../01-03-replication-and-isr/i18n/en/README.md)) plus `kafka-configs.sh --describe`, which shows the configured `retention.ms=60000`, `segment.ms=10000`, `cleanup.policy=delete`.

Restart from scratch:

```sh
make run RECREATE=true
```

To test retention more aggressively - set retention=10s, segment=5s:

```sh
make run RETENTION=10s SEGMENT=5s
```

Clean up after the lecture:

```sh
make topic-delete
```

## Takeaways

Practical implications:

1. **"Stored for X days" means up to X days plus the segment duration.** If the contract with consumers requires "guaranteed last 7 days available" - set retention.ms to 7 days with margin, not exactly. The active segment stays open until it closes and eats time on top.
2. **Earliest grows on its own.** A consumer that has fallen behind by more than the retention period will get `OFFSET_OUT_OF_RANGE` when trying to read its position. It simply doesn't exist in the log anymore. This is expected Kafka behavior. Configurable via `auto.offset.reset` (latest/earliest/none) - details in [Offset commits](../../../../03-consumer/03-02-offset-commits/i18n/en/README.md).
3. **Committed offset is a pointer to the next record.** Not to the last processed record. Mix them up and you will either re-read one message forever, or silently lose a record on startup.
4. **Retention.bytes is your friend.** Without it, one misbehaving producer with oversized messages will fill a broker's disk overnight. On critical Brew topics, both limits are set - time and size. A spare disk ordered the day before a promo would have saved Brew from a couple of incidents.

Next up - [First producer on franz-go](../../../01-05-first-producer/i18n/en/README.md) - we write the first `OrderPlaced` by hand and see how the offset returned by `ProduceSync` lands in exactly the model we covered here.
