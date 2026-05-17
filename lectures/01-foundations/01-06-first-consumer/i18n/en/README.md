# 01-06 - First Consumer

In the previous lecture `order-service` wrote ten `OrderPlaced` events to `brew.orders.v1`. The broker returned `(partition, offset)` for each one, and the sum of latest offsets across partitions added up to 10. Those ten records now sit in the log waiting for a reader. Time to look at them from the other side - the `kitchen-service` side. At Brew this service lives in every coffee shop; its job is straightforward: pull new orders out of `brew.orders.v1` and show baristas what to brew next.

This is the consumer lecture. A program that reads a partition and remembers how far it got. After the broker side (topics, partitions, ISR, retention) and the write side (`ProduceSync`, keys, acks), we shift the angle once more: the code that reads. The goal is modest. Read those same 10 `OrderPlaced` records, see the `(partition, offset)` pairs in the output, and build a working mental model of a consumer group. The deeper parameters - manual commit, processing guarantees, rebalance internals - we'll cover in words and leave for module 03.

## kgo.Client from the read side - the same long-lived object

The same `kgo.NewClient` powers both the producer and the consumer; only the options differ. `kitchen-service` creates the client once at process start, keeps it until shutdown, and reads through it. Broker connection pool, metadata cache, background goroutines - all the same machinery that `order-service` used in 01-05. The `*sql.DB` analogy still holds: one pool per process, no "open-and-close per operation".

franz-go offers two consumption modes. The first is a direct consumer without a group. You enumerate topics and partitions yourself, track offsets yourself, and store them externally. Useful for admin utilities, for backups, and when an external database holds the position (e.g., in Kafka Streams-like pipelines). In this course direct mode shows up in tools like `inspect` and `load-and-watch` from 01-02 and 01-04; for business services it is a rare guest. The full treatment is in [Offset commits](../../../../03-consumer/03-02-offset-commits/i18n/en/README.md).

The second mode is a consumer group. This is what 99% of production code uses, and what `kitchen-service` turns on at Brew. The group handles:

- partition distribution among members (one topic `brew.orders.v1` with 3 partitions + 3 instances of `kitchen-service` = one partition each);
- storing committed offsets in `__consumer_offsets` (we covered this topic in [Offsets and retention](../../../01-04-offsets-and-retention/i18n/en/README.md));
- rebalances - when a member joins or leaves, partitions are redistributed automatically;
- coordination through a group coordinator on the broker (one broker in the cluster is elected coordinator for a specific group).

A backend analogy. A consumer group is similar to a worker pool over a task queue, except the source of tasks is the Kafka log, and the group itself remembers each worker's position. PostgreSQL offers something close with `LISTEN/NOTIFY` plus advisory locks, but coordination there sits on top of tables; in Kafka it lives inside the broker and is available through one client option.

Enable it with one line:

```go
cl, _ := kgo.NewClient(
    kgo.SeedBrokers(...),
    kgo.ConsumerGroup("brew.kitchen-service"),
    kgo.ConsumeTopics("brew.orders.v1"),
)
```

`ConsumerGroup("...")` is the group.id. Kafka uses it to distinguish one logical consumer (possibly assembled from multiple processes) from another. Two processes with the same group.id are **one** group sharing partitions. Two processes with **different** group.ids are two independent groups; each keeps its own committed offset and reads the same messages in parallel without interfering. At Brew, for instance, `kitchen-service` runs in group `brew.kitchen-service`, while `analytics-service` reads the same topic in group `brew.analytics`; each group keeps its own pace, its own lag, and its own position.

`ConsumeTopics("...")` chooses which topics to subscribe to. Multiple topics are fine; the list is fixed, or use `ConsumeRegex` with a pattern (convenient when a service reads all `brew.*.events` without knowing the exact names in advance).

## PollFetches - how messages arrive

The consumer's main work sits in a loop:

```go
for {
    fetches := cl.PollFetches(ctx)
    fetches.EachRecord(func(r *kgo.Record) {
        // process record
    })
}
```

`PollFetches` is a blocking call. It waits until at least something arrives from a broker and returns `kgo.Fetches`. This is a **list of broker responses**, each containing a list of **topics**, each topic containing a list of **partitions**, each partition holding records. The multi-level shape exists because a single `Fetch` can arrive from multiple partitions across multiple topics - more efficient at the network protocol level.

For application code, this hierarchy is rarely traversed by hand. Several wrappers do the job:

- `fetches.EachRecord(fn)` - iterate over every record regardless of which partition it came from.
- `fetches.EachPartition(fn)` - iterate in batches per partition. Useful when you want to collect a batch and process everything from one partition inside a single transaction.
- `fetches.Records()` - flatten everything into a single slice.
- `fetches.Errors()` - per-partition error list.

A single record is `kgo.Record`. It carries `Topic`, `Partition`, `Offset`, `Key`, `Value`, `Headers`, `Timestamp` - exactly what the broker stored on produce. The consumer sees what went into the log: `Key="order-7"`, `Value="OrderPlaced order_id=order-7"`, `Partition=2`, `Offset=2` - the same coordinates `ProduceSync` returned in 01-05.

One important note about `PollFetches`: it is not "block forever". It returns the moment the context expires or the client closes. That is why our root ctx is `runctx.New()` - it cancels on SIGINT, `PollFetches` returns with `context.Canceled` in `Errors()`, we catch it and exit the loop. No separate shutdown channels needed.

## One partition - one consumer per group

The core rule of consumer groups. **Inside a single consumer group, one partition has at most one reader.** This is not a tunable knob, it is built into Kafka: the coordinator assigns each partition of a topic to exactly one member of the group and does not let two members read the same partition in parallel. Otherwise offsets would drift unpredictably and the committed position would lose meaning.

From the rule comes the arithmetic that determines how `kitchen-service` scales. The topic `brew.orders.v1` has three partitions. Possible layouts:

- 1 `kitchen-service` instance - all 3 partitions go to it. No read parallelism, everything runs in a single poller goroutine.
- 2 instances - the typical 2:1 split, one takes two partitions, the other one. Cooperative-sticky assignor tries not to move partitions without need.
- 3 instances - perfect balance, one partition each. This is the maximum useful parallelism for this topic.
- 4 or more - one (or several) instances will sit idle: they are in the group, the coordinator knows about them, but there are no partitions left. They send heartbeats and read nothing.

That gives a rule worth committing to memory on the first pass: **the number of partitions of a topic is the ceiling on parallel reads inside a single group**. Want to speed `kitchen-service` up with a fifth instance? It does nothing until you increase the partition count of `brew.orders.v1`. The other direction is the same - keeping five `kitchen-service` replicas on three partitions just burns RAM on two of them.

The rule does not apply across groups. `analytics-service` reads the same `brew.orders.v1` in the `brew.analytics` group and is "not in anyone's way" even if `kitchen-service` has already claimed all three partitions for itself. Kafka assigns partitions inside a group, not globally across the cluster.

## Auto-commit and two traps

Here is the nuance that half of module 03 is written for.

In franz-go (and in Kafka in general), **auto-commit** is enabled by default. Every `auto.commit.interval.ms` (5 seconds by default) the client takes the current position it has definitively read to and commits it to `__consumer_offsets`. Convenient - you write nothing, it just works.

And it is a trap. Two traps, actually.

First trap: the commit records what has been **read**. Not what has been **processed**. The moment `PollFetches` returns an `OrderPlaced`, the record counts as "read" from auto-commit's perspective. If `kitchen-service` crashes mid-processing of that record (writes to the local cooking DB, the DB drops, the process dies) - the committed offset may already have moved to or past that record. On restart we will **not** re-read it. The order is lost for the kitchen's business logic; it still sits in Kafka, but no barista will ever see it.

Second trap: the interval. There are 5 seconds between two auto-commits. Crash inside that window and on restart you can get **duplicates** - records we already processed but Kafka never heard about will arrive again. This is at-least-once in the bad sense - without an idempotent handler, a duplicate `OrderPlaced` sends a barista to brew the same cappuccino twice.

In short: auto-commit-by-default gives neither at-most-once nor at-least-once - depending on where you crash relative to the 5-second window, you can get loss (first trap) or duplicates (second trap). Not great as a guarantee. [Offset commits](../../../../03-consumer/03-02-offset-commits/i18n/en/README.md) and [Processing guarantees](../../../../03-consumer/03-03-processing-guarantees/i18n/en/README.md) cover this in depth: manual commit, `MarkCommitRecords` + `CommitMarkedOffsets`, DB dedup, and idempotent handlers.

In our teaching code, auto-commit is left enabled **on purpose** - so there is something to discuss and something to fix later. Right now it behaves like this:

1. `PollFetches` returned a batch of `OrderPlaced` records.
2. We printed them (no real processing, the barista is implied).
3. In parallel, a franz-go background goroutine sends `OffsetCommit` with the current position every 5 seconds.
4. On SIGINT we call `cl.Close()`. It stops the auto-commit goroutine and leaves the group cleanly. No final sync-commit happens in this setup: we overrode `OnPartitionsRevoked` (to print `revoked: ...` to stderr), which [per franz-go docs](https://pkg.go.dev/github.com/twmb/franz-go/pkg/kgo#OnPartitionsRevoked) disables the default commit-on-revoke. Up to five seconds of last reads can stay uncommitted - on restart the same `OrderPlaced` records arrive again. This is the second trap in the wild; module 03 fixes it with a manual `CommitUncommittedOffsets`.

That is why the output contains the line "kitchen-service остановлен по сигналу". No promise about a final commit - we honestly admit that the last fraction of position may be lost.

## Shutting down correctly

The pattern:

```go
ctx, cancel := runctx.New() // SIGINT/SIGTERM → ctx.Done()
defer cancel()

cl, _ := kafka.NewClient(...)
defer cl.Close() // leave the group and close connections (final commit comes in module 03)

for {
    fetches := cl.PollFetches(ctx)
    if fetches.IsClientClosed() {
        return
    }
    if errs := fetches.Errors(); len(errs) > 0 {
        for _, e := range errs {
            if errors.Is(e.Err, context.Canceled) { return nil }
            return fmt.Errorf("fetch %s/%d: %w", e.Topic, e.Partition, e.Err)
        }
    }
    // process
}
```

Three details matter in this template. `defer cl.Close()` is mandatory; without it the client does not leave the group cleanly (the coordinator only learns about the death from `session.timeout.ms`, and until then partitions are not redistributed). `Close()` itself does **not** perform a final commit when `OnPartitionsRevoked` is overridden - that is stated explicitly in the [franz-go docs](https://pkg.go.dev/github.com/twmb/franz-go/pkg/kgo#Client.Close); up to five seconds of last reads stay uncommitted. The error check is mandatory; without it a context cancellation leads to an infinite loop with silent errors (PollFetches returns an empty fetches.Records() and immediately blocks again). Passing `ctx` straight into `PollFetches` (rather than substituting `context.Background()`) is the channel through which SIGINT reaches the client. In the module 03 lectures we add manual commit; the template stays the same and `cl.CommitUncommittedOffsets(ctx)` shows up before `cl.Close()`.

## What our code does

`cmd/consumer/main.go` does four things:

1. Creates a `kgo.Client` in consumer-group mode `brew.kitchen-service`, subscribed to topic `brew.orders.v1`.
2. On a fresh group (no committed offset yet) resets to earliest via `ConsumeResetOffset(...AtStart())`. Otherwise those 10 `OrderPlaced` records that `order-service` wrote **before** the consumer started would not show up - the franz-go and Kafka default is latest.
3. Loops on `PollFetches` and prints a `member/partition/offset/key/value/broker-ts` table. Shuts down cleanly on SIGINT.
4. Prints `OnPartitionsAssigned` and `OnPartitionsRevoked` callbacks to stderr so it is visible which partitions this process owns. Useful when watching a rebalance (see `make run-2nd` below).

The client option block is five lines:

```go
opts := []kgo.Opt{
    kgo.ConsumerGroup(o.group),
    kgo.ConsumeTopics(o.topic),
    kgo.ClientID(fmt.Sprintf("kitchen-service-%s", o.memberID)),
    kgo.OnPartitionsAssigned(func(_ context.Context, _ *kgo.Client, m map[string][]int32) {
        fmt.Fprintf(os.Stderr, "[member=%s] assigned: %v\n", o.memberID, m)
    }),
    kgo.OnPartitionsRevoked(func(_ context.Context, _ *kgo.Client, m map[string][]int32) {
        fmt.Fprintf(os.Stderr, "[member=%s] revoked:  %v\n", o.memberID, m)
    }),
}
if o.fromStart {
    opts = append(opts, kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
}

cl, err := kafka.NewClient(opts...)
```

`ConsumeResetOffset(...AtStart())` reads "on first group start, begin from earliest". On a second run the committed offset already lives in `__consumer_offsets` and `ResetOffset` has no effect.

The main loop is `PollFetches` plus printing through `EachRecord`. Along the way we check errors: `context.Canceled` is SIGINT, everything else is a real fetch failure:

```go
for {
    fetches := cl.PollFetches(ctx)
    if fetches.IsClientClosed() {
        return nil
    }
    if errs := fetches.Errors(); len(errs) > 0 {
        for _, e := range errs {
            if errors.Is(e.Err, context.Canceled) {
                fmt.Println("kitchen-service остановлен по сигналу.")
                return nil
            }
            return fmt.Errorf("fetch %s/%d: %w", e.Topic, e.Partition, e.Err)
        }
    }

    fetches.EachRecord(func(r *kgo.Record) {
        fmt.Fprintf(tw, "%s\t%d\t%d\t%s\t%s\t%s\n",
            o.memberID, r.Partition, r.Offset,
            string(r.Key), string(r.Value),
            r.Timestamp.Format("15:04:05.000"),
        )
    })
    _ = tw.Flush()
}
```

`defer cl.Close()` cleanly leaves the group and closes connections, but it does **not** perform a final commit - with the custom `OnPartitionsRevoked` the default commit-on-revoke is disabled. Up to five seconds of reads may not reach `__consumer_offsets`, and on restart the same `OrderPlaced` records arrive again. Module 03 fixes this with an explicit `cl.CommitUncommittedOffsets(ctx)` before `Close()`.

Expected output after `make run` on a freshly created group with 10 `OrderPlaced` records across three partitions:

```
kitchen-service запущен: brew-topic="brew.orders.v1" group="brew.kitchen-service" member=1 from-start=true
читаем brew-orders; Ctrl+C - выход.

[member=1] assigned: map[brew.orders.v1:[0 1 2]]

MEMBER  PARTITION  OFFSET  KEY      VALUE                            BROKER-TS
1       0          0       order-0  OrderPlaced order_id=order-0     16:55:01.234
1       0          1       order-3  OrderPlaced order_id=order-3     16:55:01.253
1       0          2       order-6  OrderPlaced order_id=order-6     16:55:01.271
1       0          3       order-9  OrderPlaced order_id=order-9     16:55:01.289
1       1          0       order-2  OrderPlaced order_id=order-2     16:55:01.247
1       1          1       order-5  OrderPlaced order_id=order-5     16:55:01.265
1       1          2       order-8  OrderPlaced order_id=order-8     16:55:01.283
1       2          0       order-1  OrderPlaced order_id=order-1     16:55:01.241
1       2          1       order-4  OrderPlaced order_id=order-4     16:55:01.259
1       2          2       order-7  OrderPlaced order_id=order-7     16:55:01.277
```

A few notes on this output. (The Russian phrases inside log messages are intentional - the Go program prints them as-is; the rest of this README explains them.)

Records inside a single partition follow offset order: 0, 1, 2, 3. This is a **Kafka guarantee** and it always holds. Between partitions, order is completely undefined - some from 0, some from 1, some from 2 - the client reads them in parallel, and the assembly in the combined stream depends on `PollFetches` timing. Run the consumer a second time and the specific row order may shift. Per-partition ordering is the only ordering guarantee Kafka provides. There is no global order across a topic.

The `(order-id → partition)` mapping here matches exactly what `ProduceSync` returned back in 01-05: `order-0/3/6/9` in partition 0, `order-2/5/8` in partition 1, `order-1/4/7` in partition 2. That is the deterministic partitioner at work: `hash(order_id) mod 3` yields the same partition for the same key. The kitchen sees exactly the grouping the order service produced.

Member is "1" everywhere because we have one process. All three partitions went to it - the `assigned` map shows it directly.

Run `make run` again with the same group.id and the table will be empty. The committed offset is already at 4/3/3 (per partition), there are no new messages, and the consumer just sits in `PollFetches` waiting. This is "committed offset working" as intended; no bug to chase. To re-read everything from scratch use `make run-fresh` - it appends a random suffix to the group.id and gives you a fresh group with empty offsets.

## Two instances in one group - watching a rebalance

In the first terminal:

```sh
make run
```

You see that member=1 received all three partitions (`assigned: map[...:[0 1 2]]`). It finished reading and is waiting.

In the second terminal:

```sh
make run-2nd
```

A **rebalance** happens - a short pause during which the group coordinator rebuilds the partition layout across members. The first process prints `[member=1] revoked: ...` to stderr (some partitions leave) and immediately `[member=1] assigned: ...` with the reduced list. The second process prints `[member=2] assigned: ...` with the partitions handed to it. The standard cooperative-sticky distribution (the franz-go default in recent versions) is two partitions to one member and one to the other; which exact partitions depends on the implementation.

That is the whole rebalance we want to see in 01-06. The four assignors (range, round-robin, sticky, cooperative-sticky), the difference between eager and incremental cooperative, the downtime under different strategies, and tuning `session.timeout.ms` / `heartbeat.interval.ms` - all of that lives in [Groups and rebalances](../../../../03-consumer/03-01-groups-and-rebalance/i18n/en/README.md). Here we just record the fact: the coordinator automatically redistributes load across members, and from the application side this is visible through two callbacks.

Close the second process (Ctrl+C). The first one gets `revoked` plus `assigned` with all three partitions again. That is expected: the partitions returned to the only remaining member.

## Running

The sandbox must be up (`docker compose up -d` from the repo root). Before starting the consumer, the producer in [First producer](../../../01-05-first-producer/i18n/en/README.md) must have already run and written at least something to `brew.orders.v1`.

```sh
# in 01-foundations/01-05-first-producer
make run

# in 01-foundations/01-06-first-consumer
make run
```

To inspect the group's committed offset from Kafka's side:

```sh
make group-describe
```

This calls `kafka-consumer-groups.sh --describe`. It prints per-partition committed offset, lag (latest minus committed), member-id, and client host. After all 10 `OrderPlaced` records are read, lag=0 on every partition. Kill the process mid-processing and immediately run `group-describe` - you will see lag, because auto-commit had no time to fire.

To reset the group's committed offsets (e.g., to re-read those 10 records from the beginning):

```sh
make group-delete
```

Deleting the group does not touch the log data - it erases the entry in `__consumer_offsets`. On the next consumer start the group counts as "new" and reads from earliest (with `from-start=true`). In teaching lectures this is a handy shortcut; in production reach for this reset only when you understand exactly what you are zeroing out.

## What to take away

The consumer mental model that the rest of module 03 builds on:

1. **A group is a logical consumer**. group.id is its name. The same group.id across multiple processes = partition sharing. Different group.ids = independent readers of the same topic.
2. **Inside a group, one partition has one reader**. Read parallelism is capped at the partition count of the topic. Extra instances sit idle.
3. **PollFetches returns Fetches → Topics → Partitions → Records**. At the application level you almost always work via `EachRecord` or `EachPartition`.
4. **Auto-commit lies by default**. It commits the read position, which has no connection to actual processing in code. Module 03 fixes this.
5. **Shutdown through `cl.Close()` plus ctx from runctx**. `cl.Close()` cleanly leaves the group and closes connections; with `OnPartitionsRevoked` overridden it performs no final commit - up to five seconds of last reads can be lost. Fix it with an explicit `CommitUncommittedOffsets` in module 03. Without `ctx` in `PollFetches` you cannot exit the loop on SIGINT.

Next - module 02. Back to the producer side, digging into what was "default": [Keys and partitioning](../../../../02-producer/02-01-keys-and-partitioning/i18n/en/README.md), [Acks and durability](../../../../02-producer/02-02-acks-and-durability/i18n/en/README.md), [Idempotent producer](../../../../02-producer/02-03-idempotent-producer/i18n/en/README.md), [Batching and throughput](../../../../02-producer/02-04-batching-and-throughput/i18n/en/README.md), [Errors, retries and headers](../../../../02-producer/02-05-errors-retries-headers/i18n/en/README.md). Module 03 sits next to it - that is the one that turns this bare consumer into a production-grade one.
