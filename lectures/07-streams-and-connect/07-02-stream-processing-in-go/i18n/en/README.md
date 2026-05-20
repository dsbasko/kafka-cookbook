# 07-02 — Stream Processing in Go (franz-go + Pebble)

In [Stream processing: concepts](../../../07-01-stream-processing-concepts/i18n/en/README.md) we talked about ideas: event-time, windows, watermark, KStream/KTable. Time to get hands-on. Stream processing needs state — counters live somewhere between records. And that state must survive restarts, otherwise any analytics falls apart on the first `kill -9`.

The problem: there is no native Kafka Streams for Go. In Java — there is a library, straight from Confluent. In Go — nothing. The closest things (Watermill, for example) are about message routing, not stateful streams. So we build by hand: a Kafka client + a local embedded KV store + a changelog topic for durability.

In our case that is `franz-go` + `Pebble` + a compacted topic `word-count-changelog`. The result is a simplified copy of the Kafka Streams model: state lives on disk, updates are simultaneously copied to Kafka, if the disk is lost state is restored from the changelog from the beginning. No watermarks, no time windows, no complex topology — just enough to see three key mechanics on working code.

## Why we need state

Stateless processing: a record arrives, you do something with it, write it somewhere — forget it. `map`, `filter`, `flatMap`. Restart the process, nothing is lost.

Stateful is different. We compute `count`, `sum`, `top-N`, `unique users per hour`. The second record depends on what we saw in the first. Memory has to be stored somewhere. Options at a glance.

1. **In-memory only.** Just a `map[string]int` in a goroutine. Fast, zero dependencies, after `kill -9` everything resets to zero. Suitable strictly for demo scripts.
2. **External DB.** Postgres, Redis, any KV. Overhead on every increment — a network round-trip. At 50k msg/sec on the stream that already hurts.
3. **Embedded store + changelog.** Write to a local LSM (Pebble/RocksDB), simultaneously send a copy of changes to a compacted Kafka topic. Performance like a local DB (millisecond network round-trips disappear), durability at Kafka level. This is exactly "what Kafka Streams does".

We build the third option. Pebble here — because it is pure Go, no CGo (RocksDB via CGo is its own build-time pain). Pebble is CockroachDB's own LSM engine, the foundation of their storage layer — more than enough for our sandbox.

## Pebble in brief

LSM tree, embedded, key-value. The API is very simple: `Set`, `Get`, `Delete`, iteration. Writes to disk (by default to the specified directory), flushes the memtable to disk periodically. By design — a relative of RocksDB.

What matters to us from the API:

- `pebble.Open(dir, opts)` — open or create a DB on disk.
- `db.Set(key, value, sync)` — write.
- `db.Get(key)` → `(value, closer, err)` — read (`closer.Close()` is required after use).
- `db.NewIter(opts)` → iterator over the full range.
- `db.Flush()` — force the memtable to disk.

The `pebble.Sync` vs `pebble.NoSync` option controls fsync. In our code we collect all of a polling round's `Set`s into a `*pebble.Batch` without sync, then commit the batch with `pebble.Sync` — one fsync per batch instead of one per record. In production, combined with a changelog, teams often use `NoSync` even on batch commit plus a periodic `Flush`: durability is provided by Kafka, the local disk is only needed for speed.

## Architecture of our word-count

Three topics and one local directory.

- `lecture-07-02-text-events` — input. Any strings; we split them into words and count.
- `lecture-07-02-word-count-changelog` — compacted topic. For every counter update we write `(word, current_count)`. Compaction in Kafka guarantees that only the latest value is retained per key, so size does not grow linearly.
- `lecture-07-02-word-counts` — output. Every `flush` seconds (5 by default) we emit the current top-N as a snapshot.

And the `./state/` directory — Pebble stores its LSM there. Delete the directory — lose local state. Run `cmd/changelog-restorer` — restore from the changelog.

One-way flow, no loops:

```
text-events ──> [word-count] ──┬──> word-count-changelog (compact)
                               ├──> word-counts (top-N snapshot)
                               └──> ./state/ (Pebble)
```

And the reverse direction, only for state restart:

```
word-count-changelog ──> [changelog-restorer] ──> ./state/
```

## The word-count loop

The most important thing — the order of three durable writes in one polling round: the changelog produce, the Pebble batch, and the offset commit. Swap them and you either lose increments on a crash, or get duplicates on restart.

Correct order: **changelog → Pebble → offset commit**. Each step has a reason.

First, accumulate increments in an in-memory overlay (no Pebble writes yet) and build the matching changelog records:

```go
overlay := make(map[string]uint64)
var produces []*kgo.Record

fetches.EachRecord(func(rec *kgo.Record) {
    words := tokenize(string(rec.Value))
    for _, word := range words {
        cur, ok := overlay[word]
        if !ok {
            cur, _ = readUint64(w.store, []byte(word))
        }
        cur++
        overlay[word] = cur
        produces = append(produces, &kgo.Record{
            Topic: w.changelogTopic,
            Key:   []byte(word),
            Value: encodeUint64(cur),
        })
    }
})
```

The overlay matters: within one batch the same word can appear several times, and we need every produce to carry the running counter, not the stale Pebble value.

Then publish the changelog in one `ProduceSync`, persist the overlay to Pebble in one batch, and only after that commit the offsets:

```go
if err := w.client.ProduceSync(rpcCtx, produces...).FirstErr(); err != nil {
    return fmt.Errorf("changelog produce: %w", err)
}

batch := w.store.NewBatch()
for word, count := range overlay {
    _ = batch.Set([]byte(word), encodeUint64(count), nil)
}
if err := batch.Commit(pebble.Sync); err != nil {
    return fmt.Errorf("pebble batch commit: %w", err)
}

if err := w.client.CommitUncommittedOffsets(commitCtx); err != nil {
    return fmt.Errorf("commit offsets: %w", err)
}
```

Why this order. If we committed offsets first and then wrote the changelog — and were killed in that gap — after restart word-count would consider that batch processed, but the changelog has no record of it. Then if we lose Pebble and try to restore — counters come back lower. The loss is silent: nobody alerts you on a counter that quietly underreports.

Why changelog before Pebble. If a crash hits between them, the changelog has the new values and Pebble has the old ones. On restart, the offset has not been committed, so reprocessing the same input batch produces the same new values, the changelog gets duplicate writes for the same keys (compaction collapses them later), and Pebble catches up. End state is consistent. If we had written Pebble first and crashed before the changelog, restorer from changelog would yield older values than Pebble — and Pebble itself would re-increment on replay because the overlay starts from whatever Pebble already has, inflating the counter by one batch.

The whole pipeline still gives **at-least-once**, not exactly-once. A crash after Pebble commit but before offset commit will reprocess the batch on restart — Pebble re-increments because the overlay sees the already-updated values, and the changelog gets the inflated counts. To eliminate that, you need transactional producer semantics around the whole block: `kgo.NewGroupTransactSession` plus `Begin/End(TryCommit)` — see [Consume-process-produce](../../../../04-reliability/04-02-consume-process-produce/i18n/en/README.md). For our word-count, an inflation of one or two on a rare crash is an acceptable trade.

## Output: top-N snapshot

Every `flush` seconds a background goroutine walks Pebble and emits the current top-N. Print to stdout — for human eyes; write to `word-counts` — so a downstream process can consume it.

```go
func (w *wordCounter) flushTopN(ctx context.Context) error {
    rows, err := w.collectAll()
    // ... sort rows by count descending ...
    if len(rows) > w.topN {
        rows = rows[:w.topN]
    }
    // print to stdout
    // ProduceSync top-N to outputTopic
}
```

Writing to outputTopic here is a Produce without a transaction, without a combined offset commit. The snapshot is published as-is — if it is lost, the next one will arrive in 5 seconds. This is normal semantics for metric snapshots. If downstream cannot handle duplicates (we may have sent top-N and then triggered a new flush before the previous one was acknowledged) — add an idempotency key with a timestamp and discard stale entries on the consumer.

## Compacted changelog: what and why

`word-count-changelog` is a topic with `cleanup.policy=compact`. What that means. A regular topic retains all records until retention expires. A compacted topic guarantees at least the latest record for every key. Older versions of the same key are eventually removed by compaction (a background process in the broker).

Why we need this. Word-count has seen the word `kafka` a thousand times — and written to the changelog a thousand times. After compaction, only the last one or two records remain in the physical log out of that thousand (the exact number depends on timing and `min.cleanable.dirty.ratio`). The changelog size grows **linearly with the number of unique words**, not with the number of increments.

This is how you keep a "materialized view" of state in Kafka. By analogy with KTable — we have a compacted topic plus a local store, and they agree on the latest value per key.

The topic is created with specific configs:

```sh
docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka-1:9092 --create \
  --topic lecture-07-02-word-count-changelog \
  --config cleanup.policy=compact \
  --config segment.ms=60000 \
  --config min.cleanable.dirty.ratio=0.01
```

`segment.ms=60000` plus `min.cleanable.dirty.ratio=0.01` — parameters to make compaction happen frequently on small volumes. In production they are typically much larger: compaction is not cheap.

## Restore: from scratch via the changelog

The scenario: disk died, Pebble is gone. Run `cmd/changelog-restorer`. It reads `word-count-changelog` from the beginning, puts the pairs into Pebble, and stops at the high-watermark of each partition.

First, determine how far to read:

```go
end, err := admin.ListEndOffsets(rpcCtx, topic)
// ...
end.Each(func(o kadm.ListedOffset) {
    if o.Offset > 0 {
        out[o.Partition] = o.Offset
    }
})
```

Then read without a consumer group (we do not need a committed offset, we need a snapshot of the entire compacted log), track the maximum offset manually, and compare:

```go
fetches.EachRecord(func(rec *kgo.Record) {
    if rec.Offset+1 > maxOffsets[rec.Partition] {
        maxOffsets[rec.Partition] = rec.Offset + 1
    }
    if len(rec.Value) == 0 {
        // tombstone — key no longer exists
        _ = store.Delete(rec.Key, pebble.NoSync)
        return
    }
    // ... pebble.Set(key, value)
})

if reachedEnd(maxOffsets, endOffsets) {
    break
}
```

A tombstone is a record with `value=nil` in the compacted log. It means "delete this key, it no longer exists for me." In our word-count we never write tombstones (a counter can only increase), but the restorer handles them correctly regardless — in case of manual edits or future model changes.

After all partitions are read up to the end offset, call `Flush()` — Pebble flushes accumulated data to disk. After that you can start word-count with the standard `make run` — it will find state in place and continue from the point at which the changelog was at restore time.

One detail: between the restore moment and the word-count start new records may have already arrived in the changelog (if something else is writing in parallel). That is fine. Word-count at startup picks up its last committed offset from the consumer group, starts reading `text-events` from that point — and also catches up with the changelog for any new updates. Self-consistency is preserved.

## Running

The sandbox must be running (`docker compose up -d` from the root).

Create topics once:

```sh
make topic-create-all
```

In one terminal — feed input:

```sh
make seed-text
```

A loop of a dozen phrases goes into `text-events` once per second. You can also push custom text via `kafka-console-producer.sh` manually — any format works, we split by words.

In another terminal — word-count:

```sh
make run
```

Every 5 seconds it prints the top-10 words and the current number of processed events. Watch the counters grow. Kill it (`Ctrl+C`), start again — counters continue from the same value because Pebble remained on disk.

To see restore — delete the state directory and restore from the changelog:

```sh
rm -rf ./state
make restore
make run
```

After `make restore` the `./state/` directory is populated again, and word-count finds its counters at startup.

Clean up after the lesson:

```sh
make topic-delete-all
rm -rf ./state
```

## Where to go next

What we built is a stateful processing model at minimum viable complexity. Many things are missing, and it is useful to know that each of them is absent here.

- **Time windows.** Word-count does not need event-time — it counts "everything over all time." Real streams almost always want windows (see [Stream processing: concepts](../../../07-01-stream-processing-concepts/i18n/en/README.md)). On top of our scheme this looks like: the Pebble key is not `word` but `<word>:<window-start>`, plus a separate process closes windows by watermark and deletes old keys.
- **Joins.** Stream-stream and stream-table joins are a large separate topic. Briefly: both sides need to be repartitioned by the join key, then a local cache (KTable-side) must be held in Pebble.
- **Backpressure.** In our code `flushLoop` runs independently of processing. If the incoming message rate greatly exceeds the flush rate to Kafka — the buffer grows. For production: `cl.PauseFetchPartitions` on outputTopic overload (pattern from [Delivery to external systems](../../../../04-reliability/04-05-external-delivery/i18n/en/README.md)).
- **Exactly-once.** To eliminate duplicates on crashes, you need producer transactions wrapping the "changelog produce + Pebble update + offset commit" block. In franz-go v1.21.0 the public entry point is `kgo.NewGroupTransactSession` — see [Consume-process-produce](../../../../04-reliability/04-02-consume-process-produce/i18n/en/README.md).
- **State sharding.** With a large number of input partitions, a single node with a single Pebble is a bottleneck. Kafka Streams splits state by key partition; each node holds its own shard. Here — one process, one state. Scales via consumer group: each member takes its partitions and holds its own Pebble; the changelog is still shared.
- **Metrics and observability.** Input topic lag, state size, changelog-publish lag, top-N flush latency. That is [Monitoring and metrics](../../../../08-operations/08-01-monitoring-and-metrics/i18n/en/README.md).

Everything listed is built on the same foundation. Pebble + changelog + the correct order of "changelog → state → commit." The surrounding machinery changes, not the essence.

## Key takeaways

- **Stateful streams without a state store are an illusion.** In-memory works until it crashes; you need either external storage (slow) or embedded + changelog (faster and durable).
- **Pebble + compacted changelog topic — a working scheme for Go.** Not Kafka Streams, but sufficient for most practical tasks.
- **Operation order matters more than it seems.** Changelog → state → commit. Any reordering produces bad semantics (lost or inconsistent counter), and you will notice that in production long after the first incident.
- **A compacted topic is a materialized snapshot, not a log.** All reasoning about retention does not apply to it; size is bounded by the number of unique keys, not the number of records.

In [Kafka Connect](../../../07-03-kafka-connect/i18n/en/README.md) we go in a different direction — Kafka Connect and declarative ETL without custom code. For cases where Pebble + Go is overkill.
