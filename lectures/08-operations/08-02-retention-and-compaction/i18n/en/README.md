# 08-02 — Retention & Compaction

Kafka stores messages. Not forever. Every topic runs a cleanup policy that decides what to keep and what to discard. There are two policies: `delete` (by time or by size) and `compact` (by key). Simple in theory. In practice it gets interesting — segments, dirty ratio, tombstones, and why `retention.ms=7d` does not mean a message sits for exactly seven days.

This lecture covers how the log actually changes on disk under both policies. Run two demos and watch as an operator.

## What's inside

- `cmd/compaction-demo/main.go` — writes 100,000 updates across 1,000 keys to the topic `lecture-08-02-users-state` with `cleanup.policy=compact`. After writing, waits for the compactor and captures sizes via `DescribeAllLogDirs`. Then writes tombstones (`Value=nil`) for 100 keys and finally counts unique keys in the log.
- `cmd/retention-demo/main.go` — a background producer streams into the topic `lecture-08-02-events` with `cleanup.policy=delete` (plus `retention.ms=60s` and `segment.ms=10s`). Every few seconds it prints `earliest`/`latest` and disk size. Watch old segments disappear and earliest jump forward.
- `Makefile` — entry points for each demo plus `topic-describe` via `kafka-configs.sh` and `du-volume` (`du` inside `kafka-1` to check the actual size of the topic directory).

## Segment — the unit of everything

Before talking about retention and compaction, one thing needs to be clear. Kafka does not operate on "messages" when deleting or compacting. It operates on **segments** — the files a partition is sliced into.

A partition on disk is a directory `lecture-08-02-events-0/` (topic name plus partition number). Inside are file pairs like `00000000000000000000.log` and `00000000000000000000.index`. Each such pair is a segment. One segment is active (writes land there now), the rest are closed. The active segment closes and becomes "closed" under two conditions:

- it has reached `segment.bytes` in size (default 1 GiB);
- `segment.ms` has passed since the segment was created (default one week).

Here is the key point. **Retention and compaction touch only closed segments.** The active segment is untouchable. So if you have `retention.ms=1h` but `segment.ms=7d` and traffic is low — the active segment can live for a week, and the hourly retention will not fire. Messages an hour old will sit in the active segment and technically violate "their" retention. That's how it works.

The `retention-demo` sets `segment.ms=10s` for this reason — fast visible effects.

## Cleanup.policy=delete

The most common policy. Delete by age or by size.

- `retention.ms` — delete a segment if it is closed and its last record is older than N milliseconds.
- `retention.bytes` — keep no more than N bytes per partition; discard the rest.

Both can be combined. Whichever condition fires first wins.

The broker runs a dedicated thread (`log-retention-thread`) that walks all partitions and cuts segments that match the condition. The interval is `log.retention.check.interval.ms`, default 5 minutes. That gap is why "seven days" is a guideline, not a precise number. The sandbox uses the default interval; `retention-demo` sets `retention.ms=60s` and you watch minutes, not seconds.

What `retention-demo` shows. A background producer runs at `rate` messages per second, each with a 256-byte payload. Every `poll` seconds the client queries offsets and size. The background write loop:

```go
t := time.NewTicker(interval)
defer t.Stop()
payload := make([]byte, 256)
for i := range payload {
    payload[i] = byte('a' + (i % 26))
}
var seq int64
for {
    select {
    case <-ctx.Done():
        cl.Flush(context.Background())
        return
    case <-t.C:
        seq++
        rec := &kgo.Record{
            Topic: topic,
            Key:   []byte(fmt.Sprintf("k-%d", seq%16)),
            Value: payload,
        }
        cl.Produce(ctx, rec, nil)
    }
}
```

Nothing special here — just growing the log. The important part is the next block: reading earliest/latest and disk size:

```go
starts, err := admin.ListStartOffsets(rpcCtx, topic)
ends,   err := admin.ListEndOffsets(rpcCtx, topic)
size,   err := topicSize(rpcCtx, admin, topic)
// ...
fmt.Fprintf(tw, "%d\t%d\t%d\t%d\n", s.Partition, s.Offset, latest, latest-s.Offset)
fmt.Printf("size on disk (single replica): %d bytes\n", size)
```

`ListStartOffsets` returns the offset of the first still-live record in each partition. Before retention kicks in, that's 0. When the broker cuts the first closed segment, earliest jumps immediately to the offset at the start of the next segment. It always jumps a full segment at a time — earliest does not move record by record.

`topicSize` is slightly trickier. A partition with `rf=3` has three copies across three log dirs. Summing all three means "cluster size," which is confusing. So filter to the first seen replica per partition:

```go
all, err := admin.DescribeAllLogDirs(ctx, nil)
seen := make(map[int32]bool)
var size int64
all.Each(func(d kadm.DescribedLogDir) {
    d.Topics.Each(func(p kadm.DescribedLogDirPartition) {
        if p.Topic != topic {
            return
        }
        if seen[p.Partition] {
            return
        }
        seen[p.Partition] = true
        size += p.Size
    })
})
```

What to watch in the output. After one to two minutes (depending on how often `log-retention-thread` runs), earliest starts jumping and total size starts dropping. Leave it running long enough and total size stabilizes around `retention.ms × rate × payload_size`. That's the limit.

## Cleanup.policy=compact

A different model. Time-based retention takes a back seat — the log keeps "one current record per key." A state snapshot, not an event journal. Useful for topics with long-lived per-key state: `users`, `accounts`, `prices`, `inventory`. Each new record with the same key overwrites the previous one; old versions are not needed.

The compactor works differently from retention. It:

1. Splits closed segments into "dirty" (contain stale key versions) and "clean" (processed in the last compaction).
2. Computes the size ratio: `dirty_size / total_size` — that is the **dirty ratio**.
3. If dirty ratio exceeds `min.cleanable.dirty.ratio` (default 0.5) — takes dirty segments for processing. Leaves the rest alone.
4. Compacts: for each key keeps only the most recent value.
5. Rewrites segments on disk (new names, old ones deleted).

There are more control knobs than with delete:

- `min.cleanable.dirty.ratio` — trigger threshold (0.0–1.0).
- `min.compaction.lag.ms` — lower bound: the compactor will not touch a record younger than N ms (useful to let consumers catch up to the live trail).
- `max.compaction.lag.ms` — upper bound: even if dirty ratio is low, after N ms the record becomes eligible.
- `delete.retention.ms` — how long tombstones survive after the compactor has "seen" them.

In the demo these parameters are set to the minimum so the effect is visible within 30 seconds:

```
cleanup.policy             = compact
segment.ms                 = 5000
min.cleanable.dirty.ratio  = 0.001
min.compaction.lag.ms      = 0
max.compaction.lag.ms      = 10000
delete.retention.ms        = 5000
```

Do not do this in production — the compactor will spin continuously and eat IO. For a lecture, it's perfect.

What `compaction-demo` shows. The program runs five stages and prints earliest/latest/size after each. The write stage itself is a plain fire-and-forget producer, nothing tricky:

```go
for i := 0; i < updates; i++ {
    k := i % keys
    rec := &kgo.Record{
        Topic: topic,
        Key:   []byte(fmt.Sprintf("user-%05d", k)),
        Value: []byte(fmt.Sprintf(`{"v":%d,"ts":%d}`, i, time.Now().UnixMilli())),
    }
    cl.Produce(rpcCtx, rec, nil)
}
if err := cl.Flush(rpcCtx); err != nil {
    return fmt.Errorf("flush: %w", err)
}
```

100,000 records across 1,000 keys — exactly 100 versions per key. After writing and a short wait, the compactor should leave exactly one per key — meaning the log shrinks by roughly 100×.

Then comes a pause. Not an idle one: to get the active segment to close on `segment.ms=5s`, you need to write to it occasionally. Otherwise, depending on internal timers, a new segment may not be created, and the compactor will keep seeing the same closed segment. There is a dedicated helper for this:

```go
func waitWithHeartbeats(ctx context.Context, cl *kgo.Client, topic string, wait time.Duration) error {
    deadline := time.Now().Add(wait)
    tick := time.NewTicker(2 * time.Second)
    defer tick.Stop()
    hb := 0
    for {
        select {
        case <-ctx.Done():
            return nil
        case <-tick.C:
            if time.Now().After(deadline) {
                return nil
            }
            hb++
            rec := &kgo.Record{
                Topic: topic,
                Key:   []byte(fmt.Sprintf("__heartbeat-%d", hb)),
                Value: []byte("hb"),
            }
            // ...
            cl.ProduceSync(rpcCtx, rec).FirstErr()
        }
    }
}
```

These heartbeat records count toward `latest` but not toward user keys. In `STEP 5` they are filtered out by the `__` prefix.

What you'll see in the log. From a run with `keys=50, updates=2000, tombstone-keys=10`:

```
[after write]                      latest=2000  size=23.3 KB
[after first compaction]           latest=2009  size=1.4 KB    ← compactor ran
[after tombstones]                 latest=2028  size=2.7 KB    ← tombstones added
unique user-keys in log: 40                                    ← 50 minus 10 deleted
```

Size dropped sixteenfold. With 1,000 keys and 100 versions per key the ratio will be even more dramatic.

## Tombstone

To delete a key from a compact topic, write a record with `Value=nil` and the same key. That's a tombstone. The compactor will "see" it on the next compaction and do what it always does for old versions of that key — keep only the most recent. But the most recent is nil. Here `delete.retention.ms` kicks in: the tombstone must remain in the log for that duration so all consumers can read it and process the "delete." Only then is the tombstone itself discarded.

In `compaction-demo` we write 100 tombstones:

```go
for i := 0; i < n; i++ {
    rec := &kgo.Record{
        Topic: topic,
        Key:   []byte(fmt.Sprintf("user-%05d", i)),
        Value: nil,
    }
    if err := cl.ProduceSync(rpcCtx, rec).FirstErr(); err != nil {
        return fmt.Errorf("tombstone %d: %w", i, err)
    }
}
```

Then read the topic from earliest and count:

```go
fetches.EachRecord(func(r *kgo.Record) {
    read++
    k := string(r.Key)
    if len(k) > 2 && k[:2] == "__" {
        heartbeats++
        return
    }
    if r.Value == nil {
        tombstones++
        delete(keys, k)
        return
    }
    keys[k] = struct{}{}
})
```

You'll see: tombstones are still in the log (we read them before `delete.retention.ms=5s` expires), but when counting unique keys we apply them — `delete(keys, k)` — and the result matches expectations.

## Combined cleanup: compact + delete

A bonus policy: `cleanup.policy=compact,delete`. Valid. And sometimes necessary.

Scenario: a compact topic with a TTL. For example, profiles of users deleted long ago should eventually disappear even if no tombstone was ever written. Set `cleanup.policy=compact,delete` and `retention.ms=90d` (plus a moderate `min.cleanable.dirty.ratio`). The compactor compacts by key; retention discards everything older than three months — even current versions. This is a working combination for long-tail scenarios, but enable it deliberately: you can accidentally lose records that a downstream consumer depends on.

## Operations cheat-sheet

Where to look when something is wrong.

- **Log does not shrink on a compact topic.** Check `min.cleanable.dirty.ratio`: at 0.5 (default), the compactor waits until half the log is stale. On slowly changing keys that can take months. Lower the ratio or wait.
- **Active segment grows without bound.** Check `segment.ms` and `segment.bytes`. If both are too large, the segment never closes and retention/compaction never apply to it.
- **Earliest is stuck even though retention.ms expired long ago.** Wait for `log.retention.check.interval.ms` — that's the retention thread interval. Default 5 minutes. On the sandbox this sometimes looks like "a message sitting three extra minutes past its retention."
- **Tombstone does not delete the data.** Deletion only happens during compaction. If dirty ratio is low, there is no compaction and the tombstone sits as a regular record.
- **Disk usage is tens of times higher than expected.** Remember `rf=3`. `DescribeAllLogDirs` shows all replicas, not just one.

## Running

```sh
make help
make run-compaction          # ~30s after write + waiting for the compactor, default 30s
make run-retention           # background run, Ctrl+C to exit; earliest starts jumping after 1–2 minutes
make du-volume               # topic directory size on disk in kafka-1
make topic-describe          # kafka-configs.sh for both topics
make topic-delete-all        # delete both topics
```

Parameters via environment variables:

```sh
KEYS=2000 UPDATES=200000 WAIT=60s make run-compaction
RETENTION=120s SEGMENT=20s RATE=20 make run-retention
```
