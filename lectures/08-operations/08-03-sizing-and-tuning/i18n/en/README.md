# 08-03 — Sizing & Tuning

The [Retention and compaction](../../../08-02-retention-and-compaction/i18n/en/README.md) lesson covered how Kafka splits a log into segments and what retention/compaction does with them afterward. This one is a level up. The focus shifts from "how retention works" to "what retention I set on this topic and why exactly that". A couple of knobs on a topic decide what happens to your disk and how the cluster behaves during an incident. You want to understand what the operator is choosing.

Start with partitions, then disk, then walk through the top configs, and finally pull it all together into three profiles that the `topic-profiles` program creates directly on the sandbox.

## How many partitions

This is the first question when creating a new topic. The number is usually picked by gut feel, and you live with it for a long time (you can only increase it, and that reshuffles the hash distribution across keys — painful).

The base model is simple. A partition is the unit of parallelism. The number of partitions is the number of parallel consumers that can work in a single consumer group. One consumer takes one or more partitions; two groups on top of one topic work independently. The parallelism ceiling for a group equals the partition count. Below it — consumers idle.

The rough estimate goes like this. Take peak load in messages per second. Estimate how many a single consumer worker handles on your actual logic (not raw poll — real throughput with DB/HTTP calls/serialization). Divide. That's your minimum. Add 30–50% on top — room to grow and slack for rebalancing friction.

Example. Payments peak at 8000 messages/sec; one worker handles 800 (validation, DB write, idempotency check). 8000 / 800 = 10 — that's the lower bound. With headroom — 12–16 partitions. Six is definitely not enough; thirty is overkill.

The other side — partition cost. Every partition on every broker means files on disk, open file descriptors, indexes, metadata. A million and a half partitions won't drop a broker immediately, but the controller starts lagging on rebalance and startup, ISR churns, JVM heap (if you're in ZooKeeper era) bloats, and fetch requests between brokers become a chronic bottleneck. On a sandbox with three nodes that's far away, but "let's add 1000 partitions just in case" is still a bad idea even there.

One more thing. Partitions with replication factor=3 mean triple copies of everything on different brokers. If a topic has 12 partitions, the cluster physically holds 36 partition replicas. That's not nothing.

## Disk

Disk is easy to calculate if you forget nothing. The formula is bare arithmetic:

```
disk = throughput × retention × replication_factor / compression_ratio
```

Throughput — in bytes per second (after batching, before compression). Retention — in seconds. Replication factor — usually 3. Compression ratio — how much the batch compresses. On JSON payloads zstd gives ~4×, lz4 ~2.5×; on binary Protobuf with few strings compression is weaker.

Example estimate. Events at 600 bytes each, 5000/sec, retention 7 days, RF=3, lz4 compression (~2.5×):

```
3000000 bytes/s × 604800 s × 3 / 2.5 ≈ 2.18 TB
```

That's total across the cluster, not per node. To get per-node — divide by the node count and verify you have headroom for reassignments (when a broker goes down and its partitions temporarily live on two brokers instead of three — copies need to spread out, and disk must be enough).

Always reserve headroom — at minimum 30%, better 50%. At peaks everything grows: retention ticks a bit longer, compression degrades, failover eats its own budget. A broker disk at 95% means an imminent incident, no matter how much "five percent" sounds like room: writes block, the controller starts complaining, and nobody wants to deal with that at 3 AM.

## Topic configs that are actually worth tuning

The broker sets defaults for every knob. Defaults are a compromise designed for "something average". On real topics they're almost always adjusted.

Below are the knobs worth knowing off the top of your head.

### cleanup.policy

Two policies from [Retention and compaction](../../../08-02-retention-and-compaction/i18n/en/README.md) — `delete` and `compact`. There's also `compact,delete` (both compact and retention trim old data). The choice is first and foremost the answer to "what lives in this topic". Event stream — `delete`. Per-key state snapshot — `compact`. Snapshot with TTL "delete profiles older than a year even if no tombstone arrived" — `compact,delete`.

Mix them up and you'll hit pain. With `compact` on events you lose everything except the latest version per key (and per-key order is useful in itself, for auditing). With `delete` on state, retention trims needed keys and downstream is left without current state.

### retention.ms / retention.bytes

For `delete` topics — the main knobs. `retention.ms` is a TTL by time (cut by the age of the last record in a segment). `retention.bytes` is a log size limit per partition (per-partition, not per topic). Whichever condition fires first wins.

Setting only one is fine. Setting both is fine too, and sometimes necessary (for example, protection against a sudden spike: TTL=30d, but if more than 100 GB poured in over 24 hours — trim anyway). On a compact topic both knobs only make sense with `compact,delete`, and there they act as a fallback to compaction.

### segment.ms / segment.bytes

A segment is the file the active piece of a partition writes to. It closes when it hits `segment.bytes` (1 GB default) or by age `segment.ms` (one week default). Retention and compaction only touch closed segments — that was covered in [Retention and compaction](../../../08-02-retention-and-compaction/i18n/en/README.md).

The default "1 GB or one week" is good for an average topic. For high-throughput metrics (where you want fast retention) set `segment.ms=10m` — segments close frequently, retention fires close to the declared value. On a compact topic where updates are rare, `segment.ms=1d` is enough. Segments that are too short produce too many small files, metadata, FDs, and indexes; segments that are too long mean the log doesn't shrink and retention "lies".

### min.insync.replicas

With RF=3, set `min.insync.replicas=2`. This means: with `acks=all`, a write is considered successful only if at least 2 out of 3 replicas acknowledged it. If ISR drops to one broker (two nodes are down), a producer with `acks=all` gets `NOT_ENOUGH_REPLICAS` and doesn't write. That's the guard against split-brain: better to reject the client than to lose data when the failed broker comes back.

With RF=3 and `min.ISR=2` the cluster survives one node going down without losing writes. Two nodes down — it doesn't. These are the base sane settings for production topics; we keep the same in the sandbox.

### max.message.bytes

Maximum size of a single record on the broker. Default ~1 MB. If a producer sends more — `RecordTooLargeException`, the message never reaches Kafka.

Raising it deserves deliberate thought. Large messages mean:
1. Larger network round-trip and worse batching.
2. Mandatory alignment with broker-level `message.max.bytes` and `replica.fetch.max.bytes` — otherwise brokers can't replicate between themselves.
3. Longer segments, because a single record eats a noticeable chunk immediately.

If your payload is 5–10 MB — usually the right answer is to store the blob in external storage and push only the key through Kafka.

### compression.type

Levels from `none` to `zstd`. Can be set on the producer (the broker then stores the compressed batch as-is) or on the topic (the broker recompresses incoming data to match its own setting). The usual approach is to set it on the producer — less CPU on the broker.

`zstd` — best compression on text/JSON payloads, slightly more CPU. `lz4` — cheap and fast, weaker ratio. `snappy` is similar to lz4, slightly weaker. `none` — only if the payload is already binary and compression adds CPU with no benefit (image snapshots, for example).

In the sandbox profiles below, `cdc` uses `zstd` (long-term storage, disk savings matter a lot), `metrics`/`events` use `lz4` (fast and cheap, retention trims the log anyway).

### unclean.leader.election.enable

This flag is usually left at `false`. If enabled — when the leader fails and all ISR members are unavailable, Kafka may elect a replica that has fallen behind the ISR as leader. That means: writes the leader already acknowledged but hadn't yet replicated to the ISR disappear. Silent data loss. Only enabled in very specific scenarios where availability matters more than any data loss, and usually on a temporary topic.

### message.timestamp.type

Two strategies. `CreateTime` — Kafka stores the timestamp the producer set. `LogAppendTime` — Kafka sets its own timestamp at broker append time, overwriting what the client sent.

`CreateTime` is needed when event-time matters for downstream — for example, for windowing in [Stream processing: concepts](../../../../07-streams-and-connect/07-01-stream-processing-concepts/i18n/en/README.md). `LogAppendTime` — when you don't trust client clocks and retention predictability matters more than event-time. For metrics with an aggressive TTL, `LogAppendTime` is more stable: retention cuts by "broker time", not by whatever a producer with a drifted clock reports.

## Three profiles

This is the skeleton the `topic-profiles` program assembles. The program creates three topics, each with its own set of configs, and prints them via `DescribeTopicConfigs`. The idea — see side by side how the same knobs are turned in different directions for different scenarios.

```
cdc      — partitions=6,  RF=3, compact, retention=-1, zstd, max=2 MB
metrics  — partitions=12, RF=3, delete,  retention=24h, segment=10m, lz4
events   — partitions=12, RF=3, delete,  retention=7d,  segment=1d,  lz4
```

`cdc` — about per-key state. Stored indefinitely, compacted periodically, `max.message.bytes` raised to 2 MB for large table snapshots from Debezium. zstd — because of long-tail storage, where the CPU cost of compression pays back in disk savings many times over.

`metrics` — short-lived and many small records. 12 partitions so 12 workers pull in parallel. Segment of 10 minutes — retention fires close to 24 hours with no large drift. lz4 — cheap compression, don't spend CPU on zstd when the data is gone in a day anyway. `LogAppendTime` — because the metric timestamp is already in the payload, and you want predictable retention.

`events` — a one-week replay buffer. retention=7d, segment=1d — close a new segment every day; on Saturday retention starts trimming Monday. lz4. `CreateTime` — because an event may be reprocessed retroactively, and downstream needs event-time.

## What the program does

`cmd/topic-profiles/main.go` — a single pass, no long-running loop. Creates three topics, calls `DescribeTopicConfigs`, and prints a table. Run with `-recreate` to delete existing topics first.

The profiles are hardcoded — the `profiles(prefix)` list. Here's what the `cdc` profile looks like:

```go
{
    name:  "cdc",
    topic: prefix + "-cdc",
    parts: 6,
    rf:    3,
    configs: map[string]*string{
        "cleanup.policy":                 kadm.StringPtr("compact"),
        "retention.ms":                   kadm.StringPtr("-1"),
        "min.insync.replicas":            kadm.StringPtr("2"),
        "compression.type":               kadm.StringPtr("zstd"),
        "max.message.bytes":              kadm.StringPtr("2097152"),
        "min.cleanable.dirty.ratio":      kadm.StringPtr("0.1"),
        "unclean.leader.election.enable": kadm.StringPtr("false"),
        "message.timestamp.type":         kadm.StringPtr("CreateTime"),
    },
    rationale: "long-lived per-key state: compact + retention=-1 + zstd, ...",
},
```

Idempotent creation — the standard course boilerplate. If the topic already exists — `TopicAlreadyExists`, and we switch to `AlterTopicConfigs`:

```go
resp, err := admin.CreateTopic(rpcCtx, p.parts, p.rf, p.configs, p.topic)
if err == nil && resp.Err == nil {
    return nil
}
cause := err
if cause == nil { cause = resp.Err }
if !errors.Is(cause, kerr.TopicAlreadyExists) {
    return cause
}
alters := make([]kadm.AlterConfig, 0, len(p.configs))
for k, v := range p.configs {
    alters = append(alters, kadm.AlterConfig{Op: kadm.SetConfig, Name: k, Value: v})
}
alterResp, err := admin.AlterTopicConfigs(rpcCtx, alters, p.topic)
```

After creation — `DescribeTopicConfigs` with a retry (on a freshly created topic, metadata sometimes doesn't propagate to all brokers in time, and the first error is `UNKNOWN_TOPIC_OR_PARTITION`):

```go
rcs, err := admin.DescribeTopicConfigs(ctx, names...)
retryNeeded := false
for _, rc := range rcs {
    if errors.Is(rc.Err, kerr.UnknownTopicOrPartition) {
        retryNeeded = true
        break
    }
}
```

Then — straightforward table assembly: one column per profile, one row per important knob. The list of knobs is hardcoded in `shownConfigs` — the same `cleanup.policy`, `retention.*`, `segment.*`, `min.insync.replicas`, `max.message.bytes`, `compression.type`, `unclean.leader.election.enable`, `message.timestamp.type`. Values are formatted human-readable: `86400000` becomes `1d (86400000)`, `1073741824` becomes `1.0 GB`, `-1` for `retention.bytes` becomes `-1 (no limit)`.

Partition and replication factor comparison — a separate request via `ListTopics` (DescribeConfigs returns only configs, not layout):

```go
td, err := admin.ListTopics(ctx, names...)
for _, p := range sorted {
    t, ok := td[p.topic]
    if !ok || t.Err != nil { ... continue }
    fmt.Fprintf(tw, "%s\t%d\t%d\n", p.topic, len(t.Partitions), t.Partitions.NumReplicas())
}
```

## Running

```sh
make help                 # cheat sheet
make run                  # create three topics and print the table
make run-recreate         # delete first, then create (deterministic output)
make describe             # same DescribeTopicConfigs, but via kafka-configs.sh
make topic-delete-all     # clean up after yourself
```

Parameters:

```sh
PREFIX=my-topic make run    # names will be my-topic-cdc / my-topic-metrics / my-topic-events
```

In the `make run` output, three things are worth watching. The `CDC` column — almost every knob set explicitly. The `METRICS` column — same, but radically different values. The `EVENTS` column — something in between. Remove one knob from a profile — it shows the broker default in the table, and you immediately see what you were missing.

## Profile cheat sheet

| Scenario | partitions | cleanup | retention | segment | compression | timestamp |
|----------|------------|---------|-----------|---------|-------------|-----------|
| CDC / state | 6 | compact | -1 | 7d | zstd | CreateTime |
| Metrics / telemetry | 12 | delete | 24h | 10m | lz4 | LogAppendTime |
| Events / domain | 12 | delete | 7d | 1d | lz4 | CreateTime |
| Logs (raw) | 6–12 | delete | 3d | 1h | zstd | LogAppendTime |
| Audit | 3–6 | delete | 1y | 7d | zstd | CreateTime |
| Cache (key→value) | 6 | compact,delete | 90d | 1d | lz4 | CreateTime |

Treat these as working starting points rather than "correct" numbers. From here you tune to your own traffic profile and watch disk via `DescribeAllLogDirs` and lag via `kadm.Lag`.
