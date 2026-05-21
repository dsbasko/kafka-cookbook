# 01-02 - Topics and partitions

Friday morning at Brew. They launched a promo called "free coffee on Fridays": the customer holds a button in the app for three seconds and a free order drops into the queue. By 11 am the `order-service` was slogging. 8000 orders per minute into a single `brew.orders.v1` topic with one partition. One leader node, one append channel, one CPU for writes, one network card. Easy ceiling to hit.

This lecture is about cutting a topic into multiple partitions and why you'd want to.

## What a topic is

A topic is a named channel that producers write to and consumers read from. At the model level it's straightforward: name the topic `brew.orders.v1` and orders flow there; name it `brew.payments.v1` and payments flow there. No exchanges or routing keys, unlike RabbitMQ. Messages simply live under that name.

At the physical level (which [Architecture and KRaft](../../../01-01-architecture-and-kraft/i18n/en/README.md) already touched) a topic is a directory on the broker's disk. Inside the directory are log files split into segments. When a segment reaches `segment.bytes` or `segment.ms`, it closes and a new one opens. Old segments are dropped by retention (see [Offsets and retention](../../../01-04-offsets-and-retention/i18n/en/README.md)). That's everything at the physical level. No magic.

The name `brew.orders.v1` is not accidental. Brew picked the scheme `<domain>.<entity>.v<version>` so that a breaking change in event format can ship as `brew.orders.v2` next to the old topic. Producers cut over to the new schema, consumers migrate at their own pace, the old `brew.orders.v1` lives until nothing reads it any more, then it's deleted. Versioning at the topic level is the standard pattern in Kafka, because schema is per-topic and evolving it in place is painful.

But if a topic were a single file, everything would bottleneck on one node. One disk, one CPU, one network card, one broker process. That's the picture Brew saw on Friday. So topics are split into partitions.

## Partition - the unit of parallelism

A partition is a shard of a topic. An independent append-only log. Topic `brew.orders.v1` with three partitions is three independent logs: `brew.orders.v1-0`, `brew.orders.v1-1`, `brew.orders.v1-2`. Each partition lives on its own set of brokers (covered in [Replication and ISR](../../../01-03-replication-and-isr/i18n/en/README.md)), and each has its own leader - the broker through which writes flow.

A backend analogy. If you sharded a PostgreSQL `orders` table by `customer_id` across 16 shards on 4 machines, each machine would become a mini-leader of its slice. A Kafka partition is the same idea, but baked into the broker: sharding "out of the box", with all the machinery of telling producers about the new leader, rebalancing on a node failure, and replicating within a partition - already provided.

All the other Kafka properties flow from this model:

- writes parallelize: producers can write to different partitions through different leaders simultaneously;
- reads parallelize: multiple consumers in the same group divide partitions among themselves (one consumer per partition maximum, see [First Consumer](../../../01-06-first-consumer/i18n/en/README.md));
- storage scales horizontally: more partitions means more even distribution across brokers;
- ordering is guaranteed only within a partition; across partitions it doesn't exist in general.

The last point is the main stumbling block for newcomers. Brew wanted "order all my order events for me", and Kafka honestly replies: I'll order within a partition. Across partitions, ordering is undefined. If you need all events for `order_id` to arrive in the right sequence (`OrderPlaced` then `PaymentReceived` then `KitchenStarted` then `OrderReady` then `OrderDelivered`), put `order_id` in the key. All events for one order will land in one partition.

## How a message lands in a partition

When a producer writes, it sends `(topic, key, value)`. The client then decides which partition to assign it to:

- if `key` is empty: sticky-style. The default in franz-go is `kgo.UniformBytesPartitioner` (KIP-794, shipped in the Java client from 3.3): it accumulates ~64 KiB into one partition, then rolls. The explicit alternative is `kgo.RoundRobinPartitioner`;
- if `key` is present: `partition = hash(key) mod N`, where N is the number of partitions.

The default hash is murmur2 (same as the Java client, so Go and Java write to the same partition for the same key). To change the strategy use the `kgo.RecordPartitioner` option - pass `kgo.RoundRobinPartitioner`, `kgo.StickyKeyPartitioner` or your own implementation of the `kgo.Partitioner` interface. More in [Keys and partitioning](../../../../02-producer/02-01-keys-and-partitioning/i18n/en/README.md); for now the key fact is: the key decides the partition via simple arithmetic.

This formula leads to the property interviewers love. Messages with the same key land in the same partition. Guaranteed. And therefore they are read in order. That's the "one-key, one-partition, one-order" guarantee.

```
                       topic = brew.orders.v1, partitions = 3
                       hash(key) mod 3

  key="order-001"  ──┐                          ┌──> partition-0
  key="order-004"  ──┤   (h(k)%3 == 0)          │    [r0 r1 r2 ...]
  key="order-007"  ──┘                          │
                                                │
  key="order-002"  ──┐                          ├──> partition-1
  key="order-005"  ──┤   (h(k)%3 == 1)          │    [r0 r1 r2 ...]
  key="order-008"  ──┘                          │
                                                │
  key="order-003"  ──┐                          └──> partition-2
  key="order-006"  ──┤   (h(k)%3 == 2)               [r0 r1 r2 ...]
  key="order-009"  ──┘
```

Each partition is an ordered sequence of records with a monotonically increasing offset (`r0`, `r1`, `r2`, …). Offsets across partitions are not comparable - each has its own counter starting from zero.

## Picking a key: what you pay for

The key is not decoration. It decides what Kafka can do for you and what it can't. Brew weighed four candidates for `brew.orders.v1` and walked through the consequences:

- **no key** - the simplest setup: kgo spreads sticky batches across partitions, load is even, throughput is at the top. The one big downside: per-order ordering is gone. `OrderPlaced` may land in partition 2 while `PaymentReceived` lands in partition 0, and a consumer reading partitions in parallel will see the payment before the order.
- **`order_id`** - preserves the order of events for a single order. `OrderPlaced` → `PaymentReceived` → `KitchenStarted` → `OrderReady` → `OrderDelivered` for `order-123` all land in one partition. Millions of keys (every order is its own), distribution is almost random (`hash(uuid)` is, by design, uniform randomness). Analytics for one specific customer require sweeping every partition.
- **`customer_id`** - all events for one customer in one partition. Useful when a consumer builds a per-customer profile in a local cache: you can keep the cache per-partition without syncing with neighbours. The downside: a "hot" customer (a corporate account dropping 1000 orders a day) skews the partition - one partition becomes Pareto, the others idle.
- **`shop_id`** - all orders for one cafe in one partition. Sounds right for `kitchen-service`: the barista in one cafe sees a clean stream without neighbours' noise. In practice it falls apart on Friday: the top cafe in the city centre takes 30% of all traffic, and its partition carries a third of the promo.

Brew chose `order_id` for the orders and payments topics (per-order event ordering matters) and `shop_id` for the kitchen topic (baristas group by their own cafe, and the peaky partition is tolerable because that cafe also has more staff). There is no universally correct answer: every choice optimises one thing and breaks another. Later in the course ([Keys & Partitioning](../../../../02-producer/02-01-keys-and-partitioning/i18n/en/README.md)) we look at key choice under a microscope.

## Why you cannot reduce the number of partitions

This is where every other person trips up. Partitions in a topic can **only be added**. You cannot reduce. Not with any admin request. Not with any `alter`.

The reason is the `hash(key) mod N` formula. Brew created `brew.orders.v1` with N=3 and wrote orders into it for a year. Every key landed in its partition. If tomorrow you set N=2, then for the same key `hash(key) mod 2` yields a completely different number. So data for the same key ends up "historically in one partition, new data in another". The entire per-key ordering guarantee collapses. Kafka simply doesn't offer the operation - there's no button to press by mistake.

Expansion also breaks the distribution. At N=3 → N=4, the same keys that landed in partition 0 can now land in any of the four. So `kafka-topics --alter --partitions` is typically run on a fresh topic, or when a temporary key-ordering disruption is acceptable. On production topics this is not a one-click operation.

The practical takeaway: plan the partition count upfront, with room to grow. The number of partitions is what your expected throughput / per-partition limit dictates. "10 partitions because it's a round number" is a poor compass. Rough estimate: a partition handles ~10-20 MB/s of writes and the same for reads. If you expect 100 MB/s, you want at least 6-8 partitions, better with headroom up to 12-16. This is empirical - detailed breakdown in [Sizing and tuning](../../../../08-operations/08-03-sizing-and-tuning/i18n/en/README.md).

After the Friday promo Brew recreated `brew.orders.v1` with three partitions instead of one. The topic name stayed (no bump to `.v2`, because only the partition layout changed, not the event schema), but the partition count tripled. Old keys, naturally, were redistributed across the new partitions, but Brew was ready: by the time of recreation, all events from the previous week were already mirrored into S3 for long-term storage, and live analytics ran on the fresh stream. The course sandbox stays at three from here on, so the output tables fit on screen.

## What the inspect program shows

`cmd/inspect/main.go` goes through three steps. First it creates the topic `brew.orders.v1` idempotently: it tries `admin.CreateTopic(ctx, partitions, rf, configs, topic)`, and if it hits `kerr.TopicAlreadyExists` that's just "topic already there", so we describe it instead. So running `make run` a second time is quiet and prints the current state. Then it describes the topic via `admin.ListTopics(ctx, "brew.orders.v1")` - under the hood this is a metadata request to the broker, returning `TopicDetails` (a map of name → details: `TopicID`, internal flag, partition count, per-partition leader/replicas/ISR). Finally it prints a per-partition table: who the leader is, which nodes hold replicas, and which of them are in ISR.

The `-recreate=true` flag first deletes the topic, then creates it again. Useful for seeing how the controller distributes leaders across nodes - the Kafka balancer tries to spread leadership evenly (here 3 partitions on 3 nodes → one leader per node).

Here is the core of idempotent creation. The `TopicAlreadyExists` error is a normal "already there":

```go
func ensureTopic(ctx context.Context, admin *kadm.Client, topic string, partitions int32, rf int16) (bool, error) {
    resp, err := admin.CreateTopic(ctx, partitions, rf, nil, topic)
    if err == nil && resp.Err == nil {
        return true, nil
    }

    cause := err
    if cause == nil {
        cause = resp.Err
    }
    if errors.Is(cause, kerr.TopicAlreadyExists) {
        return false, nil
    }
    return false, cause
}
```

After `ensureTopic`, `ListTopics` is called - that's the metadata request:

```go
details, err := admin.ListTopics(rpcCtx, topic)
td := details[topic]
// td.Topic       - name
// td.ID          - TopicID (UUID)
// td.Partitions  - map partition → details (Leader, Replicas, ISR, OfflineReplicas)
```

And the table print itself. `Partitions.Sorted()` returns a slice sorted by partition number:

```go
parts := td.Partitions.Sorted()
for _, p := range parts {
    offline := fmt.Sprintf("%v", p.OfflineReplicas)
    if len(p.OfflineReplicas) == 0 {
        offline = "-"
    }
    fmt.Fprintf(tw, "%d\t%d\t%v\t%v\t%s\n",
        p.Partition, p.Leader, p.Replicas, p.ISR, offline)
}
```

What's in the table is what's in `PartitionDetail`. `LEADER` is `p.Leader`, `REPLICAS` is `p.Replicas`, `ISR` is `p.ISR`, `OFFLINE` is `p.OfflineReplicas`. The print just inserts numbers into the format string.

## Running it

The sandbox must be running (`docker compose up -d` from the repo root).

```sh
make run
```

Expected output (IDs and leaders will differ on your machine; the Russian phrase is intentional - the Go program prints it as-is):

```
brew-topic "brew.orders.v1" создан: partitions=3 rf=3

Topic:       brew.orders.v1
TopicID:     kcFo++q0QQ+xaKj0pnwWGA==
Partitions:  3

PARTITION  LEADER  REPLICAS  ISR      OFFLINE
0          2       [2 3 1]   [2 3 1]  -
1          3       [3 1 2]   [3 1 2]  -
2          1       [1 2 3]   [1 2 3]  -
```

Things to note:

- `LEADER` differs per partition - write load spreads across nodes;
- `REPLICAS` shows three numbers (RF=3), and the order in the list is the preferred leader: the first entry is who the controller wants as leader (the controller tries, but doesn't always succeed immediately - leader election picks a live replica, not the "right" one);
- `ISR == REPLICAS` means all replicas are in sync; at `acks=all` writes are acknowledged immediately (if a node went down, ISR would shrink, see [Replication and ISR](../../../01-03-replication-and-isr/i18n/en/README.md));
- `OFFLINE` is empty - all replicas are alive.

Compare with the CLI:

```sh
make topic-describe
```

This target runs `kafka-topics.sh --describe --topic brew.orders.v1` inside the kafka-1 container. The picture is the same (field names differ slightly, but `Leader/Replicas/Isr` match). That's the point of the lecture: what the distribution does with a shell script, franz-go does in one line: `admin.ListTopics`.

Want to see how leader assignment changes?

```sh
make topic-recreate
```

Deletes the topic and recreates it. On the freed partitions, the controller picks leaders by preferred-replica logic. Run it a few times - you'll notice the numbers are stable (the controller doesn't choose randomly), but on delete and recreate the partition assignments across nodes differ.

After the lesson, clean up:

```sh
make topic-delete
```

## What you learned

- A topic is a named channel; on disk it's a directory of log file segments; with a single partition you bottleneck on one node.
- A partition is an independent append-only log within a topic; the unit of parallelism for reads and writes; sharding baked into the broker.
- A message with a non-empty key always lands in the same partition via `hash(key) mod N`. All events for one Brew order keyed by `order_id` arrive in order.
- Picking a key is picking what you optimise and what you break: per-entity order, cache locality, or load evenness.
- Partitions can be added but not removed - otherwise the `hash(key) mod N` mapping breaks retroactively, and with it every per-key ordering guarantee.
- `admin.ListTopics` returns metadata: per-partition leader, replicas, ISR - enough to understand the current topic state without a shell.

Next ([Replication and ISR](../../../01-03-replication-and-isr/i18n/en/README.md)) we dig into what `Replicas` and `ISR` actually mean - what "a replica fell behind" means, how ISR shrinks when a node goes down, and where `min.insync.replicas` fits in.
