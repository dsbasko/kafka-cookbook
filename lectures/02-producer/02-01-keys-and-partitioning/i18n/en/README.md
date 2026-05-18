# 02-01 — Keys & Partitioning

In the previous module we wrote 10 messages and saw how the `(partition, offset)` pair maps onto the log. A separate conversation about partitioners was promised. Here it is.

This is the topic that trips up almost everyone new to Kafka. The message key looks like a small thing — just a string next to the payload. In reality it determines processing order within a stream. Get the key wrong — lose order. Change the partition count — lose order for the same keys retroactively. This lecture is about exactly how that works.

## Why the key exists

A Kafka topic is not a single queue. Internally it is split into partitions, and each partition is an independent ordered log. There is no ordering between partitions. That is the first anchor point.

When a producer writes a message without a key, the partitioner is free to place it anywhere — there are many implementations. Message 100 goes to partition 2, message 101 to partition 0, message 102 wherever is least busy.

When a producer sets a key, the partitioner must convert that key to a partition number **deterministically**. Hash the key, mod by partition count. The same key always goes to the same partition (as long as the partition count does not change — a caveat we will return to).

Why this matters. Take a typical example: a topic `orders`, with a stream of events from different users. A user adds an item to a cart. Then checks out. Then cancels. If these events (a legitimate list of three, so I will number them):

1. **`cart_updated`** — the user added an item.
2. **`order_checked_out`** — checked out.
3. **`order_cancelled`** — changed their mind and cancelled.

land in three different partitions — three different consumers will read them in arbitrary order relative to each other. You can see `order_cancelled` before `order_checked_out`. Consumer logic breaks.

The solution is a `userID` key. All events for a single user go to the same partition, read in strict write order. Ordering between different users does not matter — they share nothing.

The principle is short: **what must be ordered must live in one partition; what must be in one partition must share the same key**.

## How the default partitioner works

The default in franz-go is `kgo.UniformBytesPartitioner`. It mirrors the Kafka Java client: for records with a key it computes `murmur2(key) mod N`, where N is the partition count. Without a key — sticky strategy: one partition is kept "hot", everything arriving in sequence is poured into it until the batch is sent or hits the size limit; then sticky jumps to the next.

Murmur2 is chosen deliberately. Distribution is good, CPU cost is low, and most importantly — it is compatible with Java. If you write to the same topic from a Go service and a Java service, both will send a message with key `user-42` to the same partition. This matters for migration scenarios and hybrid stacks.

Worth stating explicitly: the hash is computed over the **raw bytes of the key**. No normalization before hashing — bytes are bytes. `"USER-42"` and `"user-42"` are different keys and will go to different partitions with probability close to 1. If a case-fold or trim happened somewhere in one part of the pipeline but not another — you lost ordering, and tracking it down will take a while.

## What happens when the partition count changes

The scenario is common: a topic was created with 3 partitions, load grew, bumped to 6. And here the painful part begins.

`hash(key) mod N` is a function of N. Change N — change the result for the same keys. A record with key `user-42` that went to partition 1 before the change may go to partition 4 after. Old messages for that user stayed in partition 1, new ones land in partition 4 — ordering between them is lost.

Two consequences follow:

- **Decreasing the partition count is simply not possible.** Kafka does not allow it. Increase partitions — yes, decrease — no. Never. To "decrease", you create a new topic and migrate the data manually.
- **Increasing is possible, but carefully.** It is a design-level decision. Either you accept a temporary ordering break for the keys that get re-mapped. Or you do it when there is no live traffic. Or you switch to consistent hashing at the application level. Or — most commonly — you pause producers for a short window, wait for consumers to drain the backlog on the old partitions, then expand. There is no standard painless way.

So in production you typically try to size the partition count with headroom from the start. Better to have 24 partitions and underuse half than to heroically solve an increase problem later.

In this lecture we create topics with 3 partitions — enough to see the distribution, and enough that the premium/regular split in `custom-partitioner` does not degenerate.

## Custom partitioner — when the default is not enough

The default partitioner is optimized for one thing: spread load evenly. That is correct for almost all cases. But sometimes you have a priority stream that must not mix with the regular one.

A real-world example. A platform processes orders; users are split into tiers — premium and regular. Premium has a processing SLA: median latency no higher than 100ms. Regular has no SLA, everything is best effort. If both streams sit in the same topic with the default partitioner, premium orders can end up in the same partition as a regular spike and wait behind someone else's backlog. SLA degradation — guaranteed.

The solution is a dedicated partition (or partitions) for premium and a separate consumer with priority on that partition. The custom partitioner puts records where the business rule dictates.

The interface in franz-go is simple. `kgo.Partitioner` is a factory for topic-level partitioners:

```go
type Partitioner interface {
    ForTopic(string) TopicPartitioner
}
```

A `TopicPartitioner` is created per topic, with two main methods:

```go
type TopicPartitioner interface {
    RequiresConsistency(*Record) bool
    Partition(*Record, int) int
}
```

`Partition(rec, n)` returns the partition index from 0 to n-1. `RequiresConsistency(rec)` answers "must the record go exactly there?" — if true, the client waits for the unavailable partition; if false, it can redirect to another on leader failure.

For premium we want a strict guarantee (partition 0 only, no alternatives). For regular — round-robin between the two remaining, and here consistency is not needed; it does not matter where it lands as long as it is written.

## What the code does

The lecture has two binaries. One demonstrates default behavior with keys, the other shows a custom partitioner.

### keyed-producer

Writes 1000 messages to `lecture-02-01-keyed`, key is `user-{i mod 10}` — 10 unique users across 1000 messages, 100 records each. Topic — 3 partitions, replication factor 3.

After writing, the code builds two tables. The first shows how many messages went to each partition — the overall distribution. The second shows which partitions each key landed on. If the partitioner works correctly (and it does), each key should have exactly one partition in the column.

The write loop — bare `ProduceSync` plus statistics accumulation:

```go
for i := 0; i < o.messages; i++ {
    key := fmt.Sprintf("user-%d", i%o.users)
    val := fmt.Sprintf("event-%d", i)
    rec := &kgo.Record{
        Topic: o.topic,
        Key:   []byte(key),
        Value: []byte(val),
    }

    rpcCtx, rpcCancel := context.WithTimeout(ctx, 10*time.Second)
    res := cl.ProduceSync(rpcCtx, rec)
    rpcCancel()
    if err := res.FirstErr(); err != nil {
        return fmt.Errorf("produce %d: %w", i, err)
    }
    got := res[0].Record

    if _, ok := keyToPartitions[key]; !ok {
        keyToPartitions[key] = make(map[int32]int)
    }
    keyToPartitions[key][got.Partition]++
    partitionCount[got.Partition]++
}
```

`got.Partition` is what the broker returned. Accumulated into two slices: overall partition distribution and per-key partition distribution.

What you will see in the output:

```
пишем 1000 сообщений с ключами user-0..user-9 в топик "lecture-02-01-keyed" (3 партиций)

распределение по партициям:
PARTITION  COUNT
0          100
1          300
2          600

в какую партицию ложился каждый ключ:
KEY     PARTITION  COUNT  NOTE
user-0  1          100    one-key-one-partition ✓
user-1  2          100    one-key-one-partition ✓
user-2  2          100    one-key-one-partition ✓
user-3  2          100    one-key-one-partition ✓
user-4  1          100    one-key-one-partition ✓
user-5  1          100    one-key-one-partition ✓
user-6  2          100    one-key-one-partition ✓
user-7  2          100    one-key-one-partition ✓
user-8  0          100    one-key-one-partition ✓
user-9  2          100    one-key-one-partition ✓

сверка с end offsets из лога:
PARTITION  LATEST
0          100
1          300
2          600
TOTAL      1000
```

What matters. First, each key maps to exactly one partition — `one-key-one-partition ✓` appears for all. That is the default partitioner's guarantee. Second, the mapping between keys is deterministic: `user-8` always goes to partition 0 (on a 3-partition topic with murmur2), `user-0`, `user-4`, `user-5` always go to partition 1, the other six end up in partition 2. On 4 partitions the mapping will be different — and stable at that value. On 6 partitions — different again. Change N — everything changes.

Third, the partition split came out 100/300/600 — a sharp skew. That is normal with 10 unique keys: 10 is too few for an even murmur2 hash across 3 partitions, one key landed alone in P0 while six clumped into P2. With 1000 unique keys it would be noticeably more balanced.

### custom-partitioner

The same scenario but with a custom partitioner. Topic `lecture-02-01-custom` with 3 partitions and RF=3, sending 1000 messages. By default 30% of records are premium (key starts with `prem-`), the rest are regular (key `reg-N`).

Premium always goes to partition 0. Regular — round-robin between 1 and 2. No premium in partition 1 or 2. No regular in partition 0.

The partitioner implementation is a small type:

```go
type premiumTopicPartitioner struct {
    rr int
}

func (p *premiumTopicPartitioner) RequiresConsistency(r *kgo.Record) bool {
    return bytes.HasPrefix(r.Key, []byte(premiumPrefix))
}

func (p *premiumTopicPartitioner) Partition(r *kgo.Record, n int) int {
    if n <= 0 {
        return 0
    }
    if bytes.HasPrefix(r.Key, []byte(premiumPrefix)) {
        if premiumPart < n {
            return premiumPart
        }
        return 0
    }
    // round-robin между rrFirst и rrSecond
    choice := rrFirst
    if p.rr%2 == 1 {
        choice = rrSecond
    }
    p.rr++
    return choice
}
```

`RequiresConsistency` for premium returns true — no alternatives, we wait for partition 0 specifically. For regular — false, round-robin does not care where it lands. The `rr` counter state has no mutex: within a single topic in franz-go the partitioner is called without concurrency (see the client docs).

Passing the partitioner to the client — one option:

```go
cl, err := kafka.NewClient(
    kgo.RecordPartitioner(premiumPartitioner{}),
)
```

Then the same `ProduceSync`, no difference from keyed-producer.

After 1000 records the program checks three invariants and prints them explicitly:

```
проверки:
  ✓ все премиум-записи лежат в партиции 0
  ✓ regular-записи не зашли в премиум-партицию 0
  ✓ round-robin сбалансирован: P1=343, P2=342 (skew=1 ≤ 34)
```

The tolerance for round-robin is 5% of total. Here the difference is 1 record out of 685 — excellent.

The partition table in the output is equally clear:

```
распределение по партициям:
PARTITION  TOTAL  PREMIUM  REGULAR  NOTE
0          315    315      0        premium-only ожидаем
1          343    0        343      round-robin для regular
2          342    0        342      round-robin для regular
```

The numbers under `--premium-pct=30` are deterministic (PCG with a stable seed) and break down as 315/343/342. With other values of `--premium-pct` the distribution will differ.

## Key takeaways

After this lecture, the following should be clear:

- The key is **deterministic addressing to a partition**. One key → one partition, as long as N does not change.
- The default partitioner: without a key — sticky, with a key — `murmur2(key) mod N`. Compatible with the Java client by default.
- Changing the partition count is painful. Decrease — impossible. Increase breaks ordering for re-mapped keys. Plan upfront.
- A custom partitioner is two methods (`Partition`, `RequiresConsistency`) plus `ForTopic`. Use it when the default "spread evenly across all partitions" does not fit the business case — for example, when you need to isolate the premium stream in a dedicated partition and consume it separately from the general backlog.

The next lecture ([Acks and durability](../../../02-02-acks-and-durability/i18n/ru/README.md)) covers `acks` and durability. Again one tiny option that determines whether you lose data when a broker goes down.

## Running

The sandbox must be running (`docker compose up -d` from the root).

Main scenario:

```sh
make run-keyed
```

Custom partitioner:

```sh
make run-custom
```

Raise the premium share to 80% to see partition 0 absorb most of the stream:

```sh
make run-custom PREMIUM_PCT=80
```

Describe both topics with `kafka-topics.sh --describe`:

```sh
make topic-describe
```

Read the topic via the CLI consumer (keyed by default):

```sh
make consume-cli                       # keyed-producer
make consume-cli T=lecture-02-01-custom  # custom-partitioner
```

Clean up after the lecture:

```sh
make topic-delete
```
