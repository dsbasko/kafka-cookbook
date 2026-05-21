# 01-03 - Replication and ISR

In the previous lecture Brew recreated the `brew.orders.v1` topic with three partitions and ran `inspect`. Every line of the output had three columns:

```
PARTITION  LEADER  REPLICAS  ISR      OFFLINE
0          2       [2 3 1]   [2 3 1]  -
1          3       [3 1 2]   [3 1 2]  -
...
```

`LEADER` we sorted out then - it's the broker through which writes flow into a partition. `REPLICAS` and `ISR` I waved off with "that's for 01-03". This is 01-03.

The topic splits in two. First, why these three columns exist at all and what they protect. Then we run `watch-isr` and manually take down a broker - and watch `ISR` shrink, recover, and the moment `acks=all` fails with `NotEnoughReplicas`.

## Why Brew needs replication

A partition is a file on a single broker's disk. If that broker dies, the file is gone. The messages it held can't be read. For `brew.orders.v1` that means orders are lost from the last backup up to the moment the broker fell. During the Friday promo "free coffee on Fridays" at 8000 orders per minute, that's thousands of lost payments and angry customers in the support chat.

The solution is obvious: keep copies. Each partition is stored on several brokers at once. That's replication. How many copies is set by `replication.factor` (RF) at the topic level. Brew's sandbox defaults to `KAFKA_DEFAULT_REPLICATION_FACTOR=3`, and three nodes each hold one copy of every partition.

RF gives two guarantees:

- **Availability.** With RF=3, you can lose one or two brokers and keep reading and writing (depends on settings).
- **Durability.** A written message is already on several disks, not sitting in RAM on a single node.

Replication is not free. Every message travels the network RF times and occupies RF×size on disk. On a Brew-scale prod cluster, that's real money: with RF=3 disk and network costs triple. RF=3 is the sensible default. RF=5 is for highly critical topics like `brew.payments.v1` in banking systems. Higher is almost never needed.

For Brew with three nodes, RF=3 is the ceiling: a fifth replica has nowhere to go. If the cluster grew to five nodes, you could push RF to 5 for the payments topic and keep `brew.orders.v1` at RF=3 - different topics tolerate different RF values just fine.

## Leader, follower, replica

Each partition has one `LEADER` and the rest are `followers`. They all sit on their brokers' disks the same way; only the role differs.

The leader does all the work:

1. Accepts writes from the producer (producers always write to the leader, never to a follower).
2. Appends to its local log.
3. Replicates to followers over the network.
4. Serves consumer fetch requests.

Followers just replicate - they pull fresh records from the leader and save them locally. They don't serve producer requests and generally don't serve consumer requests (multi-DC has `Fetch From Follower`, but that's a separate story not relevant inside a single data center).

The leader is elected by the controller (we met that role in [Architecture and KRaft](../../../01-01-architecture-and-kraft/i18n/en/README.md), it lives on one of the KRaft quorum nodes). If a partition leader dies, the controller picks a new one from the live followers and the role transfers. Both the producer and the consumer learn about it - they re-elect the leader on the fly via metadata-refresh, no app restart needed.

When Brew's `order-service` writes `OrderPlaced` to partition 0 of `brew.orders.v1`, under the hood this happens. The franz-go client checks its metadata cache, sees "partition 0 - leader=2", opens a connection to kafka-2, sends `Produce` there. If kafka-2 suddenly dies, the first request returns "not leader", the client fetches fresh metadata, learns the new leader (say kafka-3), and retries there. For the app this is two extra milliseconds of latency on a single message, no more.

## ISR - which followers are "in sync"

Here it gets interesting. Followers pull data from the leader asynchronously. One follower might lag by hundreds of milliseconds; another might be down and pull nothing until it's fixed. Which ones count as live, and which as lagging?

That's what **ISR - In-Sync Replicas** is for. It's the subset of `REPLICAS` containing those that:

- fetched fresh data from the leader within `replica.lag.time.max.ms` (default 30 seconds);
- caught up to the leader's offset within that window.

If a follower stalls, falls behind on the network, or its process restarts - it drops out of ISR. This happens after `replica.lag.time.max.ms`: while the timer ticks, the follower is considered live; when the timer expires, it's removed. Once it recovers and catches up to the end of the log, it rejoins ISR. This is normal: ISR is dynamic and constantly recalculated by the controller.

An analogy for a backend dev. ISR is something like a healthcheck pool in a load balancer. A node has a heartbeat timeout: miss two in a row, you're out of the pool, no more traffic. Come back, pass the check, you're back in. The difference: in Kafka the "healthcheck" measures one thing - whether you keep up with replication within `replica.lag.time.max.ms`. Pings alone don't count; being reachable on the network but not replicating is grounds for dropping out of ISR.

One important thing. Only replicas in ISR can become the new leader on failover (assuming `unclean.leader.election` is disabled - in Kafka 4.x this is the default, and Brew's sandbox does not override it). That means no data loss: the new leader knows every message the old leader acknowledged. That's exactly why a producer with `acks=all` waits for ISR writes - replicas outside ISR don't count toward the quorum.

Speaking of Brew's admin chat. Last week kafka-2 went into maintenance overnight; the baristas only noticed in the morning because of the alert "under-replicated partitions = 12, ISR=2/3". The cluster kept running: ISR=2 with `min.insync.replicas=2` is still normal operation. They brought kafka-2 back in ten minutes, replicas caught up - and `under-replicated` went to zero. No customer noticed anything.

## min.insync.replicas - the write threshold

Without this parameter, RF is only half a guarantee. It answers: **how many ISR replicas must acknowledge a write before a producer with `acks=all` gets OK**.

Brew's sandbox has `KAFKA_MIN_INSYNC_REPLICAS=2`, which means:

- ISR=3 - a write with `acks=all` is acknowledged normally; all good.
- ISR=2 - also acknowledged; the cluster runs in "reduced" mode but writes continue.
- ISR=1 - a write with `acks=all` fails with `NotEnoughReplicas`. The producer retries (in case ISR recovers) - in franz-go, `RecordRetries` and `RecordDeliveryTimeout` are unlimited by default, so without an explicit cap retries climb indefinitely. Reads still work.
- ISR=0 - the partition is fully offline: no writes, no reads. This is rare and usually means the whole cluster died; for Brew it's a "call the CTO" event.

The combination `RF=3 + min.insync.replicas=2 + acks=all` is the standard durable configuration. You can lose one broker and keep writing. Lose two - you can no longer write with durability (only without it, via `acks=1` or `acks=0`, but that means potential data loss). The `acks` parameter itself is covered in detail in [First producer](../../../01-05-first-producer/i18n/en/README.md); for now it's enough to know `acks=all` means "wait until all ISR replicas store the message".

`min.insync.replicas` is stored **on the topic** (or on the broker as a default). Brew can set different values per topic:

- `brew.orders.v1` - min.insync=2, because losing an order is losing revenue.
- `brew.payments.v1` - min.insync=2 (or even 3, if you want to block writes on any degradation - financial conscience).
- `brew.kitchen.v1` - min.insync=2, kitchen events matter for barista operations; losing them leads to "forgotten" orders.
- `brew.telemetry.v1` - min.insync=1, so metrics keep flowing even with two nodes down. Who needs a metric about a downed cluster if the metric itself isn't being written?

## What it looks like on the sandbox

```
              partition: brew.orders.v1-0
              RF=3, min.insync.replicas=2

   |- kafka-1 (id=1) --- replica  -|
   |                               |
   |- kafka-2 (id=2) --- LEADER  --|-- ISR={1,2,3}  acks=all OK
   |                               |
   |- kafka-3 (id=3) --- replica  -|


   stop kafka-2 -> leader moves to kafka-3 (new leader)
   replica id=2 drops from ISR after ~30s

   |- kafka-1 (id=1) --- replica  -|
   |                               |
   |- kafka-2 (down)               |-- ISR={1,3}    acks=all OK (2 of 3)
   |                               |
   |- kafka-3 (id=3) --- LEADER ---|   under-replicated = yes


   start kafka-2 -> catches up, rejoins ISR after ~5-30s

   |- kafka-1 (id=1) --- replica  -|
   |                               |
   |- kafka-2 (id=2) --- replica  -|-- ISR={1,2,3}  recovered
   |                               |
   |- kafka-3 (id=3) --- LEADER ---|
```

Which replica specifically becomes the new leader depends on which replica in ISR was first in the `Replicas` list (preferred leader logic). Your exact numbers will differ. What matters is that the pattern is the same.

## The scenario we'll reproduce

Run `make run` - the program creates topic `brew.orders.v1` with RF=3 idempotently and prints a table every 2 seconds:

```
[16:42:11]
PARTITION  LEADER  REPLICAS  ISR      UNDER-REPLICATED
0          2       [1 2 3]   [1 2 3]  no
1          3       [1 2 3]   [1 2 3]  no
2          1       [1 2 3]   [1 2 3]  no
---
```

ISR is full, each partition has its own leader node (the controller spread leadership by preferred-replica), no under-replication. The "all good" state.

In a separate terminal:

```sh
make kill-broker
```

That's `docker stop kafka-2`. The first ticks of `watch-isr` still show ISR=[1 2 3] on all three partitions - the broker is unreachable, but the leader keeps it in ISR for now. After `replica.lag.time.max.ms` (default 30 seconds), each partition's leader notices id=2 hasn't sent a fetch in a while and drops it from ISR. It fires for all partitions roughly at the same time - the timer is shared, because kafka-2 stopped sending fetches everywhere at once. Around 30 seconds after `docker stop`, `watch-isr` shows:

```
[16:42:51]
PARTITION  LEADER  REPLICAS  ISR    UNDER-REPLICATED
0          1       [1 2 3]   [1 3]  yes (missing [2])
1          3       [1 2 3]   [1 3]  yes (missing [2])
2          1       [1 2 3]   [1 3]  yes (missing [2])
---
```

What happened. Partition 0 had leader=2 - and 2 went down. The controller picked a new leader from ISR (id=1), writes continued without downtime. Partition 1 already had leader=3, nothing needed to change. Partition 2 lived on leader=1 - no switch either. Under the hood, leader elections and metadata-refreshes happened for all clients; our logs don't show that, but the `LEADER` column reflects the current truth.

Under-replication shows for all three partitions. The cluster is still operational because `min.insync.replicas=2` and ISR=2 - the threshold is met, `order-service` keeps writing orders as if nothing happened. But the safety margin is gone. One more node down and `acks=all` starts returning `NotEnoughReplicas`.

Restore the broker:

```sh
make restore-broker
```

After a few seconds you see id=2 catching up to the leader and rejoining ISR. If nothing was written during downtime, recovery is instant (nothing to catch up on). If there was traffic - proportional to the volume. After full recovery:

```
[16:43:25]
PARTITION  LEADER  REPLICAS  ISR      UNDER-REPLICATED
0          1       [1 2 3]   [1 2 3]  no
1          3       [1 2 3]   [1 2 3]  no
2          1       [1 2 3]   [1 2 3]  no
---
```

Note that leaders stayed where they ended up after failover. By default the controller does not move leaders back to their "historically correct" node; `auto.leader.rebalance.enable` and periodic leader rebalance handle that, but with a delay. The behavior is intentional - it avoids an unnecessary switch. On prod clusters, admins run `kafka-leader-election.sh --election-type preferred` manually or wait for auto-rebalancing.

## When ISR is lost entirely

Suppose Brew's kafka-2 and kafka-3 go down at the same time. One broker remains. A single node can't maintain durability with min.insync=2 - that's a hard stop.

```
ISR={1}     min.insync.replicas=2     ->     write with acks=all -> NotEnoughReplicas
```

What happens:

- A write with `acks=all` hits `NOT_ENOUGH_REPLICAS`. In franz-go this is a retryable error, and by default `kgo.RecordRetries` and `kgo.RecordDeliveryTimeout` are both unlimited - the producer will keep retrying until ISR recovers, and `order-service` will hang on the send call. To get a "couldn't accept your order, please retry" response with a predictable timeout, set an explicit cap or deadline (e.g. `kgo.RecordDeliveryTimeout(5*time.Second)` in prod). `kgo.RequestRetries` does not help here - its godoc explicitly notes that it does not apply to produce requests.
- A write with `acks=1` still works, but durability is lost if the last leader dies.
- A write with `acks=0` flies one-way without acknowledgement - the producer never learns about lost messages; for the payments topic that's unacceptable.
- Reads work, the leader is alive. `kitchen-service` can read everything committed earlier, and nothing breaks.

That's the point of `min.insync.replicas`. Kafka doesn't pretend everything is fine when it isn't. You declare a minimum replica count. Below it - stop, no writes. Getting an error and an alert beats losing data on the next failure.

In real Brew incidents the logic goes like this. One node down - the "under-replicated" alert fires, on-call investigates without rushing, customers don't suffer. Two nodes down - the "producers failing acks=all" alert fires, on-call rushes, because `order-service` is already returning 503. The third level ("the cluster is entirely down") is outside this lecture's scope - that's the DR plan, traffic failover to a backup region, and the wider infrastructure story.

## What the code does

`cmd/watch-isr/main.go` does three things. Creates the topic idempotently via `admin.CreateTopic` (if it already exists, uses it without recreating; in this lecture recreating would interfere with observation). Starts a timer with the given `-interval`. On each tick, calls `admin.ListTopics(ctx, topic)` and prints `Partitions.Sorted()`.

The `UNDER-REPLICATED` column is `len(p.ISR) < len(p.Replicas)`. When `yes` - some replicas have dropped out; the `missing` function finds the specific IDs.

The observation loop is a plain ticker with a context check:

```go
t := time.NewTicker(interval)
defer t.Stop()

if err := tick(ctx, admin, topic); err != nil { ... }
for {
    select {
    case <-ctx.Done():
        return nil
    case <-t.C:
        if err := tick(ctx, admin, topic); err != nil {
            // Don't exit on a single metadata error: if a broker goes down,
            // the client will switch to a live one on its own. Just log and
            // continue - otherwise watch-isr loses its purpose during failover.
            fmt.Fprintf(os.Stderr, "tick failed: %v\n", err)
        }
    }
}
```

One tick is `ListTopics` plus printing. The `under-replicated` logic is literally a length comparison:

```go
for _, p := range td.Partitions.Sorted() {
    under := "no"
    if len(p.ISR) < len(p.Replicas) {
        under = fmt.Sprintf("yes (missing %v)", missing(p.Replicas, p.ISR))
    }
    fmt.Fprintf(tw, "%d\t%d\t%v\t%v\t%s\n",
        p.Partition, p.Leader, p.Replicas, p.ISR, under)
}
```

The `missing` function finds replicas that are in `Replicas` but absent from `ISR` - those are the lagging nodes:

```go
func missing(replicas, isr []int32) []int32 {
    in := make(map[int32]struct{}, len(isr))
    for _, id := range isr {
        in[id] = struct{}{}
    }
    out := make([]int32, 0, len(replicas)-len(isr))
    for _, id := range replicas {
        if _, ok := in[id]; !ok {
            out = append(out, id)
        }
    }
    return out
}
```

One important code detail. A `ListTopics` error on a tick **does not kill the loop**. If the broker the client was connected to goes down, `franz-go` will pick a new seed broker on its own - but one or two requests in between may fail. If we exited on the first error, watch-isr would close exactly when the broker dies, i.e. at the most interesting moment. So errors are logged and the loop continues.

## Running

The sandbox must be running (`docker compose up -d` from the root).

```sh
make run
```

In a separate terminal:

```sh
make kill-broker     # stop kafka-2
make restore-broker  # bring it back up
```

You can stop any other node:

```sh
make kill-broker BROKER=kafka-3
make restore-broker BROKER=kafka-3
```

Compare with the CLI:

```sh
make topic-describe
```

You get the same ISR as in watch-isr, just in shell-script format - `Leader: 1 Replicas: 1,2,3 Isr: 1,3`. The idea is the same as in [Topics and partitions](../../../01-02-topics-and-partitions/i18n/en/README.md): `admin.ListTopics` returns everything needed, no shell calls required.

Delete the topic:

```sh
make topic-delete
```

## What you learned

- Replication is copies of a partition on multiple brokers. RF is set at the topic level. Brew's sandbox defaults to RF=3.
- Each partition has one leader; producers write only to the leader, followers pull from it.
- `ISR` is the followers that are "in sync" (no more than `replica.lag.time.max.ms` behind). Only ISR replicas can be elected as the new leader on failover (with `unclean.leader.election` disabled).
- `min.insync.replicas` sets the threshold: how many ISR replicas must acknowledge a write with `acks=all`. On Brew's sandbox it's 2.
- `RF=3 + min.insync.replicas=2 + acks=all` is the standard durable configuration. Survives one node failure. At ISR=1 `acks=all` starts returning `NotEnoughReplicas`.
- `admin.ListTopics` shows everything needed to observe ISR. No shell scripts required.

Next ([Offsets and retention](../../../01-04-offsets-and-retention/i18n/en/README.md)) we look at how messages live in time. We'll cover offset, log end offset, HWM, and retention. Along the way, we'll see why "our messages are stored for exactly N days" is a phrase with a hidden catch.
