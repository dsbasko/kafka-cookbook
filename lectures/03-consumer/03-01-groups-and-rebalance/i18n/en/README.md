# 03-01 — Consumer Groups & Rebalance

This lecture opens the consumer module. In the first lecture ([First consumer with franz-go](../../../../01-foundations/01-06-first-consumer/i18n/en/README.md)) we ran a single consumer and read a topic in a loop — enough to understand how PollFetches works and how to shut down cleanly. Here we look at what happens when there are multiple consumers and Kafka has to divide partitions between them.

A group is exactly that division mechanism.

## What a consumer group is

Every Kafka consumer has a `group.id` — a short string that binds several separate processes into one logical unit. A broker sees a JoinGroup request from a client and uses the group.id to reason: "these are subscribed to the same topic and call themselves one group — I'll split the partitions between them." Same topic, different group.id — two independent groups, each receiving all messages in full.

One rule governs everything: within a single group, each partition is read by exactly one consumer at any given moment. Not two simultaneously. If there are more consumers than partitions, the extras sit idle (idle members). If there are fewer, some consumer holds multiple partitions. This is what gives horizontal read scalability: add a node to the group and the partition load redistributes.

One of the brokers acts as the group coordinator. It is selected by hash(group.id) % num_partitions(`__consumer_offsets`), and it handles heartbeats, JoinGroup, SyncGroup, and OffsetCommit. When the coordinator goes down (it is just one of the brokers) — the group automatically migrates to another.

## Rebalance — what it is and when it happens

Rebalance is the redistribution of partitions across group members. Triggers:

- a member joined the group (a new consumer started);
- a member left — Ctrl+C, or kill -9, or OOM crash, or a container reboot;
- a member missed the session.timeout (see below) — the coordinator considers it dead;
- the topic was expanded — new partitions appeared and must be assigned to someone;
- an admin triggered a rebalance manually via kafka-consumer-groups.sh.

In any of these cases the coordinator opens a rebalance phase: first it collects member requests (JoinGroup), then distributes the new assignment (SyncGroup) and says "get to work." Until then — everyone waits. This is the pain of rebalance: while it runs, the group stalls and lag grows.

## Assignment strategies

The decision of "who gets which partitions" is made not by the coordinator but by the group leader — one of the members to whom the brokers have delegated the assignor role. On the client side this means: you choose the strategy yourself via `kgo.Balancers(...)`. It must be the same for all members of a group; otherwise the coordinator cannot select a common protocol.

There are several strategies, and they differ fundamentally:

**Range** — the oldest. The partitions of each topic are divided into contiguous ranges and distributed to members alphabetically by member_id. Simple, but balances poorly with multiple topics: one member may receive partition 0 of two different topics while another gets none.

**Round-robin** — partitions from all topics are laid out in one list and distributed in rotation. Better distribution, but reshuffles almost everything on membership changes — every rebalance pulls all partitions, even those that could have stayed with the same owners.

**Sticky** — tries to preserve "stickiness": during a rebalance it keeps partitions with the members that already held them and moves only what is needed for balancing. The main benefit is less work recovering local state (caches, in-flight records) after a rebalance.

**Cooperative-sticky** — sticky plus a different rebalance protocol. The protocol is covered below.

In this lecture we use sticky and cooperative-sticky — the old range/round-robin are only interesting as historical reference points. On modern clusters, set cooperative-sticky by default and switch to sticky only if you have a specific reason.

## Eager vs cooperative — different rebalance protocols, not different strategies

This is something people often confuse. The strategy (sticky, round-robin, range, cooperative-sticky) is the **algorithm** for distributing partitions. Eager and cooperative are the **protocol** of the rebalance — the way the coordinator and group members communicate during redistribution.

Eager (a.k.a. "stop-the-world"):

1. Rebalance trigger — a new member joined, for example.
2. The coordinator tells everyone: "surrender all your partitions." Each member calls `OnPartitionsRevoked` for **all** its partitions.
3. Everyone participates in JoinGroup → SyncGroup and receives a new assignment.
4. Each member calls `OnPartitionsAssigned` for the new set.

Between steps 2 and 4 the group is stopped. If a member held 100 partitions and only 1 is actually moving — it still surrenders all 100 and takes back the same 99. Full stop.

Cooperative (incremental):

1. Rebalance trigger.
2. The coordinator computes the new distribution plan and **tells each member exactly which of its partitions must move**. The member calls `OnPartitionsRevoked` only for those.
3. The coordinator finishes the first round and assigns the surrendered partitions to their new owners.
4. If the plan is not yet finalized — a second round.

Partitions that stay with the same member are never revoked. If a member holds 100 partitions and 1 is moving — it loses 1 and hands it to another, while the remaining 99 keep reading without interruption. Lag on them does not grow.

The cost of the cooperative protocol is two round-trips instead of one and a more complex client-side implementation. In practice it pays off: on large groups with slow processing, an eager rebalance kills SLO.

You cannot mix eager and cooperative within one group. They are different SyncGroup protocols; the coordinator picks the one that all members advertise, and if even one member advertises eager — everyone falls back to eager. Migration from the old protocol to cooperative is therefore done via rolling restart with a temporary dual declaration (sticky + cooperative-sticky simultaneously, then dropping sticky).

## Timings: heartbeat, session timeout, max poll interval

The coordinator needs to know that a member is still alive. To do this, the member sends a heartbeat every `HeartbeatInterval` (on the wire — `heartbeat.interval.ms`). franz-go does this automatically in the background, in a separate goroutine — the application does not need to call anything.

If the coordinator has not received a heartbeat from a member for longer than `SessionTimeout` (`session.timeout.ms`), it declares the member dead and starts a rebalance without it. franz-go v1.21.0 defaults: `SessionTimeout` = 45 seconds (Kafka 3.0+ standardised on this value after KIP-735), `HeartbeatInterval` = 3 seconds (`pkg/kgo/config.go:641-643`). A member has to miss ~15 consecutive heartbeats before being evicted. This margin covers network blips and GC pauses — a short network hiccup should not trigger a rebalance.

The third timing is `RebalanceTimeout` (`rebalance.timeout.ms`). franz-go default is 60 seconds (`config.go:642`). This is the window in which every member must surrender partitions, commit offsets and rejoin after JoinGroup. If a member fails to rejoin within that time, the coordinator treats it as gone and continues the rebalance without it.

The Java client has a separate knob `max.poll.interval.ms` (default 5 minutes) that also tracks the gap between `poll()` calls client-side and forces the consumer to voluntarily leave the group when exceeded. On the wire it travels in the same JoinGroupRequest field as `rebalance.timeout.ms`. franz-go does not do this client-side self-eviction: the heartbeat goroutine keeps signalling even if processing a single record takes half an hour. You only hit a problem if a rebalance happens during that long processing — the stuck handler will then fail to rejoin within `RebalanceTimeout`, and the broker will kick the member out.

The three timers in plain terms:

- `HeartbeatInterval` — how often I signal that I am alive;
- `SessionTimeout` — how long the broker waits for that signal;
- `RebalanceTimeout` — how long the broker waits for me to finish my share of the rebalance and rejoin.

If you are worried that a zombie process will hang in the group and not release partitions — lower `SessionTimeout`. If you are worried about false positives on network blips — raise `SessionTimeout`. If your handler can genuinely hold a partition for minutes and you still want the rebalance to complete cleanly — raise `RebalanceTimeout` (franz-go-specific caveat: the client will not interrupt the handler, but the commit inside `OnPartitionsRevoked` still has to fit inside this window).

## What the code shows

There is one binary — `cmd/loud-member`. Run several copies in the same group; each prints a rebalance event and the current assignment list. No useful work — pure observation of how Kafka moves partitions across copies.

Copies are identified via the environment variable `MEMBER_ID`. It goes into the ClientID, the InstanceID, every printEvent call, and the process startup banner:

```go
opts := []kgo.Opt{
    kgo.ConsumerGroup(o.group),
    kgo.ConsumeTopics(o.topic),
    kgo.Balancers(balancer),
    kgo.HeartbeatInterval(o.heartbeat),
    kgo.SessionTimeout(o.sessionTimeout),
    kgo.ClientID(fmt.Sprintf("lecture-03-01-loud-%s", o.memberID)),
    kgo.InstanceID(fmt.Sprintf("loud-member-%s", o.memberID)),
    kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
    kgo.OnPartitionsAssigned(func(_ context.Context, _ *kgo.Client, m map[string][]int32) {
        printEvent(o.memberID, "ASSIGNED", m)
    }),
    kgo.OnPartitionsRevoked(func(_ context.Context, _ *kgo.Client, m map[string][]int32) {
        printEvent(o.memberID, "REVOKED", m)
    }),
    kgo.OnPartitionsLost(func(_ context.Context, _ *kgo.Client, m map[string][]int32) {
        printEvent(o.memberID, "LOST", m)
    }),
}
```

Two things here are worth looking at. `InstanceID` is an optional "static" member id (`group.instance.id` in the spec). When set, after a brief disconnect (a pod restart in k8s, for example) the coordinator will not rush to declare the member dead: partitions will return to the same member once it reconnects. Without `InstanceID`, every restart = a new member = a rebalance. For the demo, the name from MEMBER_ID is used here — it is consistent across restarts, and the coordinator sees that.

There are three hooks:

1. `OnPartitionsAssigned` — the coordinator has issued you a new partition set.
2. `OnPartitionsRevoked` — the coordinator is orderly revoking some (or all) of your partitions as part of a planned rebalance. Until the rebalance completes you are still a group member and can commit offsets — this is a safe place for a final commit.
3. `OnPartitionsLost` — differs from `Revoked` in exactly one way: it fires on "fatal" group errors (`IllegalGeneration`, `UnknownMemberID`, authentication failure, an expired session timeout). The partitions are already gone, you no longer own them. A commit here will almost certainly be rejected by the coordinator — that is the practical difference from `OnPartitionsRevoked`, where committing is still possible and recommended.

The strategy is selected with the `-strategy` flag:

```go
func pickBalancer(name string) (kgo.GroupBalancer, error) {
    switch strings.ToLower(strings.TrimSpace(name)) {
    case "sticky":
        return kgo.StickyBalancer(), nil
    case "cooperative-sticky", "cooperative", "coop":
        return kgo.CooperativeStickyBalancer(), nil
    default:
        return nil, fmt.Errorf("unknown strategy %q (поддерживаем sticky | cooperative-sticky)", name)
    }
}
```

Each run uses one balancer. Multiple balancers simultaneously in one group are not allowed (see eager vs cooperative above). The default is `cooperative-sticky` — because we want to observe incremental rebalance, where the difference between "this partition is being surrendered" and "this one stays" is most visible.

The read loop is trivial — PollFetches, print partition/key/value. Messages are there for atmosphere: to show that the member is not just standing and listening, but actually pulling data.

## What to observe

The topic has 6 partitions (`make topic-create-6p`). Three runs, in three terminals:

- **Terminal 1: `make run-1`.** The first copy starts, MEMBER_ID=1. JoinGroup → SyncGroup, done in a few hundred milliseconds. The output shows `ASSIGNED: lecture-03-01-groups=[0 1 2 3 4 5]` — all 6 partitions belong to it. If `make seed` was run beforehand, it starts reading.

- **Terminal 2: `make run-2`.** The second copy. Terminal 1 shows `REVOKED: lecture-03-01-groups=[3 4 5]`, terminal 2 shows `ASSIGNED: lecture-03-01-groups=[3 4 5]`. The group balances evenly. With cooperative, the first member revokes only what is moving, not all 6. With eager (sticky) — it revokes all 6 and gets back [0 1 2].

- **Terminal 3: `make run-3`.** The third copy. Result — 2/2/2 distribution. With cooperative, the first two terminals see `REVOKED` only for the partition that moved to the third member.

- **Ctrl+C on terminal 2.** Terminal 1 gets an additional `ASSIGNED`, terminal 3 gets an additional `ASSIGNED`. The partitions of the departed second member distribute between the remaining two. Cooperative does this smoothly: terminals 1 and 3 do not lose their current partitions, they only gain new ones.

- **kill -9 the second copy (without Ctrl+C).** This is interesting: the process is killed abruptly, nothing gets to surrender cleanly. The coordinator waits out the session.timeout (30 seconds in our code by default), then starts a rebalance. Until the timer expires the group is in a normal state, nobody reads the killed member's partitions, and lag on them grows. This is the "cost of sudden death" — seconds of downtime on those partitions.

To kill exactly the right MEMBER_ID without guessing the PID:

```sh
pgrep -f 'loud-member.*MEMBER_ID=2' | xargs kill -9
```

## Key takeaways

- A consumer group is simply a shared group.id. Within a group, each partition is read by exactly one consumer.
- Rebalance is the redistribution of partitions. Triggers: member join/leave, topic expansion, session timeout, admin manual trigger.
- There are several strategies; set `cooperative-sticky` by default. Sticky without cooperative is an eager protocol with a stop-the-world rebalance.
- All members in one group must advertise a compatible rebalance protocol. Cooperative + eager → the group collapses to eager.
- Rebalance timings in franz-go are three independent knobs: `HeartbeatInterval` (default 3 s, "how often I signal"), `SessionTimeout` (45 s, "how long the broker waits for the signal"), `RebalanceTimeout` (60 s, "how long the broker waits for me to finish my share of the rebalance"). The Java-client `max.poll.interval.ms` with client-side self-eviction does not exist in franz-go — slow processing will not auto-evict you; the issue only surfaces if a rebalance overlaps with the slow handler.
- InstanceID (`group.instance.id`) provides static identification: on restart, partitions return to the same logical member without a rebalance.
- OnPartitionsLost is a separate hook for "evicted by the coordinator on timeout." Committing offsets inside it is not allowed; inside OnPartitionsRevoked it is.

The next lecture ([Offset commits](../../../03-02-offset-commits/i18n/en/README.md)) goes down to the offset commit level: auto-commit and its duplicates on restart, manual sync/async, MarkCommitRecords + CommitMarkedOffsets, and a few other knobs. Here we only touched offsets in passing — time to cover them properly.

## Running

The sandbox must be running from the repository root (`docker compose up -d`).

```sh
make topic-create-6p     # create the topic with 6 partitions
make seed                # load 60 messages (optional, to have something to read)
```

Then in three terminals:

```sh
# terminal 1
make run-1

# terminal 2 (start once the first is already running)
make run-2

# terminal 3
make run-3
```

Experiment with the eager protocol:

```sh
make run-eager           # MEMBER_ID=1 + sticky
# in the second terminal:
STRATEGY=sticky make run-2
```

Inspect group state from the broker side (useful for comparing against what the hooks printed):

```sh
make group-describe
```

Reset committed offsets (after this, the next run-* reads from earliest again):

```sh
make group-delete
```
