# 01-01 - Architecture and KRaft

This lecture introduces Kafka through a running case study. Across all six lectures of this module we hold a single scene: a fictional coffee shop chain called Brew, whose backend started life as a monolith, grew into five services, and ran into a queue. If you're a backend developer comfortable with HTTP and SQL but you've only seen Kafka in a README, this is your entry point.

## Brew - a coffee shop that got stuck in a queue

Brew runs 80 cafes in a single city. The backend started as a monolith on PostgreSQL with one worker. When the mobile app launched and a partner program with couriers came online, the monolith broke apart into five services:

- `catalog-service` - menus, prices, stock.
- `order-service` - order intake.
- `kitchen-service` - lives on the cafe side, prepares drinks.
- `delivery-service` - coordinates couriers.
- `analytics-service` - reports for management.

The services talked over HTTP. If the kitchen service fell, the order fell with it (nobody to confirm readiness). If payments fell, the order fell too (nobody to charge the card). The on-call engineer walked through the logs and killed stuck requests by hand. They installed RabbitMQ in its classic configuration (durable queues, competing consumers, no Streams and no quorum queues - those didn't exist in 2018 yet). Life got easier. Orders piled up in the queue and waited until the kitchen came back. The cascade stopped.

Six months of peace. Then the pain came back, just shaped differently.

First, the analytics team asked for a clickstream dump for the past three weeks. A classic RabbitMQ queue has nothing to re-read: a message is consumed and gone, no history. The team proposed duplicating everything to S3 in parallel with the queue. The result was two systems instead of one, plus the joys of bugs in keeping them in sync.

Second, they spun up a second instance of `notification-service` to handle peak load. RabbitMQ split messages between the two instances under competing consumers - each copy got its own subset. That works for email blasts. For a local cache, or for "every copy needs to see the whole stream" (fan-out to several independent subscribers), it doesn't.

Third, the queue filled up when a consumer fell behind. On default settings the queue grew in RAM, and on a broker restart some messages could land in the dead letter exchange.

Fourth, hooking up a new consumer meant declaring an exchange, a queue, a binding, and coordinating the schema with the producer team. Technically solvable. Organizationally, a bottleneck.

The realization: Brew needs an event log that can be re-read from any point, served to several independent consumers, and doesn't require schema coordination with the producer every time. That's Kafka. From here on - how it actually works.

> The comparison here is to RabbitMQ in its classic configuration (durable queues, competing consumers). RabbitMQ Streams (since 3.9) and quorum queues have their own scenarios, partially covering the points above. This course focuses on the classic setup because that's what teams migrating to Kafka most often have in production.

## What Kafka is

Kafka is a distributed append-only log. The word "log" trips people up: they think of a text file with errors, like `/var/log/syslog`. This is something else.

A log in Kafka is a sequence of messages ordered by write time. The closest analogy from the database world is the WAL in PostgreSQL. Before every table change, Postgres writes a record into the Write-Ahead Log: "this location, these bytes". The WAL is append-only, read strictly sequentially, replicated to standbys. Kafka works the same way, with two differences. A record is available for reading immediately (no need to wait for recovery), and several independent clients can read it in parallel.

So Kafka behaves like a queue, but a strange one:

- messages don't disappear after being read, you can re-read from any position;
- several consumers see them independently, one doesn't "take" a message away from another;
- order is guaranteed inside a partition (more on partitions in [Topics and partitions](../../../01-02-topics-and-partitions/i18n/en/README.md));
- data is kept as long as you configure - forever, or three days.

Out of that come the use cases: integrating microservices through events, CDC (Change Data Capture) from databases, analytics collection, audit logs, task queues with replay capability. Anywhere you have a stream of data and want the option to replay it.

Brew will use Kafka exactly that way. The following vocabulary will run through all six lectures of this module:

- topics `brew.orders.v1`, `brew.payments.v1`, `brew.kitchen.v1`, `brew.delivery.v1`;
- events `OrderPlaced`, `PaymentReceived`, `KitchenStarted`, `OrderReady`, `OrderDelivered`;
- retention of 30 days for orders and payments, 7 days for kitchen and delivery;
- compliance data (years of it) lives in S3, not in Kafka - covered in [Offsets and Retention](../../../01-04-offsets-and-retention/i18n/en/README.md).

### How Kafka differs from RabbitMQ (for those with prior experience)

| Aspect | RabbitMQ classic | Kafka |
| --- | --- | --- |
| Model | broker distributes messages between consumers | broker stores a log, consumer picks its own position |
| What happens after read | message deleted (ack) | message stays, dropped by retention |
| Multiple consumers on one queue | competing consumers, share the stream | consumer group shares the stream; different groups see the full stream independently |
| Replay | needs special setup or external storage | built in, just change the offset |
| Adding a new consumer | declare a queue and a binding | subscribe to the topic, the producer never knows |
| Throughput | tens of thousands of msg/s on a typical cluster | hundreds of thousands and millions of msg/s |

This doesn't mean Kafka is "better". Better fits its case. Task queues with complex routing and priorities are still more convenient in RabbitMQ. But the moment the scenario becomes "we need to re-read history" or "several independent subscribers on one stream", Kafka fits better.

## The actors

Kafka has four kinds of participants. They show up across all the other lectures, so memorise the names right away.

1. **Broker** - a node that stores data. Topics and partitions live on brokers. With two brokers data spreads across two; with five, across five. The course sandbox has three (`kafka-1`, `kafka-2`, `kafka-3`). Without a broker, there's nothing to store on.
2. **Controller** - the brain of the cluster. Assigns leaders to partitions, tracks the ISR list (in-sync replicas, see [Replication and ISR](../../../01-03-replication-and-isr/i18n/en/README.md)), reassigns partitions when nodes fall, validates topic schema changes. There's one active controller per cluster. Without a controller, nobody decides who the current leader is.
3. **Producer** - the client that writes. This is your Go code with `kgo.Client.Produce(...)`. The producer picks a topic, a key, a payload, and sends it to a broker. Without a producer, the log is empty.
4. **Consumer** - the client that reads. Also Go code, more often through a consumer group. Without a consumer, nobody reads the log, which is fine for Kafka - data can sit.

Broker and controller live on the server (in the sandbox, in docker compose). Producer and consumer live in your Go application. A single process can be both a producer and a consumer (the classic consume-process-produce pattern - see [Consume-Process-Produce](../../../../04-reliability/04-02-consume-process-produce/i18n/en/README.md)).

## Why a three-node cluster

Brew could have spun up one Kafka node and called it a day. They couldn't.

One broker is one point of failure. The broker falls, the cluster is unreachable. That's not Kafka-specific, that's any single-instance backend.

Two brokers are worse than one. When the network between them breaks, both nodes consider themselves alive and assume the other is dead. That's split brain: each half keeps accepting writes, then you try to glue them back together, the history has diverged, and you spend a week resolving conflicts.

Three brokers - a quorum. A majority (2 out of 3) agrees on a decision. When one node falls, the other two keep working, the controller is re-elected in seconds, nobody notices (if you're lucky). This is basic arithmetic of consensus: to survive N failures you need `2N + 1` nodes. For one failure - three. For two - five.

Brew picked three. The money for five wasn't there, but nobody wanted downtime every time a single node went down.

## KRaft - metadata inside Kafka

Until 2021, Kafka couldn't live without ZooKeeper. ZK was a separate cluster that held all Kafka metadata: the list of topics, ACLs, the broker-to-partition mapping, who the current leader is, who's in the ISR. Each broker opened a session with ZK, exchanged state with the others through a znode tree, and elected a new controller when one fell.

The pain points of this design had been known for a long time:

- two clusters instead of one (Kafka and ZooKeeper) with two failure modes;
- metadata through znodes scaled poorly (ceiling around 200k partitions);
- complex bootstrap (bring up ZK first, then Kafka, then wait for sync);
- extra skill on the team (DevOps has to know both systems).

KRaft - Kafka Raft. Metadata moved inside Kafka, into a special system topic `__cluster_metadata`. That topic is a regular append-only log (like all the others), replicated between nodes through Raft consensus. The nodes that participate in Raft and vote for the leader-controller are called **voters**. The active leader among voters is the current cluster controller.

What this gives you in practice:

- one system instead of two (Kafka instead of Kafka + ZK);
- one metadata format (a topic-log, not a znode tree);
- faster recovery after a controller fall (seconds rather than tens of seconds);
- easier scaling to millions of partitions (the ZK ceiling is gone).

One downside: the ecosystem is still catching up. Some tutorials and Stack Overflow answers still describe ZooKeeper. KRaft was declared production-ready in Kafka 3.3 (KIP-833, October 2022) and became the default starting with Kafka 4.0. The course sandbox runs 4.2.0, ZooKeeper isn't even mentioned.

### Raft in one minute

If you've never touched it: Raft is a consensus algorithm. Several nodes agree on the order of log entries in such a way that when a minority fails, the remaining majority keeps working.

Every so often voters hold an election. One becomes the leader, the others are followers. Any write to `__cluster_metadata` goes through the leader: it takes the request, replicates the entry to the followers, waits for an acknowledgement from a majority, and tells the client "written". If the leader falls, the remaining voters run elections again, pick a new leader, and continue.

What matters for understanding KRaft: the Raft leader and the cluster controller are the same node at any given moment. When we say "the controller fell" in the KRaft era, it means the Raft leader fell and re-election is in progress.

### Combined vs dedicated mode

Voters can live in two ways.

In **combined mode**, every node is both a broker (stores partitions of user topics) and a potential controller (participates in Raft for `__cluster_metadata`). Minimum hardware, good for small clusters and sandboxes. The course sandbox uses this mode.

In **dedicated mode**, voters and brokers are separated: 3 to 5 dedicated controller nodes run only Raft, the other nodes are pure brokers that don't participate in elections. That's what production setups with dozens of brokers do, because the controller load is isolated from user traffic and can be scaled independently.

## Sandbox topology

The course sandbox is combined mode. Three nodes, each one a broker and a voter at the same time.

```
                      host (your Mac/Linux)
                              │
          ┌───────────────────┼───────────────────┐
          │                   │                   │
   localhost:19092     localhost:19093     localhost:19094
          │                   │                   │
   ┌──────┴───────┐    ┌──────┴───────┐    ┌──────┴───────┐
   │   kafka-1    │    │   kafka-2    │    │   kafka-3    │
   │ broker + ctl │    │ broker + ctl │    │ broker + ctl │
   │  node-id 1   │    │  node-id 2   │    │  node-id 3   │
   └──────┬───────┘    └──────┬───────┘    └──────┬───────┘
          │                   │                   │
          └───────────────────┼───────────────────┘
                              │
                       Raft over :9093
                       (controller listener)
                              │
                    elect the active controller
                    replicate __cluster_metadata
```

Each broker has three listeners on different ports. Three is unsettling at first sight, so let's break them down.

- **EXTERNAL listener** (`:9094` inside the container, mapped to `19092`/`19093`/`19094` on the host) - the entry point for clients from your machine. This is where `kgo.Client` from the Go code in each lecture knocks.
- **INTERNAL listener** (`:9092`) - for broker-to-broker traffic inside the docker network. Partition replication runs here, kept off the outside world.
- **CONTROLLER listener** (`:9093`) - Raft. Voters exchange votes, replicate `__cluster_metadata`. A client has no business going there.

ClusterID is fixed (`5nnS6DRtQnKwoMjkkVxxug`) and set in `docker-compose.yml`. That way the sandbox survives `docker compose down` without losing identity: on the next start the brokers recognize each other and don't recreate metadata from scratch.

Min ISR = 2, default replication factor = 3. That means: data lives on three nodes, and a write needs an acknowledgement from two. If one falls, you won't notice. If two fall, a producer with `acks=all` will start getting `NotEnoughReplicas`. Details on these settings in [Acks & Durability](../../../../02-producer/02-02-acks-and-durability/i18n/en/README.md) and [Transactions & EOS](../../../../04-reliability/04-01-transactions-and-eos/i18n/en/README.md).

## What's inside `__cluster_metadata`

It helps to see it once with your own eyes, so the fear goes away. The topic is hidden (a system topic), but it's a real log on disk - you can dump it.

Inside are records about topics, partitions, configs, ACLs, voter membership changes. Every broker, on start, pulls the log from the beginning, rebuilds a local snapshot of metadata, then watches the tail and applies updates as they arrive. The controller writes any changes there through its Raft layer - so all nodes see the same picture of the world.

The dump looks roughly like this:

```sh
docker exec kafka-1 /opt/kafka/bin/kafka-dump-log.sh \
  --cluster-metadata-decoder \
  --files /var/lib/kafka/data/__cluster_metadata-0/00000000000000000000.log | head -50
```

You'll see records like `RegisterBrokerRecord`, `TopicRecord`, `PartitionRecord`, `ConfigRecord` and so on. Don't dive into the format - just remember it's a regular log with typed records. The same data model as Brew's order topics, just for system use.

## The quorum-status program

Brew has spun up the sandbox. How do you check that the cluster is alive, the controller is elected, all voters are present?

You can go through the CLI (`kafka-metadata-quorum.sh ... describe --status` inside the container). Or you can go through Go - which is what we do in `cmd/quorum-status/main.go`. The program prints the ClusterID, the broker count, the active Raft controller-leader, and a table of voters.

Under the hood there are two requests. One trap is buried here, and people regularly walk into it.

### Request one - `BrokerMetadata` through kadm

```go
admin := kadm.NewClient(cl)

md, err := admin.BrokerMetadata(rpcCtx)
// md.Cluster      - cluster ClusterID (the same UUID as in docker-compose.yml)
// md.Controller   - id of the proxy broker for controller-requests (NOT the Raft-leader)
// md.Brokers      - []BrokerDetail with NodeID/Host/Port/Rack
```

This request goes through the high-level `kadm.Client` (an admin wrapper around franz-go) and returns general cluster metadata. The `Controller` field here returns the id of a broker through which controller-requests can be proxied. In the KRaft world this is **not the Raft leader**, just a proxy-coordinator. In the program output it's labelled `MetadataControllerProxy` to avoid confusion.

### Request two - `DescribeQuorum` through kmsg

To get the real Raft leader (i.e. the currently active cluster controller), you need a low-level `DescribeQuorum` request against the `__cluster_metadata` topic, partition 0. kadm has no ready-made wrapper yet, so it's assembled by hand through `kmsg`:

```go
req := kmsg.NewPtrDescribeQuorumRequest()
topic := kmsg.NewDescribeQuorumRequestTopic()
topic.Topic = "__cluster_metadata"
part := kmsg.NewDescribeQuorumRequestTopicPartition()
part.Partition = 0
topic.Partitions = []kmsg.DescribeQuorumRequestTopicPartition{part}
req.Topics = []kmsg.DescribeQuorumRequestTopic{topic}

resp, err := req.RequestWith(ctx, cl)
p := resp.Topics[0].Partitions[0]
// p.LeaderID       - the real Raft leader (the active controller)
// p.CurrentVoters  - list of voters: [{ReplicaID:1}, {ReplicaID:2}, {ReplicaID:3}]
```

This is normal franz-go practice: high-level `kadm` for the common case, low-level `kmsg` for the rare and specific. Wrappers appear as demand grows; until then, you write it like above.

### The trap: MetadataControllerProxy ≠ RaftLeader

`md.Controller` is the broker's view of who the active controller is. In KRaft this value is refreshed through metadata updates pushed by the controller quorum: in a steady state it matches RaftLeader, but during a re-election it can briefly diverge (the broker hasn't received the new update yet). Building an "is the controller alive" alert on this field alone leaves a window where you'll see a stale id or `-1`.

RaftLeader from `DescribeQuorum` is asked directly of the controller quorum and reflects the current leader at request time. Our output shows both fields explicitly: on a steady cluster you'll see matching numbers, during a re-election you'll see them diverge. In production, monitor "is the controller alive" through RaftLeader via DescribeQuorum.

After that the code just glues the two answers together. The broker whose id matches `LeaderID` gets the `broker + active controller` role in the table; the other voters get `broker + voter`.

Further in the course we almost never call the CLI - everything goes through franz-go and kadm. We'll come back here in [Consumer Groups & Rebalance](../../../../03-consumer/03-01-groups-and-rebalance/i18n/en/README.md) and in [Transactions & EOS](../../../../04-reliability/04-01-transactions-and-eos/i18n/en/README.md), when you need to know who the current controller is to understand the consequences of its re-election.

## Run

The sandbox must be up (`docker compose up -d` from the repo root). Then from the lecture directory:

```sh
make run
```

Expected output (ids will differ, RaftLeader - any of 1/2/3):

```
ClusterID:               5nnS6DRtQnKwoMjkkVxxug
Brokers:                 3
MetadataControllerProxy: 1  (BrokerMetadata.Controller; in KRaft - proxy, not Raft-leader)
RaftLeader:              3  (DescribeQuorum on __cluster_metadata; this is the active controller)
CurrentVoters:           [1 2 3]

NODE  HOST       PORT   RACK  ROLE
1     127.0.0.1  19092  -     broker + voter
2     127.0.0.1  19093  -     broker + voter
3     127.0.0.1  19094  -     broker + active controller
```

If you want to confirm the Go output isn't lying, compare against the CLI version:

```sh
make quorum-cli
```

This target pokes `kafka-metadata-quorum.sh describe --status` inside the kafka-1 container - the official shell script from the Kafka distribution. The fields differ, but `LeaderId` from the CLI matches `RaftLeader` from the Go version (and `CurrentVoters` matches our list). If they match - you can now talk to Kafka from Go without a shell.

## What you learned

- Kafka is a distributed append-only log. The closest database-world analogy is the PostgreSQL WAL, except many clients can read it independently.
- Brew came to Kafka from a world of HTTP and classic RabbitMQ. Triggers for the migration: replay for analytics, fan-out to several independent subscribers, growing load, and organizational coupling with the producer.
- The cluster has brokers (which store data) and a controller (which assigns roles). The producer writes, the consumer reads. In the sandbox there are three nodes, each combining broker and voter duties.
- KRaft is Kafka without ZooKeeper. Metadata lives in the `__cluster_metadata` system topic, replicated through Raft. Voters elect a leader, and the Raft leader is the active controller.
- Any CLI metadata operation can be repeated from Go through `kadm.Client`. For KRaft-specific requests (like `DescribeQuorum`) you drop down to the `kmsg` level.
- `MetadataControllerProxy` and `RaftLeader` are different things. The first is a routing hint, the second is the real controller. Don't mix them up in monitoring.

In the next lecture ([Topics and partitions](../../../01-02-topics-and-partitions/i18n/en/README.md)) Brew will run a "free coffee on Fridays" promo, take 8000 orders per minute into a single topic, and hit the ceiling. Through that story we'll work out what a partition is, why you want several, how the partition key works, and why the partition count can't be reduced.
