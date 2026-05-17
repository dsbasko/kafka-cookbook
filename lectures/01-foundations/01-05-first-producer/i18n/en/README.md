# 01-05 - First Producer

Migration day at Brew: `order-service` writes its first `OrderPlaced` into Kafka instead of RabbitMQ. The on-call engineer hits deploy, opens the logs, and reads the line `produce ok partition=2 offset=17`. Three words, and immediately three questions about guarantees. Did the broker put the record on disk or just accept it into RAM? If the leader's cable were yanked right now, would the order survive or vanish? Will the record at offset=17 be duplicated on a retry?

This is the producer lecture. The program that takes an application event and puts it into a partition. So far we looked at Kafka from the broker side - topics, partitions, ISR, offset, retention. Now we switch sides: the code that writes.

The goal is modest. Write 10 messages with keys `order-0..order-9`, observe the `(partition, offset)` pair for each one, and confirm it matches the model from lectures 01-02 and 01-04. The harder parameters - idempotency, acks, batching - we cover in words and leave the deep dives for module 02. This lecture is raw, basic writes plus an honest inventory of guarantees.

## kgo.Client - the long-lived object

The first reflex of an HTTP developer is to open a connection, send a request, close it. Kafka does not work like that. `kgo.Client` in franz-go is a **long-lived** object: one per process, one per service. `order-service` has exactly one such client created at startup, living until shutdown.

Inside the client lives a connection pool to the brokers, a topic metadata cache (which broker is currently leader for each partition), and background goroutines for batching, sending, and retrying. Creating the client is expensive - dial to brokers, exchange of metadata requests, warm-up. Writing through an existing client is cheap - you place a `Record` in a buffer, the background does the rest.

A backend analogy. `kgo.Client` is closest to `*sql.DB` in Go: also not a single physical connection but a pool, also long-lived, also with background tasks. Nobody calls `sql.Open` and `db.Close()` on every SQL query - and the same applies here.

Basic initialization:

```go
cl, err := kgo.NewClient(
    kgo.SeedBrokers("localhost:19092", "localhost:19093", "localhost:19094"),
    kgo.ClientID("order-service"),
)
if err != nil { ... }
defer cl.Close()
```

`SeedBrokers` are **entry points**, not "the full broker list". The client connects to any of the listed addresses, fetches the current list of all cluster nodes through it, and works with that from then on. One to three addresses are enough; in the course we list all three in case one is down at startup.

In the lecture code the client is created through `internal.kafka.NewClient` - a wrapper with course-level defaults. SeedBrokers come from `KAFKA_BOOTSTRAP`, ClientID is `lectures`, plus sensible dial and retry timeouts. For non-default options, pass them as the second argument - they are appended last and can override the defaults (transactional producers in [Transactions and EOS](../../../../04-reliability/04-01-transactions-and-eos/i18n/en/README.md) use this).

## kgo.Record - what we actually write

`kgo.Record` is a struct that describes a single message. Not JSON, not protobuf, not a string - just a description of bytes plus addressing.

```go
rec := &kgo.Record{
    Topic:     "brew.orders.v1",
    Key:       []byte("order-7"),
    Value:     []byte(`{"type":"OrderPlaced","order_id":"order-7","shop":"baker-st","total":420}`),
    Headers:   []kgo.RecordHeader{{Key: "type", Value: []byte("OrderPlaced")}},
    Partition: -1, // -1 = let the partitioner decide; an explicit partition number also works
}
```

Key properties to call out up front:

- `Key` and `Value` are `[]byte`. Serialization (JSON / Protobuf / Avro) is your responsibility. The broker does not parse the data and does not validate the type: for it, this is just bytes with a length.
- `Topic` is required. `Partition` usually is not - the partitioner picks it based on the key (covered in [Keys and partitioning](../../../../02-producer/02-01-keys-and-partitioning/i18n/en/README.md)).
- `Headers` are `[]byte` pairs alongside the payload. They hold `trace_id`, `message_type`, `source_service` - anything convenient to read without deserializing the body. Detailed coverage lives in [Errors, retries, and headers](../../../../02-producer/02-05-errors-retries-headers/i18n/en/README.md); here we only mention they exist.
- `Timestamp` can be set manually or left zero. In the zero case the broker substitutes its wall-clock at write time. The final value in the log is also affected by `message.timestamp.type` at the topic level (`CreateTime` vs `LogAppendTime`); for `brew.orders.v1` the default is `CreateTime`, and Brew sets the timestamp on the producer side.

After a successful write the broker returns the populated `Partition`, `Offset`, and `Timestamp` - the ones in the log. That is the "address" of the message in Kafka. From then on `OrderPlaced order-7` lives at the coordinate `brew.orders.v1` / partition=2 / offset=17 for as long as retention allows (for `brew.orders.v1` that is 30 days - see [Offsets and retention](../../../01-04-offsets-and-retention/i18n/en/README.md)).

## ProduceSync vs Produce

franz-go has two write forms. `Produce` is asynchronous, `ProduceSync` is synchronous. There is no difference in guarantees under the hood: both use the same network channel, the same batching, the same retry logic. The difference is in how the code learns the result.

`Produce` puts the record into the client's internal buffer and **returns immediately**. The callback reports delivery later:

```go
cl.Produce(ctx, rec, func(r *kgo.Record, err error) {
    if err != nil { /* log the error */ return }
    fmt.Printf("partition=%d offset=%d\n", r.Partition, r.Offset)
})
```

You can fire a million calls in milliseconds; the client batches and sends them on its own. This is the "fast path" for hot code: the calling goroutine does not block; errors are handled separately in the callback.

`ProduceSync` blocks until the record receives an acknowledgement from the broker and returns a `kgo.ProduceResults` slice:

```go
res := cl.ProduceSync(ctx, rec)
if err := res.FirstErr(); err != nil { ... }
fmt.Printf("partition=%d offset=%d\n", res[0].Record.Partition, res[0].Record.Offset)
```

The "slow but straightforward path". Control returns only after the broker responds. You can pass several records in a single call - `ProduceSync(ctx, rec1, rec2, rec3)` - and get back a slice of results in the same order. For a teaching lecture where you want to see partition+offset right after the line `OrderPlaced`, ProduceSync is ideal.

In production on hot paths the usual choice is `Produce`. On cold paths, like "send one welcome email", `ProduceSync` is sometimes used - the synchronous overhead costs nothing, and the code is simpler. The batching and throughput question is covered separately in [Batching and throughput](../../../../02-producer/02-04-batching-and-throughput/i18n/en/README.md).

## acks - three numbers, three stories

The `acks` parameter answers the question "when does the broker tell the producer that the record is accepted". Three values are allowed: `0`, `1`, `all`. They give **different guarantee levels and different latency**. Rather than memorizing a table, walk through them via Brew stories.

`acks=0` - "send and forget". The producer ships the packet to the socket and considers the record successful as soon as it lands in the TCP buffer. The broker acknowledges nothing. If the packet is lost in transit, the partition leader crashes, or the server refuses it - the producer never finds out. The record simply disappears.

A Brew story. When the promo team launched the "free coffee on Fridays" campaign, click metrics were written to `brew.clickstream.v1` with `acks=0`. Losing an individual click is a penny problem; analytics smooths it out hourly anyway. In return, write latency was 0.5ms instead of 5ms. No one complained until someone tried `acks=0` on payments and "lost" 200 transactions in a week. The lessons are documented in the [postmortem](../../../../04-reliability/04-04-retry-and-dlq/i18n/en/README.md); `acks` on payments is now `all` with no debate.

`acks=1` - "leader confirmed". The broker replies "got it" as soon as the partition leader writes the data to its disk (technically, to the page cache - fsync is a separate conversation). Replicas may not have caught up yet. If the leader crashes immediately after the response and failover picks a replica that did not yet pull the record, the data is lost.

This is the middle ground: latency is lower than `all`, the guarantee is stronger than `0`. Suitable for metrics and logs where "99.99% gets through" is fine. Too weak for business events: a cascading failure of kafka-1 at Brew once cost half a day of telemetry precisely because of `acks=1` on `brew.telemetry.v1`. After that telemetry moved to `acks=all` and the limits were re-evaluated.

`acks=all` - "all ISR confirmed". The broker waits until every replica in the ISR (see [Replication and ISR](../../../01-03-replication-and-isr/i18n/en/README.md)) acknowledges the record before responding to the producer. With `min.insync.replicas=2` and RF=3 that means: the record is on disk on at least two nodes by the time `ProduceSync` returns. Failover loses data only if **all** ISR replicas fall together - a scenario after which Brew has bigger problems than a lost order.

The price of `acks=all` is latency. It adds one network round-trip between the leader and followers in the same rack (1-3ms on the Brew sandbox). For critical topics that is a bargain. For metrics with hundreds of thousands of events per second it is noticeable.

In franz-go the default is already `acks=all` (the `kgo.RequiredAcks(kgo.AllISRAcks())` option). This lecture's producer relies on that default - the option is not set explicitly. `brew.orders.v1` and `brew.payments.v1` use `acks=all`, and nobody plans to change that. The detailed walkthrough of `acks` and related guarantees lives in [Acks and durability](../../../../02-producer/02-02-acks-and-durability/i18n/en/README.md).

## Idempotency - the double-charge story

In April the Brew payments team got two angry tickets back to back. Customers were charged twice: one order, two identical `PaymentReceived` records in `brew.payments.v1`. Analysts walked the logs - everything checks out: the producer in `payment-service` sent a record, caught a `connection reset`, retried, and the second attempt succeeded. The broker had no idea it was the same record - it wrote both.

The problem is called a **retry duplicate**. It is built into any at-least-once RPC semantics: the client does not know whether the request or the response was lost, and retries out of desperation. For payments this is a catastrophe.

The Kafka solution is the **idempotent producer** (`enable.idempotence=true`). The broker assigns each producer a `Producer ID` (PID) on first connection, and the producer numbers its messages within the session with a monotonic `sequence number`. The broker then sees an incoming record, checks the `(PID, sequence)` pair, and:

- if such a `(PID, sequence)` is already accepted - quietly responds "ok" and **does not write** the duplicate;
- if the sequence is the next in order - accepts the record and advances the counter;
- if the sequence is from the future (the producer skipped one) - replies with `OUT_OF_ORDER_SEQUENCE_NUMBER`, the producer rebuilds the batch;
- if the sequence is from the past beyond the window (5 batches back) - also an error, requires external resolution.

Idempotency is free in latency and almost free in throughput. franz-go has it **on by default** (it used to require `EnableIdempotence`; not anymore). That means our 10 `OrderPlaced` records will not be duplicated on a network loss - the broker sees the repeat by `(PID, seq)` and drops it. The full internals of the idempotent producer wait in [Idempotent producer](../../../../02-producer/02-03-idempotent-producer/i18n/en/README.md).

What idempotency does **not** cover. Duplicates between process restarts are possible: PID lives from connection to connection, and if the process dies between a retry and the acknowledgement, a new process gets a new PID and the broker cannot link its records to the old ones. It does not save you on the source side either: write the same event into your DB twice and the producer will dutifully ship both records to Kafka - from the PID/sequence point of view those are two distinct calls. A separate scenario is the zombie producer: a process hangs, someone forks its copy, both keep writing with different PIDs, both succeed.

The full "exactly once" answer is transactions, with a whole separate lecture in [Transactions and EOS](../../../../04-reliability/04-01-transactions-and-eos/i18n/en/README.md). Here we lock in the level: within a single producer session Kafka catches duplicates itself; between sessions it is the application's job.

## Batching and compression - what they buy in numbers

When `order-service` writes 10 orders per second, nothing interesting happens. When it writes 10000 orders per second during peak hours, the choice between `acks=all` with batching and `acks=all` without batching is a 5x difference in throughput.

The idea is simple. Each network round-trip to the broker costs ~1ms (LAN). One round-trip per record caps you around 1000 msg/s per partition - past that you are bound by RTT. Buffer records in RAM for 5ms and then send them as a single packet of 50-200 records, and the same thread does 200000 msg/s.

The parameters that drive this:

- `linger.ms` - how long to buffer before sending. The Java client default is 0 (immediate); franz-go's default is already 10ms, so the lecture's producer gets reasonable batching out of the box. Production ranges 5-20ms.
- `batch.size` (in franz-go that is `ProducerBatchMaxBytes`) - the per-batch byte cap. Default 1MB, larger for big payloads.
- `compression.type` - `none`, `gzip`, `snappy`, `lz4`, `zstd`. On Brew's JSON payloads zstd compresses 3-5x; the saving applies to network **and** broker disk.
- `max.in.flight.requests.per.connection` - how many unacknowledged batches to keep in flight at once. With the idempotent producer franz-go keeps it ≤5 to preserve order.

Numbers from the Brew sandbox (RF=3, `acks=all`, 1KB JSON payload, single partition):

| Config | Throughput | p99 latency |
|---|---|---|
| `linger=0`, no compression | ~1200 msg/s | 4ms |
| `linger=10ms`, no compression | ~85000 msg/s | 14ms |
| `linger=10ms`, zstd | ~140000 msg/s | 16ms |

Latency grew by ~10ms (that is the `linger`), throughput x70-x100. For a backend that is a very cheap deal: 10ms of delay in exchange for tens of times less load on the network and brokers.

Subtleties. On a partitioned topic batches are built **per-partition** - a batch lives in one partition. So link throughput growth to partition count: 12 partitions give 12 parallel batching channels. The full discussion lives in [Batching and throughput](../../../../02-producer/02-04-batching-and-throughput/i18n/en/README.md), including how `linger` interacts with `acks=all` and idempotency.

In our teaching producer we write 10 messages one at a time and tune neither `linger` nor compression. Latency is interesting; throughput is not. The franz-go defaults are fine.

## Message key and partition assignment

The `Key` field in `kgo.Record` is the **steering wheel for the partitioner**, not a random identifier. By default (sticky-hash) the partitioner computes `hash(key) mod N` and places the record in the chosen partition. The same key always lands in the same partition (as long as the partition count does not change), which preserves the order of records sharing that key.

For `brew.orders.v1` this is critical. The key is `order_id`: all events for one order (`OrderPlaced`, `PaymentReceived`, `OrderReady`, `OrderDelivered`) land in the same partition and are read strictly in order. If the key were `shop_id`, you would get balance across shops, but the order sequence within one order would scatter across partitions. The full breakdown of candidates and trade-offs was in [Topics and partitions](../../../01-02-topics-and-partitions/i18n/en/README.md), with deeper detail in [Keys and partitioning](../../../../02-producer/02-01-keys-and-partitioning/i18n/en/README.md).

If `Key` is empty, the partitioner runs in round-robin mode: records spread across partitions evenly, but order is not guaranteed for any subgroup. That is fine for metrics and logs, bad for business events.

Headers and timestamp are mentioned in one phrase here: `Headers` are metadata pairs alongside the payload (`trace_id`, `message_type`); `Timestamp` is whatever the producer set or whatever the broker substituted. The detailed walkthrough is in [Errors, retries, and headers](../../../../02-producer/02-05-errors-retries-headers/i18n/en/README.md).

## What our code does

`cmd/producer/main.go` does exactly what was promised.

1. Creates a `kgo.Client` via the shared helper.
2. Creates the topic `brew.orders.v1` idempotently: `partitions=3`, `rf=3`. If it already exists, it moves on silently.
3. Loops from 0 to 9, builds a `kgo.Record` with `Key="order-N"`, `Value="OrderPlaced order_id=order-N"`, and writes via `ProduceSync`.
4. After each write, prints a table row - N, KEY, VALUE, PARTITION, OFFSET, BROKER-TS.
5. After the loop, calls `kadm.ListEndOffsets`, prints the per-partition latest and the total. On a freshly created topic the total equals exactly 10 - clear proof the records landed.

The write loop itself is the "bare" producer work of the course:

```go
for i := 0; i < o.messages; i++ {
    key := fmt.Sprintf("order-%d", i)
    val := fmt.Sprintf("OrderPlaced order_id=order-%d", i)
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
    fmt.Fprintf(tw, "%d\t%s\t%s\t%d\t%d\t%s\n",
        i, key, val, got.Partition, got.Offset,
        got.Timestamp.Format("15:04:05.000"))
}
```

`got.Partition` and `got.Offset` are **what the broker returned**, not what we requested. Those are the coordinates in the log. The partition came from the key via `hash(key) mod N`; the offset was issued by the partition leader at write time (for earliest/latest and LEO/HWM see [Offsets and retention](../../../01-04-offsets-and-retention/i18n/en/README.md)).

After the loop - a final check via `ListEndOffsets`:

```go
ends, err := admin.ListEndOffsets(rpcCtx, topic)
ends.Each(func(o kadm.ListedOffset) {
    fmt.Fprintf(tw, "%d\t%d\n", o.Partition, o.Offset)
    total += o.Offset
})
fmt.Fprintf(tw, "TOTAL\t%d\n", total)
```

The sum of latest offsets across all partitions of a fresh topic equals the number of written messages. Run the program a second time and the sum becomes 20, and so on.

What you will see in the output (the Russian phrases are intentional - the Go program prints them as-is; this README explains them):

```
brew-topic "brew.orders.v1" создан: partitions=3 rf=3

пишем 10 OrderPlaced в топик "brew.orders.v1" через ProduceSync

N  KEY      VALUE                              PARTITION  OFFSET  BROKER-TS
0  order-0  OrderPlaced order_id=order-0        0          0       16:55:01.234
1  order-1  OrderPlaced order_id=order-1        2          0       16:55:01.241
2  order-2  OrderPlaced order_id=order-2        1          0       16:55:01.247
3  order-3  OrderPlaced order_id=order-3        0          1       16:55:01.253
4  order-4  OrderPlaced order_id=order-4        2          1       16:55:01.259
5  order-5  OrderPlaced order_id=order-5        1          1       16:55:01.265
6  order-6  OrderPlaced order_id=order-6        0          2       16:55:01.271
7  order-7  OrderPlaced order_id=order-7        2          2       16:55:01.277
8  order-8  OrderPlaced order_id=order-8        1          2       16:55:01.283
9  order-9  OrderPlaced order_id=order-9        0          3       16:55:01.289

готово. Смотрим ту же картину со стороны лога:
PARTITION  LATEST
0          4
1          3
2          3
TOTAL      10
```

A few observations from this output.

Each record got **its own** offset within its partition. `OrderPlaced` for `order-0`, `order-3`, `order-6`, `order-9` landed in partition 0 with offsets 0, 1, 2, 3 - four messages, latest=4. Partition 1 has three messages, latest=3. The sum of latest across all partitions is 10. Everything adds up.

Partition assignment is **deterministic**, not random. The same `order_id` always lands in the same partition. Restart the program with the same set of keys and the distribution repeats (but offsets advance because it is a new write on top of the existing log). The partitioner logic is covered in [Keys and partitioning](../../../../02-producer/02-01-keys-and-partitioning/i18n/en/README.md).

If you ran the program twice without `topic-delete`, the specific offsets in your output will differ - all offsets will be 10 higher in total because the second run appended on top of the first. That's expected. The key-to-partition mapping stays the same: the same `order-N` always lands in the same partition.

## Running

The sandbox must be up (`docker compose up -d` from the repo root).

```sh
make run
```

In a second terminal you can watch the same topic via the CLI consumer in parallel:

```sh
make consume-cli
```

This is `kafka-console-consumer.sh` running inside `kafka-1` with the `--from-beginning` flag. It prints partition, offset, key, and value. You should see the same 10 `OrderPlaced` our program produced - Kafka does not distinguish a "message from a Go client" from a "message from kafka-console-producer"; they look identical in the log.

Describe the topic via `kafka-topics.sh`:

```sh
make topic-describe
```

Clean up after the lecture:

```sh
make topic-delete
```

## What to take away

This is the foundation everything in module 02 builds on. After this lecture the mental model should be:

1. **kgo.Client is long-lived.** Create it once, reuse it until shutdown. This is not an HTTP request.
2. **kgo.Record is bytes plus addressing (topic + key + headers + timestamp).** You own serialization. The broker does not check the type.
3. **`acks` defines the guarantee level.** `0` - fire-and-forget (metrics, clickstream). `1` - leader wrote (compromise). `all` - all ISR confirmed (business events). `brew.orders.v1` and `brew.payments.v1` use `all`.
4. **Idempotency is on by default.** Within a single producer session Kafka itself catches duplicates by `(PID, sequence)`. Between sessions you need transactions - see [Transactions and EOS](../../../../04-reliability/04-01-transactions-and-eos/i18n/en/README.md).
5. **The broker issues the offset, not the client.** The returned `(partition, offset)` pair is the message's coordinate in the log for its entire life until retention sweeps it.

Next up - [First consumer on franz-go](../../../01-06-first-consumer/i18n/en/README.md). We will read those 10 `OrderPlaced` records from the `kitchen-service` side, confirm the offsets in the output match what `ProduceSync` returned, and through `auto-commit` get a first look at what a consumer group's committed offset really means.
