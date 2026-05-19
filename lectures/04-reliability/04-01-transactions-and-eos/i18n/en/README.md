# 04-01 — Transactions & EOS

The previous module covered committing offsets and processing messages at-least-once. Duplicates. Idempotent handlers. A dedup table in Postgres. That works, but there's a direct gap: when each incoming message produces one or more new messages in Kafka, ordinary commits cannot guarantee that the whole batch either appears or doesn't appear at all.

Say the `orders` handler must produce a record to `payments` and a record to `shipments`. First, write the payment — success. Between the two `Produce` calls the process crashes. On restart the offset is not committed. The handler starts the order over. Writes the payment again — a second time. Then the shipment. No crash this time. Even if both `Produce` calls were idempotent via producer-id, the restart gave us a new producer with a new id — idempotency didn't help. System state is split.

Kafka transactions address this. They provide atomic multi-partition write — a group of `Produce` requests that either all become visible to the consumer or are all discarded, plus a way to bind a consumer offset commit to that group (covered in the next lecture, [Consume-process-produce](../../../04-02-consume-process-produce/i18n/en/README.md)). This lecture covers transactions themselves and the foundation: transactional.id, producer epoch, control records, read isolation.

## TransactionalID and producer epoch

The idempotent producer from the [Idempotent producer](../../../../02-producer/02-03-idempotent-producer/i18n/en/README.md) lecture is just a `producer-id` plus per-partition sequence numbers. The broker sees "hello, I'm the same producer, don't duplicate my record" within one session. But `producer-id` only lives as long as the client does. Process restarts — new id, no memory of the previous session.

Transactions add `transactional.id` on top — a stable, human-readable identifier you set yourself. For a service with N instances, the usual pattern is `<service>-<instance-id>` or `<service>-<consumer-group>-<partition>`. The key property: stable across restarts and unique per logical role.

On the first `BeginTransaction` the client contacts the transaction coordinator (a designated broker, selected by hash of `transactional.id`) and requests a `producer-id` and `epoch`. The coordinator records "this transactional.id belongs to epoch=N", and as long as there is one connection — everything is clean. But what if the process went into a GC pause for 30 seconds, we decided it was dead, and started a new one? The new process with the same `transactional.id` calls the coordinator → the coordinator increments the epoch to N+1. If the old process wakes up and tries to write something under the old epoch — the coordinator returns `InvalidProducerEpoch` (or `ProducerFenced`). Every write and every `EndTransaction` from the old producer is rejected. It's a zombie. Nothing broken gets through.

That's zombie fencing. Without it, exactly-once would mean: "we have guarantees as long as nothing restarts." Useless.

```
producer A      coord                  producer B
   |    pid=42, epoch=1                    |
   |---- BeginTxn -------->                |
   |    OK, epoch=1                        |
   |    [GC pause]                         |
   |                       <----- pid=42, epoch=2 (B starts)
   |                                       |
   |    [woke up, writing]                 |
   |---- Produce(pid=42, e=1) ->           |
   |    InvalidProducerEpoch ❌            |
```

The demo is in `cmd/zombie-fence/main.go`. Run two processes with `-transactional-id=lecture-04-01-zombie` in sequence; the first gets `FENCED` after the second starts and exits.

The first process loop is a bare Begin → Produce → EndTransaction:

```go
if err := cl.BeginTransaction(); err != nil {
    return fmt.Errorf("BeginTransaction: %w", err)
}

results := cl.ProduceSync(ctx, &kgo.Record{
    Topic: o.topic,
    Key:   []byte(o.role),
    Value: []byte(fmt.Sprintf(`{"role":%q,"attempt":%d}`, o.role, attempt)),
})
if produceErr := results.FirstErr(); produceErr != nil {
    _ = cl.EndTransaction(ctx, kgo.TryAbort)
    return produceErr
}

return cl.EndTransaction(ctx, kgo.TryCommit)
```

In the output, look for the string `FENCED` in the first process after the second one starts. The real-world error is either `ProducerFenced` or `InvalidProducerEpoch`; the franz-go client returns it immediately from ProduceSync or from EndTransaction. We catch both:

```go
func isFenced(err error) bool {
    return errors.Is(err, kerr.ProducerFenced) ||
        errors.Is(err, kerr.InvalidProducerEpoch)
}
```

The zombie then has two honest options — crash with an alert or exit silently (in production, usually the former, so the orchestrator doesn't leave the process spinning idle).

## Atomic multi-partition write

The base scenario: write to N topics (or N partitions of one topic, it doesn't matter), and we need an all-or-nothing guarantee. Without transactions there's no atomicity: each `Produce` is a separate network round-trip. Between them the process may die, the partition leader may die, a timeout may fire, a network partition between client and broker may open.

Inside a transaction, it works like this. On the first `Produce` to a new partition, the client sends `AddPartitionsToTxn` to the coordinator — "this partition is now part of my transaction with epoch=N". The coordinator records it. Then a normal `Produce` goes to the partition leader. Records land on disk just like any others. You cannot distinguish a transactional record from a non-transactional one by the data itself.

The decisive step is `EndTransaction`. The coordinator takes the list of all partitions it collected via `AddPartitionsToTxn` for that epoch and sends each of them a control record — a special internal batch with a `COMMIT` or `ABORT` marker. These markers are written into the normal partition log and have their own offset. They cannot be read as user records — fetch filters control records out — but they do occupy space in the log.

The demo is `cmd/transactional-producer/main.go`. On each attempt it sends three linked records to three topics:

1. `tx-orders` — the order itself (status "created");
2. `tx-payments` — a payment instruction for the same `order_id`;
3. `tx-shipments` — a shipment task.

Then it flips a coin: commit or abort. At the end it prints the counters.

The attempt core:

```go
if err := cl.BeginTransaction(); err != nil {
    return false, fmt.Errorf("BeginTransaction: %w", err)
}

orderID := strconv.Itoa(attempt)
produceErr := produceTriple(ctx, cl, orderID)

wantCommit := rand.Float64() < commitProb
if produceErr != nil {
    wantCommit = false // commit in this state would fail anyway
}

commit := kgo.TryAbort
if wantCommit {
    commit = kgo.TryCommit
}
return wantCommit, cl.EndTransaction(ctx, commit)
```

And `produceTriple` itself — three records at once via `ProduceSync`:

```go
results := cl.ProduceSync(ctx,
    &kgo.Record{Topic: topicOrders,    Key: []byte(orderID), Value: orderJSON},
    &kgo.Record{Topic: topicPayments,  Key: []byte(orderID), Value: paymentJSON},
    &kgo.Record{Topic: topicShipments, Key: []byte(orderID), Value: shipmentJSON},
)
return results.FirstErr()
```

Run:

```sh
make topic-create-all
make run-tx-producer ATTEMPTS=20 COMMIT_PROB=0.7
```

The output lists `[#XX] commit ✓` and `[#XX] abort ✗` entries, and a final summary with the end-offset delta per topic. With 14 commits and 6 aborts, the total end-offset shift is roughly (20 × 3 records) + (20 × 3 control markers) = 120 across all three topics. Only a read_committed consumer sees the actual useful records, and there will be exactly 14 × 3 = 42 of them.

## TransactionTimeout

The coordinator doesn't trust the producer forever. If the producer started a transaction and disappeared, the coordinator aborts it after the timeout the client passed at init time. The franz-go v1.21.0 default for `TransactionTimeout` is 40 seconds (`pkg/kgo/config.go:603`); the sandbox raises it to one minute via `kgo.TransactionTimeout(60*time.Second)` in the producer code so three records to three topics plus markers fit with margin. The coordinator-side timeout protects read_committed consumers from unnecessary blocking: they wait for a commit or abort marker, and without it they would stall indefinitely.

If you spend a long time doing work inside a transaction (reading, enriching, writing back) - increase `kgo.TransactionTimeout`. Don't confuse it with the broker-side `transaction.max.timeout.ms` - that caps what the client is allowed to request. Default 15 minutes (Kafka 4.2.0 on the sandbox reports `transaction.max.timeout.ms=900000`).

## Isolation: read_committed vs read_uncommitted

The consumer has `isolation.level`. The Kafka default is `read_uncommitted`: read everything in the log as soon as it arrives. No waiting for markers. Transactional records are delivered as soon as the producer wrote them — even if the transaction is later aborted. This level is for cases where transactions don't concern you.

`read_committed` is different. On fetch, the broker delivers to the consumer only those transactional batches that already have a commit marker. Aborted batches disappear entirely (their offsets are consumed — from the client's perspective they don't exist in the stream). Records from a pending transaction (commit hasn't arrived yet) are also withheld — fetch delivers everything up to the so-called LSO (last stable offset), which is the minimum offset of any still-open transaction. So one stalled producer can "freeze" the entire partition for read_committed consumers until its timeout. That's the price of the guarantees.

The demo is `cmd/read-committed/main.go`, switched with the `-isolation` flag:

```go
opts := []kgo.Opt{
    kgo.ConsumerGroup(o.group),
    kgo.ConsumeTopics(o.topics...),
    kgo.FetchIsolationLevel(level), // ReadCommitted() or ReadUncommitted()
    kgo.ClientID("lecture-04-01-rc"),
    kgo.DisableAutoCommit(),
}
```

Hands-on scenario:

```sh
make topic-create-all
# terminal 1 — produce 20 transactions, 70% commit
make run-tx-producer ATTEMPTS=20 COMMIT_PROB=0.7

# terminal 2 — what read_committed sees
make run-rc-consumer COUNT=100 IDLE=3s

# terminal 3 — what read_uncommitted sees
make run-ru-consumer COUNT=100 IDLE=3s
```

Terminal 2 will show ~14 × 3 = 42 records (from committed transactions). Terminal 3 will show all 60, because for uncommitted there's no difference between "commit went through" and "aborted later". Same cluster state — two different views of the world. That's what isolation level means.

## What transactions do NOT provide

When people say "exactly-once in Kafka", it's useful to know where the boundary is.

1. **End-to-end EOS — Kafka↔Kafka only**. If a consumer reads a topic, does something, and writes to another topic — yes, a transaction (plus `SendOffsetsToTransaction` from [Consume-process-produce](../../../04-02-consume-process-produce/i18n/en/README.md)) gives you an atomic act of "read → write → commit offset". But if you call an HTTP API or write to Postgres without the outbox pattern in between — the Kafka transaction knows nothing about that external write. External sides need separate mechanisms (outbox in [Outbox pattern](../../../04-03-outbox-pattern/i18n/en/README.md), idempotent receivers in [Delivery to external systems](../../../04-05-external-delivery/i18n/en/README.md)).

2. **Transaction ≠ "won't fail"**. A transaction fails cleanly: it either commits or aborts. If the process dies mid-transaction before `EndTransaction`, the coordinator aborts it on timeout. No magic will fill in the missing half of the records. For the scenario to be correct, your code must be able to retry with the same input — i.e., be idempotent at the business logic level.

3. **Throughput cost is real**. Each `EndTransaction` is a round-trip to the coordinator, then marker writes to each partition, fsyncs under the markers, plus coordination with participants. Per Confluent load tests — typically 3–10% overhead against pure acks=all. Not catastrophic, but not free.

4. **On the consumer side, pending transactions freeze read_committed**. If one producer goes into a long transaction (or simply stalls), all read_committed readers of the partitions it joined will see a pause. On metrics this looks like lag that doesn't drop. Fix it with a short `transaction.timeout.ms` or by monitoring `LastStableOffset`.

5. **`transactional.id` outlives the process**. If you chose `transactional.id = "service-instance-7"` and instance 7 died permanently, its id stays in the coordinator with an open transaction until timeout. That's why ids are usually derived from the logical role (a common trick: the input topic partition number). Binding to a k8s pod-id produces a zombie id on every pod restart — don't do that.

## Lead-in to [Consume-process-produce](../../../04-02-consume-process-produce/i18n/en/README.md)

We can now atomically write to N partitions. But the classic pattern is broader: "read → process → write → commit offset", and we need to include the consumer offset commit inside the transaction too. Otherwise a restart fits into the window "already written, not yet committed", and we get a duplicate. This combination — `SendOffsetsToTransaction` plus read_committed on the downstream consumer — is called consume-process-produce and is covered in the next lecture. This lecture gave you the bricks; next comes the wall.

## Lecture files

- `cmd/transactional-producer/main.go` — Begin → 3× Produce → End with a random commit/abort and an end-offset summary.
- `cmd/zombie-fence/main.go` — two processes sharing the same `transactional.id`; the first catches the fence after the second starts.
- `cmd/read-committed/main.go` — a consumer on three transactional topics, toggled between read_committed and read_uncommitted by a flag.
- `Makefile` — `topic-create-all`, `run-tx-producer`, `run-zombie-1`/`run-zombie-2`, `run-rc-consumer`/`run-ru-consumer`, `clean`.

## Commands for the run

```sh
# Setup
make topic-create-all

# 1. Atomic multi-partition write
make run-tx-producer ATTEMPTS=20 COMMIT_PROB=0.7
# in a separate terminal — what read_committed sees
make run-rc-consumer COUNT=100 IDLE=3s
# and what read_uncommitted sees
make run-ru-consumer COUNT=100 IDLE=3s

# 2. Zombie fencing — two terminals with the same txn-id
make run-zombie-1     # terminal A
# after 3–5 seconds:
make run-zombie-2     # terminal B
# terminal A should catch FENCED and exit

# Cleanup
make clean
```
