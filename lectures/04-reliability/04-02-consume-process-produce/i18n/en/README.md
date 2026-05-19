# 04-02 — Consume-Process-Produce

The previous lesson covered transactional writes to multiple topics as a single atomic operation. That's half the solution. The other half: what to do when you're also reading from Kafka. Read → process → write → commit offset. This cycle lives in every other service, and it's trickier than it looks.

Say there's a consumer on the `orders` topic. It reads an order, enriches it (DB lookup, mixes in a customer profile, calculates a discount, checks fraud score). Then writes the result to `orders-enriched`. Then wants to tell the broker "I processed offset N, move on." Between these steps are three potential failure points, each producing its own type of inconsistency.

Scenario one: crash AFTER produce, BEFORE offset commit. On restart — offset didn't move, we read the same order again, re-enrich it, write it a second time. Duplicate on output. The idempotent producer from [Idempotent producer](../../../../02-producer/02-03-idempotent-producer/i18n/en/README.md) doesn't help here: the new process has a different producer-id, sequence numbers start from zero.

Scenario two: crash AFTER offset commit, BEFORE produce. On restart — offset is past this order, we won't come back to it. Gap on output.

Scenario three: both produce and commit happened, but in different order relative to the crash — you get a duplicate or a gap depending on which came first. Without an atomic link between "wrote records to output" and "advanced the offset in input" — exactly-once is impossible. It's at-least-once with duplicates or at-most-once with losses. There's no third option. Until you pull both steps into a single transaction.

In the transaction from [Transactions and EOS](../../../04-01-transactions-and-eos/i18n/en/README.md) it was `BeginTransaction → Produce → EndTransaction(Commit)`. Here another participant is added — the group offset commit. Kafka can write it into the same transaction via a special request `TxnOffsetCommit`. If the transaction commits, both effects (records in output and offset in `__consumer_offsets`) become visible atomically. If it aborts — neither appears. At the output level, a read_committed consumer won't see them; at the offset level, the group stays where it was. On restart — the same input records are read again. Re-enriched. Written to output again. From outside, it looks as if processing happened exactly once.

## GroupTransactSession

At the Kafka wire-protocol level, this is a `TxnOffsetCommit` request to the group coordinator inside an open transaction, plus careful rebalance handling. The Java client exposes `producer.sendOffsetsToTransaction(offsets, groupMetadata)` for this. franz-go intentionally does NOT export its equivalent (`commitTransactionOffsets` in `pkg/kgo/txn.go:939`) - the comment plainly says «gigantic footgun if not done properly». The only public path to an EOS consumer in franz-go v1.21.0 is the wrapper `kgo.GroupTransactSession`, which does three useful things:

1. Takes the current consumer offsets from its group state and puts them into the transaction via `TxnOffsetCommitRequest`.
2. Wraps rebalance handling in its own logic. If a revoke arrives during a transaction — `End(TryCommit)` returns `committed=false` and aborts the transaction to avoid committing an offset on a partition we no longer own. This is critical: without this guard, two consumers playing the same partition produce duplicates.
3. Performs a Flush before End on the commit path, so all Produce calls reach the broker.

The loop itself looks almost like a normal consume + produce, just with Begin/End around the batch:

```go
for {
    fetches := sess.PollFetches(pollCtx)
    if fetches.Empty() { continue }

    if err := sess.Begin(); err != nil {
        return fmt.Errorf("Begin: %w", err)
    }

    fetches.EachRecord(func(r *kgo.Record) {
        enriched := enrich(r)
        sess.Produce(ctx, &kgo.Record{
            Topic: o.output,
            Key:   r.Key,
            Value: enriched,
            Headers: []kgo.RecordHeader{
                {Key: "source.topic", Value: []byte(r.Topic)},
                {Key: "source.partition", Value: []byte(fmt.Sprintf("%d", r.Partition))},
                {Key: "source.offset", Value: []byte(fmt.Sprintf("%d", r.Offset))},
            },
        }, /* promise */)
    })

    committed, err := sess.End(ctx, kgo.TryCommit)
}
```

`End(TryCommit)` atomically performs three steps:

1. flush the producer buffer so all Produce calls reach the broker
2. `TxnOffsetCommit` for the group's current positions — writes them to the coordinator as part of our transaction
3. `EndTxnRequest(commit)` to the coordinator — after this request, changes become visible to read_committed consumers

If any step fails — `committed=false` is returned, and externally that means "start from the same offset."

## Consumer-side configuration

EOS requires two important flags.

`kgo.FetchIsolationLevel(kgo.ReadCommitted())` — read only from committed transactions. This controls which records the broker delivers. Our pipeline is already EOS on the write side, but if the INPUT topic is written by another transactional producer — without this flag we'll read records from uncommitted transactions, try to process them, and if that upstream transaction aborts — our output will contain records that never existed on the input. Classic antipattern.

`RequireStableFetchOffsets` — previously a separate flag, in franz-go 1.21 it's enabled permanently by default (see config.go: "Deprecated: now permanently enabled"). It ensures fetch doesn't return records for which the group coordinator is not yet "sure" — meaning an offset commit is still in-flight in a parallel transaction. Without this mechanism, two groups reading the same topic could temporarily diverge in position, and one of them would read the same record twice.

One more point — `TransactionalID`. A stable per-role identifier that survives restarts. If you have two instances of the same consumer, each must have its own `transactional.id`, typically tied to `<service>-<member-id>` or to the partition assignment. If both take the same id — the second will evict the first via zombie fencing (see [Transactions and EOS](../../../04-01-transactions-and-eos/i18n/en/README.md)), and one of the roles will stop working.

## What the code demonstrates

The directory has two binaries — `cmd/cpp-pipeline` and `cmd/downstream-rc`. The pipeline reads `cpp-orders`, enriches each record (mock — appends `vip` based on key prefix), and writes the result to `cpp-orders-enriched`. Downstream is a simple read_committed consumer on the output. Counts unique keys, checks for duplicates.

The key part of the pipeline is the `GroupTransactSession` setup:

```go
opts := []kgo.Opt{
    kgo.SeedBrokers(seeds...),
    kgo.TransactionalID(o.txnID),
    kgo.TransactionTimeout(60 * time.Second),
    kgo.ConsumerGroup(o.group),
    kgo.ConsumeTopics(o.input),
    kgo.FetchIsolationLevel(kgo.ReadCommitted()),
    kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
}
sess, err := kgo.NewGroupTransactSession(opts...)
```

`NewGroupTransactSession` is `NewClient` plus correctly wired `OnPartitionsRevoked` / `OnPartitionsLost` hooks, so `End` can detect "we were kicked from the group mid-transaction" and return `committed=false`.

The crash simulation sits between `Flush` and `End`. The idea: records are already in the output log, but the commit marker hasn't been written yet. The coordinator will abort "our" transaction on timeout, leaving the records orphaned in the log:

```go
if err := sess.Client().Flush(ctx); err != nil {
    return fmt.Errorf("flush: %w", err)
}

if o.crashProb > 0 && rand.Float64() < o.crashProb && batchOut > 0 {
    fmt.Fprintf(os.Stderr, "💥 crash before End: %d records already in output log, ...\n", batchOut)
    os.Exit(2)
}

committed, err := sess.End(ctx, kgo.TryCommit)
```

Without an explicit `Flush`, Produce would be async batching — records wouldn't reach the broker before `os.Exit`, and read_uncommitted wouldn't see the abort "traces." For the demo we need a visible effect, so we force the write to the broker.

## Demo

Bring up the sandbox, create topics, seed 30 orders to input, run the pipeline with a guaranteed crash so the first transaction definitely aborts.

```sh
make topic-create-all
make seed SEED_COUNT=30
make run-pipeline-crash CRASH_PROB=1.0   # crashes before End on the first transaction
```

What you'll see: the pipeline read N records (where N is records from the first partition encountered), delivered them to output via `Flush`, printed "💥 crash", and exited with `os.Exit(2)`. The output now has data — without a commit marker.

Immediately (before the transaction timeout expires) run both consumers. First, read_committed:

```sh
make run-downstream
```

Shows 0 records. The broker holds them back on the fetch side — they're beyond the last stable offset, the transaction has neither an abort nor a commit marker yet.

```sh
make run-downstream-ru
```

Shows exactly those N records the pipeline sent before `os.Exit`. This is the point of an aborted transaction in the log: data is physically written, but logically doesn't exist for read_committed.

Now wait 60 seconds (our `TransactionTimeout`) for the coordinator to abort the orphaned transaction. You don't have to wait — the next pipeline run with the same `transactional.id` will trigger zombie fencing and speed everything up. The second instance bumps the first's epoch. The coordinator immediately writes an abort marker for the orphaned transaction, and the rest of the input becomes readable without waiting.

```sh
make run-pipeline-crash CRASH_PROB=0     # runs without crashes, picks up the remainder
```

The pipeline starts, sees that the committed offset for its group is past the first batch of the first partition (one that was successfully committed earlier — if any; on a fresh demo there's none). Reads the remaining 30-N records. Processes them and commits the transactions. The output has 30 unique keys.

Check again:

```sh
make run-downstream      # 30 records, 30 unique keys, 0 duplicates
make run-downstream-ru   # 30 + N (aborted-transaction records remain in the log)
```

This is EOS on the consumer side for downstream. The aborted records remain physically in the log, occupying offsets, but a read_committed client will never return them. Until log retention.

## Limitations

The EOS we built here is about Kafka↔Kafka. If the pipeline touches anything outside Kafka (a DB write or a call to a downstream service) — the external side doesn't participate in the transaction. It may execute while the Kafka transaction aborts. On restart, the pipeline will repeat it. If the external side isn't idempotent — double email. Kafka EOS won't help here. Other approaches help — the outbox pattern (the next lesson covers it, [Outbox pattern](../../../04-03-outbox-pattern/i18n/en/README.md)), idempotent handlers on the external receiver side. XA transactions theoretically solve this too, but in practice they're rarely used — too much operational overhead.

The second limitation is fetch-offset reset. If the pipeline consumer first arrives on a topic while the input has an active transactional producer with long in-flight transactions — our fetch will hit the last stable offset and stall. Fix it with a short `TransactionTimeout` on the source, or by starting from a specific known position instead of waiting for LSO.

Last — `TransactionTimeout`. We explicitly set 60 seconds (`pkg/kgo/config.go:603` — the franz-go v1.21.0 default is 40 seconds, we override it to match the Java client default `transaction.timeout.ms=60000`). If batch processing takes longer, the coordinator will abort the transaction internally, and `End(TryCommit)` returns `InvalidTxnState`. The broker-side ceiling is `transaction.max.timeout.ms`, default 15 minutes (`kafka-configs.sh --describe` on the Kafka 4.2.0 stand). If processing is heavy (ML model, large DB batch), raise the timeout along with `delivery.timeout.ms` on the downstream — and not above the broker ceiling.

## Full run

```sh
make topic-create-all
make seed SEED_COUNT=100

# run with various crash-prob, restart until idle
make run-pipeline-crash CRASH_PROB=0.3
make run-pipeline-crash CRASH_PROB=0.3
make run-pipeline-crash CRASH_PROB=0       # final — no crashes, picks up the remainder

make run-downstream                         # 100 records, 100 unique keys
```

For understanding the mechanics, also useful:

- `make group-describe` — the pipeline group's committed offset after a series of crashes. Should match the input end-offset.
- `make end-offsets` — see the "extra" records in output (aborted) and control records (commit/abort markers).
- `make verify` — quick sanity check: compare input count and read_committed output count. They should be equal.
