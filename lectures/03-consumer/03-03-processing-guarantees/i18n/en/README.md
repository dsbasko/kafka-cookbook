# 03-03 — Processing Guarantees

In the previous lecture we tweaked commit offset knobs and caught duplicates of varying magnitude. No combination gave zero. It's not a bug in specific code — bare commits are designed so that duplicates are always possible; only the window changes. Here we cover a different approach. Let duplicates arrive. What matters is that they don't reach the business effect.

## Three levels of guarantees — and where to apply them

Literature on processing guarantees traditionally breaks the world into categories. The names are familiar: at-most-once and at-least-once on one end, exactly-once on the other. These labels work well on slides but they confuse, because they say nothing about **where** exactly the guarantee applies. A guarantee of what? Byte delivery? A DB write? A customer charge? A notification sent? These are different layers, each about something different.

Breaking it down by layer:

1. **Byte delivery from broker to your process.** Here at-least-once is the only realistic mode in production. Network hiccupped, request went out, ack was lost — there will be a retry. Bare Kafka and any client on top of it.
2. **Shifting the committed offset in `__consumer_offsets`.** Here you can play with at-most/at-least depending on when you commit — before or after processing (see the [Offset commits](../../../03-02-offset-commits/i18n/en/README.md) lecture). No third option at this layer.
3. **The processing effect in your application or external system.** Here you can achieve exactly-once — through a property of the handler itself, not through commit.

The third layer is the subject of this lecture. If the handler is idempotent, calling it again with the same input changes nothing — and it no longer matters that Kafka delivered the record twice. At the system level the result is the same.

## Idempotency is math, not magic

A function `f(x)` is idempotent if `f(x) == f(f(x))`. Apply it once — get a result. Apply it twice — get the same result. Apply it ten times — same.

Not every handler is like that by nature. `account.balance += 100` — no: two calls give +200. But `account.balance = 100` — yes, no matter how many times you call it. `INSERT INTO orders ...` — no. `INSERT ... ON CONFLICT DO NOTHING` on a unique key — yes.

Idempotency doesn't appear on its own. You have to design it into the handler: either choose an operation that is idempotent by nature (like SET instead of INCR), or put in a dedup — a table/index that cuts off repeats.

In this lecture — the second path. Each message from Kafka wants to insert into the `messages` table. But the table's primary key is `(topic, partition, offset)`. These three numbers uniquely identify a specific message in Kafka. If it already exists in the table — `INSERT ... ON CONFLICT DO NOTHING` silently skips it. The effect on the DB is the same, no matter how many times you try.

## Why `(topic, partition, offset)` as the dedup key

It seems natural to use a business key — an order id or customer_id. This sometimes works, but has an unpleasant edge case: the same business object can arrive in Kafka twice from different sources or with different update semantics. Then dedup by business key silences a legitimate "same id, new state".

`(topic, partition, offset)` is the address of a record in the Kafka log. It is guaranteed unique within a topic. The same offset in the same partition is **exactly the same record**. Byte-for-byte identical. A duplicate of that record on the consumer side can only appear from a restart without a commit. That is exactly what we want to cut off.

The downside of this approach — if you migrate data between topics or change the partitioning scheme, the key stops being stable (the offset will change). But that is a different class of problems — data migration — and is solved separately.

If the scenario allows — combine: business key for semantics plus an idempotency-key from the payload or headers that the producer generates once per record. Then even after re-partitioning the dedup survives.

## What our code does

The whole program is one poll-process-commit loop. The key thing inside is order: insert into DB **before** committing the offset in Kafka. If it crashes between these two points — on restart the message arrives again, but `ON CONFLICT` removes it. If it crashes before the insert — on restart the message arrives again and inserts normally.

Connection to Postgres via pgxpool, to Kafka — via franz-go with auto-commit disabled:

```go
opts := []kgo.Opt{
    kgo.ConsumerGroup(o.group),
    kgo.ConsumeTopics(o.topic),
    kgo.DisableAutoCommit(),
    // ...
}
```

The insert itself — a bare `pool.Exec` with SQL where the PRIMARY KEY does all the work:

```go
const insertSQL = `
INSERT INTO messages (topic, partition, "offset", payload, processed_at)
VALUES ($1, $2, $3, $4, NOW())
ON CONFLICT (topic, partition, "offset") DO NOTHING
`

tag, err := pool.Exec(ctx, insertSQL,
    r.Topic, int32(r.Partition), r.Offset, string(r.Value))
```

`tag.RowsAffected()` answers the question "was this a new record or a repeat". One — new, zero — duplicate, already processed at some point. In the demo we print this (INSERT vs DUP) to make the protection effect visible.

After all records in the batch have gone through the insert (some as INSERT, some as DUP — we don't care), we commit the offset:

```go
err := cl.CommitRecords(commitCtx, batch...)
```

Only now does Kafka "forget" these offsets — they move into the discarded past for our group.

## Where the crash we simulate happens

The code has `-crash-after N`. When the processed counter reaches N — `os.Exit(1)` with no `Close`, no commit. Important detail: this happens **between the insert and the commit**. The insert has already executed, the transaction in Postgres has committed (we use pgx autocommit), and Kafka still doesn't know we reached that offset. The scenario:

```
PollFetches → 11 records in batch
  insert #1   ✓
  insert #2   ✓
  ...
  insert #10  ✓
  os.Exit(1)             ← crash, NO CommitRecords
```

On restart Kafka delivers the same batch from the beginning (committed offset didn't advance). We go into the second insert of the same record — `ON CONFLICT DO NOTHING` — `RowsAffected()=0` — DUP in the log. The business effect happened exactly once.

## How to run the scenario manually

First, bring up the sandbox:

```sh
make up                    # Postgres from docker-compose.override.yml
make db-init               # messages table with PK (topic, partition, offset)
make topic-create          # topic with 3 partitions
make topic-load            # 30 messages (k-1..k-30 → event-1..event-30)
```

Then the crash run:

```sh
make run CRASH=10          # processes 10 records, crashes BEFORE commit
make db-count              # 10 rows in messages
```

The group now only knows about commits from batches that finished completely — for us that's the first batch (or the part that succeeded). The remaining offsets for the group are as if they were never read. On the second run they'll arrive again:

```sh
make run                   # Ctrl+C when the log stops growing
make db-count              # exactly 30 — no duplicates
```

The second run's output shows `DUP ... (already seen — ON CONFLICT)` lines for the records that arrived again. That is visible proof that dedup worked. In the table after two runs — exactly 30 unique records, matching the number of messages in the topic.

Full cleanup:

```sh
make clean                 # truncate + delete group + delete topic
make down                  # stop Postgres + delete volume
```

## Tradeoffs of the idempotent approach

Exactly-once-effect costs four things.

First — every insert hits the DB, even if the record turns out to be a duplicate. At high throughput (tens of thousands of messages per second) this is noticeable load on Postgres. Fix it with batch inserts: accumulate N records and send them in one `INSERT ... ON CONFLICT` or `COPY ... ON CONFLICT` (via a staging table). Dedup stays, overhead spreads out.

Second — the table grows. If topic retention is 7 days and the dedup table is forever, it will become enormous fast. Fix it with either TTL on `processed_at` (a cron job cleans old records), or `range_partitioning` by date with DROP of old partitions. In both cases the dedup window must be **larger** than the expected interval between retries — otherwise after cleanup an old record becomes "new" again and passes through a second time.

Third — this only works while DB and Kafka run in parallel. If the DB went down after the insert but before the commit — Kafka didn't advance the offset, we restarted, DB came back up, the repeated insert got `ON CONFLICT` — all good. But if the DB lost data (restored from a backup to an earlier point in time) — our dedup is broken, because the duplicate is no longer recognized as a duplicate. That is a catastrophic scenario; it needs a separate recovery plan.

Fourth — atomicity on the DB side. Here we have one insert, and Postgres commits it atomically. If business logic is more complex (multiple UPDATEs plus an external API call) — you either wrap everything in one transaction, or move it outside via transactional outbox ([Outbox pattern](../../../../04-reliability/04-03-outbox-pattern/i18n/en/README.md)). If neither fits — accept that some operations may be not-exactly-once, and compensate after the fact.

## Where this approach won't fit

If processing has an **external side effect that is not idempotent** — for example, sending an email via SMTP, charging money through a payment gateway without an idempotent API — bare ON CONFLICT won't help. The insert into the local DB will pass idempotently, but the email will go out a second time.

The solutions are known. Either the downstream supports idempotency-key natively (payment gateways usually do). Or you build an outbox: insert into DB and outbox table in one transaction, a separate publisher sends the outbox to Kafka, an idempotent consumer pulls it to the outside. Then "sent email" = "updated outbox.sent_at = NOW()", and the actual send went through an idempotent downstream.

That is already [Outbox pattern](../../../../04-reliability/04-03-outbox-pattern/i18n/en/README.md) and [External system delivery](../../../../04-reliability/04-05-external-delivery/i18n/en/README.md). In this lecture everything is simpler: one insert into one table. Everything else lives in the same DB; dedup closes the question.

## What else to try

- run `make run CRASH=15` — crash after 15 records; then `make run` without crash — see duplicates in the log and `db-count` shows exactly 30;
- increase `WORK_DELAY=400ms` — processing is slower, you have time to compare `make db-count` in another console while the run is in progress;
- delete the group's committed offset (`make group-delete`) and run `make run` again — all 30 records in the log will be DUP, but `db-count` stays 30; that is "exactly-once-effect";
- do `make db-truncate` without deleting the group — on restart the group reads from where it stopped (committed offset intact), but the table will have fewer than 30, because we won't "see" some records again;
- replace the PRIMARY KEY with `(topic, partition)` (no offset) — dedup starts cutting off legitimate repeated messages from the same producer to the same partition.

## Next

This approach is the foundation for everything in module 04. Transactions ([Transactions and EOS](../../../../04-reliability/04-01-transactions-and-eos/i18n/en/README.md)) give exactly-once at the Kafka-to-Kafka pipeline level. Outbox ([Outbox pattern](../../../../04-reliability/04-03-outbox-pattern/i18n/en/README.md)) — exactly-once at the DB-Kafka boundary. External delivery ([External system delivery](../../../../04-reliability/04-05-external-delivery/i18n/en/README.md)) — what to do when the downstream is not idempotent. The idea is the same everywhere: **find a way to recognize a repeat and make it a no-op**. Only the tool changes.

The next lecture ([Error handling](../../../03-04-error-handling/i18n/en/README.md)) — on error handling: what to do when the handler finishes with an error. Skip, retry, retry-topic, DLQ. Idempotency stays in the background — it's needed everywhere there is a retry, and error handling has retries by definition.
