# 07-04 — Debezium CDC

CDC stands for "change data capture". The idea is simple: instead of periodically polling a table (`SELECT * FROM users WHERE updated_at > $1`), you read the changes themselves. Every INSERT, UPDATE, and DELETE is a separate event — precise to the row, with old and new values, in transaction commit order.

Where does this come from? From the database's own log. In Postgres, that's the WAL — write-ahead log, which the database writes everything it's about to do to data before actually doing it. WAL is needed for crash recovery and replication between master and replica. If you can read it from the outside, you get a stream of changes as-is, without extracting the table itself.

Who can actually read it — Debezium. It's a set of Kafka Connect connectors, one per supported engine (Postgres, MySQL, MongoDB, SQL Server, Oracle, a couple more). This lecture covers only the Postgres variant.

## Why CDC at all

Four scenarios where it saves you:

1. **Analytics.** Postgres is great for OLTP, but building reports on terabytes is painful. CDC → Kafka → ClickHouse / BigQuery / Snowflake. The business database stays clean, analytics runs on a separate engine.
2. **Search.** Postgres → Elasticsearch. Every row edit → document reindex. Without CDC you'd either batch-reload the entire database once an hour, or bake double-writes into application code (and double-write without a transaction = pain).
3. **Microservices.** Old monolithic database, new microservice that needs its data. Subscribe to CDC — it lives on fresh data without synchronously calling the source service.
4. **Outbox pattern.** From the [Outbox pattern](../../../../04-reliability/04-03-outbox-pattern/i18n/en/README.md) lecture you remember: transactional outbox solves "DB-write + Kafka-publish atomically". But there the publisher is a poller that SELECTs the outbox every 100ms — more expensive than you want. With CDC the publisher is gone entirely — Debezium reads WAL and publishes directly.

The fourth point is the final form of the outbox, and in this lecture we assemble it.

## How Debezium reads Postgres WAL

There are several layers here. Let's go through them in order.

Postgres WAL is written in physical format — a byte representation of page changes on disk, without SQL statements like "INSERT INTO users VALUES (...)". Such a stream is nearly useless from the outside. To extract logical changes from it, Postgres since version 10 supports "logical replication" — a decoder that sits on top of WAL and converts physical records into logical events (INSERT/UPDATE/DELETE with column sets).

The decoder is selected via the `plugin.name` parameter. The built-in baseline is `pgoutput`. Previously you had to install `wal2json`, but Debezium since 2.0 supports pgoutput out of the box, and plugin installation is no longer needed.

Access to the stream goes through two objects:

- **publication** — a SQL object that lists the tables for streaming. Essentially a list of "what we're listening to".
- **replication slot** — a position in the WAL. Postgres maintains a pointer for each slot to the oldest WAL record the slot hasn't yet confirmed. While the slot exists and has no acks — Postgres retains all WAL from that position without purging it.

This is critical. A replication slot is powerful and simultaneously a trap. If you created a slot, then deleted the connector without dropping the slot, then forgot about it — Postgres will accumulate WAL indefinitely. The disk fills up, the database grinds to a halt. Our `make connector-delete-all` includes an explicit `pg_drop_replication_slot` — without it you can easily bury yourself. In production this is covered with monitoring on `pg_replication_slots.confirmed_flush_lsn` and alerts on lagging slots.

## Event structure

What does Debezium put in Kafka when an UPDATE happens in users? Here's the skeleton:

```json
{
  "before": {"id": 42, "email": "old@x.com", "status": "active",  "full_name": "User 42"},
  "after":  {"id": 42, "email": "old@x.com", "status": "blocked", "full_name": "User 42"},
  "source": {"version": "3.5.0.Final", "ts_ms": 1714723200000, "lsn": 281474976710732, "table": "users"},
  "op": "u",
  "ts_ms": 1714723200123
}
```

`op` — the operation symbol:

- `c` — create (INSERT)
- `u` — update
- `d` — delete (`after` will be null)
- `r` — read (a row from the initial snapshot — Debezium on first start reads the entire table via SELECT and marks each row as `r`)
- `t` — truncate

`before` for UPDATE/DELETE only arrives if the table has `REPLICA IDENTITY FULL` set. By default Postgres writes only the row's PK to WAL — enough for physical replication, but for CDC you get a stub: `{"id": 42}` without the remaining fields. In our `db/init.sql` we manually set `REPLICA IDENTITY FULL` — the cost is slightly larger WAL volume.

Tombstone is a separate thing. When a row is deleted and `tombstones.on.delete=true`, Debezium sends an additional message with the same key and `value=null` after the `op=d` event. This is needed for compact topics: log compaction removes all versions whose key matches a tombstone. If the CDC topic is configured with `cleanup.policy=compact` (which is common), tombstones are the only way to evict deleted rows from history.

## Topic naming convention

Debezium creates a topic for each table named `<topic.prefix>.<schema>.<table>`. We use `topic.prefix=cdc`, schema `public`, table `users` — giving `cdc.public.users`.

If the connector subscribes to 10 tables — there will be 10 topics. Each with its own partition set (1 by default, typically raised for production). The message key is the table's primary key (as JSON). This gives stable partitioning: all events for the same row go to the same partition, order is preserved.

## Snapshot, then streaming

When the connector starts for the first time with `snapshot.mode=initial`, it does:

1. Takes a snapshot of the WAL position (`pg_current_wal_lsn()`).
2. Runs `SELECT *` on all tables from `table.include.list` and sends each row as `op=r`.
3. After the snapshot, switches to reading WAL from the recorded position and continues as a stream.

This gives a consistent picture: subscribe — first dump the entire current database into Kafka, then stream incremental changes. No gaps, no races.

`snapshot.mode` has multiple options — `initial` (our default), `no_data` (only new changes, no historical data; in Debezium 2.x this mode was called `never`), `initial_only` (snapshot and stop), `when_needed`. For analytics, typically `initial`. For the outbox table — `no_data`, historical outbox is usually not needed.

## Outbox event router

With plain CDC you have topic `cdc.public.outbox` — a dump of outbox table rows. Zero utility: the consumer would have to parse the outbox structure, extract `aggregate_type`, and figure out what the event even is.

Debezium handles this through an SMT (Single Message Transform) called `EventRouter`. Configured as:

- `route.by.field=aggregate_type` — takes the name from this column.
- `route.topic.replacement=events.${routedByValue}` — substitutes into the template.
- `table.field.event.payload=payload` — the message value is taken from this column.
- `table.field.event.key=aggregate_id` — the message key.
- `table.fields.additional.placement=type:header:eventType,...` — extra columns go to headers.

The result: instead of one `cdc.public.outbox` you get a set of topics `events.user`, `events.order`, `events.payment` (based on what's in the `aggregate_type` column), and each message already has a proper business key and payload without the wrapper. The consumer subscribes to `events.user` and doesn't know there was an outbox inside.

This SMT is the final form of the outbox pattern. DB↔Kafka atomicity is provided by the transaction on the service side (it writes to `users` and `outbox` in one TX), and delivery is handled by Debezium through WAL. No poller in the business service.

## What's in our sandbox

Postgres runs as a separate container in the same Docker network as kafka-connect — Connect reaches it via hostname `lecture-07-04-postgres`. Parameters required for logical replication are set in `command:`:

```yaml
command: >
  postgres
    -c wal_level=logical
    -c max_replication_slots=4
    -c max_wal_senders=4
```

Without `wal_level=logical` pgoutput won't start and will return an error when creating the slot. With the default `replica` we get only physical replication.

The init script creates two tables and one publication:

```sql
CREATE TABLE users (id BIGINT PRIMARY KEY, email TEXT, full_name TEXT, status TEXT, updated_at TIMESTAMPTZ);
ALTER TABLE users REPLICA IDENTITY FULL;

CREATE TABLE outbox (id UUID PRIMARY KEY, aggregate_type TEXT, aggregate_id TEXT, type TEXT, payload JSONB, created_at TIMESTAMPTZ);
ALTER TABLE outbox REPLICA IDENTITY FULL;

CREATE PUBLICATION dbz_publication FOR TABLE users, outbox;
```

We create the publication ourselves with `publication.autocreate.mode=disabled` in the connector — this makes it clearer which tables are actually being streamed, and removes the temptation to add a table via ALTER without understanding the implications.

## Two connectors

This lecture uses two, each with its own purpose.

The first — `lecture-07-04-debezium-pg-source`. Raw CDC on the `users` table, no SMT. Every change goes to `cdc.public.users` in before/after/op format. This is the case where the consumer parses the structure itself — for example, an analytics pipeline that needs all the details.

The second — `lecture-07-04-debezium-outbox`. CDC on the `outbox` table plus EventRouter SMT. Output — topics `events.user`, `events.order` (depending on what's in `aggregate_type`). This is outbox delivery for business events.

Note: both connectors connect to the same database, but through **different replication slots**. Each slot walks through WAL independently, with its own position. This is normal practice: a slot is a "subscriber", and different purposes need different subscribers.

## Demo program

`db-loader` is a change generator for Postgres. It inserts N users, then updates half of them, then deletes a quarter. Each change — in a single transaction with a write to outbox.

A transaction is the only way to guarantee atomicity. If you wrote to users but failed on outbox — Debezium sees the INSERT in users without a corresponding outbox event, and the consumer never gets the event. Here's the core insert:

```go
return pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error {
    _, err := tx.Exec(ctx, `
        INSERT INTO users (id, email, full_name, status, updated_at)
        VALUES ($1, $2, $3, 'active', NOW())
    `, id, email, fullName)
    if err != nil {
        return err
    }
    payload := fmt.Sprintf(`{"id":%d,"email":%q,"full_name":%q}`, id, email, fullName)
    _, err = tx.Exec(ctx, `
        INSERT INTO outbox (id, aggregate_type, aggregate_id, type, payload)
        VALUES ($1, 'user', $2, 'user.created', $3::jsonb)
    `, uuid.New(), fmt.Sprintf("%d", id), payload)
    return err
})
```

`pgx.BeginFunc` is a helper that commits on `nil` and rolls back on error from the lambda. No manual `tx.Commit()` or `defer tx.Rollback()` — a closed abstraction.

The second process — `cdc-consumer`. Subscribes simultaneously to `cdc.public.users` and all `events.*` topics — for this we enable regex mode in franz-go:

```go
cl, err := kafka.NewClient(
    kgo.ConsumerGroup(defaultGroup),
    kgo.ConsumeRegex(),
    kgo.ConsumeTopics(`^cdc\.public\.users$|^events\..+$`),
    kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
)
```

With `ConsumeRegex()`, each string in `ConsumeTopics` is interpreted as a regex. Convenient: however many `events.<aggregate_type>` topics appear as `db-loader` writes new types — the subscription picks them up automatically.

We print human-readable output: for CDC events — op + before/after, for outbox-router events — headers + payload as-is.

## Running

```sh
# from the repo root: verify the Debezium plugin is installed
make connect-install-plugins

# from this directory:
make up                       # Postgres
make db-init                  # users + outbox + publication
make connect-plugin-check     # verify Debezium is visible via REST
make connector-create-all     # source + outbox connectors

# in one terminal:
make run-cdc-consumer

# in another:
make run-loader COUNT=10
```

The consumer terminal will first receive the users snapshot (`op=r` for each row), then INSERTs (`op=c`), then UPDATEs (`op=u`), then DELETEs (`op=d` plus tombstone). In parallel — events in `events.user` via the outbox router: with aggregate_id in the key, event type in headers, and clean payload without CDC wrapper.

Check slot status:

```sh
make slot-status
```

You'll see two active slots — `lecture_07_04_users_slot` and `lecture_07_04_outbox_slot`, each with its own `confirmed_flush_lsn`.

## Guarantees and pitfalls

Debezium provides **at-least-once**. No exactly-once here — the consumer must be idempotent. If the connector restarted between fetching from WAL and publishing to Kafka — an event may arrive twice. On the consumer side, typically deduplicate by (topic, partition, offset) or by business key from the payload (see the [Processing guarantees](../../../../03-consumer/03-03-processing-guarantees/i18n/en/README.md) lecture).

Order is guaranteed per-key, not globally. All events for the same row (by PK) land in the same partition and preserve commit order. Events for different rows can interleave — that's expected. If global order is required, set partitions=1 (at the cost of scalability).

WAL accumulates until the slowest slot confirms its position. If a connector dies and isn't fixed — disk runs out. This isn't theory: real incidents of "our database crashed because of an abandoned Debezium slot" happen regularly. Monitor `pg_replication_slots`.

Snapshots are slow. If the table is a terabyte — the initial snapshot is also a terabyte, and incremental streaming won't start until it finishes. For huge tables, use `incremental snapshot` (the `signal.data.collection` flag) — that's a separate Debezium feature not covered in this lecture.

Schema changes (DDL): Debezium on Postgres catches them automatically — add a column, it appears in new events. Drop a column — it won't be in `after`. But `before` with old events is already published, so the consumer must be schema-tolerant — again, Protobuf / Avro with Schema Registry helps, as discussed in module 05.

## What's next

This is the last lecture in module 07. Next is module 08 on operations (monitoring, retention, sizing, troubleshooting), and in the module 09 use cases this same Debezium appears twice:

- [Postgres → ClickHouse with anonymization](../../../../09-use-cases/03-pg-to-clickhouse/i18n/en/README.md) — Postgres → ClickHouse via Debezium + Go anonymizer + ClickHouse Sink
- [Postgres → Elasticsearch](../../../../09-use-cases/04-pg-to-elasticsearch/i18n/en/README.md) — Postgres → Elasticsearch via Debezium + ES Sink (no Go at all, declarative ETL)

Here you have the conceptual foundation. If you've grasped how WAL → slot → connector → topic stack together — the use cases ahead are variations on this theme.
