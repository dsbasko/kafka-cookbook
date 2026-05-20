# 07-03 — Kafka Connect

Before this lecture we wrote producers and consumers by hand. Connect, send records via `cl.ProduceSync`, read via `cl.PollFetches`, deal with offsets. When the task is "move data from a DB into Kafka and back into another DB", that code looks identical in every project: connect to the source, map rows to events, retry, idempotency, monitoring. And you have to write it anyway. And maintain it.

Connect is an attempt to not write it forty times. A standard service that runs alongside Kafka and accepts connector plugins. One connector knows how to read from Postgres, another knows how to write to ClickHouse. You only configure: connection URL, table name, poll interval, field mapping. Startup, retry, offsets, parallelism, and load balancing across Connect nodes are handled for you.

In our sandbox Connect is already running — the `kafka-connect` container on :8083 (see the root `docker-compose.yml`). The REST API lives at `http://localhost:8083`. This lecture is a first walk through it. Next comes [Debezium CDC](../../../07-04-debezium-cdc/i18n/en/README.md) with Debezium as a source connector; here we build a sink — forward messages from Kafka into a Postgres table, no Go code on the receiving side.

## What a connector actually is

A connector comes in two kinds: **source** and **sink**. A source reads from an external system and writes into Kafka. A sink does the opposite: consumes a topic and writes to an external system. A single Connect process can run both kinds simultaneously.

Internally a connector is just a Java plugin. Confluent publishes them in bulk: JDBC (source and sink for any relational DB), Elasticsearch sink, S3 sink, Debezium for CDC from PG/MySQL/Mongo. Third-party ones exist too — ClickHouse Sink from Altinity, for example. In our sandbox the `connect-plugins/` directory is mounted into the container; plugins go there as extracted archives, and Connect picks them up on startup.

The `confluentinc/kafka-connect-jdbc` plugin handles both sides: JDBC source (reads via SELECT on an incrementing column) and JDBC sink (writes INSERT/UPDATE from a topic). We only need the sink.

### Connect distributed mode in one line

Our `kafka-connect` runs in distributed mode. That means Connect state lives inside Kafka itself, across three topics:

1. `_connect-configs` — connector configs.
2. `_connect-offsets` — checkpoints for source connectors (sinks store their offset in a regular consumer group).
3. `_connect-status` — connector and task statuses.

All three are created automatically when Connect starts. If you bring up a second worker with the same `group.id`, they automatically split tasks between themselves. Standalone mode (everything local on disk of a single process) is not used in this course.

## REST API in three requests

No UI, via curl:

```sh
# what is installed
curl http://localhost:8083/connector-plugins | jq '.[] | .class'

# create a connector (body — JSON with "name" and "config" fields)
curl -X POST -H 'Content-Type: application/json' \
     --data @connectors/jdbc-sink-orders.json \
     http://localhost:8083/connectors

# check its state
curl http://localhost:8083/connectors/lecture-07-03-jdbc-sink-orders/status
```

The `/status` response matters. The `connector.state` field is the routing process itself (`RUNNING`, `PAUSED`, `FAILED`). The `tasks[].state` field is the state of each worker thread — one entry per task. If `connector.state=RUNNING` but `tasks[0].state=FAILED`, the connector is alive but one of its threads has died. In production you catch this in Grafana via the `connect-worker-metrics` metric. If you just ignore it, data stops flowing.

## What this lecture covers

We take the `JdbcSinkConnector` sink connector and wire it to Postgres, which we bring up locally via `docker-compose.override.yml`. Postgres joins the same `sandbox-kafka_kafka-net` network so the Connect container can reach it by hostname `lecture-07-03-postgres`. Host port 15436 is exposed separately for `psql` from your machine — Connect uses internal container network ports and has no use for 15436.

Data flow:

```
[Go orders-producer] ──> Kafka topic `lecture-07-03-orders` ──> [kafka-connect / JdbcSink] ──> Postgres orders
```

One caveat: Connect cannot guess a table schema from bare JSON. How would it know that `amount` is a `double` and not a `string`? So the value must carry a schema.

## Converters and why the value is longer than it looks

When Connect reads a record from Kafka, it does not understand raw bytes on its own. It needs a **converter** — a component that turns a byte array into a typed structure. There are several converters:

- `AvroConverter` — bytes carry a magic byte + schema_id; the converter calls Schema Registry, fetches the schema, and parses. The most compact wire format.
- `JsonConverter` (with `schemas.enable=true`) — a JSON object `{"schema": {...}, "payload": {...}}`. The schema travels with every record.
- `JsonConverter` (with `schemas.enable=false`) — bare JSON. Connect has no type information, the sink does not work.
- `StringConverter` — just a string. Fine for the key, almost never for the value.

In the root `docker-compose.yml` the default for all of Connect is set to Avro + Schema Registry. Convenient for production. But in this lecture we deliberately skip Schema Registry so the wire format is visible in `kafka-console-consumer` without decoders. That is why the connector config **overrides** `value.converter` to `JsonConverter` with the schema embedded.

In real life you typically use Avro/Protobuf via Schema Registry — records are 10–20× smaller. But this is a learning sandbox, so JSON.

## JDBC sink config — line by line

File `connectors/jdbc-sink-orders.json`. The key parts:

```json
{
  "connector.class": "io.confluent.connect.jdbc.JdbcSinkConnector",
  "topics": "lecture-07-03-orders",
  "connection.url": "jdbc:postgresql://lecture-07-03-postgres:5432/lecture_07_03",
  "connection.user": "lecture",
  "connection.password": "lecture",
  "value.converter": "org.apache.kafka.connect.json.JsonConverter",
  "value.converter.schemas.enable": "true",
  "table.name.format": "orders",
  "insert.mode": "upsert",
  "pk.mode": "record_value",
  "pk.fields": "id"
}
```

What to pay attention to.

Three flags together produce "UPSERT by the `id` field from the payload":

1. `insert.mode=upsert` — write mode (alternatives: `insert` and `update`).
2. `pk.mode=record_value` — where to get the PK: from the record value. Alternatives: `record_key` (PK is in the record key), `kafka` (PK = Kafka coordinates `topic+partition+offset`) and `none` (no PK; upsert is then impossible).
3. `pk.fields=id` — which fields are treated as the primary key.

If `insert.mode=insert`, a second record with the same id would fail on a duplicate key error. If `pk.mode=none`, upsert does not work at all.

`auto.create=false` and `auto.evolve=false` — we made a deliberate choice: the table schema is created by hand via `db/init.sql`. A sink with `auto.create=true` would issue `CREATE TABLE` on its own, but then you lose control over types and indexes. In production almost always `false`.

`errors.tolerance=none` — on any routing error the task fails. In production you sometimes set `all` plus `errors.deadletterqueue.topic.name=...` so bad records go to a DLQ topic instead of blocking the stream.

## What our Go code sends

Go lives on the producer side only. The sink side is Connect itself; no code to write.

`cmd/orders-producer/main.go`. It is structured exactly like the first producer from [First producer with franz-go](../../../../01-foundations/01-05-first-producer/i18n/en/README.md), the only difference is the value format. Each message is JSON with two top-level fields: `schema` (structure description) and `payload` (data).

The schema is built once and reused:

```go
func ordersSchema() connectSchema {
    return connectSchema{
        Type: "struct",
        Name: "lecture_07_03.orders",
        Fields: []connectField{
            {Field: "id", Type: "int64", Optional: false},
            {Field: "customer_id", Type: "string", Optional: false},
            {Field: "amount", Type: "double", Optional: false},
            {Field: "status", Type: "string", Optional: false},
            {Field: "created_at", Type: "string", Optional: false},
        },
    }
}
```

The record key is the order id as a string. This serves two purposes. First, identical ids always land in the same partition — on sink restart they are processed in the original order. Second, `pk.mode=record_value` takes `id` from the value, but partitioning is still the producer's responsibility, and a stable key makes observation easier.

The message body is assembled into a single struct and marshalled:

```go
envelope := orderEnvelope{
    Schema: schema,
    Payload: orderPayload{
        ID:         id,
        CustomerID: customerID,
        Amount:     amount,
        Status:     "created",
        CreatedAt:  time.Now().UTC().Format(time.RFC3339Nano),
    },
}
valueBytes, _ := json.Marshal(envelope)
```

A raw record in Kafka looks roughly like this (one record):

```json
{
  "schema": {
    "type": "struct",
    "name": "lecture_07_03.orders",
    "fields": [
      {"field": "id", "type": "int64", "optional": false},
      {"field": "customer_id", "type": "string", "optional": false},
      {"field": "amount", "type": "double", "optional": false},
      {"field": "status", "type": "string", "optional": false},
      {"field": "created_at", "type": "string", "optional": false}
    ]
  },
  "payload": {
    "id": 1,
    "customer_id": "cust-17",
    "amount": 642.31,
    "status": "created",
    "created_at": "2026-05-01T12:00:00Z"
  }
}
```

Size is a real pain here. The schema block duplicates in every message. At 1M messages that is tens of extra megabytes. That is why production pipelines like this use Avro: the schema lives in Schema Registry, and Kafka carries only a magic byte + schema_id plus a compact binary payload. That is covered in the [Schema Registry](../../../../05-contracts/05-03-schema-registry/i18n/en/README.md) / [Schema evolution](../../../../05-contracts/05-04-schema-evolution/i18n/en/README.md) lectures — here the format is intentionally simple.

## How to run

Standard sequence:

```sh
make up                  # Postgres from override.yml
make db-init             # CREATE TABLE orders
make topic-create        # Kafka topic with 3 partitions
make connect-plugin-check
make connector-create    # POST /connectors
make connector-status    # should be RUNNING/RUNNING

make run-producer COUNT=200
sleep 2
make db-count            # 200
```

If `connect-plugin-check` reports the plugin is missing — the installed set for the Connect image `cp-kafka-connect:8.0.0` varies slightly between sandboxes, and sometimes JDBC is bundled, sometimes not. The Makefile error message includes a ready-made `confluent-hub install` command to run directly inside the container. After installation run `docker compose restart kafka-connect`, then check again.

Re-running with the same ids demonstrates upsert behavior:

```sh
make run-producer COUNT=200 START_ID=1
make db-count    # still 200, but rows were updated (check statuses in the DB)
```

If `make connector-status` returns `tasks[0].state=FAILED`, the `trace` field contains the exception from the sink. The most common causes:

- the table does not exist in the DB (`db-init` was skipped)
- types in the schema do not match types in the table (`amount=string` vs `DOUBLE PRECISION`)
- Postgres is unreachable by that hostname (`override.yml` is not attached to `kafka-net`)
- the PostgreSQL JDBC driver is missing from the plugin directory (rare, but happens after manual operations on `connect-plugins/`)

## SMT in one line, for reference

SMT (Single Message Transforms) is a separate layer between the converter and the connector. In transit you can rename a field, drop it, mask it, or cast its type. Configured like this:

```json
"transforms": "rename,mask",
"transforms.rename.type": "org.apache.kafka.connect.transforms.ReplaceField$Value",
"transforms.rename.renames": "customer_id:user_id",
"transforms.mask.type": "org.apache.kafka.connect.transforms.MaskField$Value",
"transforms.mask.fields": "amount"
```

This lecture does not use SMT — the table and payload are built to match each other. SMT will come back later: full coverage in the use case [Postgres → ClickHouse with anonymization](../../../../09-use-cases/03-pg-to-clickhouse/i18n/en/README.md), where they are needed for the outbox event router.

## What is left out

This lecture is intentionally thin. Left out:

- Distributed mode with multiple worker nodes and automatic task shifting.
- Deduplication and exactly-once on the sink side (for Postgres this works via UPSERT; for S3/Elasticsearch there are their own mechanics).
- DLQ (`errors.deadletterqueue.topic.name`) and strategies for handling bad records.
- Connect metrics (lag, throughput, error rate, restart count). These need Grafana dashboards.
- Source connectors. They are conceptually easier to understand after the [Debezium CDC](../../../07-04-debezium-cdc/i18n/en/README.md) lecture — that one covers source.

For a deeper dive into the JDBC sink specifically, the Confluent configuration reference is thorough, with documented behavior for edge cases on every field. There are around fifty parameters.

## What's next

In [Debezium CDC](../../../07-04-debezium-cdc/i18n/en/README.md) we cover CDC via Debezium. That is a different connector — a source, not a sink. It subscribes to a Postgres logical replication slot and streams every row change into Kafka as a `before`/`after` event. After that, the "Debezium → JDBC sink" pair gives you asynchronous replication between databases, and the entire use case [Postgres → ClickHouse with anonymization](../../../../09-use-cases/03-pg-to-clickhouse/i18n/en/README.md) is built on exactly that pair.
