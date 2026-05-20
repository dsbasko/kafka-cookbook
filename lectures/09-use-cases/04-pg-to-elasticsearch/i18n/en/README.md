# 09-04 — Postgres → Elasticsearch

Declarative ETL for search. On the left — Postgres with a product catalog, articles, and users. On the right — Elasticsearch, where that same data needs to land so full-text search works on it. Nothing custom between them — only Debezium and the Elasticsearch Sink connector. Go code exists in this use case, but it's diagnostic: db-loader and search-tester don't participate in the pipeline; they're convenient for verifying that everything arrived.

This contrasts with [Postgres → ClickHouse with anonymization](../../../03-pg-to-clickhouse/i18n/en/README.md). There, a Go anonymizer sat between Debezium and the Sink — unavoidable, since PII masking requires logic. Here there's no logic: the Postgres schema maps almost one-to-one onto an ES JSON document, and the format transforms (unwrap the Debezium envelope, extract the key, rename the topic to an index name) are handled by standard Single Message Transforms inside Connect. The main benefit — the pipeline can be built and maintained without a separate service to deploy and monitor.

## What we're building

```
Postgres (15443)
   │
   │  WAL (logical replication, slot)
   ▼
kafka-connect: Debezium PostgresConnector
   │
   ▼
search.public.products / search.public.articles / search.public.users
   │
   │  ExtractNewRecordState (unwrap)
   │  ExtractField$Key      (id from key into _id)
   │  RegexRouter           (search.public.X → X_v1)
   ▼
kafka-connect: ES Sink connector (v1)
   │
   ▼
Elasticsearch (19200) → indices products_v1 / articles_v1 / users_v1
```

Compare with [Postgres → ClickHouse with anonymization](../../../03-pg-to-clickhouse/i18n/en/README.md): the same number of Connect links, just an ES Sink instead of a ClickHouse Sink, plus the SMT chain because ES expects a flat document while Debezium sends a wrapped one. Postgres schema, publication, replication slot, RF — unchanged.

## Why Single Message Transforms here

Debezium puts an event into Kafka in envelope format:

```json
{
  "before": null,
  "after": {"id": 1, "name": "Product 1 alpha", "price_cents": 200, ...},
  "source": {"lsn": 12345, "ts_ms": ...},
  "op": "c"
}
```

The ES Sink will store this object as-is out of the box. The result is an index with `before`, `after`, `source`, `op` fields — not what you expect when writing `match: {name: "alpha"}`. SMT in this use case does three simple things.

**ExtractNewRecordState** — extracts `after` from the envelope and puts it at the root. For DELETE operations (`op=d`), `after` is empty and Debezium sets value=null — this triggers `behavior.on.null.values=delete` in the ES Sink, and the document disappears.

**ExtractField$Key** — Debezium sends the key as an object `{"id": 1}`, but ES wants a string or number in `_id`. The SMT extracts the `id` field.

**RegexRouter** — the topic name becomes the index name. `search.public.products` → `products_v1`. The regex `search\.public\.(.*)` plus replacement `$1_v1` solves this in a single line. For version switching (see below), the v1 suffix is hardcoded in the config — for v2, a separate connector with `$1_v2` is used.

## Index template, or why the mapping lives in the repository

ES can infer field types from the first few documents — this is called dynamic mapping. On a small sandbox it works fine, but in production it backfires: the first document arrives with `tags: ["a","b"]`, the type is inferred as text → the next document with `tags: 42` fails with a mapping conflict error. The only fix is recreating the index with an explicit mapping.

The right approach is to fix the structure upfront via an index template. The template is applied to indexes by name pattern, and any freshly created `products_v2` already starts with the correct analyzer and types.

```json
{
  "index_patterns": ["products_*", "articles_*", "users_*"],
  "template": {
    "settings": {"number_of_shards": 1, "number_of_replicas": 0,
      "analysis": {"analyzer": {"ru_en_text": {...}}}},
    "mappings": {"dynamic": true, "properties": {
      "name":  {"type": "text", "analyzer": "ru_en_text"},
      "price_cents": {"type": "long"}, ...
    }}
  }
}
```

The full version is in `es-template.json`. The analyzer here is for learning purposes: lowercase plus asciifolding (fold Cyrillic and Latin into similar forms). In production you would also attach a language-specific morphology tokenizer; in the sandbox it would only complicate the setup.

## What the code shows

`cmd/db-loader/main.go` — a simple Postgres filler. A thin wrapper over INSERT/UPDATE/DELETE with predictable values (numeric ids, name contains the word "alpha" so you can run a match query on it later). Here is the core — three INSERTs in one transaction:

```go
_, err := tx.Exec(ctx, `
    INSERT INTO products (id, sku, name, description, category, price_cents, stock)
    VALUES ($1, $2, $3, $4, $5, $6, $7)
`,
    pid,
    fmt.Sprintf("SKU-%07d", pid),
    fmt.Sprintf("Product %d %s", pid, randomWord()),
    fmt.Sprintf("Product %d description. Fits for %s. Excellent quality.", pid, ...),
    categories[int(pid)%len(categories)],
    int64(100+rand.IntN(990_000)),
    rand.IntN(100),
)
```

There's no direct Kafka interaction here — Debezium reads WAL and publishes CDC on its own. This is the point of declarative ETL: the Postgres writer knows nothing about the existence of Elasticsearch.

`cmd/search-tester/main.go` — the reverse diagnostic cut. Counts rows in Postgres, documents in ES (`_count` API), runs a match query on a field, and prints the top-5 hits. Useful for quickly determining whether data arrived or not.

```go
pgCount, err := countPostgres(ctx, pool, *pgTable)
esCount, err := countES(ctx, *esURL, *alias)

if pgCount != esCount {
    fmt.Printf("MISMATCH: %d (PG) vs %d (ES)\n", pgCount, esCount)
}

hits, err := matchQuery(ctx, *esURL, *alias, *matchField, *query)
```

If the divergence is persistent — the pipeline is stuck somewhere. Run `make connector-status` next, and you'll typically see either a FAILED task in the Sink or that Debezium is stuck on the slot (Postgres holds a replication slot until it's explicitly dropped, and if the connector crashed, the slot accumulates WAL and will eventually exhaust disk space).

## Blue-green reindex

This is why the pattern of "index with a version suffix plus alias" exists at all. Simple case: you add a column to Postgres, update the index template, recreate the index — search temporarily returns nothing while CDC re-streams the entire table. That's not acceptable.

Solution: the `products` alias points to `products_v1`, the application reads only through the alias. When you need to move to a new mapping — bring up a second ES Sink writing to `products_v2`, wait for it to catch up with v1 (usually a couple of minutes for hundreds of thousands of rows), switch the alias to v2 with a single atomic request, delete the v1 sink. The application noticed nothing.

The key step is switching the alias. ES does this atomically, and if a request arrived a millisecond before and a millisecond after — it never sees an intermediate state. Under the hood:

```bash
curl -X POST http://localhost:19200/_aliases -d '{
  "actions": [
    {"remove": {"index": "products_v1", "alias": "products"}},
    {"add":    {"index": "products_v2", "alias": "products"}}
  ]
}'
```

Within a single request with `actions`, Elasticsearch guarantees atomicity — there is no moment when the alias points at nothing. This is exactly the difference from doing DELETE and POST separately.

The `make reindex-blue-green` target automates the entire scenario: creates the v2 sink, waits for catch-up, switches the alias, deletes the v1 sink. For the lesson this is sufficient; in production you'd also wire up canary reads from v2 in advance (via alias with `is_write_index` plus a read-only alias) to avoid switching blind.

## How to bring it up

The root sandbox (kafka-1/2/3, kafka-connect, schema-registry) must be running. Plugins for Connect (Debezium PostgresConnector + Confluent ES Sink) — installed via `make connect-install-plugins` from the `lectures/` root. This is a one-time operation, described in Task 34.5 of the plan.

Then, from the use case directory:

```sh
make up                                 # Postgres + Elasticsearch
make pg-init                            # tables + publication
make es-init                            # index template
make topic-create-all                   # CDC + DLQ topics
make connect-plugin-check               # check plugins
make connector-create-all               # Debezium + ES Sink v1
make run-load DLOAD_COUNT=200           # load data into Postgres
make connector-status                   # both tasks in RUNNING
make run-search                         # compare counts + top-5 hits
```

After a minute or two, `products_v1` in ES should contain 200 documents, and search-tester should find them by a word from the `name` field. If there's a divergence — `make connector-restart` (it will reset failed tasks) and run `make connector-status` again.

For the blue-green demo:

```sh
make reindex-blue-green                 # creates alias (if missing), creates v2, catches up, switches
make alias-show                         # shows that products → products_v2
```

`reindex-blue-green` depends on `alias-init` — which idempotently creates `products → products_v1`. Without the alias, the atomic remove+add will fail entirely (ES rolls back on the first failing action in `_aliases`).

To tear down everything:

```sh
make clean                              # deletes connectors, slot, topics, containers
```

## What the integration test checks

`test/integration_test.go` (build tag `integration`) does the same thing as the manual scenario above, plus two additional checks.

- UPDATE of one row in Postgres → tracking the name field in ES until the change arrives. Deadline 90 sec, usually completes in 2–5 seconds (Sink linger.ms=200 + CDC propagation time).
- DELETE of one row → waiting until the document disappears (`HEAD /_doc/<id>` returns 404). Also 90 sec.
- Blue-green: a v2 sink is created with a unique suffix (to avoid collisions with other runs), we wait for catch-up, switch the alias. Verifies that the alias actually points to v2.

N=200 is for run speed on a dev machine. The pattern is identical at any scale: 50k or 500k will behave the same, only the deadline numbers change. The test run takes approximately 1–2 minutes; most of the time is the startup of the two Connect connectors and waiting for Debezium's snapshot phase.

```sh
make up && make pg-init && make es-init && make test-integration
```

The test itself truncates Postgres, deletes old `products_v*` indexes, and drops old replication slots (`usecase_09_04_it_%`) — no manual cleanup needed between runs.

## Files

```
04-pg-to-elasticsearch/
├── README.md                          ← this file
├── Makefile                           ← targets up/down/connector-*/test-integration/reindex-blue-green
├── docker-compose.override.yml        ← Postgres (15443) + ES (19200)
├── go.mod                             ← dependencies (pgx + franz-go for tests)
├── es-template.json                   ← index template (settings + mappings)
├── connectors/
│   ├── debezium-pg-source.json        ← Debezium PostgresConnector
│   ├── es-sink.json                   ← ES Sink, route → *_v1
│   └── es-sink-v2.json                ← same, route → *_v2 (for blue-green)
├── db/
│   └── init.sql                       ← tables products/articles/users + publication
├── cmd/
│   ├── db-loader/main.go              ← INSERT/UPDATE/DELETE into Postgres
│   └── search-tester/main.go          ← diagnostics: PG count vs ES count + match-query
└── test/
    └── integration_test.go            ← E2E with blue-green reindex
```

## What's left out

The sandbox version is intentionally stripped down. In production the same schema is typically extended as follows.

ES authorization — here `xpack.security.enabled: "false"` because the lesson teaches the pattern, not security setup. In a real cluster, the Sink works via `connection.username`/`connection.password` or an API key. The ES Sink config accepts both without any changes to the SMT chain.

Schema Registry — here Debezium and the Sink communicate via JsonConverter. This is convenient for debugging (open a console and read). Under load, JSON is inefficient, and the chain is typically switched to Avro via SR — both connectors support this, you just need to replace the `*.converter` pairs and bring up SR (it's already in the root sandbox; the [Schema Registry](../../../../05-contracts/05-03-schema-registry/i18n/en/README.md) lesson covers it separately).

Multi-tenant indexes — in this lesson `products_v1` is global. If the catalog is split by store/tenant, the usual approach is a `products_<tenant>_v1` template via the same RegexRouter, plus an extended template. The logic stays the same.

Reactive canary during reindex — here the alias switch is a single action. In production you first set the alias with `is_write_index=true` for v2 and a read-only alias on v1, both indexes live for a period, and reads can be gradually migrated. Implemented via the same `_aliases` API, just with more actions.

ETL backfill from a source of truth other than the current Postgres — here Debezium with `snapshot.mode=initial` snapshots the entire table on first run. If data comes from somewhere else (S3 snapshot, dump from an old DB), the backfill is done as a separate process, typically via bulk indexing directly into ES, and only new changes go through CDC. The pattern: "historical bulk + live CDC" — standard for search migrations.
