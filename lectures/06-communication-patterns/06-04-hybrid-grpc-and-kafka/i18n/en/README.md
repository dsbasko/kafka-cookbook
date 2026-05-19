# 06-04 — Hybrid: gRPC + Kafka

The previous lecture compared synchronous gRPC and asynchronous Kafka on the same "user signed up" scenario. There is no winner. One gives low latency and predictable errors. The other gives decoupling and replay. In a real service you rarely pick just one. More often you take both and build a hybrid.

This lecture covers the most common shape of that hybrid: write-side with a gRPC API, an event bus through Kafka, and a separate read-side. The lecture is conceptual, on a single-node setup; the production variant with multi-node, integration tests, and failure recovery is the use case [Microservices communication](../../../../09-use-cases/01-microservices-comm/i18n/en/README.md) in module 09. Here — the pattern in its pure form.

## Why both at all

Take a typical order flow. The client creates an order. What happens next:

1. The client wants an immediate "accepted/rejected" response. That's a synchronous story — gRPC.
2. Inventory needs to reserve stock. Analytics wants an event log entry. Notifications needs to send an email. Each of these consumers doesn't care about the others.

If you do everything through gRPC — order-service knows all downstream URLs, synchronously calls each one, waits for all of them, and fails in a cascade. If you do everything through Kafka — the client waits for the async pipeline to confirm everything, which requires hacks like long-polling.

The hybrid splits it evenly. The client gets a short synchronous API, a response right after the DB COMMIT. All downstreams get an event in Kafka, their own consumer group, their own pace. Nobody blocks anyone.

## What a typical hybrid looks like

Three services. Named by what they do, not by which protocol they speak.

```
┌──────────────┐  CreateRequest      ┌────────────────────┐
│ gRPC client  ├────────────────────►│   order-service    │
└──────────────┘                     │  (CommandService)  │
                                     │                    │
                                     │  Postgres TX:      │
                                     │  orders + outbox   │
                                     │  ↓                 │
                                     │  outbox publisher  │
                                     └────────┬───────────┘
                                              │
                                              ▼
                                     ┌────────────────────┐
                                     │   Kafka topic      │
                                     │ order.created      │
                                     └────────┬───────────┘
                                              │
                              ┌───────────────┼───────────────┐
                              ▼               ▼               ▼
                    ┌──────────────┐ ┌──────────────┐ ┌──────────────┐
                    │  inventory   │ │ order-query  │ │ analytics?   │
                    │   service    │ │   service    │ │ notifications│
                    │              │ │ (projector + │ │  (whatever)  │
                    │ reservations │ │  QueryService│ │              │
                    └──────────────┘ └──────────────┘ └──────────────┘
                                              ▲
                                              │ Get(id)
                                     ┌──────────────┐
                                     │  gRPC client │
                                     └──────────────┘
```

The left half is the write path. One gRPC handler, one transaction, two tables in one DB. The outbox publisher runs as a goroutine in the same process.

The right half is the read path and downstream services. They listen to the bus and know nothing about each other. Whoever needs to — joins a new consumer group, reads everything from the beginning, and starts responding.

The lecture has three processes, one binary each:

1. `cmd/order-service` — gRPC `CommandService.Create` + outbox publisher as a goroutine
2. `cmd/inventory-service` — consumer on `order.created`, writes to `inventory_reservations`
3. `cmd/order-query-service` — gRPC `QueryService.Get` + projector into `orders_view`

One Postgres for all three for compactness — in production each service has its own DB. One Kafka topic `lecture-06-04-order-created`.

## Write path: orders + outbox in one transaction

The key rule of the write path: no Produce inside the RPC handler. If the process crashes after Produce and before COMMIT — there's a Kafka event for an order that doesn't exist in the DB. No amount of idempotency fixes that. The [Outbox pattern](../../../../04-reliability/04-03-outbox-pattern/i18n/en/README.md) lecture covered this in detail — here we reuse the same pattern.

In the transaction we write both the order itself and a "publish later" row to the outbox. That's it. The actual publishing is a separate step, and a publishing failure does not break DB consistency.

Here is the core of the Create handler — validation omitted, transaction body:

```go
err = pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
    if _, err := tx.Exec(ctx, insertOrderSQL,
        id, req.GetCustomerId(), req.GetAmountCents(), req.GetCurrency(),
        ordersv1.OrderStatus_ORDER_STATUS_NEW.String(),
    ); err != nil {
        return fmt.Errorf("INSERT orders: %w", err)
    }
    aggregateID := "order-" + id
    if err := tx.QueryRow(ctx, insertOutboxSQL, aggregateID, s.topic, string(payload)).Scan(&outboxID); err != nil {
        return fmt.Errorf("INSERT outbox: %w", err)
    }
    return nil
})
```

The key point: `tx` is shared, both INSERTs under one COMMIT, no `cl.ProduceSync(...)` anywhere in this block. This is the boundary of the "synchronous part" of the hybrid: COMMIT succeeds — we reply `OK` to the client.

The outbox publisher runs as a goroutine in the same process. Every 500ms it reads unpublished rows, sends them to Kafka, marks them as published_at:

```go
records := make([]*kgo.Record, len(batch))
for i, r := range batch {
    var evt orderEvent
    _ = json.Unmarshal([]byte(r.payload), &evt)
    records[i] = &kgo.Record{
        Topic: r.topic,
        Key:   []byte(r.aggregateID),
        Value: []byte(r.payload),
        Headers: []kgo.RecordHeader{
            {Key: "outbox-id", Value: []byte(strconv.FormatInt(r.id, 10))},
            {Key: "aggregate-id", Value: []byte(r.aggregateID)},
            {Key: "trace-id", Value: []byte(evt.TraceID)},
            {Key: "tenant-id", Value: []byte(evt.TenantID)},
            {Key: "event-type", Value: []byte("order.created")},
        },
    }
}
results := cl.ProduceSync(ctx, records...)
```

The record key is `aggregate-id` (`order-<uuid>`). All events for one order go to the same partition, per-key ordering is preserved. Headers contain outbox-id (for deduplication on consumers) and propagation fields.

The guarantee is at-least-once. There is a window between ProduceSync and UPDATE published_at. A crash in that window → the Kafka record remains, the outbox `published_at` is still NULL, on restart we send it again. Duplicate protection lives on the consumers. Here it's simple: PRIMARY KEY (consumer, outbox_id) in `processed_events` and INSERT ON CONFLICT DO NOTHING before each processing step. RowsAffected = 0 → already seen, skip.

## CQRS: write side and read side as separate services

A standard pattern that appears in any microservices textbook, and in the hybrid it emerges naturally.

`CommandService.Create` lives in `order-service` and writes to `orders`. No Get — intentional. If Get were on the same service, it would read the same `orders` table, and reads would compete with writes. Read and write scale differently: writes are often limited by the DB, reads by cache and replicas.

`QueryService.Get` lives in `order-query-service` and reads `orders_view`. That's a separate table, updated by a separate projector process which is itself a consumer on the same `order.created` topic. Get never touches `orders`. Its API is simpler, its DB is simpler, its cache invalidation (if it ever comes) is a separate concern.

The two APIs share no code at all. Only proto. One proto, two services, two processes, two tables. That's it.

Get looks like this:

```go
err := s.pool.QueryRow(ctx, selectViewSQL, req.GetId()).Scan(
    &id, &customerID, &amountCents, &currency, &statusStr, &createdAt,
)
if err != nil {
    if errors.Is(err, pgx.ErrNoRows) || errors.Is(err, sql.ErrNoRows) {
        return nil, status.Errorf(codes.NotFound,
            "order %q not found in read-store (eventual consistency lag)", req.GetId())
    }
    return nil, status.Errorf(codes.Internal, "select view: %v", err)
}
```

Notice the NotFound message — it's intentional. If you call Get with the same id immediately after Create, you may get this error. That's the contract: there is lag between the write-side COMMIT and the projector's UPSERT. The worse the network and load, the larger the lag. A rebalance on the projector's consumer group also temporarily slows the catch-up.

The projector is a regular consumer with manual commit and deduplication. The core:

```go
tag, err := pool.Exec(ctx, dedupSQL, consumerName, outboxID)
if err != nil {
    return fmt.Errorf("dedup outbox-id=%d: %w", outboxID, err)
}
if tag.RowsAffected() == 0 {
    skipped.Add(1)
    continue
}
// ... unmarshal evt ...
if _, err := pool.Exec(ctx, upsertViewSQL,
    evt.ID, evt.CustomerID, evt.AmountCents, evt.Currency, evt.Status, createdAt,
); err != nil {
    return fmt.Errorf("upsert view order=%s: %w", evt.ID, err)
}
```

UPSERT, not INSERT. If a status-change event arrives later (we don't publish those in this lecture, but in production it's standard), `ON CONFLICT DO UPDATE` updates the view.

## Eventual consistency. Where it hurts

Briefly — everywhere the UI expects to read what it just wrote.

Standard workarounds:

1. After Create, the client holds state locally and shows "processing" until the projector catches up. Get is used only for durable display.
2. Read-your-writes via stickiness: `Get` is routed to the specific shard this client writes to, which has a read replica with low lag (or alternatively — write to cache directly from write-side, a separate thread).
3. Sticky session to a single node set, if it's stateful.

Doing any of this by default is overkill. First decide whether read-your-writes matters to this specific UI. Often a 200ms lag bothers nobody.

Inventory doesn't suffer from lag at all. It's a different service with its own source of truth (the reservation). Eventual consistency between it and order-side is a feature: they're loosely coupled, full stop.

```go
if _, err := pool.Exec(ctx, reserveSQL, evt.ID, evt.CustomerID, evt.AmountCents); err != nil {
    return fmt.Errorf("reserve order=%s: %w", evt.ID, err)
}
```

One UPSERT into `inventory_reservations`, no references to orders. In a real system there would be its own DB, its own stock check, and if the reservation fails — publishing an `order.rejected` event that returns to order-side and transitions the order to CANCELLED. That's a choreography saga, a separate lecture ([Saga: choreography vs orchestration](../../../06-05-saga-choreography/i18n/en/README.md)).

## Tracing context propagation

A small but critical detail. Any chain through Kafka breaks ordinary gRPC tracing: spans from one process don't propagate to another automatically. The fix is trivial — put `trace_id` (and `tenant_id` while we're at it) in the payload and Kafka headers. Consumers extract them first thing and start their own span as a child of what arrived in the headers.

In the Create code — the fields are just saved:

```go
evt := orderEvent{
    ID:          id,
    CustomerID:  req.GetCustomerId(),
    AmountCents: req.GetAmountCents(),
    Currency:    req.GetCurrency(),
    Status:      ordersv1.OrderStatus_ORDER_STATUS_NEW.String(),
    CreatedAt:   createdAt.Format(time.RFC3339Nano),
    TraceID:     req.GetTraceId(),
    TenantID:    req.GetTenantId(),
}
```

In the Kafka record they're duplicated in both payload and headers — negligible in bytes, and convenient for both sides: visible in kcat/kafka-ui without parsing, and accessible in consumer code without unmarshaling.

Auth context (user-id, scopes) in an outbox flow usually goes the same way. The lecture shows only trace-id for brevity — adding more fields is copy-paste.

## Running

The sandbox from the repository root must be running (`docker compose up -d`).

Then from the lecture directory:

```sh
make up && make db-init    # Postgres on :15434, schema created
make topic-create          # lecture-06-04-order-created (3 partitions, RF=3)
make run-order             # terminal 1: gRPC :50061 + outbox publisher
make run-inventory         # terminal 2: consumer → inventory_reservations
make run-query             # terminal 3: gRPC :50062 + projector → orders_view
```

The scenario trigger is grpcurl:

```sh
make grpcurl-create        # → response with id
make grpcurl-get ID=<uuid> # right after Create — may return NotFound (lag)
                           # retry after ~100ms — returns Order from orders_view
```

Useful counters while experimenting:

```sh
make orders-count          # orders in write-side
make view-count            # projected into read-side
make reservations-count    # reserved in inventory
make outbox-pending        # not yet published by publisher
```

In normal flow, after a pause all three counts converge. If view-count or reservations-count lags — check whether the corresponding consumer is still running.

Cleanup between runs:

```sh
make db-truncate           # truncate all tables (RESTART IDENTITY)
```

## What to try manually

- Start ONLY `make run-order`, without inventory and query, create 50 orders via grpcurl or simply `for i in $(seq 1 50); do make grpcurl-create; done`. Then start inventory and query — they'll catch up from the beginning because we use `ConsumeResetOffset(AtStart())`.
- Kill `run-query` mid-stream, create more orders, bring query back up. orders_view will catch up. If you disable dedup (delete `processed_events` via `make db-truncate` before starting) — you'll see that reprocessing is idempotent thanks to UPSERT.
- Run query and inventory simultaneously with different group-ids (that's how they're configured): two different consumer groups read the same messages in parallel, without interfering — that's pub/sub.

## What this lecture deliberately doesn't do

- No multi-node. Each service has one instance. The [Microservices communication](../../../../09-use-cases/01-microservices-comm/i18n/en/README.md) use case will have 2-3 nodes per service, the recommended production setup.
- No integration tests. Lectures aren't tested; tests live in the use cases.
- No failure recovery beyond at-least-once + dedup. No sagas, no compensation, no reject flow. That's a use case or [Saga: choreography vs orchestration](../../../06-05-saga-choreography/i18n/en/README.md).
- No Schema Registry. Payload is raw JSON. This is the concept level; the production variant is Protobuf through SR (lecture [Schema Registry](../../../../05-contracts/05-03-schema-registry/i18n/en/README.md)), but here we focus on the hybrid itself to avoid pulling SR into every file.
- The outbox publisher is in the same process as the gRPC server. That's fine for a lecture and for small services; in large systems it's extracted into a separate binary (or CDC via Debezium — lecture [Debezium CDC](../../../../07-streams-and-connect/07-04-debezium-cdc/i18n/en/README.md)).

## Key takeaways

The gRPC + Kafka hybrid splits the work across two axes. The synchronous API answers the client right now. Async effects happen later, without looking back at the client. Outbox closes the gap between the DB and Kafka. CQRS separates write from read — each side evolves at its own pace. Eventual consistency here is a contract. It fires reliably, and treating it as a bug means designing the system with the wrong expectation.

This picture is worth keeping in mind for anyone designing a backend of any complexity beyond "one service → one DB". Next come sagas (when you need to coordinate multiple services in a single business process) and stream processing (when the event log is the primary carrier of business logic). Lectures [Saga: choreography vs orchestration](../../../06-05-saga-choreography/i18n/en/README.md) and 07-* cover that.
