# 06-05 — Saga: Choreography vs Orchestration

A distributed transaction across services. Postgres `BEGIN/COMMIT` doesn't work here — each service has its own database, there's no shared two-phase commit, and nobody will sign up for 2PC over Kafka. So we build a business transaction differently: as a chain of local steps with compensations. That's a saga.

This lecture covers two ways to assemble a saga. Choreography: services communicate through events via Kafka, nobody "conducts." Orchestration: one service with `saga_state` in Postgres walks each saga step by step. Same business scenario — different architecture. Different trade-offs. The goal is to see this hands-on, through code that actually runs.

## Scenario

A customer placed an order for `N` cents. To fulfill it, three steps are required:

1. **Payment** — authorize the payment.
2. **Inventory** — reserve the item in the warehouse.
3. **Shipment** — assign a courier and dispatch.

If anything breaks at any step — roll back the previous ones. Charged the card, no stock — refund. Reserved stock, no couriers — release the reservation and the same refund. Rollbacks don't try to "undo everything atomically": each compensating action is a separate step, with its own success or failure, and it's published as an event just like the forward action.

That's what "compensation" means: the opposite action, performed as an explicit step.

## Where they differ

A saga always has two roles. Someone executes the steps (executors), and someone knows the order (coordination). Choreography spreads coordination across the executors: each service subscribes to its upstream event and publishes its downstream. Nobody sees the saga as a whole.

Orchestration separates coordination into a dedicated service — the orchestrator. It alone knows who comes after whom, it has `saga_state` in the database, it sends commands to executors and waits for replies.

Side-by-side comparison:

| | choreography | orchestration |
|---|---|---|
| Who knows the step order | spread across services | one service |
| Where saga state lives | nowhere in full (each service holds its part) | in `saga_state` in Postgres |
| Service coupling | low (only via topics) | medium (executors know the cmd/reply contract) |
| Adding a new step | subscribe to the right event and publish a new one | update the orchestrator's state machine |
| Saga progress visibility | only via topic logs | one `SELECT` |
| Risk of event loops | real, needs monitoring | zero — the orchestrator won't loop with itself |
| Debugging complex flows | hard | easier |

The main argument for choreography — low coupling. The main argument against — no single place to see the full saga. For simple 2–3 step scenarios, choreography wins. For long and complex ones — orchestration. The marker for "complex" is more than four steps or branching logic like "after X go to Y or Z depending on...".

## Choreography

Topics (`saga-choreo.<service>-<verb>`):

```
order-requested      ─→ payment-completed ─→ inventory-reserved ─→ shipment-scheduled  (happy path)
                       └ payment-failed                                                  (terminal FAILED)
                                              └ inventory-failed ─→ payment-refunded   (money rollback)
                                                                  └ shipment-failed     ─→ inventory-released ─→ payment-refunded
```

No `*-cmd` or `*-reply` here. Only facts about what happened: "payment completed", "reservation failed", "no courier found". A service that has something to compensate subscribes to the "failure" events downstream.

Who subscribes to what:

- **payment-service** listens to `order-requested`, `inventory-failed`, `inventory-released`. On `order-requested` — hits the payment provider (in this sandbox — a fake via `FAIL_RATE`). On the other two — publishes `payment-refunded`.
- **inventory-service** listens to `payment-completed`, `shipment-failed`. On the first — reserves stock. On the second — releases the reservation.
- **shipment-service** listens to `inventory-reserved`. Schedules delivery or fails it.
- **order-service-choreo** listens to all nine topics and builds an in-memory timeline of each saga. This is an observability watcher, added only so the saga's progress is visible in one terminal. In production this role is covered by tracing infrastructure, without a dedicated service.

The key thing to internalize: the compensation cascade is itself a chain of events. `shipment-failed` triggers an action in `inventory-service`, which publishes `inventory-released`, which `payment-service` catches and runs the refund. Nobody calls a "rollback all" list. Each link reacts to its own event.

The handler inside `payment-service` is a plain dispatch wrapper by topic. Here it is in full:

```go
case sagaio.TopicChoreoOrderRequested:
    var evt sagav1.OrderRequested
    if err := sagaio.Unmarshal(r, &evt); err != nil {
        return err
    }
    now := timestamppb.New(time.Now().UTC())
    if shouldFail(failRate) {
        return sagaio.Produce(ctx, cl, sagaio.TopicChoreoPaymentFailed, evt.GetSagaId(),
            &sagav1.PaymentFailed{
                SagaId: evt.GetSagaId(), Reason: "card declined", OccurredAt: now,
            })
    }
    paymentID := "pay-" + uuid.NewString()[:8]
    return sagaio.Produce(ctx, cl, sagaio.TopicChoreoPaymentCompleted, evt.GetSagaId(),
        &sagav1.PaymentCompleted{
            SagaId: evt.GetSagaId(), PaymentId: paymentID,
            AmountCents: evt.GetAmountCents(), Currency: evt.GetCurrency(),
            OccurredAt: now,
        })
```

What to notice: payment knows nothing about the next step. It catches the request, hits the payment provider, publishes a fact. Inventory is not its problem. And it reacts the same way to `inventory-failed`: publishes `payment-refunded` and moves on.

### What's unpleasant about this

There are many subscriptions, nobody sees the saga as a whole. If three months later you add "a notification after shipment" — that's not a one-file change. It means figuring out who subscribes to what, whether the new event loops back to an existing subscriber, and whether in-flight sagas break.

And — an important point — `payment-refunded` arrives from two different causes: after `inventory-failed` and after `inventory-released`. The service must handle both. If it does `if reason == "shipment-cascade" return refund` — it breaks on the second scenario. Idempotency by `saga_id` is mandatory here; otherwise the saga withdraws the refund twice.

## Orchestration

Same scenario, but at the center sits an `orchestrator`. It has its own database on port `15435`, with a `saga_state` table holding one row per saga. Topics are split into `cmd/reply` pairs:

```
saga-orch.place-order        ─ orchestrator listens (entry point)
saga-orch.payment-cmd        ─ payment-service[orch] listens
saga-orch.payment-reply      ─ orchestrator listens
saga-orch.inventory-cmd      ─ inventory-service[orch] listens
saga-orch.inventory-reply    ─ orchestrator listens
saga-orch.shipment-cmd       ─ shipment-service[orch] listens
saga-orch.shipment-reply     ─ orchestrator listens
```

Six executor topics plus one entry topic — three services with a `cmd/reply` pair each, plus the entry. Count it. Not magic.

Executor services here are simpler than in choreography. They process `<X>Command`, do their job (in this sandbox — pseudo), reply with `<X>Reply`. They don't know what came before them or what comes after. They don't know about compensations in the sense of "correct ordering." A `payment-cmd` with `action=REFUND` will arrive — they'll run the refund.

Saga logic lives entirely in the orchestrator. It's a finite state machine. Steps are named `current_step` in `saga_state`:

```
                place-order
                    │
                    ▼
            AWAITING_PAYMENT
              │           │
       ok=true            ok=false
              │              │
              ▼              ▼
       AWAITING_INVENTORY  DONE/FAILED
         │             │
    ok=true          ok=false
         │             │
         ▼             ▼
   AWAITING_SHIPMENT  COMPENSATING_PAYMENT (refund)
     │            │
ok=true          ok=false
     │            │
     ▼            ▼
DONE/SUCCESS  COMPENSATING_INVENTORY (release)
                  │
                  ▼
               COMPENSATING_PAYMENT (refund)
                  │
                  ▼
               DONE/FAILED
```

Each graph node is a row state in `saga_state`. Each edge is a reply event arriving, leading to an UPDATE of that row and publishing the next command.

The actual transition code after a successful `payment.AUTHORIZE`:

```go
if rep.GetOk() {
    pid := rep.GetPaymentId()
    if err := updateSaga(ctx, pool, rep.GetSagaId(),
        stepAwaitingInventory, statusRunning, "payment.authorized",
        &pid, nil, nil, nil); err != nil {
        return err
    }
    return sagaio.Produce(ctx, cl, sagaio.TopicOrchInventoryCmd, rep.GetSagaId(),
        &sagav1.InventoryCommand{
            SagaId:      rep.GetSagaId(),
            Action:      sagav1.InventoryAction_INVENTORY_ACTION_RESERVE,
            CustomerId:  row.customerID,
            AmountCents: row.amountCents,
        })
}
```

What's visible here: UPDATE state first, then ProduceSync the next command. Same pattern on every graph edge — that's why the entire orchestrator fits in three handlers plus an entry point. And — pay attention — there's a weak spot here.

### Where sagas hurt even in orchestration

UPDATE succeeded, then the process crashed before ProduceSync. `saga_state` says `AWAITING_INVENTORY`, but no command was sent. The saga is stuck. In production this is covered by the transactional outbox (see `04-03`) — UPDATE and INSERT into outbox in one transaction, a separate publisher sends to Kafka and marks the record as published. In this lecture we deliberately keep it simple so the focus stays on the state machine, not the infrastructure. Remember: standalone UPDATE + Produce is at-least-once with a hang risk, and the outbox fixes it.

The second weak spot — duplicate messages from the executor itself. A reply can arrive twice (consumer restarts before offset commit). The lecture's code has no protection for this: `UPDATE saga_state` (see `cmd/orchestrator/main.go:52`) filters only on `saga_id`, without checking `current_step`. A duplicate `<X>-reply` will quietly roll the step backwards and re-emit the next command. Only `place-order` is idempotent, via `INSERT ... ON CONFLICT DO NOTHING`. In production this is fixed either with `WHERE current_step = $expected` in the UPDATE, or with a `processed_events` table keyed on the reply partition offset. Executor services still carry the requirement: "process a command idempotently by `saga_id` and `action`", because the orchestrator will redeliver the same command.

## Who, when, and why

Choreography is good when:

- Steps are few (2–4).
- The team is split across microservices and doesn't want to share a "common" orchestrator.
- Coupling cost is high — for example, services run in different languages and sharing a common command-protocol contract is impractical.

Orchestration is good when:

- Many steps or branching logic.
- Saga state visibility is needed — via a select, a dashboard, a runbook.
- The business needs metrics like "how many sagas are currently in COMPENSATING_PAYMENT" — in choreography, that data doesn't exist anywhere.

An intermediate mode: orchestration for critical sagas and choreography for everything else. There's no need to make "one correct choice for the entire system" — these are different tools.

## Running it

First, bring up Postgres and create the topics:

```sh
make up
make db-init
make topic-create-all
```

### Choreography

Four terminals. Each — a separate service. Start order doesn't matter, any order works.

```sh
make run-payment-choreo      # terminal 1
make run-inventory-choreo    # terminal 2
make run-shipment-choreo     # terminal 3
make run-order-choreo        # terminal 4 — observability
```

Trigger the saga:

```sh
make run-place-order MODE=choreo COUNT=3
```

In the fourth terminal (`run-order-choreo`) you'll see a timeline for each saga as it progresses through the steps. Happy path — four events, ending with `shipment.scheduled`.

To observe compensation, run shipment-service with a forced failure:

```sh
make chaos-fail-shipment    # instead of the regular run-shipment-choreo
```

Then run `make run-place-order MODE=choreo COUNT=1` again. The timeline will show the full cascade: `order-requested → payment-completed → inventory-reserved → shipment-failed → inventory-released → payment-refunded`. Six events instead of four — that's the cost of a rollback: two extra steps to release the reservation and refund the money.

### Orchestration

Same four terminals, but now with `-mode=orch`:

```sh
make run-orchestrator        # terminal 1 — needs Postgres from make up
make run-payment-orch        # terminal 2
make run-inventory-orch      # terminal 3
make run-shipment-orch       # terminal 4
```

Trigger:

```sh
make run-place-order MODE=orch COUNT=3
```

Saga state lives in `saga_state`. To view the current picture:

```sh
make saga-list
```

You'll see one row per saga with `current_step`, `status`, and payment/reservation/shipment IDs. Compare with choreography: there, state doesn't exist anywhere in full — it's spread across service logs.

Compensation in orchestration — same `chaos-fail-shipment`, but run the shipment service in orch mode:

```sh
SHIPMENT_FAIL_RATE=1 make run-shipment-orch
make run-place-order MODE=orch COUNT=1
make saga-list   # you'll see DONE/FAILED, failure_reason populated
```

Here you read the saga outcome directly from the table. In choreography you'd have to scan logs across all services or hook up the `run-order-choreo` observer.

## How it relates to the rest of the course

`04-01` (transactions and EOS) and `04-03` (outbox pattern) — adjacent topics. Saga solves a different problem: it doesn't make a local transaction atomic (that's the outbox), and it doesn't make a write to N topics of a single service atomic (that's EOS). Saga provides "integrity" of a business operation spread across services through explicit steps and compensations. Outbox and EOS are the building blocks that make saga easier. Without them it runs at-least-once with a hang risk, as in this lecture.

`06-04` (hybrid gRPC + Kafka) — the neighboring lecture, where gRPC + outbox + one topic. Saga is the natural extension from there: more topics, more services, more states. In essence, in orchestration the orchestrator lives by the same scheme — "command → event → next command" — just coordinated.

## Files

- `cmd/place-order/main.go` — saga trigger, shared by choreo/orch.
- `cmd/order-service-choreo/main.go` — choreography observability watcher.
- `cmd/payment-service/main.go` — payment, two modes.
- `cmd/inventory-service/main.go` — reservation, two modes.
- `cmd/shipment-service/main.go` — shipment, two modes.
- `cmd/orchestrator/main.go` — state machine in orchestration with `saga_state` in Postgres.
- `proto/saga/v1/saga.proto` — choreography events and orchestration command/reply pairs.
- `db/init.sql` — `saga_state` table and index on status.
- `docker-compose.override.yml` — Postgres on port 15435.

And — a final thought. Saga doesn't remove the complexity of a distributed transaction. It relocates it: from "find a 2PC algorithm" to "correctly describe each step and its compensation, and make both idempotent." When that thought becomes natural — most infrastructure decisions in a distributed system start falling into place on their own.
