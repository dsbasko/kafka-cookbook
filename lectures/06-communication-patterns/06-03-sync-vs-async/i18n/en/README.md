# 06-03 — Sync vs Async: gRPC vs Kafka on the "user signed up" scenario

The business situation is straightforward. A new user registered — and a whole bunch of services need to know about it immediately: analytics, notifications, billing, anti-fraud, CRM. The day after tomorrow, analytics migrates from Postgres to ClickHouse, and we'll need to change — what?

That "what" is the whole lesson.

The solution "one service fans out to a list of recipients via gRPC" and the solution "one service writes one event to Kafka, the rest read it" look the same from the outside. In code — two different worlds. Below we build both and talk about what each one costs.

## Scenario

`user-service` registered a user and wants to "notify everyone interested". There are several interested parties to start with:

1. `analytics` — writes the registration fact to its storage for reports.
2. `notifications` — sends a welcome email.
3. `billing` — opens an empty account for future charges.

Tomorrow, the day after — more will join.

Two implementation options:

- **Sync (gRPC fan-out).** `user-service` knows the URLs of all recipients. Calls each via a separate unary RPC `Notify(UserSignedUp)`. All three replied `OK` — registration is considered complete.
- **Async (Kafka).** `user-service` writes one message to the `user-events` topic. Recipients subscribe themselves (each in its own consumer group) and read independently. The sender has no further concern about them.

Both variants are in the repository and build. We'll run them, poke around, compare.

## What's in the code

```
06-03-sync-vs-async/
├── proto/users/v1/users.proto         # shared contract UserSignedUp + service UserEventService
├── cmd/grpc-broadcast/                # sync sender
├── cmd/grpc-listener/                 # sync receiver (run N copies on different ports)
├── cmd/kafka-broadcast/               # async sender
└── cmd/kafka-listener/                # async receiver (run N copies with different -group)
```

`UserSignedUp` is the same proto schema for both branches. So there's no temptation to compare apples and oranges.

## Sync: gRPC fan-out

The contract — a single unary RPC, implemented by each recipient:

```proto
service UserEventService {
  rpc Notify(NotifyRequest) returns (NotifyResponse);
}
```

The receiver sits on its port and waits to be called. Inside — no logic, just a log entry "got this user":

```go
func (s *listenerServer) Notify(_ context.Context, req *usersv1.NotifyRequest) (*usersv1.NotifyResponse, error) {
    ev := req.GetEvent()
    if ev.GetUserId() == "" {
        return nil, status.Error(codes.InvalidArgument, "user_id is required")
    }
    fmt.Printf("[%s] got user_id=%s email=%s country=%s\n",
        s.name, ev.GetUserId(), ev.GetEmail(), ev.GetCountry())
    return &usersv1.NotifyResponse{Accepted: true}, nil
}
```

The key thing on the sender side — the list of URLs. Where it comes from is a separate discussion (env, config, service discovery — pick your flavor). The point is that the sender explicitly knows about each recipient's existence:

```go
targets := flag.String("targets", "",
    "comma-separated list of recipient URLs; if empty, taken from LISTENER_URLS")
```

Next — the fan-out itself. Two modes, because the course wants to show both effects: "one slow recipient stalls everyone" (sequential) and "one failure is isolated" (parallel):

```go
if !parallel {
    for _, c := range clients {
        callOne(ctx, c, ev, timeout)
    }
    return
}
var wg sync.WaitGroup
for _, c := range clients {
    wg.Add(1)
    go func(c targetClient) {
        defer wg.Done()
        callOne(ctx, c, ev, timeout)
    }(c)
}
wg.Wait()
```

Now point by point — what we got and what we paid.

1. **Coupling — tight.** To add a new recipient, you redeploy `user-service` (or, at best, reload the config). This is not "decoration" — every new downstream becomes part of the deploy checklist for the upstream service.
2. **Latency — sum or max.** Sequential — total tail = sum of all `Notify` calls. Parallel — max. One slow recipient stalls the whole use case.
3. **Delivery — best-effort.** Recipient crashed between `accept` and processing? Event is lost. Want retry — write it yourself. Want deduplication — also yourself. There's no queue, nowhere to hold events.
4. **Replacing a recipient.** Analytics migrates to a new version — you need blue-green with two URLs in the list, a coordinated switchover moment, complex partial failure handling.

On the other hand, the advantages are real. Latency is predictable (no guaranteed delay from a broker), errors at the recipient are visible immediately — the sender knows whether delivery succeeded. For synchronous commands ("charge money, wait for confirmation, then continue") that's exactly what you need. This just isn't our scenario.

## Async: Kafka publish/subscribe

Sender — one producer, one topic, one message per registration. No URLs, no "list of recipients" in its code at all. Here's the core:

```go
ev := mockUser(i)
payload, err := proto.Marshal(ev)
// ...
rec := &kgo.Record{
    Topic: *topic,
    Key:   []byte(ev.GetUserId()),
    Value: payload,
}
res := cl.ProduceSync(rpcCtx, rec)
```

The key is `user_id`. Not for routing between recipients (we don't know the recipients at all), but to guarantee "events for one user go to one partition". If any recipient tomorrow wants stateful per-user processing — ordering is already there.

Receiver — a standard consumer group. The group name identifies who it is: `analytics`, `notifications`, `billing`. Each group reads the topic independently, each with its own committed offset:

```go
opts := []kgo.Opt{
    kgo.ConsumerGroup(group),
    kgo.ConsumeTopics(topic),
    kgo.ClientID(fmt.Sprintf("lecture-06-03-listener-%s", group)),
}
if fromStart {
    opts = append(opts, kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
}
```

Then — the standard `PollFetches`, `proto.Unmarshal`, print loop. No HTTP ports, no retry loops in the sender's code — all of that moved into Kafka's infrastructure.

What we got:

1. **Coupling — loose.** The sender knows nothing about recipients. Tomorrow CRM wants to subscribe — they bring up a service with a new group `crm`, the sender won't even know.
2. **Latency — two hops.** Sender → broker → receiver. This is slower than direct gRPC; on typical sandboxes the difference is in the single-digit milliseconds. For most event-driven scenarios — not a problem.
3. **Delivery — at-least-once.** The message lives in the log until retention expires. The recipient crashed, restarted, resumed from the committed offset. Replay (re-read everything for the past week) — built in, free. Deduplication — on the recipient side (covered in [Processing guarantees](../../../../03-consumer/03-03-processing-guarantees/i18n/en/README.md)).
4. **Replacing a recipient.** Bring up a new version of analytics with group `analytics-v2`, it reads from earliest and catches up on history. Compare counters, switch downstream consumers to v2. The old version can sit alongside for another week — it doesn't interfere.

The cost is also real. The sender loses visibility into "did the event actually reach the recipient" — it only knows "the broker wrote it". Between "written" and "processed" there can be hours of distance if the recipient falls behind. Eventual consistency. Accept it.

## Decision matrix

Not a "right/wrong" table, but axes along which the choice clearly tilts one way or the other.

| Criterion                          | gRPC fan-out                            | Kafka pub/sub                           |
|------------------------------------|-----------------------------------------|-----------------------------------------|
| Sender knows recipients            | yes, explicit list                      | no, sender writes to a topic            |
| Add a new recipient                | redeploy sender                         | bring up a new consumer group           |
| Latency end-to-end                 | sum (seq) or max (par)                  | produce + topic lag                     |
| One slow recipient                 | stalls everyone (seq) / itself (par)    | stalls only itself                      |
| Sender learns of recipient error   | immediately, via gRPC status            | never (not its concern)                 |
| Replay past events                 | manually (must store separately)        | built in (by topic retention)           |
| Delivery guarantee                 | best-effort                             | at-least-once (with correct acks)       |
| Backpressure                       | sender blocks on slow recipient         | broker buffers, sender doesn't wait     |
| Ordering                           | by call order                           | per-partition, by key                   |

Seven dimensions (eight if you count ordering). In practice, it's usually not one factor but a combination of two or three.

A rough rule that holds in 90% of cases. If there is **one** recipient and a response is needed — gRPC unary. If there are **many** recipients or there will be — Kafka. If the recipient is **maintained by a team outside your responsibility** — definitely Kafka, otherwise the neighboring team's tech lead will be at your desk forever asking "update our URL in the config".

## Anti-patterns

A short list of things that look tempting but end badly in production.

**Kafka instead of synchronous RPC "because it's trendy".** "Let's do the order through Kafka — it'll be async, clean". User clicked "buy", waits for a redirect to the success page, and the order is rolling through the broker, then through the billing consumer, then the response back through another topic. Latency — seconds, debugging — hell. If a synchronous response is needed — use synchronous RPC. Period.

**gRPC fan-out with five recipients instead of an event bus.** Six months pass, there are now five recipients, redeploying each one is its own drama. Every new internal service is responsible for getting into someone else's config. At some point someone adds a fallback "if URL is unavailable — log and skip". Then silent event loss begins. As soon as you see the URL list in the config growing — that's the signal to migrate to a topic.

**Mixing everything into one RPC.** "Let's make a gRPC API that synchronously handles order creation, also publishes an event to Kafka, and also calls the email service". Three failure points in one operation, three places where transactionality breaks. If you absolutely need "respond immediately and publish" — that's the [Outbox pattern](../../../../04-reliability/04-03-outbox-pattern/i18n/en/README.md), not three parallel calls in one handler.

**Kafka "instead of REST" for queries.** An RPC "give me a user profile by id" via a `user-requests` topic and a response `user-responses` topic with a correlation_id — you see this from people who love Kafka more than synchronous APIs. Latency is tolerable, debugging is impossible, observability is zero. Don't do this. Requests — gRPC/HTTP. Kafka — for events that happened.

## What to run manually

The sandbox from lectures 01–04 should already be running (`docker compose up -d` in the repository root). Then open a bunch of terminals.

**Sync variant** (4 terminals):

```
make run-grpc-listener-1            # terminal 1, on :50061, name analytics
make run-grpc-listener-2            # terminal 2, on :50062, name notifications
make run-grpc-listener-3            # terminal 3, on :50063, name billing
make run-grpc-broadcast USERS=5     # terminal 4, sends 5 events to each
```

Each of the first three terminals will show 5 lines `[name] got user_id=...`. The fourth — a table of "where it was sent, what was returned, how long it took". Then stop any listener (Ctrl+C) and run broadcast again — you'll see `FAIL code=Unavailable` for the stopped one, the rest work fine.

Parallel mode: `make run-grpc-broadcast-parallel USERS=5`. The "one slow recipient stalls everyone" effect is not visible in this demo without an artificial `time.Sleep`; in production — it's the most common cause of end-to-end latency degradation.

**Async variant** (4 terminals):

```
make topic-create                                 # once
make run-kafka-listener-analytics                 # terminal 1, group=analytics
make run-kafka-listener-notifications             # terminal 2, group=notifications
make run-kafka-listener-billing                   # terminal 3, group=billing
make run-kafka-broadcast USERS=5                  # terminal 4
```

The first three terminals quietly wait. After running broadcast — each will print 5 lines `[group] partition=X offset=Y user_id=...`. Stop the `analytics` listener, run broadcast again. `notifications` and `billing` received new events, `analytics` — missed them. Now bring `analytics` back up — and it catches up. No retry loops in the sender, no knowledge of "who's alive and who's not".

Experiment separately with adding a new recipient. In the sync variant — start a fourth listener, add its URL to `LISTENER_URLS`, restart broadcast. In the async variant — start a fourth listener with a new group `make run-kafka-listener -group=crm` (or explicitly `go run ./cmd/kafka-listener -group=crm -from-start=true`). The sender writes the same thing in both cases — but in the second case it didn't need to be touched at all.

## What comes next

In the next lesson ([gRPC + Kafka hybrid](../../../06-04-hybrid-grpc-and-kafka/i18n/en/README.md)) we take the hybrid — a synchronous gRPC API for writes plus the outbox pattern with Kafka for events. The result is the "sync on write, async on side-effects" architecture that's almost always chosen in production when there are more than two services and real load.

The [Microservices communication](../../../../09-use-cases/01-microservices-comm/i18n/en/README.md) use case rolls out the same thing on a multi-node setup with an integration test and failure recovery — check it if you want to verify that what's built here actually holds under load.
