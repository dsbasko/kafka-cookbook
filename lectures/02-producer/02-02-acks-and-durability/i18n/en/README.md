# 02-02 — Acks & Durability

In the previous lesson ([Keys and partitioning](../../../02-01-keys-and-partitioning/i18n/ru/README.md)) the key determined **where** a record lands. Now we cover a different small option — `acks`. It answers a completely different question: **when** the producer considers a record written. The answer determines whether you lose data when a broker goes down.

The option looks simple. Three values, a number. Behind that number sits a different consistency model and a different durability ceiling. Mix it up and you'll find out either in your first serious incident or in a postmortem review for a neighboring team.

## What "record accepted" actually means

Writing a message to Kafka is not a single atomic step. At the producer level it is a sequence.

1. Serialize the payload, place it in the local buffer.
2. Send the batch to the partition leader.
3. The leader writes the batch to its log (to disk or at least to page cache).
4. Followers pull the batch from the leader and also write it.
5. The leader sees that N replicas confirmed the write and responds to the producer.

`acks` answers the question: **at which of these steps** do we consider the write successful and return control to the application.

There are three levels, and each is a trade-off between latency and durability:

1. **`acks=0`** — the producer writes to the socket and immediately considers the job done. The leader sends no response at all. The fastest mode. The least reliable: if the leader crashes between receiving and writing to disk, nobody ever finds out the record was lost. Metrics will show the producer "sent everything".
2. **`acks=1`** — the leader responds once it has written the batch to itself. Followers may not yet have a copy. If the leader crashes immediately after the ack and replicas were behind, the new leader elected from the ISR may not know about those records — the loss is silent but real.
3. **`acks=all`** (also `acks=-1`) — the leader waits until **all ISR replicas** confirm the write, then responds. The most expensive mode in terms of latency. The most durable — you lose data only if the entire cluster goes down, or if the ISR shrinks below `min.insync.replicas` and the producer sees the error itself.

"Write to disk" in the steps above usually means page cache, not a synchronous fsync. Kafka relies on replication by default, not fsync (see `flush.messages` / `flush.ms`). That is a separate dimension of durability — covered in module 08.

## How min.insync.replicas fits in

This is a paired parameter on the topic side (or broker), and it only applies with `acks=all`. At other levels it is simply ignored — the producer did not ask the leader to wait for ISR acknowledgements, so the leader does not wait.

The logic is this. With `acks=all`, the leader looks at the current ISR (in-sync replicas, not lagging) and checks: is their count ≥ `min.insync.replicas`? If yes — it writes, waits for ISR confirmations, and responds. If no — it responds with `NOT_ENOUGH_REPLICAS` (or `NOT_ENOUGH_REPLICAS_AFTER_APPEND` if it already accepted the write), without waiting for confirmations.

Why `min.insync.replicas` is needed separately. Without it, `acks=all` means "wait for all ISR" — but the ISR can shrink to 1 (leader only). Then `acks=all` effectively becomes `acks=1`. `min.insync.replicas` sets a floor: "either I have N in-sync replicas, or I refuse to accept the write". It is a safeguard against silent durability degradation.

The standard production formula: RF=3, `min.insync.replicas=2`. RF=3 gives three copies. min.ISR=2 says: "I can afford to lose one broker from the ISR — writes keep going. If I lose two — I'd rather fail with an error than silently write to a single replica and lose it when that replica crashes". In the sandbox we set `min.insync.replicas=3` on these topics for the demo — this pattern exists only for this lesson to trigger `NOT_ENOUGH_REPLICAS` when one broker is stopped; in real production use 2.

## What gets lost in each case

Scenario: the leader crashes immediately after the producer receives the ack.

With `acks=0` the scenario is even simpler: the producer already considers the write successful. The leader may not have even received the packet (`ECONNRESET` after write is valid: the kernel accepted the data into the socket buffer, promised to send it, but didn't make it). The record is lost; the producer doesn't know.

With `acks=1` the packet definitely arrived and the leader wrote it to itself. Followers may not have caught up yet. If the leader crashes before any ISR member pulled that record, the new leader elected from the ISR doesn't see it. The record is lost; the producer thinks everything is fine.

With `acks=all` the leader responded only after **all ISR** confirmed. If the ISR had >=2 replicas and the leader crashes — another ISR replica already has that record, it becomes the new leader, nothing is lost. This is what "survives a broker failure" means. The limit is `min.insync.replicas`: if the ISR drops below it, the producer sees an error and **does not receive** an ack — the application decides what to do (retry, DLQ, 5xx to the client).

Short rule: `acks=all` plus `min.insync.replicas >= 2` plus RF=3 — that is the point where Kafka does not lie about durability. Everything else is a trade-off with conscious data loss.

## Idempotency — a side rule

There is an important detail about franz-go (and the Java client too). By default the producer is **idempotent** — it can deduplicate retries. Idempotency requires `acks=all`. If you set `acks=0` or `acks=1`, explicitly disable idempotency with `kgo.DisableIdempotentWrite()`. Otherwise the client will error on initialization with `idempotency requires acks=all`.

More on the idempotent producer in the next lesson ([Idempotent producer](../../../02-03-idempotent-producer/i18n/ru/README.md)). For now, remember: franz-go default = idempotent producer with acks=all. To lower acks, you must also disable idempotency.

## What the code does

There is one binary: `cmd/bench-acks`. It runs three producers with three different acks values and compares latency and throughput under the same load profile.

There are three topics because we want to see the isolated effect of each mode — one producer must not interfere with another. Each topic is created idempotently with `partitions=3`, `replication.factor=3`, `min.insync.replicas=3`.

The mode configuration itself — three entries in an array:

```go
deliveryTimeout := kgo.RecordDeliveryTimeout(5 * time.Second)
modes := []ackMode{
    {"acks=0", "0", []kgo.Opt{kgo.RequiredAcks(kgo.NoAck()), kgo.DisableIdempotentWrite(), deliveryTimeout}},
    {"acks=1", "1", []kgo.Opt{kgo.RequiredAcks(kgo.LeaderAck()), kgo.DisableIdempotentWrite(), deliveryTimeout}},
    {"acks=all", "all", []kgo.Opt{deliveryTimeout}}, // franz-go default: idempotent + AllISRAcks
}
```

`DisableIdempotentWrite()` is required for `acks=0` and `acks=1` — without it the client won't start. For `acks=all` nothing is needed, the default is already correct. `RecordDeliveryTimeout(5s)` sets an upper bound on full delivery of a single record — without it, franz-go under a degraded ISR will keep retrying `NOT_ENOUGH_REPLICAS` until it hits the global context, and this is hard to see in the output.

For latency measurement I used synchronous `ProduceSync` per-record — the client never has more than one message in flight. This underestimates throughput compared to a real async pipeline (where batching gives x10–x50), but it gives honest per-record latency. P50/P99 show how long one full round-trip takes for specific acks, not "the time to deliver an entire batch amortized across all records inside".

The write loop itself — bare `ProduceSync` with a timestamp around it:

```go
for i := 0; i < msgs; i++ {
    if err := ctx.Err(); err != nil {
        break
    }
    rec := &kgo.Record{Topic: topic, Value: payload}
    rpcCtx, rpcCancel := context.WithTimeout(ctx, 15*time.Second)
    sendAt := time.Now()
    out := cl.ProduceSync(rpcCtx, rec)
    took := time.Since(sendAt)
    rpcCancel()

    if err := out.FirstErr(); err != nil {
        res.failed++
        res.errs[classifyErr(err)]++
        continue
    }
    res.sent++
    res.latencies = append(res.latencies, took)
}
```

After the run all three goroutines wait for each other, then the summary table is printed — sorted latencies are reduced to percentiles with a simple:

```go
func percentile(sorted []time.Duration, p float64) time.Duration {
    if len(sorted) == 0 {
        return 0
    }
    idx := int(float64(len(sorted)-1) * p)
    return sorted[idx]
}
```

At the end — as a separate step — `kadm.ListEndOffsets` per topic, to verify that exactly as many records settled in the log as we counted as `SENT`.

## How to read the output

Under a healthy sandbox (`make run` without kill-broker) a typical result:

```
параллельно пишем 1000 сообщений по 1024 B на каждый режим acks (partitions=3, rf=3, min.insync.replicas=3)

результаты:
MODE      SENT  FAILED  ELAPSED   THROUGHPUT   P50      P99     P99.9    MAX
acks=0    1000  0       42.37ms   23603 msg/s  16.0µs   78.0µs  1.91ms   18.32ms
acks=1    1000  0       680.24ms  1470 msg/s   582.0µs  1.80ms  14.47ms  19.78ms
acks=all  1000  0       1.22s     819 msg/s    753.0µs  2.70ms  14.36ms  268.45ms
```

What matters. P50 for `acks=0` is tens of microseconds. That is just the time to hand the packet to the kernel socket — no broker round-trip at all. For `acks=1` it is already milliseconds — the leader wrote and responded. For `acks=all` slightly more — the leader additionally waited for ISR followers. The difference between `acks=1` and `acks=all` is usually small on a healthy cluster with a fast network — 30–50% overhead. On a slow network or under loaded followers the gap widens sharply.

Throughput is inverse: 23k/1.4k/0.8k msg/s. The numbers look frighteningly low, but we are synchronous — no batching, no parallelism. With a normal async producer and a linger — a different story; in [Batching and throughput](../../../02-04-batching-and-throughput/i18n/ru/README.md) we measure throughput specifically.

The `MAX` column is a separate story. Hundreds of milliseconds can appear there sometimes (268ms for `acks=all` in my run). This is typical: the first write to a topic after client startup triggers a metadata refresh, leader lookups, and connection opening to the relevant brokers. So `MAX` is most likely the first record, not representative latency. P99/P99.9 show the real tail.

## What happens with `make kill-broker`

Stop `kafka-2` (`docker stop kafka-2`), wait a few seconds for the controller to notice, then run `make run` again. ISR is now 2 on each partition (`Isr: 1,3`). We have `min.insync.replicas=3` — so for `acks=all` the leader cannot satisfy the condition.

What you will see:

```
результаты:
MODE      SENT  FAILED  ELAPSED   THROUGHPUT  P50      P99      P99.9   MAX
acks=0    300   0       55.21ms   5434 msg/s  54.0µs   495.0µs  4.62ms  24.31ms
acks=1    300   0       165.93ms  1808 msg/s  307.0µs  2.67ms   5.28ms  30.98ms
acks=all  0     12      60.08s    0 msg/s     0        0        0       0

[acks=all] классы ошибок:
ERROR                                                        COUNT
DEADLINE_EXCEEDED                                            1
records have timed out before they were able to be produced  11
```

`acks=0` and `acks=1` wrote all 300 as if nothing happened — leaders of all partitions are alive (`kafka-1` and `kafka-3` stayed up), the request does not need ISR. `acks=all` delivered 0 records and accumulated 12 timeout errors over 60 seconds (after which the runtime cancels the goroutine via the global timeout). In a 60-second window with `RecordDeliveryTimeout(5s)`, exactly ~12 records physically get a chance — each waits 5 seconds of internal retries and gives up.

The error from franz-go looks like `records have timed out before they were able to be produced`. Internally the client was retrying on `NOT_ENOUGH_REPLICAS` (a retriable error — the cluster might recover), ran out of time, and issued a record-level timeout. To see `NOT_ENOUGH_REPLICAS` explicitly, set `kgo.RecordRetries(0)` — then the first error from the leader surfaces immediately. Don't do this in production (any transient hiccup will kill produce), but for diagnostics it is a valid technique.

After the experiment — `make restore-broker`. The command brings `kafka-2` back up and waits a few seconds for followers to catch up with the log. ISR on the topics returns to `1,2,3` and `acks=all` works again.

## Key takeaways

After this lesson these points should be clear:

- `acks` is a choice between durability and latency. The default in franz-go (and in most production configurations) is `acks=all`. That is the correct default.
- `acks=0` — for metrics, telemetry, audit trails where "more is better but loss is acceptable". Any serious payload — not here.
- `acks=1` — a trade-off that is tempting on latency but allows silent loss when the leader crashes. In my experience — almost always a bad choice. If you genuinely need latency savings, it is usually better to tune batching and compression with `acks=all` than to lower durability.
- `acks=all` does nothing on its own. It works together with RF≥2 and `min.insync.replicas≥2`. Without these, `acks=all` can silently become `acks=1` when the ISR shrinks.
- When `min.insync.replicas` is stricter than the current ISR, the producer sees an error. This is a **feature**, not a bug — better to not write than to write to a single replica and lose it.
- Idempotency in franz-go is enabled by default and requires `acks=all`. To lower acks — `kgo.DisableIdempotentWrite()`. Idempotency itself is the topic of the next lesson ([Idempotent producer](../../../02-03-idempotent-producer/i18n/ru/README.md)).

In [Idempotent producer](../../../02-03-idempotent-producer/i18n/ru/README.md) we cover what the idempotent producer actually does — why it protects against duplicates on retries and why it does **not** protect against the zombie scenario between sessions.

## Running

The sandbox must be running (`docker compose up -d` from the root).

Basic run against a healthy cluster:

```sh
make run
```

With a different message count and payload:

```sh
make run MESSAGES=2000 PAYLOAD=2048
```

Broker failure demo. Run in two terminals or sequentially:

```sh
make kill-broker      # stop kafka-2, ISR drops to 2
make run              # acks=all fails with timeout, acks=0/1 keep working
make restore-broker   # kafka-2 back up, ISR recovers
```

Describe topics via `kafka-topics.sh --describe`:

```sh
make topic-describe
```

Clean up after the lesson:

```sh
make topic-delete
```
