# 02-03 — Idempotent Producer

In [Acks and durability](../../../02-02-acks-and-durability/i18n/ru/README.md) we briefly mentioned that franz-go is an idempotent producer with `acks=all` by default. Here we'll break down what that means, what it protects against — and what it doesn't. Most importantly, we'll watch the difference play out: same code, flip a flag, behavior changes.

The idempotent producer arrived in Kafka 0.11 precisely because without it any reliable producer in production collected duplicates. Duplicates surfaced from the nature of networking — application bugs are secondary here. The scenario is painfully simple.

## Where duplicates come from

The producer sends a batch to the broker. The broker writes the batch to the log, replicates to followers, responds "ok" — and somewhere along the way that response vanishes. Sources are many: `ECONNRESET` at the TCP level, GC pause in the client, leader switch at exactly the wrong moment, network partition between two rack switches. The client times out the RPC and knows nothing about what happened to the batch on the broker side. The log already has the entry. The client sees "no response received" and retries. The broker, completely unaware, writes the same batch **a second time**. From the consumer's perspective: two identical messages with different offsets.

This is not a theoretical problem. Under decent load, duplicates surface regularly even on a healthy cluster — precisely because timeouts and retries are necessary to survive network blips. Without retries the producer drops messages. With retries — it duplicates. A closed loop.

The idempotent producer breaks that loop.

## How idempotency works at the protocol level

When a client with idempotency enabled goes to the cluster for the first time, it sends an `InitProducerId` request. The broker issues a producer identifier and the client starts counting sequence numbers per partition. Three values hold everything together:

1. **producer-id (PID)** — a 64-bit integer, monotonically increasing on the broker. Unique per session.
2. **producer-epoch** — a short number, incremented on restart of the same `transactional.id` (that's the transactions world; for bare idempotency it's usually 0).
3. **sequence number** — a counter starting at 0, incremented for each message to that partition. Separate for each partition.

Every record leaves with the triple `(PID, epoch, sequence)` in the batch header.

The broker, on the partition side, tracks the **last accepted sequence** for each (PID, epoch) pair. The verification algorithm:

- Batch arrives with `sequence == last + 1` — normal. Write it, update `last`.
- Batch arrives with `sequence == last` or lower — **duplicate**. Return "as if written", but don't write to the log.
- Batch arrives with `sequence > last + 1` — **out-of-order** (a gap somewhere in the middle). The broker rejects with `OUT_OF_ORDER_SEQUENCE_NUMBER`. This signals the client that something has gone seriously wrong.

That's the whole magic trick. Deduplication happens on the partition side, keyed by (PID, epoch, sequence). Memory overhead is small (last sequence per PID), and it's stored in `producer-snapshot` files in log segments — it survives segment rotation and broker restarts.

Requirements:

- `acks=all` — mandatory. Without it the idempotency protocol won't activate. The connection isn't obvious but it's direct: deduplication only works if the broker is certain the batch was stored reliably (replication confirmed); otherwise the "already-accepted sequence" could be lost, and the client's retry would get a false positive.
- `max.in.flight.requests.per.connection ≤ 5` — otherwise retries can reorder batches in the log. franz-go enforces this limit itself.
- `enable.idempotence=true` — the default in franz-go. To disable: `kgo.DisableIdempotentWrite()`.

## What idempotency doesn't cover

The boundary is exactly where the **producer session** ends. A PID lives as long as the client lives. Restart the process — get a new PID, sequence resets to zero. The broker sees a "new producer" and accepts any sequence as valid.

What this means in practice. A producer started, managed to send a record (PID=42, seq=17), didn't receive an ack, crashed with OOM. It came back up — `InitProducerId`, now PID=43, seq=0. If the application remembers that record as "unconfirmed" and decides to resend it in the new session — that's a duplicate on the partition side. From the broker's perspective, these are two different records from two different producers.

The zombie scenario is a separate story, exactly about this. The old producer process hung — long GC pause, network loss, swap thrash, anything. The process didn't crash, the OS didn't kill it. A new process with the same logical role started — got its own PID. The old one "wakes up" with a batch in flight and delivers it. The broker accepts: the PIDs are different, to it these are two independent producers. Both copies land in the log.

Idempotency alone can't fix this. Protection from zombies is **transactional.id** + producer-epoch fencing, the subject of the next module ([Transactions and EOS](../../../../04-reliability/04-01-transactions-and-eos/i18n/ru/README.md)). Idempotency protects against duplicates **within** a single producer session, not between sessions.

Remember this boundary. It's a common source of confusion.

## What our code demonstrates

One binary: `cmd/forced-retry`. The idea — take a regular producer, wrap the TCP dialer with something that drops Read calls at a given probability **after** the request has already been sent to the broker. The broker accepts the write and puts it in the log; the client gets an EOF reading the response; the client thinks "connection died, need to retry." This produces exactly the situation idempotency was built for — a lost response on a successful write.

The wrapper itself is a couple dozen lines:

```go
type lossyConn struct {
    net.Conn
    parent *lossyDialer
    reads  atomic.Int64
}

func (c *lossyConn) Read(p []byte) (int, error) {
    n := c.reads.Add(1)
    if !c.parent.disabled.Load() && n > c.parent.warmupReads && rand.Float64() < c.parent.dropRate {
        c.parent.dropped.Add(1)
        _ = c.Conn.Close()
        return 0, io.EOF
    }
    return c.Conn.Read(p)
}
```

The warm-up skips the first few Reads — so the handshake and `InitProducerId` go through undisturbed. After that, on each Read with probability `drop-rate` we close the connection and return EOF. The connection has already delivered the request to the broker at this point; the response just won't arrive.

The client is built depending on the flag:

```go
opts := []kgo.Opt{
    kgo.Dialer(dropper.DialContext),
    kgo.RecordDeliveryTimeout(o.deliveryTimeout),
    kgo.RecordRetries(o.retries),
}
if !o.idempotent {
    opts = append(opts,
        kgo.DisableIdempotentWrite(),
        kgo.RequiredAcks(kgo.AllISRAcks()),
    )
}
```

In the default branch (`-idempotent=true`) nothing extra is added — franz-go enables idempotency with `acks=all` on its own. In the alternative branch we explicitly disable idempotency and keep `acks=all` (the franz-go default is already `AllISRAcks`; I set the option explicitly for symmetry and readability). I keep `acks=all` the same in both modes — to rule out that differences in the log came from different durability settings rather than idempotency.

The write loop — a bare `ProduceSync` per record:

```go
for i := 0; i < o.messages; i++ {
    rec := &kgo.Record{
        Topic: o.topic,
        Key:   []byte(fmt.Sprintf("k-%04d", i)),
        Value: []byte(fmt.Sprintf("msg-%04d", i)),
    }
    out := cl.ProduceSync(rpcCtx, rec)
    if err := out.FirstErr(); err != nil {
        res.failed++
        res.errs[classifyErr(err)]++
        continue
    }
    res.sent++
}
```

Sequential, one record in flight at a time — what matters is that each record goes through its own retry history independently. With batching, a whole batch would fail together and the individual story would blur.

The metric — delta of end-offsets before and after the run:

```go
ends, err := admin.ListEndOffsets(rpcCtx, topic)
if err != nil { return 0, err }
var total int64
ends.Each(func(o kadm.ListedOffset) {
    if o.Err != nil { return }
    total += o.Offset
})
```

The sum across partitions is "how many physical records are in the log." The delta (after − before) is compared against the number of successful `ProduceSync` calls. If idempotency works, the delta matches. If not — the delta is larger by the number of duplicates.

## What the run shows

Healthy cluster, 200 messages, drop-rate 0.35.

With idempotency (default):

```
mode: idempotent=true drop-rate=0.35 delivery-timeout=1m0s retries=30
topic=lecture-02-03-idempotent end offsets before run: 0

results:
METRIC                       VALUE
intended                     200
client SENT (FirstErr==nil)  200
client FAILED                0
log delta (after-before)     200
duplicates (delta - SENT)    0
TCP reads dropped            51
elapsed                      14.103s
```

Without idempotency (same drop-rate, same topic after `make topic-delete`):

```
mode: idempotent=false drop-rate=0.35 delivery-timeout=1m0s retries=30
topic=lecture-02-03-idempotent end offsets before run: 0

results:
METRIC                       VALUE
intended                     200
client SENT (FirstErr==nil)  200
client FAILED                0
log delta (after-before)     252
duplicates (delta - SENT)    52
TCP reads dropped            52
elapsed                      14.887s
```

Look at two things. First — `client SENT` is 200 in both cases; the client thinks it successfully sent everything. Second — `log delta` is 200 and 252 respectively. With idempotency the broker deduplicates exactly as many times as we dropped the response; nothing extra in the log. Without idempotency, 52 dropped responses = 52 duplicates in the log. One-for-one.

`TCP reads dropped` matches `duplicates` in non-idempotent mode almost exactly — this is a direct illustration of "lost response → retry → one more record in the log." In idempotent mode the same number appears in `dropped`, but no duplicates — the broker did its job.

Raise `drop-rate` high enough (e.g. 0.6) and some records start hitting `RecordDeliveryTimeout`, producing an error column. That's a different mode — degradation, not duplicates. For demonstrating duplicates, 0.3–0.4 is enough.

## Key takeaways

- The idempotent producer is partition-side deduplication keyed by `(producer-id, producer-epoch, sequence)`. A pure protocol mechanism — no duplicate store on the client.
- Enabled by default in franz-go with `acks=all`. To disable: `kgo.DisableIdempotentWrite()`. Only makes sense to disable if you deliberately want `acks=0/1` (and understand what you're giving up).
- Protects against **duplicates on retry within a single producer session**. This is the most common source of duplicates in production and the cheapest protection.
- Does not protect against duplicates the application generates itself — repeated calls on service restart with unconfirmed records, duplicates from the data source. And not against zombie producers across sessions: for those — `transactional.id` + epoch fencing (module 04).
- `max.in.flight.requests.per.connection` idempotent mode keeps at ≤ 5 on its own. Batch reordering in the log due to async retries is impossible.
- The cost of enabling it is near zero on a healthy cluster. A little metadata in the batch, one extra RPC on startup (`InitProducerId`). After that — the same data flow.

In [Batching and throughput](../../../02-04-batching-and-throughput/i18n/ru/README.md) we'll cover batching and compression — and from there understand why `max.in.flight ≤ 5` barely affects throughput.

## Running

The sandbox must be up (`docker compose up -d` from the repo root).

Run with idempotency:

```sh
make run-with-idempotence
```

Run without idempotency:

```sh
make run-without-idempotence
```

With a different message count and drop rate:

```sh
make run-without-idempotence MESSAGES=500 DROP_RATE=0.4
```

Between runs you can clean up the topic so the delta starts from zero (otherwise the delta is still calculated correctly, but end-offsets accumulate):

```sh
make topic-delete
```

How many physical records are in the log — separately from the Go program:

```sh
make topic-count
```

Describe the partitions:

```sh
make topic-describe
```
