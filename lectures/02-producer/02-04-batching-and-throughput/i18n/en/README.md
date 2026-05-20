# 02-04 — Batching & Throughput

In [Acks and durability](../../../02-02-acks-and-durability/i18n/en/README.md) we measured latency on honest round-trips via ProduceSync — one record at a time, one in flight at a time. That was a demo about acks, not speed. Here we take the other side of the coin — throughput. What actually gives a producer speed, how to use batching, and where it starts to get in the way.

Spoiler up front: Kafka producer throughput is about batch size, not message size. Thicker batches mean fewer round-trips per unit of payload. Compression tags along — it works at the batch level, and without batching it does almost nothing.

## Batching is not "when I call Flush"

The core misconception: people think a batch in Kafka is when you assemble an array of messages in code and hand it over in one call. That's not how it works. The producer assembles batches itself. You write records one at a time, the producer puts them into a per-partition queue, and periodically sends the accumulated data to the broker.

What "periodically" means. In franz-go (and in the standard Java client) this fires on one of two events:

1. `ProducerBatchMaxBytes` has accumulated (franz-go v1.21.0 default is 1,000,012 bytes, mirroring Kafka's `max.message.bytes` default) — send immediately, no point holding it.
2. `ProducerLinger` fires — a timer after which everything queued is sent. franz-go v1.21.0 default is 10 ms (switched in v1.20.0; before that it was 0; the Java client still defaults to 0).

The 10 ms default is effectively a tiny built-in linger: even on a dense stream the client groups records that landed in the queue between "put in queue" and "producer is about to send the request" into one batch. Without an explicit `ProducerLinger(0)` batches are noticeably thicker than under "send immediately." On a sparse stream those 10 ms are barely visible in end-to-end latency, while compression actually has a decent batch to chew on.

`ProducerLinger > 10ms` forces the producer to wait even longer, accumulating more. On a very high-rate stream the gain is almost nothing (the batch is already thick); on a slow stream the timer eats into latency. linger is therefore a trade-off between "send almost immediately" and "send efficiently."

In our code the batch is assembled exactly like this:

```go
opts := []kgo.Opt{
    kgo.DefaultProduceTopic(topic),
    kgo.ProducerLinger(s.linger),
    kgo.ProducerBatchCompression(s.codec),
    kgo.ProducerBatchMaxBytes(1 << 20), // 1 MiB
    kgo.MaxBufferedRecords(200_000),
}
```

`MaxBufferedRecords` is the limit at which `Produce` starts blocking. The default of 10,000 hits backpressure fast under 100k load, and you end up measuring buffer wait time instead of linger. We raised it to 200k so the producer never stumbles.

## Per-partition, not per-topic

The batch queue is per-partition. That matters. If you write to a topic with 24 partitions and distribute messages evenly by key, you have 24 parallel queues, each assembling its own batch independently.

Practical implications:

- Thick batches are easier to get when many messages go to a single partition. On a sparse pattern — one or two records per second per key — the batch never fills up, and the default 10 ms linger sends each one nearly immediately.
- The franz-go v1.21.0 default partitioner is `UniformBytesPartitioner(64 KiB, adaptive=true, keys=true, nil)` (KIP-794, Java default since 3.3). With an empty key it "sticks" to one partition until ~64 KiB has accumulated, then switches to the next one (sticky-bytes, not per-record round-robin). This is intentional, so batches occasionally fill up even without a key.
- Increasing partition count without a corresponding increase in write rate makes batching **worse**, not better. Each partition gets less — batches get thinner.

This applies to the "we're slow, let's add more partitions" conversation. First check whether batches are filling up at all — otherwise you'll only make things worse.

## Compression lives on the batch

Compression in Kafka is applied to the **whole batch**, not to each record. One batch — one compressed block. When the consumer reads, it decompresses the whole batch and passes the records on.

That's both the power and the limit of compression. On a thick batch (dozens or hundreds of records with similar structure) — compression ratio is excellent: repeated fields, shared prefixes, the dictionary works. On a thin batch (one record per batch) — almost zero gain; CPU spent, ratio ≈ 1.

This is why linger + compression make sense together. Without linger, during low load batches stay thin and compression doesn't pay off. With a moderate linger (5–20 ms) a decent batch accumulates even on a slow stream — and the codec starts earning its keep.

Codecs from fastest/weakest to slowest/strongest:

1. **none** — no compression. Simple, minimal CPU, ratio = 1.
2. **lz4** — very fast, ratio around 1.5–2× on structured data. The production default.
3. **snappy** — similar ratio to lz4. Older; supported in franz-go via `kgo.SnappyCompression()`.
4. **zstd** — more CPU, but much better ratio: 2–4× on JSON, more on text. Supported since Kafka 2.1.
5. **gzip** — slower than zstd, ratio often worse. Rarely used; mainly for compatibility.

In our benchmark we run **none / lz4 / zstd** — three points that show the practical difference.

## What the code shows

One binary: `cmd/bench`. It runs a matrix of three linger values (0/5/50 ms) and three codecs (none/lz4/zstd), writing 100,000 JSON messages of ~1 KB each for every combination. The output is a table with throughput, P50/P99/P99.9 latency, and disk size.

Each combination writes to its own topic — otherwise disk sizes mix and the compression column loses meaning. Topic names: `lecture-02-04-batching-l<linger>-<codec>`.

Writes are fundamentally async — `cl.Produce` plus a callback. If we used ProduceSync (as in [Acks and durability](../../../02-02-acks-and-durability/i18n/en/README.md)), batch effects would disappear: the next record doesn't leave until the previous one finishes the round-trip.

The loop is just `cl.Produce` in a loop, with time measured up to the callback:

```go
for i := 0; i < msgs; i++ {
    rec := &kgo.Record{Value: payloads[i]}
    sendAt := time.Now()
    cl.Produce(ctx, rec, func(_ *kgo.Record, err error) {
        took := time.Since(sendAt)
        // write to res.latencies, increment counters
    })
}

flushCtx, flushCancel := context.WithTimeout(ctx, 2*time.Minute)
_ = cl.Flush(flushCtx)
flushCancel()
res.elapsed = time.Since(start)
```

Two subtleties here. First — `Flush` is mandatory. Without it the loop finishes instantly (we handed 100k records to the buffer queue in milliseconds), and elapsed is fake. Second — the latency we record in the callback is **not** a round-trip to the broker. It is the full journey of a record: "put in queue" → "landed in a batch" → "batch sent" → "broker replied" → "callback fired." Under heavy load the first records in the queue have enormous latency — they wait for all the batches ahead of them to clear. The last ones have small latency. P50 across 100k is roughly the average step across the whole run, not "time of a single RPC."

That's fine for our comparison — we compare scenarios against each other under identical load. For honest per-record latency at low load, that's a different experiment (and it was done in [Acks and durability](../../../02-02-acks-and-durability/i18n/en/README.md) with ProduceSync).

Disk size is calculated via `kadm.DescribeAllLogDirs`:

```go
all, err := admin.DescribeAllLogDirs(rpcCtx, nil)
// ...
all.Each(func(d kadm.DescribedLogDir) {
    d.Topics.Each(func(p kadm.DescribedLogDirPartition) {
        if !wanted[p.Topic] { return }
        if seen[p.Topic][p.Partition] { return }
        seen[p.Topic][p.Partition] = true
        sizes[p.Topic] += p.Size
    })
})
```

The delta `after − before` is how many bytes landed in the logs of **one replica**. With rf=3 the cluster actually holds three times more; for scenario comparison this doesn't matter. We strictly take the first replica found for each partition (via the `seen` map), otherwise with rf=3 the size would multiply by three.

The payload generator is also not arbitrary code. If I reused the same buffer for all 100k records, zstd would encode it as "repeat N times" and compression ratio would shoot into the sky. So each record gets a unique JSON: a fixed structure (keys `seq`, `id`, `ts`, `event`, `payload`) and a random hex filler up to the target size. That way zstd sees a realistic picture — the structure compresses, the random part doesn't.

## What shows up on a run

Healthy cluster, 100,000 messages of ~1 KB JSON, partitions=3, rf=3. The run takes ~30 seconds across all nine scenarios.

```
SCENARIO                      SENT    FAILED  ELAPSED   THROUGHPUT    P50       P99       P99.9     DISK
linger=0ms  compression=none  100000  0       1.10s     91200 msg/s   363.79ms  1.07s     1.08s     98.6MB
linger=0ms  compression=lz4   100000  0       1.28s     77887 msg/s   548.80ms  1.25s     1.26s     92.2MB
linger=0ms  compression=zstd  100000  0       1.01s     99229 msg/s   531.73ms  968.58ms  969.76ms  48.2MB
linger=5ms  compression=none  100000  0       1.45s     69135 msg/s   936.45ms  1.42s     1.42s     98.6MB
linger=5ms  compression=lz4   100000  0       752.63ms  132868 msg/s  386.57ms  708.90ms  717.08ms  92.2MB
linger=5ms  compression=zstd  100000  0       704.46ms  141952 msg/s  436.75ms  632.30ms  636.41ms  48.2MB
linger=50ms compression=none  100000  0       1.69s     59335 msg/s   930.63ms  1.64s     1.66s     98.6MB
linger=50ms compression=lz4   100000  0       2.60s     38507 msg/s   1.06s     2.51s     2.52s     92.2MB
linger=50ms compression=zstd  100000  0       1.24s     80911 msg/s   781.35ms  1.14s     1.14s     48.3MB
```

What you can read from this.

The DISK column is the cleanest. On 1 KB JSON:

- `none` — 98.6 MB. That's exactly 100,000 × ~1 KB plus a little batch metadata.
- `lz4` — 92.2 MB. Only 6% compression. On JSON-with-random-id there is structure, but not much; most of the data is random hex, which lz4 can't compress.
- `zstd` — 48.2 MB. Half of `none`. Same random hex, but zstd uses a dictionary and handles repeated structure much better.

Numbers are stable across runs: compression is a function of the data, not of timing.

The THROUGHPUT column — now it gets interesting. The best combination is **linger=5ms × zstd** (~142k msg/s). The worst is **linger=50ms × lz4** (~38k msg/s), which is counterintuitive: shouldn't linger help?

The explanation is that we're writing all 100k at once, so the producer builds large batches regardless. We push `cl.Flush()`, and it waits for all linger timers to expire. With linger=50ms every trailing chunk waits another 50 ms before sending — and there are many such trailing chunks at the end (partition tails). On a sparse stream linger=50 would be useful; in a "fed the client and now waiting" scenario it just slows down the tail.

That's another lesson. Linger is for **smoothing out uneven load**, not "always better." If you have a steady stream and want minimal end-to-end latency — leave it at 0. If the stream is bursty, with peaks and troughs — 5–20 ms removes the saw. 50 ms in production is almost always too much.

P99 / P99.9 at 100k async-produce is "how long did the path take for the latest record in the buffer." The best tails are on linger=5ms × {lz4, zstd} (~700 ms). The worst are on linger=50ms × lz4 (~2.5 s). Under real sparse-stream load these numbers would look different — there P99 would be determined by that 50 ms linger, not by buffer size.

## What about message size

We used 1 KB — a typical size for a JSON event (id, order fields, metadata). If you push the payload to 10 KB:

- Disk size grows proportionally.
- Compression ratio improves — more repetition inside a single record.
- Throughput in messages/s drops; throughput in bytes/s stays in the same order.

If you drop to 100 bytes:

- Record metadata (headers, key, partition, offset, CRC, timestamp) becomes a meaningful fraction of the record. Compression barely works — too little data per record.
- Throughput in messages/s can climb into hundreds of thousands of msg/s — but byte throughput drops.

The boundary is somewhere around 200–300 bytes. Below that — either batch multiple events into one record (if business logic allows) or accept the overhead. Above that — batching and compression behave as expected.

## Takeaways

- Batching in Kafka is on the producer side, per-partition. The producer assembles batches itself and sends them itself. Size is controlled by `ProducerBatchMaxBytes` and `ProducerLinger`.
- The franz-go v1.21.0 default is `ProducerLinger=10ms`. It's not "send immediately," but it's not aggressive lingering either: natural batches end up thicker than at 0, and end-to-end latency barely moves.
- linger > 10ms is for **sparse streams** — to give batches time to fill up. On a dense stream it only increases tail latency; leave the default or set an explicit `ProducerLinger(0)`.
- Compression works on the batch. On a thin batch it gives nothing; on a thick batch it gives everything.
- Codecs. `lz4` is cheap on CPU and compresses moderately. `zstd` costs more but delivers 2–4× ratio on JSON. On random bytes no codec helps.
- `MaxBufferedRecords` is the limit at which `Produce` blocks. The default of 10,000 needs to be raised under real load; otherwise backpressure hits the buffer before the broker.
- When sizing partitions, look at write rate into a single partition, not the topic. Too many partitions with low throughput = thin batches = poor compression ratio.

In [Errors, retries, and headers](../../../02-05-errors-retries-headers/i18n/en/README.md) we'll cover producer error classes, retry/timeout settings, and headers — the last piece that completes the producer picture.

## Running

The sandbox must be up from the repository root (`docker compose up -d`). Then:

```sh
make run
```

This creates 9 topics and runs the matrix with defaults (100,000 × 1024 bytes). The run takes about 30 seconds on a laptop.

For faster iterations use a lighter load:

```sh
make run MESSAGES=10000 PAYLOAD=512
```

Between runs it helps to clean up topics, otherwise disk size just accumulates:

```sh
make topic-delete
```

Describe partitions:

```sh
make topic-describe
```
