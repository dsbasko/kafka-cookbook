# 03-02 — Offset Commits

In the previous lesson we ran several copies of the consumer in one group and watched how Kafka splits partitions. There was almost no message processing code — just `PollFetches` and `printf`. Here we go deeper: what "reading a message" means, when the consumer tells Kafka it's done with an offset, and how that silently becomes the main source of bugs.

## What a committed offset is

Each group inside Kafka has a table: "for group G, topic T, partition P, the last processed offset is N". This table is an ordinary compact topic `__consumer_offsets` (compact, because we only need the latest record for each key `(group, topic, partition)`). When a consumer starts, it asks the coordinator: "what offset should I start from?". The coordinator looks in `__consumer_offsets` and replies.

Committed offset N means one thing: "everything before offset N (not including N) has been processed by group G". The next poll delivers N. If the consumer crashes, it restarts and begins from the same N. The closer N is to the work actually done, the fewer duplicates or losses you get after a restart.

The non-obvious part: N is not "received over the network", and not "placed into a local buffer". It is your promise to the broker. Promise too early, before you actually processed — you lose messages. Promise later than you processed — you get duplicates on the next restart. There is no third option.

## Auto-commit and why it lies by default

The default behavior of franz-go (like most clients) — auto-commit is on. Every 5 seconds (`AutoCommitInterval`) a background goroutine takes the offset returned by the last `PollFetches` and sends it to the broker. Sounds convenient: nothing to call, everything happens on its own.

The subtlety is in the wording. Auto-commit commits what **your code received via PollFetches**, not what it processed. Those are different things. Between "received" and "actually processed" lives your business logic, which can take seconds, minutes, hours. If auto-commit told the broker "read up to 200" at a moment when the application had only actually reached 150 — and then the application crashed — after restart the consumer gets offset 200 as its starting point. Records 150–199 are lost. That is at-most-once with silent data loss, and you won't see it without a specific test.

The reverse scenario. Auto-commit has not fired yet, you processed 150 records and crashed. The committed offset stayed at 0 (or wherever it was at startup). Restart — the group hands you everything from the beginning. Duplicates. That is the "at-least-once with duplicates" you read about everywhere.

Which of the two happens depends on timing. Started, polled 200 → began processing → after 5 seconds auto-commit (committed=200) → crashed at 150 → restart → losses. Or: started → polled → crashed at 50 → committed stayed at 0 → restart → duplicates from 0 to 49. A lottery.

In this lesson we reproduce exactly the second scenario (duplicates) — it's more illustrative. The demo writes each processed message to a file `processed-auto.log`, then we run the consumer with `crash-after`, it dies without committing, then we restart and see that some offsets appear in the log twice. If you want to see the first scenario with losses — increase `WORK_DELAY` so the loop definitely outlasts 5+ seconds of auto-commit, then crash afterward. The behavior is symmetric and equally bad.

## Manual sync — guarantees in exchange for latency

The fix is obvious: commit when processing actually finishes. The most direct way — `kgo.DisableAutoCommit()` plus a manual `cl.CommitRecords(ctx, records...)` after each batch.

The semantics are clear. Poll a batch. Process all records in it. If processing reaches the end — commit the whole batch in one call. If you crash in the middle — `CommitRecords` was never called, the committed offset stays at the start of the batch. Duplicates are possible (this is still at-least-once), but the window is exactly one batch, not an entire session since the previous auto-commit.

The cost — `CommitRecords` goes to the broker and waits for an ack. It is a blocking call inside the poll loop. Add +5–20 ms per batch, sometimes more. If batches are small (a couple of records), the overhead is noticeable. If batches are normal-sized (hundreds), the cost is spread out.

One more subtlety — ordering. `CommitRecords` takes the **maximum** offset per partition from the passed records and commits it. If you call it out of order with respect to increasing offsets — you can roll the commit back. In practice this means: commit the whole batch at once in a single call and don't split it.

## Manual async — a compromise with mark + flush

Sync-commit on every batch blocks the loop. You can do it cheaper if you don't wait for the ack synchronously.

The idea — `kgo.AutoCommitMarks()`. This mode tells franz-go: "regular auto-commit is off; only commit what I **marked** as processed via `MarkCommitRecords`". The result is a hybrid: you mark each processed record yourself, and a background goroutine periodically syncs the marked offsets to the broker.

The guarantee is different. Mark is local state in the client: "I consider this record processed, it can be committed". Flushing that state to the broker happens either asynchronously by timer (`AutoCommitInterval`), or explicitly via `cl.CommitMarkedOffsets(ctx)`. Between `MarkCommitRecords` and the actual flush — a loss window. If the process crashes in that window, on restart you get duplicates equal in size to the window.

The window size is set by `AutoCommitInterval`. Set it to 200 ms — the window is 200 ms of work, typically tens of messages at low/medium rate. Set it to 5 seconds — the window is much larger. To make the window predictable at the end of a batch, call `CommitMarkedOffsets` manually between batches — it is fast (if the background flush has already drained everything, the call does almost nothing), and guarantees that everything marked is flushed by the time the batch ends.

The demo does exactly this: `MarkCommitRecords` on each record + `CommitMarkedOffsets` after the batch. Between those two points the background `AutoCommitInterval` timer runs, so on long batches it lightens the load and some commits happen in the background, while at the end of the batch we always have an honest sync-flush.

## What this looks like in code

The design of all three main loops is the same: open a log file for writing processed messages, start a poll loop, for each message "do work" (sleep + log), on `crash-after` — `os.Exit(1)` without `Close`. The only difference is what we do with offsets.

`auto-commit/main.go` — nothing at all: the default auto-commit does the work for us.

```go
opts := []kgo.Opt{
    kgo.ConsumerGroup(o.group),
    kgo.ConsumeTopics(o.topic),
    kgo.AutoCommitInterval(o.commitEvery),  // on the demo = 2s
    // ...
}
```

`manual-sync/main.go` — disable auto-commit and commit the batch explicitly.

```go
opts := []kgo.Opt{
    kgo.ConsumerGroup(o.group),
    kgo.ConsumeTopics(o.topic),
    kgo.DisableAutoCommit(),
    // ...
}
// ...
batch := make([]*kgo.Record, 0)
fetches.EachRecord(func(r *kgo.Record) { batch = append(batch, r) })
// process each record from batch...
err := cl.CommitRecords(commitCtx, batch...)
```

Note: the batch is collected in full first, then processed, then committed in one call. If you crash in the middle — `CommitRecords` was never called, the committed offset stayed at the start of the batch.

`manual-async/main.go` — `AutoCommitMarks` plus mark on each record plus manual flush between batches.

```go
opts := []kgo.Opt{
    kgo.ConsumerGroup(o.group),
    kgo.ConsumeTopics(o.topic),
    kgo.AutoCommitMarks(),
    kgo.AutoCommitInterval(o.commitEvery),  // 500ms — loss window
    // ...
}
// ...
fetches.EachRecord(func(r *kgo.Record) {
    // processing...
    cl.MarkCommitRecords(r)
})
// between batches — explicit flush, to not depend only on the background timer:
err := cl.CommitMarkedOffsets(flushCtx)
```

Each `MarkCommitRecords` is a local mark, not a network call. Cheap. The network part happens once per `AutoCommitInterval` or on an explicit `CommitMarkedOffsets`.

## Demo: counting duplicates from the log file

In each of the three main loops, every actually processed message writes a line to a file in the form `partition,offset,key,value`. After running the loop several times with `crash-after` and without, you have a log. It's easy to count from it:

- total number of lines = "how many times the application processed anything"
- unique `partition,offset` pairs = "how many distinct messages went through processing"
- the difference = duplicates

The target `make count-auto` (and `count-sync`, `count-async`) does exactly this:

```sh
$ make count-auto
processed-auto.log: total=30 unique-offsets=20 duplicates=10
```

This means: the application processed something 30 times, but there were only 20 unique messages — ten were processed twice. That is the cost of auto-commit with an artificial crash before the 5-second interval.

The full demo scenario looks like this. First, prepare the sandbox:

```sh
make topic-create
make topic-load                # load 30 messages into the topic
make group-delete-all          # clear committed offsets for all three groups
make clean-logs                # delete old processed-*.log
```

Then the first "crash" run with auto-commit:

```sh
make run-auto CRASH=10         # processes 10, crashes without committing
```

And the second run, which should read the rest:

```sh
make run-auto CRASH=0          # reads the rest; Ctrl+C when the log stops growing
make count-auto
```

If auto-commit didn't fire during the first processing run — you'll see 10 duplicates (offsets 0..9 processed twice) plus 20 unique ones read after. Repeat the same for `run-sync` and `run-async`. Manual-sync will also have duplicates — exactly one batch that wasn't committed in time. Manual-async can also have duplicates, but the window is smaller — only what was marked but not yet flushed.

## What "commit offset N" means and where it physically goes

Internally all three variants do the same thing: send an `OffsetCommit` request to the broker with the pair `(topic, partition) → offset`. The broker acting as the group coordinator writes this to the compact topic `__consumer_offsets` with the key `(group, topic, partition)`. Compact guarantees that for one group and one partition only the latest record is kept in the log (previous ones are collapsed by compaction).

Two practical implications. First — committed offsets survive broker restarts and coordinator migration: they are physically on disk, like regular messages. Second — `__consumer_offsets` is itself an ordinary topic; you can describe it with `kafka-topics.sh --describe`, check its size, see which broker is currently the coordinator. Consumer groups are not magic — they are built on top of Kafka's ordinary mechanism.

`kafka-consumer-groups.sh --describe --group <name>` (or `make group-describe-auto` in this lesson) reads exactly these committed offsets and compares them against the current LEO — that is how lag is derived. Lag = LEO − committed. Large lag = the group is falling behind. And now you can see where it comes from: two numbers, one from the group commit, the other from the partition log.

## What to pick in a real project

Short answer: default auto-commit — only in demos, tests, and one-shot utilities. Use manual commit for production code.

Sync-commit on every batch — the default choice when processing runs in even moderately sized chunks (tens to hundreds of records). Commit latency adds milliseconds, and that is a normal cost. The duplicate window is one batch.

`AutoCommitMarks` + mark on each record + explicit flush between batches — the choice when every extra sync-commit shows up in throughput (small batches, very high rate, or business logic is fast and any loop blocking is expensive). The duplicate window is `AutoCommitInterval` — tune it explicitly to match your risk profile.

If you need zero duplicates — a commit strategy alone won't get you there. That calls for exactly-once via transactions ([Transactions and EOS](../../../../04-reliability/04-01-transactions-and-eos/i18n/en/README.md)) or an idempotent handler ([Processing guarantees](../../../03-03-processing-guarantees/i18n/en/README.md) — the next lesson). A bare offset commit on the consumer side never gives exactly-once, no matter how you configure it.

## What to try hands-on

- run `make run-auto CRASH=15` without waiting for the 2-second auto-commit — then `make run-auto CRASH=0`, see duplicates in `processed-auto.log`;
- increase `WORK_DELAY=400ms` and `CRASH=20` — processing will stretch to 8 seconds, auto-commit will fire; the second run will show that some offsets are **lost** (loss = total < SEED_MESSAGES);
- run the same experiment on `run-sync` and `run-async` — on sync the duplicates are exactly one batch in size; on async — the size of the commit window;
- check `make group-describe-auto` after each run: see the committed offset per partition;
- delete the group's committed offset (`make group-delete-auto`) and run again — the group starts from earliest and the entire topic is re-read, just like the first run.

## Next

This lesson covered the mechanics of commits. The next one ([Processing guarantees](../../../03-03-processing-guarantees/i18n/en/README.md)) covers why a commit strategy alone can't give you exactly-once, and why the handler needs idempotency plus a dedup table. There — Postgres, `INSERT ... ON CONFLICT DO NOTHING`, and `kill -9` in the middle of processing no longer produces duplicates in the database — because the handler is protected, not the consumer.
