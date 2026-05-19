# 05-03 — Schema Registry

In [Protobuf in Go](../../../05-02-protobuf-in-go/i18n/en/README.md) the producer wrote raw protobuf bytes to Kafka and put a `schema: orders.v1.Order` string in the headers. That works exactly as long as you are the only one writing to this topic. Then the familiar story begins. A new Python service joins the team with its own generated code and its own view on how to marshal. Someone changes a field without telling anyone. Someone dumps JSON in there "temporarily, for a demo." Six months later the topic holds a zoo of four incompatible formats, and nobody remembers which one is "correct."

Schema Registry is an attempt to treat exactly that pain. A single source of truth for schemas. Every producer gets a `schema_id` from it before writing and puts that id in the first bytes of the payload. Every consumer finds the schema by that id — and once it knows the schema, it parses the message correctly.

The lecture covers how this is arranged on the wire, how `franz-go` handles it through `sr.Serde`, and where the approach has a weak spot.

## What it is

Confluent Schema Registry is a standalone service with a REST API. It has its own storage: the key is a `(subject, version)` pair, the value is the schema text plus its type. SR supports multiple notations — we use Protobuf, Avro usually lives alongside it. On registration the service returns a globally-unique `schema_id` — an integer that travels into Kafka.

Subject is the "logical contract name." The default convention (TopicNameStrategy) is `<topic>-value` for values and `<topic>-key` for keys. Inside a single subject lives a chain of versions — you add a field to the schema, register a new version, get a new id; old ids stay. Schema evolution is covered in [Schema Evolution](../../../05-04-schema-evolution/i18n/en/README.md).

In our sandbox SR runs on `http://localhost:8081`. It has a few dozen endpoints; the ones we need are:

- `POST /subjects/<subject>/versions` — register a schema, get an id.
- `GET /schemas/ids/<id>` — fetch schema text by id.
- `GET /subjects` — list all subjects in the registry.
- `DELETE /subjects/<subject>` — delete a subject (soft).

Everything else is variations of these four plus admin operations.

## Wire format

This is the least obvious part. SR does not touch the bytes in Kafka directly — it is just a separate HTTP service. But it dictates the payload format that all clients must follow. Without that there is no compatibility.

Confluent wire format for a value:

```
+---------+---------------+----------------+----------------+
| 1 byte  |    4 bytes    |   N bytes      |   M bytes      |
| magic=0 |  schema_id    |  message-index |  proto/avro    |
|         |  (big-endian) |  (PB only)     |  payload       |
+---------+---------------+----------------+----------------+
```

First — a zero magic byte. Then four bytes of `schema_id` in big-endian. Then the message-index: a length-prefixed array of zigzag-encoded varints (for protobuf) that carries the path to the target message inside the `.proto` file (multiple top-level messages — the array picks one; nested messages — a full path through the tree). For the common case `[0]` (first top-level message) there is a shortcut: a single byte `0x00` instead of two (`[len=1][idx=0]`). Only then comes the serialized payload itself.

Avro and JSON do not need a message-index — they unambiguously describe a single message, magic byte and id are enough. Protobuf is historically like this because a single `.proto` can contain any number of messages.

In code all of this hides behind `sr.Serde`. But it is worth seeing it by hand at least once. In our producer I print the magic and the id exactly as they sit in the first five bytes:

```
ok  id=ord-00000 status=ORDER_STATUS_CREATED   magic=0x00 schema_id=1 bytes=67 -> ...
```

If something looks wrong in the first five bytes — Kafka has nothing to do with it; the problem is in the client writing "the wrong thing."

## sr.Serde — how it is assembled in franz-go

`franz-go/pkg/sr` is franz-go's native SR client. Inside it are two things: `sr.Client` (HTTP to the registry) and `sr.Serde` (glues id + encode/decode for a specific type).

Serde works on a simple model. You register a binding once — "schema_id + Go type + EncodeFn." After that `Encode(v)` automatically prepends the correct header (magic byte and schema_id, plus message-index for protobuf), and `Decode(b, &v)` goes the reverse path: parses the first five bytes and applies the registered DecodeFn to the rest.

Here is the registration core in the producer:

```go
serde := sr.NewSerde()
serde.Register(
    id,
    &ordersv1.Order{},
    sr.EncodeFn(func(v any) ([]byte, error) {
        return proto.Marshal(v.(*ordersv1.Order))
    }),
    sr.Index(0),
)
```

Three pieces are visible. `id` is the global schema_id obtained from SR before the Serde was created. `sr.EncodeFn` is a plain wrapper around `proto.Marshal`, no magic. `sr.Index(0)` is the Protobuf-mandatory message-index that says: "top-level Order, no nesting." If a second message lived in the same `.proto` and we wanted to encode it — it would be `sr.Index(1)`.

Encode then looks ordinary:

```go
payload, err := serde.Encode(order)
// payload = [0x00, id_b3, id_b2, id_b1, id_b0, 0x00, ...proto bytes...]
```

Remember — `payload` is placed into `Record.Value` in Kafka as a whole. No separate headers for schema_id, no workarounds. The wire format lives inside the value.

## Schema registration — what and when

The most common question: "when do I register a schema?" Different approaches give different answers.

First option — statically, through CI. The pipeline has a step that looks at `.proto` files in the repo, calls SR, compares against registered versions, and registers new ones. Production code starts with an already-known id (or finds it through a subject lookup). This option is cleaner and better suited for regulated environments.

Second — dynamically, at application startup. The service registers its schema on start and caches the id in memory. Simple, but it gives the service write access to the Registry — sometimes that is not what you want.

In the lecture I show the second option because it is shorter and more illustrative. In production — usually the first.

Registration in our producer is five lines:

```go
cl, err := sr.NewClient(sr.URLs(url))
ss, err := cl.CreateSchema(ctx, subject, sr.Schema{
    Schema: orderProtoSchema,
    Type:   sr.TypeProtobuf,
})
return ss.ID, nil
```

`orderProtoSchema` is simply the text of my `.proto` file, hardcoded as a constant in `main.go`. Better that than reading from a runtime FS — fewer surprises with relative paths. If the same schema is already registered (by normalized content) — SR returns the same id and no new version appears. This is convenient: you can run `make run-producer` any number of times and the registry will have exactly one entry.

## What our consumer shows

The consumer is more interesting. It does not know in advance which schema_id will arrive in the first message. And generally, a single topic can hold messages from different versions of the same schema — each with its own id.

So the strategy is straightforward. Unpack the id from the first five bytes. If Serde already has it — decode immediately. If not — go to SR for the schema, register a DecodeFn under that id, then decode. Each new id costs one HTTP request and is cached inside Serde after that.

The core of the loop:

```go
id, _, err := serde.DecodeID(rec.Value)
if _, ok := registered.Load(id); !ok {
    schema, err := srCl.SchemaByID(ctx, id)
    serde.Register(
        id,
        &ordersv1.Order{},
        sr.DecodeFn(func(b []byte, v any) error {
            return proto.Unmarshal(b, v.(*ordersv1.Order))
        }),
        sr.Index(0),
    )
    registered.Store(id, struct{}{})
}

var order ordersv1.Order
if err := serde.Decode(rec.Value, &order); err != nil { ... }
```

There is a subtle point here. I do `proto.Unmarshal` into a local type `ordersv1.Order` — meaning the consumer still knows which Go type to expect. Schema Registry itself **does not produce** generated Go code; it returns only the `.proto` text. Nobody usually tries to turn that text into Go structs on the fly (technically possible via `dynamicpb`, but cumbersome).

This means: SR is useful for **wire format validation** and **evolution management**, but codegen still stays on the developer's side. The schema changes — generate new Go code via `buf generate`, redeploy the service.

Dynamic Decode (via `dynamicpb`) is used mainly in tooling — kcat, Kafka UI, various test utilities, sometimes debug sidecars. A production service is typically pinned to a specific version.

Printing itself uses the ordinary generated getters, as in [Protobuf in Go](../../../05-02-protobuf-in-go/i18n/en/README.md):

```go
fmt.Printf("--- %s/%d@%d key=%s schema_id=%d ---\n",
    rec.Topic, rec.Partition, rec.Offset, string(rec.Key), schemaID)
fmt.Printf("  status       = %s\n", o.GetStatus().String())
if ts := o.GetCreatedAt(); ts != nil {
    fmt.Printf("  created_at   = %s\n", ts.AsTime().Format("2006-01-02 15:04:05Z07:00"))
}
```

I print `schema_id` for every message — in production this is useful for debugging: "which version did the producer use to write this record" is immediately visible.

## Caching and performance

One important detail about SR — it lives separately. An HTTP call to it costs latency. If you hit `/schemas/ids/...` for every Kafka message, performance collapses instantly.

`sr.Serde` solves this: after the first `Register(id, ...)`, all decoding goes through a local map with no HTTP. SR is touched exactly once per new id. If producers keep stable ids (not changing the schema every 5 minutes) — that is fewer than ten calls over the consumer's entire lifetime.

The producer has it simpler. One HTTP request at startup — register the schema, get the id. After that Encode runs in memory with no Registry calls. SR never appears on the hot path at all.

In our lecture the consumer hits SR once — on the very first fetch it sees an unknown id, fetches the schema, caches it in Serde, and then flies through memory. The log shows this moment honestly:

```
INFO msg="registering schema id" id=1 type=PROTOBUF
```

After that line there are no more HTTP calls to SR.

## What is in the message: looking by hand

Sometimes you want to verify that the producer is actually writing the correct wire format. The cheapest way is `kcat` with the `-s value=s` flag (decode the value through SR):

```sh
kcat -b localhost:19092 -t lecture-05-03-orders-sr -C -e -o beginning \
     -s value=s -r http://localhost:8081
```

If installed — you will see parsed messages. If not — hit SR directly:

```sh
make sr-list-subjects        # shows lecture-05-03-orders-sr-value
make sr-describe             # returns schema text and its id
```

These endpoints help when something goes wrong. A message won't parse — check if the right schema is in the registry. A subject is missing — someone deleted it (`DELETE /subjects/...` can be destructive, especially with `?permanent=true`).

## Running

Binaries required in `$PATH`:

```sh
go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
go install github.com/bufbuild/buf/cmd/buf@latest
```

Sequence:

```sh
make proto-gen          # generate gen/orders/v1/order.pb.go
make topic-create       # lecture-05-03-orders-sr, RF=3, 3 partitions
make run-producer       # registers schema in SR + writes 10 Orders
make sr-list-subjects   # cross-check: subject appeared
make sr-describe        # inspect text and id
make run-consumer       # reads, prints schema_id for each record
```

On the first run the producer log shows `schema registered subject=... id=N`. Remember N — that is the global id now physically living in the first 5 bytes of every value in this topic. After the first fetch the consumer shows `registering schema id=N type=PROTOBUF`. That is it fetching the definition from SR and registering a DecodeFn under that id.

Run `make run-producer` again — the id does not change. SR sees the same schema (by normalized content) and returns the same record.

## Things to pay attention to

- I hardcoded the `.proto` text as a constant directly in `cmd/producer/main.go`. Not ideal for production — if the schema changes, you must remember to regenerate both the Go code and the constant. In a real service the typical approach is either `embed.FS` from the file `proto/orders/v1/order.proto` or a separate registration pipeline. In the lecture the constant keeps the example self-contained.
- TopicNameStrategy (`<topic>-value`) is the most common but not the only one. There is RecordNameStrategy (subject = full message type name) and TopicRecordNameStrategy (a combination). If a topic holds different message types — TopicNameStrategy does not fit; switch to RecordName. Not covered in the course — it is a rare case.
- `sr.Index(0)` for a top-level message is mandatory for Protobuf; without it the magic byte `0x00` after schema_id will not appear and the Confluent Java client won't be able to read it. Avro and JSON do not require this.
- `sr.Serde` stores the `id -> tserde` mapping under an `atomic.Value`. That means — safe for concurrent reads, registration under a mutex. Using one Serde from multiple goroutines is safe (and the right approach).
- DELETE on a subject is an operation to treat carefully. Soft-delete (`DELETE /subjects/<sub>`) removes the subject but the id stays in the registry under its own key — old messages in Kafka can still be decoded. Hard-delete (`?permanent=true`) erases the id permanently, making old messages unreadable. In our Makefile `clean` does a hard-delete for reproducibility — do not do that in production.
- `gen/` is committed to the repo. Same reasoning as in [Protobuf in Go](../../../05-02-protobuf-in-go/i18n/en/README.md): reproducibility without `buf` on a clean clone, and reviewers can see how generated code reacts to schema edits.

## What's next

[Schema Evolution](../../../05-04-schema-evolution/i18n/en/README.md) covers schema evolution. What BACKWARD/FORWARD/FULL compatibility means, which Protobuf changes SR will accept and which it won't. And how `buf breaking` catches breaking changes before they even reach SR.

From here it is already clear why the Registry exists at all: it alone knows exactly which schemas are live in the system, and it blocks anything that would break consumers from entering the registry.
