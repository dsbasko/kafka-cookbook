# 05-02 — Protobuf in Go

In the previous lesson we encoded Protobuf by hand — via `protowire`, following tags from a `.proto` file. That was useful once, to see that there is no magic under the hood: just a plain wire format. Nobody writes that way going forward. Nobody hand-codes `appendString(buf, 4, o.Currency)` in production. Everyone lives on generated code.

This lesson covers what a normal workflow looks like. One `.proto` file, run through `buf generate`, output — a typed Go package with `*Order`, `*OrderItem`, `OrderStatus` enum, and `Reset/String/Marshal/Unmarshal` methods on every type. Then you write ordinary Go.

## What .proto is and what generated code is

`.proto` is a text description of a message. The file lives in the repository, goes through code review, shows up in diffs, and `buf breaking` catches breaking changes against it (more on that in the [Schema evolution](../../../05-04-schema-evolution/i18n/en/README.md) lesson). The contract is stored separately from the code and is human-readable.

Generated Go code is the `*.pb.go` that `protoc` or its wrapper `buf` emits. Inside — ordinary structs with tags and methods for marshal/unmarshal. An example from our `gen/orders/v1/order.pb.go` (a fragment of generated code):

```go
type Order struct {
    Id             string                 `protobuf:"bytes,1,opt,name=id,proto3" ...`
    CustomerId     string                 `protobuf:"bytes,2,opt,name=customer_id,json=customerId,proto3" ...`
    AmountCents    int64                  `protobuf:"varint,3,opt,name=amount_cents,json=amountCents,proto3" ...`
    Status         OrderStatus            `protobuf:"varint,7,opt,name=status,proto3,enum=orders.v1.OrderStatus" ...`
    CreatedAt      *timestamppb.Timestamp `protobuf:"bytes,8,opt,name=created_at,json=createdAt,proto3" ...`
    ReservationTtl *durationpb.Duration   `protobuf:"bytes,9,opt,name=reservation_ttl,json=reservationTtl,proto3" ...`
    Note           *string                `protobuf:"bytes,11,opt,name=note,proto3,oneof" ...`
    // ... more fields and unexported internals
}
```

Snake_case fields carry a `json=camelCase` marker in the tag — that is what `protojson` uses to serialize Protobuf into JSON following the camelCase convention. `Note` ends with `oneof` in its tag — proto3 `optional` is implemented under the hood as a synthetic single-field oneof, and the generated code makes that explicit.

You never write manual `appendString` calls again. Fields are ordinary Go types, getters are auto-generated (`GetId()`, `GetStatus()`, `GetItems()`), and serialization is a single call to `proto.Marshal(order)`.

## Conventions that break compatibility if violated

Protobuf forgives a lot and does not forgive one class of mistake: anything related to **field numbers**. A field number is part of the wire format. Change it — every old byte in Kafka becomes garbage. These conventions are not a matter of taste.

1. **Field names in `.proto` are written in `snake_case`.** In generated Go they will become `CamelCase` anyway (`customer_id` becomes `CustomerId`). But in `.proto` itself — `snake_case`, because that is what the style guide requires and `buf` lint will complain about `customerId`.
2. **Field numbers are assigned explicitly and permanently.** In our `Order`, numbers 1..6 match those from [Why contracts and wire formats](../../../05-01-why-contracts-and-wire-formats/i18n/en/README.md). Compatibility is paid for exactly this way — adding a field means a new number; removing a field means reserving its number forever.
3. **Removed fields are reserved by both number and name:**
   ```proto
   reserved 10;
   reserved "customer_email";
   ```
   Without this, six months later someone may "reuse" number 10 — and your old messages in Kafka, which had an email there, will start decoding as the new field. The pain will be silent.
4. **Enums start with a zero-value.** The first element must have value 0 and carry the meaning "unspecified". In our `OrderStatus` that is `ORDER_STATUS_UNSPECIFIED = 0`. The default state of a message where the `status` field was never set is exactly that. This is spec, not taste.
5. **Enum value names are prefixed with the enum name itself.** `ORDER_STATUS_CREATED`, not `CREATED`. In Protobuf, enum values share a flat namespace with other enums in the same file — without a prefix, collisions are guaranteed.

These rules are checked automatically by `buf` lint. Next, let's see how it fits into the pipeline.

## Well-known types

Sometimes you need to put a timestamp or a duration into a message. You can use `int64 created_at_unix` (as we did in [Why contracts and wire formats](../../../05-01-why-contracts-and-wire-formats/i18n/en/README.md)), and for most cases that is enough. But Protobuf provides built-in types — `google.protobuf.Timestamp` and `google.protobuf.Duration` — which most clients automatically map to the language's native type.

In Go these are `*timestamppb.Timestamp` and `*durationpb.Duration` from `google.golang.org/protobuf/types/known/...`. They have `.AsTime()`, `.AsDuration()`, and constructors `timestamppb.Now()`, `durationpb.New(d)`. Usage:

```go
order := &ordersv1.Order{
    Id:             fmt.Sprintf("ord-%05d", i),
    Status:         ordersv1.OrderStatus_ORDER_STATUS_PAID,
    CreatedAt:      timestamppb.Now(),
    ReservationTtl: durationpb.New(15 * time.Minute),
}
```

Our schema keeps both the old `created_at_unix` field and the new `created_at` via well-known Timestamp — to show how they coexist. In production you normally keep one, and it is Timestamp.

## Optional in proto3

In proto3, all scalars have zero-value semantics by default: an empty string is not written to the wire, a zero `int64` is not either. Because of this, "field absent" and "field equals zero" are indistinguishable. When that distinction matters, mark the field with the `optional` keyword:

```proto
optional string note = 11;
```

In generated Go this becomes a pointer: `Note *string`. A consumer that wants to know "was the field sent or omitted" checks `o.Note != nil`. Without `optional`, distinguishing an empty string from an absent field is impossible.

## buf is the new protoc

For years the proto codegen workflow looked like this: install `protoc` (a C++ binary), install plugins (`protoc-gen-go`, `protoc-gen-go-grpc`), write a long command with a dozen `--*_opt` flags, wire it into a `Makefile`. It worked, but after a year the command turns into something like:

```sh
protoc -I proto -I third_party --go_out=. --go_opt=paths=source_relative \
       --go-grpc_out=. --go-grpc_opt=paths=source_relative \
       --validate_out=lang=go,paths=source_relative:. \
       proto/orders/v1/*.proto
```

`buf` is a wrapper that hides all of this behind two commands: `buf generate` and `buf lint`. Config lives in `buf.yaml` (module and linter) and `buf.gen.yaml` (plugins and output paths).

Our `buf.gen.yaml` is minimal — just one plugin:

```yaml
version: v2
inputs:
  - directory: proto
plugins:
  - local: protoc-gen-go
    out: gen
    opt:
      - paths=source_relative
```

`local: protoc-gen-go` means the plugin binary is already in `$PATH` (`go install google.golang.org/protobuf/cmd/protoc-gen-go@latest`). `paths=source_relative` puts output files in the same directory structure as the `.proto` files; without this option `protoc-gen-go` tries to lay them out by import path, which makes a mess.

Run it:

```sh
make proto-gen  # internally: buf generate
```

And `gen/orders/v1/order.pb.go` appears with `package ordersv1` containing all types. We don't do this manually because in real development the file may be regenerated ten times a day as the schema changes.

`buf lint` runs separately — it does not require generation, it checks the `.proto` files themselves against the `STANDARD` ruleset. If you forget to reserve a removed field, or name an enum value without a prefix — `buf lint` will tell you before the commit.

## What our producer shows

In `cmd/producer/main.go` an Order is assembled from generated types and written to Kafka. The key part:

```go
order := mockOrder(i)

payload, err := proto.Marshal(order)
if err != nil {
    logger.Error("proto marshal", "err", err)
    os.Exit(1)
}

rec := &kgo.Record{
    Topic: *topic,
    Key:   []byte(order.GetId()),
    Value: payload,
    Headers: []kgo.RecordHeader{
        {Key: "content-type", Value: []byte("application/x-protobuf")},
        {Key: "schema", Value: []byte("orders.v1.Order")},
    },
}

res := cl.ProduceSync(ctx, rec)
```

Three things worth noting here. First, `proto.Marshal` accepts any `proto.Message` (an interface from `google.golang.org/protobuf/proto`) — `*ordersv1.Order` satisfies it, because generated code implements the required interface automatically. Second, the header `content-type: application/x-protobuf` is a discipline, not a protocol requirement; the consumer still needs to know which type to `Unmarshal` into. Third, the header `schema: orders.v1.Order` is our manual substitute for a `schema_id` from Schema Registry. In [Schema Registry](../../../05-03-schema-registry/i18n/en/README.md) this string will be replaced by `magic byte + schema_id`, and the Registry will store the `.proto` files themselves.

Assembling an Order from generated types is ordinary Go:

```go
return &ordersv1.Order{
    Id:             fmt.Sprintf("ord-%05d", i),
    CustomerId:     fmt.Sprintf("cus-%03d", rand.IntN(100)),
    AmountCents:    int64(1000 + rand.IntN(50000)),
    Currency:       "USD",
    Status:         status,
    CreatedAt:      timestamppb.Now(),
    ReservationTtl: durationpb.New(15 * time.Minute),
    Note:           &note,
}
```

`Note` is `*string` (an `optional` field), so it is passed as a pointer. The rest are plain values.

## What our consumer shows

`cmd/consumer/main.go` reads the topic and unpacks Protobuf. The core of the loop:

```go
fetches.EachRecord(func(rec *kgo.Record) {
    var order ordersv1.Order
    if err := proto.Unmarshal(rec.Value, &order); err != nil {
        logger.Error("proto unmarshal",
            "err", err,
            "partition", rec.Partition,
            "offset", rec.Offset,
        )
        return
    }
    printOrder(rec, &order)
})
```

`proto.Unmarshal` is the inverse of `proto.Marshal`. It takes `[]byte` and a pointer to a message, and mutates it. If consumer and producer are built on the same version of `.proto` — the bytes unfold back into exactly the same Order. If the producer has moved to v2 with the old numbers intact — the old consumer reads what it knows and ignores unknown field numbers. That is forward compatibility (covered in detail in [Schema evolution](../../../05-04-schema-evolution/i18n/en/README.md)).

Printing via auto-generated getters:

```go
fmt.Printf("  status       = %s\n", o.GetStatus().String())
if ts := o.GetCreatedAt(); ts != nil {
    fmt.Printf("  created_at   = %s\n", ts.AsTime().Format("2006-01-02 15:04:05Z07:00"))
}
if d := o.GetReservationTtl(); d != nil {
    fmt.Printf("  reservation  = %s\n", d.AsDuration())
}
```

Getters work safely on a nil message — `((*Order)(nil)).GetStatus()` returns the zero-value enum instead of panicking. On messages that may be partially populated (after a schema migration, for example) this removes a lot of `if != nil` checks.

## Running

Two binaries are needed in `$PATH`:

```sh
go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
go install github.com/bufbuild/buf/cmd/buf@latest
```

Then:

```sh
make proto-gen          # generate gen/orders/v1/order.pb.go
make proto-lint         # buf lint, should pass silently
make topic-create       # create lecture-05-02-orders-proto, RF=3, 3 partitions
make run-producer       # write 10 Orders
make run-consumer       # read and print the structure
```

In a separate terminal you can run `kafka-console-consumer.sh` — you will see raw bytes where ASCII fragments (`sku-...`, `cus-...`, currency `USD`) are readable and numeric values look like garbage. That is expected: Protobuf is binary, and without knowing the schema a human cannot read it. That is the point.

## Things to pay attention to

- `gen/` is committed **to the repository**, not in `.gitignore`. Two arguments for this. First: reproducibility without a `buf` dependency in CI on a fresh clone. Second: a reviewer in a PR will see how generated code changed when `.proto` was edited. Some advocate the opposite — generate on every build, don't commit. Both camps have arguments; in this course we choose "commit" — simpler for learning reproducibility.
- I put `kgo.Record.Key` as `[]byte(order.GetId())`. This means all Orders with the same id land in the same partition (see [Keys and partitioning](../../../../02-producer/02-01-keys-and-partitioning/i18n/en/README.md)). To balance by `customer_id` instead — change it to `[]byte(order.GetCustomerId())`. This has no effect on payload serialization.
- Headers carry `content-type: application/x-protobuf` and `schema: orders.v1.Order`. Useful discipline, but without Schema Registry the consumer still trusts its own code — it unmarshals into whichever type it knows. We will address that weakness in [Schema Registry](../../../05-03-schema-registry/i18n/en/README.md).
- If you edit `.proto` (add a field, for example) and forget to run `make proto-gen` — the Go build will not fail, because the old `*.pb.go` is still valid. But the new fields will be unavailable in code. That is why `proto-gen` is the first target in the Makefile.

## What's next

In [Schema Registry](../../../05-03-schema-registry/i18n/en/README.md) we add Schema Registry: the producer will register the schema, the payload will carry `magic byte + schema_id + protobuf-bytes`, and the consumer will extract `schema_id` from the first five bytes and use it as a cache key. In [Schema evolution](../../../05-04-schema-evolution/i18n/en/README.md) — what counts as a breaking change in Protobuf and how `buf breaking` catches it automatically.

This lesson is enough. From here you can write ordinary Go services that push typed messages through Kafka, without suffering over hand-assembled wire bytes.
