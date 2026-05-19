# 05-04 — Schema Evolution

In [Schema Registry](../../../05-03-schema-registry/i18n/en/README.md) we taught the producer and consumer to coordinate through Schema Registry: one registers the schema, the other fetches it by `schema_id`. While there's only one schema — everything is quiet. But the contract lives. A month later a request comes in: "let's add a currency to Order". Six months later — "now a shipping address too". A year from now someone will propose changing `amount_cents` to string because the frontend finds it convenient. And that's where things get interesting.

This lecture is about the discipline of change. What you can safely change in Protobuf, what — never. What compatibility modes SR supports. What `buf breaking` does and why it belongs in CI. How all of this fits into rolling deployment, when producer-3 and consumer-1 are running in production at the same time.

## Four compatibility modes

Schema Registry stores a per-subject `compatibility` setting. This is the rule by which SR allows or rejects registering a new schema version under an existing subject. Four options:

- **NONE** — no check. Any schema passes. Whatever happens, happens.
- **BACKWARD** (Confluent SR default) — the new schema must be able to read data written by the old one. This is about upgrading consumers: catching up to the new version can be gradual, because the new code understands old messages.
- **FORWARD** — the old schema must be able to read data written by the new one. This is about upgrading producers: new code writes, old consumers read.
- **FULL** — both. The strictest mode; evolution proceeds in very small steps.

In practice, 90% of environments choose BACKWARD: catching consumers up is simpler than rolling out a new producer and keeping old clients alive indefinitely. But if dozens of teams are reading one topic and upgrading at different speeds, FORWARD or FULL is insurance against "we shipped a producer with a new field and all readers crashed at once."

In our sandbox the compat is global by default (`/config`), but overrides per-subject (`/config/<subject>`). The lecture explicitly sets BACKWARD on the subject — without this, all attempts to "register v4" depend on the global setting of the specific run.

## What Protobuf considers compatible

Protobuf at the wire level is a sequence of `(tag, value)` pairs. A tag is `field_number << 3 | wire_type`. There are no field names in the payload — the field name lives only in the schema; only the number travels on the wire. The rules follow from this.

Safe changes:

- **Add a new field with a new number.** Old consumers don't know the tag and store the bytes in unknown fields. New ones see the value. BACKWARD ✅, FORWARD ✅.
- **Delete a field no longer written by anyone.** Old readers won't see it (they get the default), new writers won't send it. Usually safer to mark the number `reserved` to avoid accidentally reusing it. BACKWARD ✅.
- **Rename a field without changing its number.** The name lives only in the schema; wire format doesn't change. BACKWARD ✅, FORWARD ✅.

Dangerous changes:

- **Change a field's type.** Was `int64`, became `string` — wire type differs (varint vs length-delimited), the payload won't decode. BACKWARD ❌.
- **Change a field's number.** Different tag — old bytes won't be found. BACKWARD ❌.
- **Delete a field and reuse its number under a different type.** Never. Use `reserved`.
- **Change an enum: adding is only forbidden in rare compilers**, usually safe. Deleting a value is dangerous — old messages with that tag will decode as `0` (UNSPECIFIED).

In this lecture v1 → v2 → v3 is a series of safe steps: each time fields are added. v4_breaking changes the type of field 3 and moves field 4 to number 7. SR will catch this, `buf breaking` will catch it, any reasonable CI will catch it.

## What's in `proto/`

Four `.proto` files. The structure:

```
proto/orders/
├── v1/order.proto              # 3 fields
├── v2/order.proto              # +currency
├── v3/order.proto              # +shipping_address (+ Address)
└── v4_breaking/order.proto     # broken attempt at v3
```

v1, v2, v3 are normal versions with separate packages `orders.v1`, `orders.v2`, `orders.v3`. Each generates its own Go package in `gen/`. v4_breaking is trickier — it deliberately declares `package orders.v3`, because `buf breaking` will only compare when the fully-qualified name matches. To keep the main buf module from failing with "Address declared multiple times", v4_breaking is excluded from the module via `buf.yaml`:

```yaml
modules:
  - path: proto
    excludes:
      - proto/orders/v4_breaking
```

Go code for v4_breaking is not generated — we don't want anyone accidentally using that type. The file exists for exactly two demonstrations: `make proto-breaking-check` and `make try-register-v4`.

## Subject and proto-package: the subtle part

Confluent SR checks compatibility within a single subject. Within a subject, all schema versions must share the same proto-package — otherwise the compat-check rejects registration with `PACKAGE_CHANGED`. This is an important detail.

In this lecture, v1 has package `orders.v1` and v3 has `orders.v3`. This is done for clean Go code: each version generates its own Go package (`gen/orders/v1`, `gen/orders/v3`), and producer-v1 and producer-v3 work with different `Order` types. Because the packages differ, SR won't put both versions into the same subject. So the lecture works with two subjects:

- `lecture-05-04-orders-v1-value` — v1 is registered here (package `orders.v1`).
- `lecture-05-04-orders-v3-value` — v3 is registered here (package `orders.v3`), and `make try-register-v4` attempts to add it as a second version.

In real life you wouldn't do this: normally a `.proto` file lives in one package and evolves by adding fields. Versions are git commits, not separate packages. Separate packages appear in this lecture only so that the example binaries have different Go types to illustrate forward compatibility at the wire level.

## buf breaking — gate in CI

`buf breaking` compares two states of a schema and reports incompatibilities according to a chosen set of rules. Our `buf.yaml` has `breaking: use: FILE` — buf's strictest set (FILE ⊃ PACKAGE ⊃ WIRE_JSON ⊃ WIRE), catching wire-format changes, field renames, field deletions, and file renames. It checks type, number, required-ness, presence — everything on the list of rules buf publishes in its docs.

In a real project you typically compare "the current PR" against "main". The lecture doesn't have git-ref infrastructure, so the Makefile does it manually: copies `proto/orders/v3/order.proto` and `proto/orders/v4_breaking/order.proto` into a tmp directory, builds them into buf images, and runs breaking against each other:

```makefile
proto-breaking-check:
	@tmpdir=$$(mktemp -d); \
	  trap 'rm -rf $$tmpdir' EXIT; \
	  mkdir -p $$tmpdir/v3 $$tmpdir/v4; \
	  cp proto/orders/v3/order.proto $$tmpdir/v3/; \
	  cp proto/orders/v4_breaking/order.proto $$tmpdir/v4/; \
	  ( cd $$tmpdir && \
	      buf build v3 -o v3.bin && \
	      buf build v4 -o v4.bin && \
	      buf breaking v4.bin --against v3.bin ); \
	  rc=$$?; \
	  ...
```

The run produces something like:

```
order.proto:32:1:Previously present field "4" with name "currency" on message "Order" was deleted.
order.proto:35:3:Field "3" with name "amount_cents" on message "Order" changed type from "int64" to "string".

OK: buf correctly reported the v3 → v4_breaking incompatibility
```

The Makefile logic inverts the exit code: `buf breaking` returns 100 when violations are found, and in our demo that's the desired outcome. If buf returns 0 — it means we accidentally made v4_breaking compatible, and the demo test is broken. The message goes both ways.

In real CI, `buf breaking` is a separate step before push, typically `buf breaking --against '.git#branch=main'`. On push to main, or on PR — if it breaks, the PR doesn't merge. This is cheap insurance against exactly what SR catches at runtime.

## SR and compat check

After running buf breaking locally, you move on to SR. There's a compatibility check there too — but of a different nature. buf looks at abstract rules ("field type changed"), SR looks at what passes the real constraints of the Confluent serializer (for Protobuf it's close to buf's FILE rules, but not identical).

The workflow for the v3 subject is laid out explicitly in the Makefile:

1. `make register-v3` — register v3 into `lecture-05-04-orders-v3-value`. Get version 1.
2. `make sr-set-compat-backward-v3` — lock the mode. Without this the global default can work against us; the lecture doesn't want that.
3. `make try-register-v4` — send v4_breaking to the same subject. SR applies the compat check, sees the type change `amount_cents` int64 → string, responds 409:

```json
{
  "error_code": 409,
  "message": "Schema being registered is incompatible with an earlier schema for subject \"lecture-05-04-orders-v3-value\", details: [{errorType:\"FIELD_SCALAR_KIND_CHANGED\", description:\"The kind of a SCALAR field at path '#/Order/3' in the new schema does not match its kind in the old schema\"}, ...]"
}
```

This is exactly what we want to see. The subject keeps living with version 1 (v3), v4_breaking never made it into the registry, and no producer can get a `schema_id` for it.

If you set `make sr-set-compat-none-v3` — the same `try-register-v4` will pass. SR then checks nothing, and the world gets "version 2, which diverged from previous versions." That's usually how production burns when someone forgot to look at the compat settings.

`make register-v1` is separate — it registers v1 in its own subject (`lecture-05-04-orders-v1-value`). In the lecture this exists solely so the subject exists and producer-v1 can operate under it. It's not related to the compat demonstration.

## Sliding deployment in practice

The lecture has three binaries: `producer-v1`, `producer-v3`, `consumer-v1`. Each producer has its own topic and subject. The scenario the whole setup is built for:

1. Run `producer-v3` — writes 5 Orders to `lecture-05-04-orders-v3` with all five fields.
2. Run `consumer-v1` (subscribed to the same topic by default, `-topic=lecture-05-04-orders-v3`).
3. consumer-v1 reads the messages and sees the first three fields. Currency and shipping_address go into unknown fields; the program doesn't crash.

producer-v3 registers the v3 schema in SR (gets its `schema_id`) and writes in Confluent wire format with that id. consumer-v1 is deliberately "dumb" — it doesn't call SR, strips the first 5 bytes of the header plus the protobuf message-index, and feeds the remainder to `proto.Unmarshal` into `*ordersv1.Order`:

```go
schemaID, payload, err := stripWireFormatHeader(rec.Value)
// ...
var order ordersv1.Order
if err := proto.Unmarshal(payload, &order); err != nil {
    logger.Error("unmarshal v1", "err", err)
    return
}
```

And here Protobuf's forward compatibility shows itself. The consumer doesn't know about the new fields; it doesn't care. The proto-runtime neatly stores the bytes of unknown tags in the `unknown_fields` of the struct. The program works, the v1 fields are populated as before:

```
--- lecture-05-04-orders-v3/2@1 key=ord-v3-00003 schema_id=15 ---
  id           = ord-v3-00003
  customer_id  = cus-052
  amount_cents = 12345
  unknown      = 47 bytes (v3 fields that v1 doesn't know)
```

The `schema_id` is visible in the log — it shows that SR holds v3 under that id. But consumer-v1 didn't use it for decoding.

This is rolling deployment: the producer was updated; consumers will update when they can. Nobody is down. When it's time, deploy consumer-v3 and it starts reading the new fields. Until then the data isn't lost: it's in Kafka's logs, and the new code will read it when it arrives.

In the reverse direction — producer-v1 writes, consumer-v3 reads — it also works. consumer-v3 sees the first three fields, currency is an empty string, shipping_address is nil. Those are the default values when a tag is absent from the payload.

## What to keep in mind

Compat in SR is a runtime gate. It protects against someone accidentally registering a broken schema. But it won't make your Go code remember unknown fields, won't teach your application to handle gaps, won't fix business logic. Schema Registry provides compatibility at the serialization level, not the semantic level.

buf breaking is a compile-time gate. It's faster, cheaper, and runs in CI before the new schema ever reaches SR. The good practice is both steps: buf breaking in CI plus SR-compat in production. One catches errors before merge, the other catches them on registration.

If your evolution is frequent (once a week or more) — consider FORWARD or FULL compat mode, especially if you have many readers on different deployment cycles. If it's infrequent (once a quarter) — BACKWARD is enough.

One last thing. If you find yourself where schema sliding breaks — the right path is usually not "how to push it past compat" but a new subject. `orders-v2-value` alongside `orders-v1-value`, two topics, two sets of consumers, migration on the application side. It's more expensive, but honest: broken compatibility in a single subject is a silent bomb that will go off somewhere in the middle of the night.

## Files

- `proto/orders/v1/order.proto` — starting version, 3 fields
- `proto/orders/v2/order.proto` — +currency
- `proto/orders/v3/order.proto` — +shipping_address (nested Address)
- `proto/orders/v4_breaking/order.proto` — broken variation of v3
- `cmd/producer-v1/main.go` — writes Orders using the v1 schema
- `cmd/producer-v3/main.go` — writes Orders using the v3 schema
- `cmd/consumer-v1/main.go` — reads the topic into `*v1.Order`, demonstrates unknown fields
- `buf.yaml`, `buf.gen.yaml` — module config, lint, breaking-check, codegen
- `Makefile` — all run targets

## Running

The sandbox must be up from the repository root (`docker compose up -d`).

```sh
make proto-gen                    # generate gen/orders/{v1,v2,v3}
make proto-lint                   # buf lint
make proto-breaking-check         # compare v3 and v4_breaking, expect buf's report

make topic-create-v1
make topic-create-v3

# SR-compat demo in the v3 subject
make register-v3                  # put v3 as version 1
make sr-set-compat-backward-v3    # lock compat mode
make try-register-v4              # v4_breaking — 409 rejected
make sr-list-versions-v3          # confirm only v3 (one version) is in the subject

# Wire-level forward compat demo
make register-v1                  # register v1 in its own subject
make run-consumer-v1              # start consumer (subscribed to topic v3)
make run-producer-v3              # 5 Orders via v3 (consumer-v1 reads them and sees unknown fields)
make run-producer-v1              # for contrast: 5 Orders via v1 into its own topic

make clean                        # delete topics, subjects, and gen/
```

## Related lectures

- [Schema Registry](../../../05-03-schema-registry/i18n/en/README.md) — wire format and basic registration
- [Protobuf in Go](../../../05-02-protobuf-in-go/i18n/en/README.md) — `.proto`, buf, codegen
- [Why contracts and wire formats](../../../05-01-why-contracts-and-wire-formats/i18n/en/README.md) — why schemas exist at all
