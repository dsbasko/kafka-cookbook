# 06-01 — gRPC Basics

So far there was one way to talk between services — Kafka. The producer put a message, the consumer read it eventually. That's async. Convenient for events ("order created", "payment processed"), but not ideal when the frontend needs a response right now. "Create an order and return its id" doesn't map naturally to a topic — you'd have to build request-reply on top of Kafka, track a `correlation_id`, wait for the response from a second topic. Doable. But cumbersome.

For a synchronous request-response there's another tool. HTTP/REST is the classic. gRPC is the same idea, but with types, binary format, and code generation. This lesson covers gRPC.

## gRPC in one paragraph

gRPC is an RPC framework on top of HTTP/2. Serialization is Protobuf (covered in module 05). You describe the service in a `.proto` file, the code generator produces a Go server interface and a Go client. Implement the interface — you get a working server. Import the client — you get a ready stub with typed methods. No manual JSON marshaling, no URL routers.

The transport underneath is HTTP/2. That gives you multiplexing (many calls over one connection), streams (covered in [gRPC streaming](../../../06-02-grpc-streaming/i18n/en/README.md)), binary frames, and header compression. At the network level it's still TCP plus TLS, but the frames are HTTP/2.

Four RPC types:

1. **Unary** — a regular request-response. The client sends one message, the server returns one. This lesson covers unary.
2. **Server-stream** — the client sends one request, the server responds with a stream of messages. Subscriptions, progress of long-running operations.
3. **Client-stream** — the client streams data, the server responds with a single summary at the end. Batch uploads.
4. **Bidi-stream** — both sides stream simultaneously. Chat-like scenarios, bidirectional synchronization.

Streams are a separate lesson. Here it's unary only. That's enough to get your first working server and client.

## What we're building

A small order service. Two methods:

- `Create(customer_id, amount, currency) -> Order` — creates an order, returns its id.
- `Get(id) -> Order` — retrieves by id.

Storage is a `map[string]*Order` under an `RWMutex`. No database, no Kafka. This is a gRPC lesson — everything else is stripped.

## The .proto file

The contract is described in a single file. Type safety and compatibility come from Protobuf, same as in [Protobuf in Go](../../../../05-contracts/05-02-protobuf-in-go/i18n/en/README.md) / [Schema Registry](../../../../05-contracts/05-03-schema-registry/i18n/en/README.md). What's new here is the `service` keyword and method declarations.

```proto
service OrderService {
  rpc Create(CreateRequest) returns (CreateResponse);
  rpc Get(GetRequest) returns (GetResponse);
}
```

Each method is `rpc <Name>(<request>) returns (<response>)`. The request and response are regular Protobuf messages. Convention for unary: a separate `XxxRequest` / `XxxResponse` pair per method. It sounds redundant, but it pays off at the first schema evolution — adding a field to `CreateRequest` doesn't affect the `Create` response or `GetRequest`. Use a shared type and you'll be untangling it later.

The actual lesson contract lives in `proto/orders/v1/orders.proto`. Besides the service it contains `Order`, `OrderStatus` (enum with the `ORDER_STATUS_` prefix — that's a buf convention), `CreateRequest`, `CreateResponse`, `GetRequest`, `GetResponse`.

Code generation runs via `buf generate`. Two plugins are wired up in `buf.gen.yaml`:

```yaml
plugins:
  - local: protoc-gen-go
    out: gen
  - local: protoc-gen-go-grpc
    out: gen
```

The first produces `*.pb.go` — ordinary Go structs. The second produces `*_grpc.pb.go` with the server interface, registrar, and client stub. Without the second plugin you get types but no server or client. A common trap — forgetting to install it.

Install locally (if first time on this machine):

```sh
go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
```

Then `make proto-gen` drops the generated files into `gen/orders/v1/`.

## Server

`grpc-server` listens on a TCP port, registers the OrderService implementation, and handles unary calls. This is the standard shape for any gRPC application.

```go
lis, err := net.Listen("tcp", *addr)
if err != nil { ... }

srv := grpc.NewServer(
    grpc.UnaryInterceptor(loggingUnaryInterceptor(logger)),
)

ordersv1.RegisterOrderServiceServer(srv, &orderServer{store: store})
reflection.Register(srv)

if err := srv.Serve(lis); err != nil { ... }
```

`grpc.NewServer` accepts options — here that's `UnaryInterceptor`, covered below. `RegisterOrderServiceServer` comes from `*_grpc.pb.go` and binds the implementation to the server. `reflection.Register` makes the server respond to service enumeration requests, which is needed for grpcurl without a `.proto` file. In production, reflection is usually disabled because it exposes the API unnecessarily.

The unary method implementation looks like a regular Go function:

```go
func (s *orderServer) Create(_ context.Context, req *ordersv1.CreateRequest) (*ordersv1.CreateResponse, error) {
    if req.GetCustomerId() == "" {
        return nil, status.Error(codes.InvalidArgument, "customer_id is required")
    }
    ...
    o := &ordersv1.Order{
        Id:          uuid.NewString(),
        CustomerId:  req.GetCustomerId(),
        AmountCents: req.GetAmountCents(),
        ...
    }
    s.store.put(o)
    return &ordersv1.CreateResponse{Order: o}, nil
}
```

Pay attention to errors. Not `errors.New`, not `fmt.Errorf`. Use the `google.golang.org/grpc/status` package with codes from `google.golang.org/grpc/codes`. That's the gRPC error model.

## Error model

gRPC transmits status in HTTP/2 trailers. A status has a code (a fixed enum) and a message (an arbitrary string). The set of codes is concrete — no need to invent "custom" codes or serialize errors as JSON.

Common ones:

- `OK` — all good. Returned when the handler returned a `nil` error.
- `InvalidArgument` — the client sent bad data. Don't confuse with `FailedPrecondition` (data is valid, but the server's current state doesn't allow it).
- `NotFound` — the requested resource doesn't exist.
- `AlreadyExists` — attempt to create something that already exists.
- `PermissionDenied` — auth is present but insufficient permissions.
- `Unauthenticated` — auth is absent or invalid.
- `DeadlineExceeded` — the client or intermediate gateway exceeded the deadline.
- `Internal` — something broke inside the server, no details.
- `Unavailable` — temporarily unavailable, try later (often means the connection died — for retry policies this signals "safe to retry").

In our `Create`, an empty `customer_id` is `InvalidArgument`. In `Get`, a missing order is `NotFound`. The server doesn't need to explicitly signal "this is retriable, this isn't" — the client or intermediate infrastructure reads the code and decides.

```go
return nil, status.Errorf(codes.NotFound, "order %q not found", req.GetId())
```

This isn't just a Go error — it's a typed gRPC error whose code will arrive at the client correctly. The client can then inspect it via `status.Code(err)`.

## Client

`grpc-client` connects, creates the stub, calls Create, calls Get, prints the result. As a bonus it fetches a non-existent id to confirm the code arrives as `NotFound`.

```go
conn, err := grpc.NewClient(
    *addr,
    grpc.WithTransportCredentials(insecure.NewCredentials()),
    grpc.WithUnaryInterceptor(loggingUnaryClientInterceptor(logger)),
)
if err != nil { ... }
defer conn.Close()

client := ordersv1.NewOrderServiceClient(conn)
```

`grpc.NewClient` is the modern API that replaced the deprecated `grpc.Dial`. The actual connection is lazily established on the first call. `insecure.NewCredentials` — because we have a plaintext server on localhost; in production use TLS.

The call itself:

```go
createCtx, cancel := context.WithTimeout(ctx, *timeout)
defer cancel()
createResp, err := client.Create(createCtx, &ordersv1.CreateRequest{
    CustomerId:  *customerID,
    AmountCents: *amount,
    Currency:    *currency,
})
```

The important detail here is `context.WithTimeout`. This is a deadline for the specific RPC. gRPC sends it in the request metadata; the server sees it and can abort processing if it hangs. Without a deadline, a hung server will block the client until the OS tears down the TCP connection — that can be minutes.

Rule: set a deadline on every client RPC. On the server — respect the incoming `ctx.Done()`, don't start long operations without checking the context.

Error inspection on the client uses the same `status` package:

```go
_, err = client.Get(notFoundCtx, &ordersv1.GetRequest{Id: "no-such-order"})
if code := status.Code(err); code != codes.NotFound {
    logger.Warn("ожидали NotFound", "got_code", code)
}
```

`status.Code(nil)` returns `OK`, so you can check with a single comparison. If the error isn't a gRPC error at all (e.g., a transport-level disconnect), the code will be `Unknown`.

## Interceptors

Both the server and client code had the same construct — `UnaryInterceptor`. This is gRPC's middleware mechanism. Every unary call passes through an interceptor chain before reaching the handler (server-side) or the network (client-side).

A server interceptor looks like this:

```go
func loggingUnaryInterceptor(logger *slog.Logger) grpc.UnaryServerInterceptor {
    return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
        start := time.Now()
        resp, err := handler(ctx, req)
        dur := time.Since(start)
        code := status.Code(err).String()
        if err != nil {
            logger.Error("rpc", "method", info.FullMethod, "code", code, "dur", dur, "err", err)
            return resp, err
        }
        logger.Info("rpc", "method", info.FullMethod, "code", code, "dur", dur)
        return resp, nil
    }
}
```

It's just a wrapper around the handler. Record the time, call it, grab the code via `status.Code`, log. For production you also add tracing (trace-id from metadata into logs and OTel spans), panic recovery (so a panic doesn't kill the whole server), metrics collection, and authentication (read the token from metadata, validate it, place claims in the context).

The client interceptor is the mirror:

```go
func loggingUnaryClientInterceptor(logger *slog.Logger) grpc.UnaryClientInterceptor {
    return func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
        start := time.Now()
        err := invoker(ctx, method, req, reply, cc, opts...)
        ...
    }
}
```

This is where retry logic goes, auth headers are added (via `metadata.AppendToOutgoingContext`), and client-side metrics are collected. Standard chain for any production gRPC client — auth → tracing → retry → metrics.

## Comparison with REST/HTTP

If you expected "gRPC is always better than HTTP" — no, it isn't. Where gRPC wins:

- Binary protocol. Fewer bytes on the wire than JSON.
- Code generation for both sides. No "read a field, it's a string, should be a number" at runtime.
- Streams out of the box. HTTP/1.1 has none; in HTTP/2 you can hand-roll them with chunked encoding, but that's DIY.
- Deadlines propagate through the call chain. In REST you carry `X-Request-Timeout` by hand, or die silently.

Where REST/HTTP still makes sense:

- The browser. gRPC doesn't natively work in browsers (you need gRPC-Web or a proxy like Envoy). REST works there without ceremony.
- External APIs for third parties. Everyone speaks HTTP+JSON; not everyone wants to deal with Protobuf.
- Simple admin interfaces — spin up curl, call it, inspect. With gRPC you need grpcurl (and reflection enabled to avoid carrying a `.proto`).

Inside one perimeter, where both sides are under your control, gRPC saves effort. On the boundary with the outside world — usually REST or GraphQL.

## What `grpcurl` does

`grpcurl` is the curl equivalent for gRPC. Use it to call the server by hand, without starting a client. The Makefile has a few examples:

```sh
grpcurl -plaintext localhost:50051 list
grpcurl -plaintext localhost:50051 describe orders.v1.OrderService.Create
grpcurl -plaintext -d '{"customer_id":"cus-007","amount_cents":2599,"currency":"EUR"}' \
  localhost:50051 orders.v1.OrderService/Create
```

This works because reflection is enabled. Without reflection you'd need to pass `-proto proto/orders/v1/orders.proto -import-path proto`. Convenient for debugging, inconvenient for CI — there you typically write a dedicated Go client for checks.

## Running it

Start the server in one terminal:

```sh
make run-server
```

In another terminal — the client:

```sh
make run-client
```

The server output shows the interceptor log: method, code, duration. The client shows three blocks: created, got, notfnd. The third one intentionally misses to show that NotFound is a typed code, not a text "error".

To call the server directly with grpcurl:

```sh
make grpcurl-list                                 # list services
make grpcurl-create                               # create an order
make grpcurl-get ID=<uuid from Create response>  # retrieve it back
```

Note — the server is stateful, the store lives in memory. Restart the server and all orders are gone. That's the boundary of this lesson: we built a bare gRPC service, no database, no Kafka, no authentication. In [Hybrid gRPC + Kafka](../../../06-04-hybrid-grpc-and-kafka/i18n/en/README.md) this same service grows Postgres, an outbox table, and event publishing — there you'll see how gRPC and Kafka coexist in one process.

## What's next

[gRPC streaming](../../../06-02-grpc-streaming/i18n/en/README.md) — streams. Server-stream, client-stream, bidi. Also covers backpressure on streams and how a gRPC stream fundamentally differs from a Kafka stream (short answer — durability and replay).

[Sync vs async: gRPC and Kafka](../../../06-03-sync-vs-async/i18n/en/README.md) — decision matrix: when to use gRPC, when to use Kafka. Using "user signed up" as an example with honest trade-offs for both approaches.

[Hybrid gRPC + Kafka](../../../06-04-hybrid-grpc-and-kafka/i18n/en/README.md) — hybrid: gRPC for the synchronous API + Kafka for events + outbox for atomicity.

[Saga: choreography vs orchestration](../../../06-05-saga-choreography/i18n/en/README.md) — sagas and compensations, choreography vs orchestration.

For now — go to the terminal and run `make run-server` plus `make run-client`. Look at the log. Then we move on.
