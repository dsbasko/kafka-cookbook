# 06-01 — gRPC Basics

До сих пор у нас был один способ говорить между сервисами — Kafka. Producer положил сообщение, consumer когда-нибудь прочитал. Это асинхрон. Удобно для событий («заказ создан», «платёж прошёл»), но не очень удобно, когда фронту нужен ответ прямо сейчас. «Создай заказ и верни мне его id» через топик не делается естественно — пришлось бы городить request-reply поверх Kafka, заводить correlation_id, ждать ответ из второго топика. Можно. Но громоздко.

Для синхронного запроса с ответом есть другой инструмент. HTTP/REST — классика. gRPC — то же самое, только с типами, бинарным форматом и кодогенерацией. Лекция про него.

## gRPC в одном абзаце

gRPC — это RPC-фреймворк поверх HTTP/2. Сериализация — Protobuf (мы его уже знаем из модуля 05). Описываешь сервис в `.proto`-файле, кодогенератор делает Go-интерфейс под сервер и Go-клиент. Заполняешь интерфейс — получаешь рабочий сервер. Импортируешь клиент — получаешь готовый стаб с типизированными методами. Никаких ручных JSON-маршалов, никаких URL-роутеров.

Транспорт под капотом — HTTP/2. Отсюда мультиплексирование (много вызовов в одном соединении), стримы (об этом [gRPC streaming](../../../06-02-grpc-streaming/i18n/ru/README.md)), бинарные фреймы, header compression. На сетевом уровне всё ещё TCP плюс TLS, но фреймы уже HTTP/2.

Четыре типа RPC:

1. **Unary** — обычный запрос-ответ. Клиент шлёт одно сообщение, сервер возвращает одно. Это лекция про unary.
2. **Server-stream** — клиент шлёт один запрос, сервер отвечает потоком сообщений. Подписки, прогресс долгой операции.
3. **Client-stream** — клиент льёт поток, сервер отвечает одним резюме в конце. Загрузка батчей.
4. **Bidi-stream** — обе стороны одновременно шлют потоки. Чат-подобные сценарии, двусторонняя синхронизация.

Стримы — отдельная лекция. Тут только unary. Этого достаточно, чтобы получить первый рабочий сервер и клиент.

## Что мы пишем

Маленький сервис заказов. Два метода:

- `Create(customer_id, amount, currency) -> Order` — создаёт заказ, выдаёт id.
- `Get(id) -> Order` — отдаёт по id.

Хранилище — `map[string]*Order` под `RWMutex`. Никаких БД, никакого Kafka. Лекция про gRPC, всё лишнее уберём.

## .proto-файл

Контракт описывается в одном файле. Type-safety и совместимость — за счёт Protobuf, всё как в [Protobuf в Go](../../../../05-contracts/05-02-protobuf-in-go/i18n/ru/README.md) / [Schema Registry](../../../../05-contracts/05-03-schema-registry/i18n/ru/README.md). Новое тут — ключевое слово `service` и описание методов.

```proto
service OrderService {
  rpc Create(CreateRequest) returns (CreateResponse);
  rpc Get(GetRequest) returns (GetResponse);
}
```

Каждый метод — это `rpc <Имя>(<запрос>) returns (<ответ>)`. Запрос и ответ — обычные Protobuf-сообщения. Соглашение для unary: на каждый метод отдельная пара `XxxRequest` / `XxxResponse`. Звучит избыточно, но окупается на первой же эволюции — добавил поле в `CreateRequest`, и это никак не задело `CreateResponse` или `GetRequest`. Если заводить общий тип — придётся выкручиваться позже.

Реальный контракт лекции лежит в `proto/orders/v1/orders.proto`. Кроме сервиса там сидят `Order`, `OrderStatus` (enum с префиксом `ORDER_STATUS_` — это buf-конвенция), `CreateRequest`, `CreateResponse`, `GetRequest`, `GetResponse`.

Кодогенерация поднимается через `buf generate`. В `buf.gen.yaml` подключены два плагина:

```yaml
plugins:
  - local: protoc-gen-go
    out: gen
  - local: protoc-gen-go-grpc
    out: gen
```

Первый делает `*.pb.go` — обычные Go-структуры. Второй — `*_grpc.pb.go` с интерфейсом сервера, регистратором и клиентским стабом. Без второго плагина у тебя будут типы, но не будет сервера и клиента. Частая ловушка — забыть его поставить.

Установка локально (если впервые на машине):

```sh
go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
```

Дальше `make proto-gen` выкатит файлы в `gen/orders/v1/`.

## Сервер

`grpc-server` слушает TCP-порт, регистрирует реализацию OrderService, обрабатывает unary-вызовы. Стандартный shape для любого gRPC-приложения.

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

`grpc.NewServer` принимает опции — здесь это `UnaryInterceptor`, про него ниже. `RegisterOrderServiceServer` приходит из `*_grpc.pb.go`, она привязывает реализацию к серверу. `reflection.Register` — сервер начинает отвечать на запросы перечисления своих методов, это нужно для grpcurl без `.proto`. В проде reflection обычно отключают, потому что это лишнее раскрытие API.

Сама реализация unary-метода выглядит как обычная Go-функция:

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

Обрати внимание на ошибки. Не `errors.New`, не `fmt.Errorf`. Используется пакет `google.golang.org/grpc/status` плюс коды из `google.golang.org/grpc/codes`. Это и есть error model gRPC.

## Error model

gRPC передаёт статус в HTTP/2-trailers. У статуса есть код (фиксированный enum) и сообщение (произвольная строка). Кодов конкретный набор — не нужно изобретать «пользовательские» коды или сериализовать ошибку в JSON.

Ходовые:

- `OK` — всё хорошо. Возвращается, если handler вернул `nil`-ошибку.
- `InvalidArgument` — клиент прислал неправильные данные. Не путать с `FailedPrecondition` (данные правильные, но состояние сервера сейчас не позволяет).
- `NotFound` — запрошенного ресурса нет.
- `AlreadyExists` — попытка создать то, что уже существует.
- `PermissionDenied` — auth есть, но прав не хватает.
- `Unauthenticated` — auth вообще нет или невалидна.
- `DeadlineExceeded` — клиент или промежуточный gateway превысил deadline.
- `Internal` — что-то сломалось внутри сервера, без подробностей.
- `Unavailable` — временно нельзя, попробуй позже (часто означает, что соединение умерло — для retry-политик это сигнал «можно ретраить»).

В нашем `Create` пустой `customer_id` — это `InvalidArgument`. В `Get` отсутствующий заказ — `NotFound`. Серверу не приходится отдельно говорить «это retriable, это нет» — клиент или промежуточная инфраструктура смотрят на код и сами решают.

```go
return nil, status.Errorf(codes.NotFound, "order %q not found", req.GetId())
```

Это не просто Go-ошибка — это типизированная gRPC-ошибка, у которой код прилетит на клиент честно. Клиент потом может разобрать её через `status.Code(err)`.

## Клиент

`grpc-client` подключается, создаёт стаб, делает Create, делает Get, печатает результат. Бонусом ходит за несуществующим id, чтобы убедиться, что код приходит как `NotFound`.

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

`grpc.NewClient` — это современный API, заменивший устаревший `grpc.Dial`. Реальное соединение лениво поднимется при первом вызове. `insecure.NewCredentials` — потому что у нас plaintext-сервер на localhost, в проде там TLS.

Сам вызов:

```go
createCtx, cancel := context.WithTimeout(ctx, *timeout)
defer cancel()
createResp, err := client.Create(createCtx, &ordersv1.CreateRequest{
    CustomerId:  *customerID,
    AmountCents: *amount,
    Currency:    *currency,
})
```

Тут важная деталь — `context.WithTimeout`. Это deadline на конкретный RPC. gRPC шлёт его в метаданные запроса, сервер видит и может своими руками прервать обработку, если зависает. Без deadline зависший сервер заблокирует клиента до тех пор, пока TCP-соединение не оборвёт OS — это могут быть минуты.

Правило: на каждый клиентский RPC ставь deadline. На сервере — респектуй пришедший `ctx.Done()`, не лезь в долгие операции без проверки контекста.

Разбор ошибки на клиенте делается через тот же `status`-пакет:

```go
_, err = client.Get(notFoundCtx, &ordersv1.GetRequest{Id: "no-such-order"})
if code := status.Code(err); code != codes.NotFound {
    logger.Warn("ожидали NotFound", "got_code", code)
}
```

`status.Code(nil)` вернёт `OK`, поэтому проверять можно одним сравнением. Если ошибка — это не gRPC-ошибка вовсе (например, transport-level разрыв), код будет `Unknown`.

## Interceptors

В коде сервера и клиента стояли две одинаковые штуки — `UnaryInterceptor`. Это middleware-механизм gRPC. Любой unary-вызов проходит через цепочку interceptor'ов до того, как дойти до handler'а (на сервере) или до сети (на клиенте).

Серверный interceptor выглядит так:

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

Это просто обёртка над handler'ом. Засекаешь время, дёргаешь, забираешь код через `status.Code`, логируешь. Для production туда же добавляют tracing (trace-id из metadata в логи и в OTel-span), recover от panic'а (чтобы panic не убивал весь сервер), сбор метрик, аутентификацию (читать токен из metadata, валидировать, класть claims в context).

Клиентский interceptor — зеркало:

```go
func loggingUnaryClientInterceptor(logger *slog.Logger) grpc.UnaryClientInterceptor {
    return func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
        start := time.Now()
        err := invoker(ctx, method, req, reply, cc, opts...)
        ...
    }
}
```

Тут место для retry-логики, добавления auth-заголовков (через `metadata.AppendToOutgoingContext`), сбора client-side метрик. Стандартная цепочка для любого продового gRPC-клиента — auth → tracing → retry → metrics.

## Сравнение с REST/HTTP

Если ты ожидал «gRPC всегда лучше HTTP» — нет, не всегда. Где gRPC выигрывает:

- Бинарный протокол. Меньше байт на проводе, чем JSON.
- Кодогенерация на обе стороны. Никакого «прочитал поле, оно строка, а должно быть число» в рантайме.
- Стримы из коробки. В HTTP/1.1 их нет, в HTTP/2 их можно сделать вручную через chunked, но это самодельщина.
- Deadlines прорастают через цепочку вызовов. В REST это руками таскать `X-Request-Timeout` или умирать молча.

Где REST/HTTP всё ещё уместен:

- Браузер. gRPC в браузер штатно не ходит (нужен gRPC-Web или прокси типа envoy). REST туда ходит без церемоний.
- Внешние API для третьих сторон. Все умеют HTTP+JSON, не все хотят разбираться с Protobuf.
- Простые admin-интерфейсы — поднял curl, дёрнул, посмотрел. С gRPC надо grpcurl (и reflection включить, чтобы не таскать .proto).

Внутри одного периметра, где обе стороны под твоим контролем — gRPC экономит силы. На границу с внешним миром — обычно REST или GraphQL.

## Что делает `grpcurl`

`grpcurl` — это аналог curl для gRPC. Через него удобно дёргать сервер руками, без поднятия клиента. В нашем Makefile есть пара примеров:

```sh
grpcurl -plaintext localhost:50051 list
grpcurl -plaintext localhost:50051 describe orders.v1.OrderService.Create
grpcurl -plaintext -d '{"customer_id":"cus-007","amount_cents":2599,"currency":"EUR"}' \
  localhost:50051 orders.v1.OrderService/Create
```

Это работает потому, что у нас включён reflection. Без reflection пришлось бы передавать `-proto proto/orders/v1/orders.proto -import-path proto`. Удобно для дебага, неудобно для CI — там обычно пишут отдельный Go-клиент для проверок.

## Запуск

Поднимаем сервер в одном терминале:

```sh
make run-server
```

В другом терминале — клиент:

```sh
make run-client
```

В выводе сервера видно лог из interceptor'а: метод, код, длительность. На клиенте — три блока: created, got, notfnd. Третий специально промахивается, чтобы показать, что NotFound — это типизированный код, а не текстовая «ошибка».

Если хочется потрогать сервер напрямую через grpcurl:

```sh
make grpcurl-list                                 # перечислить сервисы
make grpcurl-create                               # создать заказ
make grpcurl-get ID=<uuid из ответа Create>      # достать его обратно
```

Заметь — server stateful, store живёт в памяти. Перезапустил сервер — все заказы пропали. Это и есть граница лекции: мы сделали голый gRPC, без БД, без Kafka, без аутентификации. Дальше в [Гибрид gRPC + Kafka](../../../06-04-hybrid-grpc-and-kafka/i18n/ru/README.md) этот же сервис обрастёт Postgres, outbox-таблицей и публикацией событий — там уже будет видно, как gRPC уживается с Kafka в одном процессе.

## Что дальше

В [gRPC streaming](../../../06-02-grpc-streaming/i18n/ru/README.md) — стримы. Server-stream, client-stream, bidi. Там же будет про backpressure на стриме и про то, чем gRPC-стрим принципиально отличается от Kafka-стрима (короткий ответ — durability и replay).

В [Sync vs async: gRPC и Kafka](../../../06-03-sync-vs-async/i18n/ru/README.md) — decision matrix: когда брать gRPC, когда Kafka. На примере «user signed up» с честными плюсами и минусами обоих подходов.

В [Гибрид gRPC + Kafka](../../../06-04-hybrid-grpc-and-kafka/i18n/ru/README.md) — гибрид: gRPC для синхронной API + Kafka для событий + outbox для атомарности.

В [Saga: choreography vs orchestration](../../../06-05-saga-choreography/i18n/ru/README.md) — saga и компенсации, choreography vs orchestration.

Пока — выйди в терминал и сделай `make run-server` плюс `make run-client`. Посмотри на лог. Дальше двигаемся.
