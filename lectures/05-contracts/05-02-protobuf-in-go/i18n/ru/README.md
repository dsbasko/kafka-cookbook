# 05-02 — Protobuf in Go

В прошлой лекции мы кодировали Protobuf руками — через `protowire`, по тегам из `.proto`-файла. Это было полезно один раз, чтобы увидеть: под капотом у Protobuf'а нет магии, обычный wire-формат. Дальше так писать никто не будет. Никто не прописывает руками `appendString(buf, 4, o.Currency)` в проде. Все живут на сгенерированном коде.

Эта лекция про то, как устроен нормальный workflow. Один `.proto`-файл, прогон через `buf generate`, на выходе — типизированный Go-пакет с `*Order`, `*OrderItem`, `OrderStatus`-enum и методом `Reset/String/Marshal/Unmarshal` на каждый тип. Дальше пишешь обычный Go.

## Что такое .proto и что такое сгенерированный код

`.proto` — это текстовое описание сообщения. Файл лежит в репозитории, по нему ревью, по нему diff, по нему `buf breaking` ловит ломающие изменения (про последнее — лекция [Эволюция схем](../../../05-04-schema-evolution/i18n/ru/README.md)). Контракт хранится отдельно от кода и читается человеком.

Сгенерированный Go-код — это `*.pb.go`, который выплёвывает `protoc` или его обёртка вроде `buf`. Внутри — обычные структуры с тегами и методы для marshal/unmarshal. Пример из нашего `gen/orders/v1/order.pb.go` (фрагмент сгенерированного):

```go
type Order struct {
    Id             string                 `protobuf:"bytes,1,opt,name=id,proto3" ...`
    CustomerId     string                 `protobuf:"bytes,2,opt,name=customer_id,json=customerId,proto3" ...`
    AmountCents    int64                  `protobuf:"varint,3,opt,name=amount_cents,json=amountCents,proto3" ...`
    Status         OrderStatus            `protobuf:"varint,7,opt,name=status,proto3,enum=orders.v1.OrderStatus" ...`
    CreatedAt      *timestamppb.Timestamp `protobuf:"bytes,8,opt,name=created_at,json=createdAt,proto3" ...`
    ReservationTtl *durationpb.Duration   `protobuf:"bytes,9,opt,name=reservation_ttl,json=reservationTtl,proto3" ...`
    Note           *string                `protobuf:"bytes,11,opt,name=note,proto3,oneof" ...`
    // ... еще поля и unexported служебные
}
```

В тегах поля с `snake_case` именем содержат маркер `json=camelCase` — это нужно для `protojson`, который сериализует Protobuf в JSON по camelCase-конвенции. У `Note` в конце тега стоит `oneof` — proto3 `optional` под капотом реализуется как синтетический oneof из одного поля, и сгенерированный код это явно фиксирует.

Никаких ручных `appendString` ты больше не пишешь. Поля — обычные Go-типы, getter'ы автогенерятся (`GetId()`, `GetStatus()`, `GetItems()`), а сериализация — это один вызов `proto.Marshal(order)`.

## Конвенции, которые ломают совместимость, если их нарушить

Protobuf прощает многое и не прощает один класс ошибок: всё, что связано с **field-номерами**. Номер поля — это часть wire-формата. Поменял — все старые байты в Kafka стали мусором. Поэтому конвенции тут — не вкусовщина.

1. **Имена полей в `.proto` пишутся в `snake_case`.** В сгенерированном Go это всё равно превратится в `CamelCase` (поле `customer_id` станет `CustomerId`). Но в самом `.proto` — `snake_case`, потому что это требование style guide и линтер `buf` будет ругаться на `customerId`.
2. **Номер поля задаётся явно и навсегда.** В нашем `Order` номера 1..6 совпадают с теми, что были в [Зачем контракты и wire-форматы](../../../05-01-why-contracts-and-wire-formats/i18n/ru/README.md). Совместимость за это и платится — добавить поле = новый номер; удалить поле = `reserved` его номер навсегда.
3. **Удалённые поля резервируют по номеру и по имени:**
   ```proto
   reserved 10;
   reserved "customer_email";
   ```
   Без этого через полгода кто-то может «переиспользовать» номер 10 — и ваши старые сообщения в Kafka, у которых там лежал email, начнут декодироваться как новое поле. Боль будет тихая.
4. **Enum'ы начинаются с zero-value.** Первый элемент должен быть со значением 0 и нести смысл «не указано». В нашем `OrderStatus` это `ORDER_STATUS_UNSPECIFIED = 0`. Дефолтное состояние сообщения, в котором поле `status` забыли выставить, — ровно оно. Это spec, не вкусовщина.
5. **Имена enum'ов префиксуются именем самого enum'а.** `ORDER_STATUS_CREATED`, не `CREATED`. В Protobuf enum-значения находятся в плоском namespace вместе с другими enum'ами того же файла — без префикса будут коллизии.

Эти правила линтер `buf` проверяет автоматически. Дальше посмотрим, как он встаёт в pipeline.

## Well-known types

Иногда нужно положить в сообщение время или длительность. Можно завести `int64 created_at_unix` (как мы сделали в [Зачем контракты и wire-форматы](../../../05-01-why-contracts-and-wire-formats/i18n/ru/README.md)), и для большинства задач этого достаточно. Но Protobuf даёт встроенные типы — `google.protobuf.Timestamp` и `google.protobuf.Duration`, — которые в большинстве клиентов сами разворачиваются в нативный тип языка.

В Go это `*timestamppb.Timestamp` и `*durationpb.Duration` из пакетов `google.golang.org/protobuf/types/known/...`. У них есть `.AsTime()`, `.AsDuration()`, и есть конструкторы `timestamppb.Now()`, `durationpb.New(d)`. Выглядит так:

```go
order := &ordersv1.Order{
    Id:             fmt.Sprintf("ord-%05d", i),
    Status:         ordersv1.OrderStatus_ORDER_STATUS_PAID,
    CreatedAt:      timestamppb.Now(),
    ReservationTtl: durationpb.New(15 * time.Minute),
}
```

В нашей схеме оставлено и старое поле `created_at_unix`, и новое `created_at` через well-known Timestamp — чтобы было видно, как они уживаются. На проде обычно остаётся одно, и оно — Timestamp.

## Optional в proto3

В proto3 по умолчанию все скаляры имеют zero-value-семантику: пустая строка не пишется на wire, нулевой `int64` тоже. Из-за этого «поля нет» и «поле равно нулю» неразличимо. Если такая разница важна — поле помечается ключевым словом `optional`:

```proto
optional string note = 11;
```

В сгенерированном Go это превращается в указатель: `Note *string`. Если консьюмер хочет понять «поле прислано или забыто» — проверяет `o.Note != nil`. Без `optional` отличить пустую строку от отсутствующего поля невозможно.

## buf — это новый protoc

Долгие годы proto-кодген выглядел так: ставишь `protoc` (бинарь на C++), ставишь к нему плагины (`protoc-gen-go`, `protoc-gen-go-grpc`), пишешь длинную команду с десятком `--*_opt` флагов, прописываешь её в `Makefile`. Работало, но через год команда превращается во что-то вроде:

```sh
protoc -I proto -I third_party --go_out=. --go_opt=paths=source_relative \
       --go-grpc_out=. --go-grpc_opt=paths=source_relative \
       --validate_out=lang=go,paths=source_relative:. \
       proto/orders/v1/*.proto
```

`buf` — это обёртка, которая прячет всё это за двумя командами: `buf generate` и `buf lint`. Конфиг лежит в `buf.yaml` (модуль и линтер) и `buf.gen.yaml` (плагины и куда складывать).

Наш `buf.gen.yaml` — минимальный, всего один плагин:

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

`local: protoc-gen-go` — это значит, что бинарь плагина уже стоит в `$PATH` (`go install google.golang.org/protobuf/cmd/protoc-gen-go@latest`). `paths=source_relative` — чтобы выходные файлы клались по той же структуре каталогов, что и `.proto`-файлы; без этой опции `protoc-gen-go` пытается раскладывать по импортируемому пути, и получается каша.

Запуск:

```sh
make proto-gen  # внутри: buf generate
```

И в `gen/orders/v1/order.pb.go` появляется `package ordersv1` со всеми типами. Мы не делаем этого вручную, потому что в реальной разработке этот файл может перегенериться десять раз в день при правке схемы.

`buf lint` гоняется отдельно — он не требует генерации, проверяет сами `.proto`-файлы по набору правил `STANDARD`. Если ты, например, забудешь резервировать удалённое поле или назовёшь enum-значение без префикса — `buf lint` об этом скажет до коммита.

## Что показывает наш producer

В `cmd/producer/main.go` собран Order через сгенерированные типы и записан в Kafka. Смотри ключевую часть:

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

Три вещи тут стоит зафиксировать. Во-первых, `proto.Marshal` принимает любой `proto.Message` (это интерфейс из `google.golang.org/protobuf/proto`) — `*ordersv1.Order` им и является, потому что сгенерированный код реализует нужный интерфейс автоматически. Во-вторых, header `content-type: application/x-protobuf` — это дисциплина, не требование протокола; consumer всё равно должен знать, в какой тип `Unmarshal`-ить. В-третьих, header `schema: orders.v1.Order` — наша ручная замена schema_id из Schema Registry. В [Schema Registry](../../../05-03-schema-registry/i18n/ru/README.md) эту строку заменит `magic byte + schema_id`, а Registry будет хранить сами `.proto`-файлы.

Сборка Order'а через сгенерированные типы — обычный Go:

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

`Note` — это `*string` (поле `optional`), и поэтому передаётся через указатель. Остальные поля — обычные значения.

## Что показывает наш consumer

`cmd/consumer/main.go` читает топик и распаковывает Protobuf. Сердце цикла:

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

`proto.Unmarshal` — обратная операция к `proto.Marshal`. Принимает `[]byte` и указатель на сообщение, мутирует его. Если consumer и producer собраны на одной версии `.proto` — байты раскладываются обратно ровно в тот же Order. Если producer успел уехать на v2 со старыми номерами — старый consumer прочитает то, что знает, и проигнорирует неизвестные номера. Это и есть forward compatibility (про неё подробно — [Эволюция схем](../../../05-04-schema-evolution/i18n/ru/README.md)).

Печать через автогенерированные getter'ы:

```go
fmt.Printf("  status       = %s\n", o.GetStatus().String())
if ts := o.GetCreatedAt(); ts != nil {
    fmt.Printf("  created_at   = %s\n", ts.AsTime().Format("2006-01-02 15:04:05Z07:00"))
}
if d := o.GetReservationTtl(); d != nil {
    fmt.Printf("  reservation  = %s\n", d.AsDuration())
}
```

Getter'ы безопасно работают на nil-message — `((*Order)(nil)).GetStatus()` вернёт zero-value enum'а вместо паники. На сообщениях, которые могут быть частично заполнены (например, после миграции схемы), это убирает кучу `if != nil` проверок.

## Запуск

Нужны два бинаря в `$PATH`:

```sh
go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
go install github.com/bufbuild/buf/cmd/buf@latest
```

Дальше:

```sh
make proto-gen          # сгенерить gen/orders/v1/order.pb.go
make proto-lint         # buf lint, должен пройти молча
make topic-create       # создать lecture-05-02-orders-proto, RF=3, 3 партиции
make run-producer       # записать 10 Order'ов
make run-consumer       # прочитать и распечатать структуру
```

В отдельном терминале можно запустить `kafka-console-consumer.sh` — увидишь сырые байты, в которых читаются ASCII-фрагменты (`sku-...`, `cus-...`, валюта `USD`), а числовые значения — мусором. Это нормально: Protobuf — бинарь, и без знания схемы человек его не прочитает. В этом и смысл.

## На что обратить внимание

- `gen/` лежит **в коде**, не в `.gitignore`. У этого подхода два аргумента. Первый: воспроизводимость без `buf`-зависимости в CI на свежем clone'е репо. Второй: ревьюер в PR увидит, как сгенерированный код поменялся при правке `.proto`. Есть сторонники противоположного — генерировать на каждом билде, в репо не коммитить. У обоих лагерей есть аргументы, в курсе мы выбираем «коммитим» — проще для учебной воспроизводимости.
- `kgo.Record.Key` я кладу как `[]byte(order.GetId())`. Это значит, что все Order'ы с одинаковым id попадут в одну партицию (см. [Ключи и партиционирование](../../../../02-producer/02-01-keys-and-partitioning/i18n/ru/README.md)). Если хочется балансировки по customer_id — поменяй на `[]byte(order.GetCustomerId())`. На сериализации payload'а это никак не отражается.
- В headers лежит `content-type: application/x-protobuf` и `schema: orders.v1.Order`. Это полезная дисциплина, но без Schema Registry consumer всё ещё доверяет своему коду — какой тип знает, в такой и `Unmarshal`-ит. В [Schema Registry](../../../05-03-schema-registry/i18n/ru/README.md) эту слабость разберём.
- Если ты поменяешь `.proto` (например, добавишь поле) и забудешь сделать `make proto-gen` — Go-сборка не упадёт, потому что старый `*.pb.go` ещё валидный. Но новые поля в коде использовать не получится. Поэтому `proto-gen` — первая цель в Makefile.

## Что дальше

В [Schema Registry](../../../05-03-schema-registry/i18n/ru/README.md) добавим Schema Registry: producer будет регистрировать схему, в payload поедет `magic byte + schema_id + protobuf-bytes`, consumer будет вытаскивать schema_id из первых пяти байт и использовать его как ключ кеша. В [Эволюция схем](../../../05-04-schema-evolution/i18n/ru/README.md) — про эволюцию: что в Protobuf считается ломающим изменением и как `buf breaking` ловит это автоматически.

А этой лекции хватит. Отсюда уже можно писать обычные Go-сервисы, которые гоняют типизированные сообщения через Kafka, и не страдать от руками-собранных wire-байтов.
