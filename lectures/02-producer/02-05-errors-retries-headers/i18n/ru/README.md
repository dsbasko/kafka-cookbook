# 02-05 — Errors, Retries & Headers

Это последняя лекция модуля про продьюсер. Закрываем три темы, которые остались висеть с предыдущих: что делать с ошибкой записи, как работают ретраи, и зачем у каждого record'а есть отдельный слот headers, не входящий в payload.

Ошибки разные. Что-то клиент должен ретраить сам, что-то — отдать наверх и не трогать. Это и есть деление на retriable и non-retriable. На уровне Go-кода оно выглядит как «вернулась ошибка из ProduceSync», а вот что внутри этой ошибки и что с ней делать — зависит от кода ошибки, который пришёл с брокера.

## Retriable vs non-retriable — формальная разница

В Kafka каждая ошибка протокола имеет 16-битный код. Часть кодов помечена как retriable, часть — как non-retriable. Это поле жёстко зашито в спецификации Kafka, не в клиенте.

Логика простая. Retriable — это «попробуй снова, скорее всего пройдёт». Лидер партиции сейчас перевыбирается, временно нет нужного числа реплик в ISR, контроллер мигрирует — всё временное, через секунду-другую само рассосётся. Брокер открыто говорит «я не могу прямо сейчас, повтори».

Non-retriable — «это не починится повторной попыткой, разбирайся». Сообщение больше, чем брокер готов принять. Топика не существует, и автосоздание выключено. Авторизация не прошла. Тело record'а сломано на уровне CRC. Нет смысла слать те же байты снова и ждать, что в этот раз получится — формат запроса именно такой, и он именно такой будет на следующей попытке.

В franz-go проверка ровно через `kerr.IsRetriable(err)`:

```go
func IsRetriable(err error) bool {
    var kerr *Error
    return errors.As(err, &kerr) && kerr.Retriable
}
```

Каждая известная ошибка имеет poll Retriable. Например:

- `MESSAGE_TOO_LARGE` — Retriable=false. Запрос больше `max.message.bytes`. Хоть тысячу раз шли — всё равно отлуп.
- `NOT_ENOUGH_REPLICAS` — Retriable=true. ISR просел ниже `min.insync.replicas`. Подождали, ISR поднимется, повтор пройдёт.
- `LEADER_NOT_AVAILABLE` — Retriable=true. Лидер партиции переезжает, через секунду метаданные обновятся.
- `TOPIC_AUTHORIZATION_FAILED` — Retriable=false. У клиента нет прав. Хоть сто раз шли — права не появятся.

Клиент видит код, читает Retriable-флаг и решает: либо положить запрос в очередь ретраев, либо отдать ошибку наверх через future.

## Что делает franz-go с retriable-ошибкой

Когда Produce-запрос вернулся с retriable-ошибкой, клиент **не** говорит сразу `ProduceSync(...) → err`. Он держит record в внутренней очереди и сам пробует заново. Сколько раз — лимитирует параметр `RecordRetries` (по умолчанию `math.MaxInt64`, то есть «пока время не выйдет»). Сколько по времени — лимитирует `RecordDeliveryTimeout`.

`RecordDeliveryTimeout` — это общий бюджет на доставку **одного** record'а. Включает в себя всё: первая попытка, ожидание перед ретраем, вторая попытка, метаданные, всё-всё. По умолчанию его нет (= ∞), и тогда лимит фактически только в `RequestTimeoutOverhead` плюс `RecordRetries`. На практике лучше явно поставить какой-то потолок — секунд 30, минута, — иначе ретраемый record может зависнуть в буфере на несколько минут, забивая `MaxBufferedRecords`.

Связка с `RequestTimeoutOverhead` важна. Этот параметр — добавка ко времени, которое сам брокер ждёт перед ответом. Он не лимитирует доставку record'а целиком, он лимитирует **одну попытку**. Если хочешь, чтобы клиент быстро понял «брокер не отвечает» и перешёл к ретраю — снижай `RequestTimeoutOverhead`. Если хочешь дать record'у больше времени в сумме — поднимай `RecordDeliveryTimeout`. Это разные ручки для разных задач.

При исчерпании delivery timeout franz-go возвращает не голый `kerr.NotEnoughReplicas`, а свою ошибку `kgo.ErrRecordTimeout`, и заворачивает в неё последнюю наблюдённую retriable-причину через `%w`. Значит на стороне приложения проверка идёт через два уровня:

```go
if errors.Is(pErr, kgo.ErrRecordTimeout) {
    // делать что-то с тем фактом, что доставка не пробилась
}
if errors.Is(pErr, kerr.NotEnoughReplicas) {
    // частный случай — последней причиной было NER
}
```

Любая non-retriable ошибка возвращается **сразу** после первой неудачной попытки. Без ожидания, без ретраев. ProduceSync на этом завершается, и приложение получает ошибку через `out.FirstErr()`.

## Что показывает наш код

Бинарник один — `cmd/error-classes`, два режима через флаг `-mode`. Это редкий случай, когда два почти-разных сценария логичнее держать в одном файле, чем растаскивать по двум: общая инфраструктура (admin client, ensure-topic, создание клиента) одинаковая, отличается только конфиг топика и ожидание.

**Non-retriable.** Создаём топик с `max.message.bytes=1024`, пишем туда record на 4 КБ случайных байт. Ждём `MESSAGE_TOO_LARGE` мгновенно:

```go
if err := ensureTopicWithMaxBytes(ctx, admin, topic, "1024"); err != nil {
    return fmt.Errorf("ensure topic %s: %w", topic, err)
}

cl, err := kafka.NewClient(
    kgo.DefaultProduceTopic(topic),
    kgo.ProducerBatchMaxBytes(1<<20),
    kgo.MaxBufferedRecords(10),
    kgo.ProducerBatchCompression(kgo.NoCompression()),
)
```

`ProducerBatchCompression(kgo.NoCompression())` — обязательная штука. Дефолт franz-go — Snappy. Без явного выключения наш «плотный» payload (`a`-`z` циклом) сожмётся в ~300 байт, влезет под лимит и брокер примет. С NoCompression и реально случайными байтами размер на проводе совпадает с длиной record'а, и брокер срабатывает как ожидалось.

После ProduceSync смотрим:

```go
out := cl.ProduceSync(rpcCtx, rec)
pErr := out.FirstErr()
switch {
case errors.Is(pErr, kerr.MessageTooLarge):
    // ожидаемый сценарий — non-retriable
}
```

В выводе видно, что весь цикл ProduceSync занял десятки миллисекунд — это один round-trip, без ретраев. И в самой ошибке брокер прямо пишет, сколько байт пришло (`uncompressed_bytes=4114, compressed_bytes=4114`).

**Retriable.** Создаём топик с `min.insync.replicas=3` на RF=3, пишем с `acks=all`. До запуска оператор делает `make kill-broker` (стопит kafka-2), и ISR падает до 2. Каждая попытка возвращается с NOT_ENOUGH_REPLICAS — клиент ретраит, пока не истечёт `RecordDeliveryTimeout`.

Чтобы было видно, как именно идут ретраи, включаем встроенный логгер franz-go на debug-уровне:

```go
opts := []kgo.Opt{
    kgo.DefaultProduceTopic(topic),
    kgo.RecordDeliveryTimeout(deliveryTimeout),
    kgo.RequiredAcks(kgo.AllISRAcks()),
}
if debug {
    opts = append(opts, kgo.WithLogger(kgo.BasicLogger(os.Stderr, kgo.LogLevelDebug, nil)))
}
```

В дебаг-логе видна вся цепочка: первый Produce → брокер ответил `NOT_ENOUGH_REPLICAS` → клиент пишет `rewinding produce sequence to resend pending batches` → новый запрос → опять NER → … → пока не упрёмся в delivery timeout.

После исчерпания таймера `out.FirstErr()` возвращает `kgo.ErrRecordTimeout`. Внутри (через `errors.Unwrap`) лежит `NOT_ENOUGH_REPLICAS` — последняя причина, которая помешала доставить.

Восстанавливаешь брокер (`make restore-broker`) — ISR возвращается к 3, запуск проходит за пару сотен миллисекунд, без ошибок.

## Headers — отдельный слот, не часть payload

Теперь про headers. У каждого record'а в Kafka, начиная с протокола 0.11, есть отдельная секция headers — массив пар `(key string, value []byte)`. Брокер их хранит, передаёт консьюмерам, не интерпретирует. Это твои данные, к которым у Kafka один подход — «не моё дело, сохраню как есть».

Зачем нужны? Headers — место для **служебной** метаинформации, которая нужна инфраструктуре, а не бизнес-логике потребителя. Стандартный набор:

- `traceparent` (или `b3`) — distributed tracing context. Producer прокидывает свой текущий trace, consumer его читает и продолжает span. Всё работает на уровне инфраструктуры; бизнес-обработчик вообще не знает, что есть трейсинг.
- `correlation-id` — id запроса, по которому можно связать логи нескольких сервисов. Полезен, когда trace через OpenTelemetry не настроен.
- `message-type` — версия и тип события (`order.created.v1`, например). Консьюмер по нему выбирает schema/decoder, без необходимости заглядывать в payload.
- `source-service` — кто произвёл событие. Полезно для аудита, фильтрации в DLQ, многосервисной отладки.
- `timestamp` (если нужен — встроенного-то достаточно), `idempotency-key`, `tenant-id` — список можно продолжать сколько угодно.

Почему **не** держать это в payload? Несколько причин.

Во-первых, payload — это твой бизнес-контракт, schema. Туда лезут поля события как такового: `order_id`, `amount`, `currency`. Если ты будешь забивать туда `traceparent` — он попадёт в schema, попадёт в schema registry, и его нельзя будет менять без формального ребейза. А tracing-контекст совершенно ортогонален event'у — он привязан к транспорту, не к смыслу.

Во-вторых, без распарсивания payload ты до headers всё равно доберёшься. Это важно для маршрутизаторов, фильтров, DLQ-обработчиков, которые не хотят знать схему события и просто принимают решения по типу/корреляции/источнику. Distributed tracing collector тоже не должен парсить protobuf или Avro каждого события, чтобы вытащить trace-id.

В-третьих, headers — стандарт. OpenTelemetry, W3C Trace Context, CloudEvents — все они формализуют именно через headers. Если ты уйдёшь в payload, придётся писать свой парсер для каждого случая.

## Что делает наш headers-demo

Бинарник `cmd/headers-demo` — компактный round-trip: producer пишет 5 record'ов с набором headers, consumer тут же читает и печатает headers вместе с payload. По умолчанию запускаются оба в одном процессе через режим `-mode=roundtrip`, для двух терминалов есть `-mode=producer` и `-mode=consumer` отдельно.

Структура record'а внутри producer'а:

```go
rec := &kgo.Record{
    Key:   []byte(fmt.Sprintf("order-%d", i+1)),
    Value: []byte(fmt.Sprintf(`{"id":"order-%d","status":"created"}`, i+1)),
    Headers: []kgo.RecordHeader{
        {Key: "traceparent", Value: []byte(trace)},
        {Key: "correlation-id", Value: []byte(correlationID)},
        {Key: "message-type", Value: []byte(msgType)},
        {Key: "source-service", Value: []byte(service)},
    },
}
```

Headers — обычный slice; ключ строкой, значение байтами. Никаких ограничений на ключи: один и тот же ключ может встретиться несколько раз (это допустимо протоколом, бывает полезно для multi-value headers). Значения произвольные — UTF-8 строка, бинарь, JSON, что угодно. Единственное практическое правило: общая длина headers + key + value тоже считается в `max.message.bytes`. Не пихай туда мегабайты.

В consumer'е каждое поле приходит обратно ровно тем же:

```go
fetches.EachRecord(func(r *kgo.Record) {
    fmt.Fprintf(tw, "  %d\t%d\t%s\t%s\t%s\n",
        r.Partition, r.Offset,
        string(r.Key),
        formatHeaders(r.Headers),
        string(r.Value),
    )
})
```

Видно две вещи. Headers — это `[]kgo.RecordHeader`, тот же тип, что у producer'а; их порядок сохраняется. Брокер ничего с ними не делает: что положили, то и пришло, до байта.

`traceparent` собран в формате W3C: `00-<trace-id 32 hex>-<span-id 16 hex>-01`. Никаких реальных span'ов мы тут не открываем — в боевом коде туда подставлялся бы текущий trace из OpenTelemetry. Здесь просто рандом, чтобы видно было, что значения разные на каждом record'е.

## Что взять с собой

- Ошибки протокола Kafka делятся на retriable (временные, имеет смысл повторять) и non-retriable (повторять бесполезно). Деление зашито в спеке, не в клиенте.
- franz-go сам ретраит retriable-ошибки. Лимиты — `RecordRetries` и `RecordDeliveryTimeout`. По умолчанию retries фактически бесконечны, поэтому без явного delivery timeout record может висеть в буфере очень долго.
- `RecordDeliveryTimeout` — общий бюджет на доставку одного record'а; `RequestTimeoutOverhead` — добавка к одному round-trip'у. Это разные ручки.
- При исчерпании delivery timeout приходит `kgo.ErrRecordTimeout`, оборачивающий последнюю причину. Чекать через `errors.Is(err, kgo.ErrRecordTimeout)` или внутрь, через `errors.Is(err, kerr.NotEnoughReplicas)` и подобные.
- Non-retriable падает сразу. ProduceSync возвращает ошибку без ретраев, по таймингу видно: единицы-десятки миллисекунд.
- Headers — отдельный слот record'а. Туда кладут всё инфраструктурное: tracing context, correlation-id, message-type, source-service, idempotency-key и тому подобное. Брокер их не трогает.
- В payload идут только бизнес-поля события, то, что описано в его schema. Headers — для всего остального, что нужно транспорту и инфраструктуре.

В модуле 03 переключаемся на сторону консьюмера: группы, ребалансы, гарантии обработки. Headers пригодятся там в каждой второй лекции — особенно в [Обработка ошибок](../../../../03-consumer/03-04-error-handling/i18n/ru/README.md), где DLQ-сообщения принципиально требуют `error.message`, `error.class`, `original.offset` именно в headers, а не в payload.

## Запуск

Стенд из корня репозитория должен быть поднят (`docker compose up -d`).

Non-retriable сценарий:

```sh
make run-errors
```

Создаёт топик `lecture-02-05-non-retriable` с `max.message.bytes=1024`, пишет туда 4 КБ случайных байт, ловит MESSAGE_TOO_LARGE.

Retriable сценарий:

```sh
make kill-broker      # стопит kafka-2, ISR падает до 2
make run-errors-retriable
make restore-broker   # стартует kafka-2, ISR возвращается к 3
```

В дефолтном режиме клиент пишет с дебаг-логгером и видно каждую попытку Produce → NOT_ENOUGH_REPLICAS → rewind → опять. Через 20 секунд (delivery-timeout) приходит `kgo.ErrRecordTimeout`.

Headers-demo:

```sh
make run-headers
```

Один процесс в режиме roundtrip: пишет 5 record'ов с traceparent/correlation-id/message-type/source-service, тут же читает их в консьюмер-группе и печатает в табличке.

Перепрогон чистым тоже полезен, особенно если что-то осталось от прошлых запусков:

```sh
make topic-delete
```
