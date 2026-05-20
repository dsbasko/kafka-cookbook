# 07-03 — Kafka Connect

До этой лекции мы писали продьюсеров и консьюмеров руками. Подключился, кидаешь записи через `cl.ProduceSync`, читаешь через `cl.PollFetches`, разбираешься с offset'ами. Когда задача — «перетаскивать данные из БД в Kafka и обратно в другую БД», такой код становится одинаковым в каждом проекте: подключение к источнику, маппинг строк в события, retry, идемпотентность, мониторинг. И его всё равно надо писать. И поддерживать.

Connect — это попытка не писать его сорок раз. Стандартный сервис рядом с Kafka, в который заливают плагины-коннекторы. Один коннектор знает, как читать из Postgres, другой — как писать в ClickHouse. Ты только конфигурируешь: connection url, имя таблицы, частоту опроса, маппинг полей. Запуск, retry, offset'ы, парраллелизм, балансировку между нодами Connect берёт на себя.

В нашем стенде Connect уже крутится — `kafka-connect` контейнер на :8083 (см. корневой `docker-compose.yml`). REST API живёт на `http://localhost:8083`. Эта лекция — первая прогулка через него. Дальше идёт [Debezium CDC](../../../07-04-debezium-cdc/i18n/ru/README.md) с Debezium как source-коннектором; тут мы делаем sink — перекидываем сообщения из Kafka в Postgres-таблицу, ничего на Go не пишем для приёмной стороны.

## Что вообще такое коннектор

Коннектор бывает двух видов: **source** и **sink**. Source читает из внешней системы и пишет в Kafka. Sink — наоборот: вычитывает топик и кладёт во внешнюю систему. Один процесс Connect может тащить и тех, и других одновременно.

Внутри коннектор — это просто Java-плагин. Confluent выкладывает их пачками: JDBC (источник и приёмник для любой реляционной БД), Elasticsearch sink, S3 sink, Debezium для CDC из PG/MySQL/Mongo. Сторонние тоже есть — ClickHouse Sink от Altinity, например. У нас в стенде директория `connect-plugins/` смонтирована в контейнер; туда плагины кидают распакованным архивом, Connect находит их при старте.

Плагин `confluentinc/kafka-connect-jdbc` делает обе стороны: JDBC source (читает SELECT'ом по incrementing-колонке) и JDBC sink (пишет INSERT/UPDATE по топику). Нам нужен только sink.

### Connect distributed mode в одну строку

Наш `kafka-connect` запущен в distributed mode. Это значит, что состояние Connect живёт в самой Kafka, тремя топиками:

1. `_connect-configs` — конфиги коннекторов.
2. `_connect-offsets` — чек-пойнты source-коннекторов (sink'и хранят свой offset в обычной consumer-group).
3. `_connect-status` — статусы коннекторов и task'ов.

Все три создаются сами при старте Connect. Если поднять второй worker с тем же `group.id` — они автоматом разделят между собой задачи. Standalone-режим (всё локально на диске одного процесса) у нас в курсе не используем.

## REST API в трёх запросах

Без UI, через curl:

```sh
# что вообще установлено
curl http://localhost:8083/connector-plugins | jq '.[] | .class'

# создать коннектор (тело — JSON с полем "name" и "config")
curl -X POST -H 'Content-Type: application/json' \
     --data @connectors/jdbc-sink-orders.json \
     http://localhost:8083/connectors

# посмотреть что с ним
curl http://localhost:8083/connectors/lecture-07-03-jdbc-sink-orders/status
```

Ответ `/status` важный. Поле `connector.state` — это сам процесс маршрутизации (`RUNNING`, `PAUSED`, `FAILED`). Поле `tasks[].state` — состояние воркеров; на каждый таск своё. Если `connector.state=RUNNING`, а `tasks[0].state=FAILED` — значит коннектор живой, но один из его потоков сдох. Это надо ловить в Grafana по метрике `connect-worker-metrics`, в проде. Если просто молчать, данные перестанут течь.

## Что у нас в лекции

Берём sink-коннектор `JdbcSinkConnector` и склеиваем его с Postgres'ом, который поднимаем тут же через `docker-compose.override.yml`. Postgres цепляется в ту же сеть `sandbox-kafka_kafka-net`, чтобы Connect-контейнер видел его по hostname'у `lecture-07-03-postgres`. Хост-порт 15436 — отдельно для psql'а с твоей машины (Connect ходит по internal-портам контейнерной сети, ему 15436 ни к чему).

Поток данных:

```
[Go orders-producer] ──> Kafka topic `lecture-07-03-orders` ──> [kafka-connect / JdbcSink] ──> Postgres orders
```

И нюанс: Connect не умеет угадывать схему таблицы по голому JSON'у. Откуда ему знать, что `amount` — это `double`, а не `string`? Поэтому value придётся снабжать схемой.

## Конвертеры и почему value длиннее, чем кажется

Когда Connect читает запись из Kafka, он не понимает байты сам по себе. Ему нужен **converter** — компонент, который превращает массив байт в типизированную структуру. Конвертеров несколько:

- `AvroConverter` — байты несут magic byte + schema_id, конвертер ходит в Schema Registry, тащит схему, парсит. Самый компактный wire-format.
- `JsonConverter` (с `schemas.enable=true`) — JSON-объект `{"schema": {...}, "payload": {...}}`. Схема таскается с каждой записью.
- `JsonConverter` (с `schemas.enable=false`) — голый JSON. Connect не знает типов, sink не работает.
- `StringConverter` — просто строка. Для ключа годится, для значения — почти никогда.

В корневом `docker-compose.yml` дефолт для всего Connect выставлен в Avro + Schema Registry. Это удобно для production. Но в этой лекции мы специально делаем без SR — чтобы wire-format был руками виден в `kafka-console-consumer` без декодеров. Поэтому в конфиге коннектора **переопределяем** `value.converter` на `JsonConverter` со schema-внутри.

В реальной жизни обычно Avro/Protobuf через SR — записи весят в 10–20 раз меньше. Но тут учебный sandbox, поэтому JSON.

## Конфиг JDBC sink — построчно

Файл `connectors/jdbc-sink-orders.json`. Самое важное:

```json
{
  "connector.class": "io.confluent.connect.jdbc.JdbcSinkConnector",
  "topics": "lecture-07-03-orders",
  "connection.url": "jdbc:postgresql://lecture-07-03-postgres:5432/lecture_07_03",
  "connection.user": "lecture",
  "connection.password": "lecture",
  "value.converter": "org.apache.kafka.connect.json.JsonConverter",
  "value.converter.schemas.enable": "true",
  "table.name.format": "orders",
  "insert.mode": "upsert",
  "pk.mode": "record_value",
  "pk.fields": "id"
}
```

Что важно посмотреть.

Связка из трёх флагов делает «UPSERT по полю id из payload'а»:

1. `insert.mode=upsert` — режим записи (есть ещё `insert` и `update`).
2. `pk.mode=record_value` — откуда взять PK: из value записи. Альтернативы — `record_key` (PK в ключе записи), `kafka` (PK = координаты Kafka `topic+partition+offset`) и `none` (без PK, тогда апсёрт невозможен).
3. `pk.fields=id` — какие именно поля считать первичным ключом.

Если бы `insert.mode=insert`, второй раз с тем же id sink упал бы по duplicate key. Если `pk.mode=none`, апсёрт не работает в принципе.

`auto.create=false` и `auto.evolve=false` — мы решили заранее: схему таблицы создаём руками через `db/init.sql`. Sink на `auto.create=true` сам бы накатил `CREATE TABLE`, но тогда теряется контроль над типами и индексами. Для production почти всегда `false`.

`errors.tolerance=none` — на любой ошибке маршрутизации task падает. В проде иногда ставят `all` плюс `errors.deadletterqueue.topic.name=...`, чтобы плохие записи уезжали в DLQ топик и не блокировали поток.

## Что отправляет наш Go-код

Go тут только на producer-стороне. Sink-сторона — это сам Connect, кода писать не надо.

`cmd/orders-producer/main.go`. Всё устроено как первый продьюсер из [Первый продьюсер на franz-go](../../../../01-foundations/01-05-first-producer/i18n/ru/README.md), разница — в формате value. Каждое сообщение это JSON с двумя верхними полями: `schema` (описание структуры) и `payload` (данные).

Сама схема собирается один раз и переиспользуется:

```go
func ordersSchema() connectSchema {
    return connectSchema{
        Type: "struct",
        Name: "lecture_07_03.orders",
        Fields: []connectField{
            {Field: "id", Type: "int64", Optional: false},
            {Field: "customer_id", Type: "string", Optional: false},
            {Field: "amount", Type: "double", Optional: false},
            {Field: "status", Type: "string", Optional: false},
            {Field: "created_at", Type: "string", Optional: false},
        },
    }
}
```

Ключ записи — id заказа как строка. Это нужно для двух вещей. Во-первых, одинаковые id всегда летят в одну партицию — sink при перезапуске обрабатывает их в исходном порядке. Во-вторых, `pk.mode=record_value` берёт id из value, но партиционирование всё равно лежит на producer'е, и стабильный ключ облегчает наблюдение.

Тело сообщения собирается в одну структуру и маршалится:

```go
envelope := orderEnvelope{
    Schema: schema,
    Payload: orderPayload{
        ID:         id,
        CustomerID: customerID,
        Amount:     amount,
        Status:     "created",
        CreatedAt:  time.Now().UTC().Format(time.RFC3339Nano),
    },
}
valueBytes, _ := json.Marshal(envelope)
```

Сырая запись в Kafka выглядит примерно так (одна штука):

```json
{
  "schema": {
    "type": "struct",
    "name": "lecture_07_03.orders",
    "fields": [
      {"field": "id", "type": "int64", "optional": false},
      {"field": "customer_id", "type": "string", "optional": false},
      {"field": "amount", "type": "double", "optional": false},
      {"field": "status", "type": "string", "optional": false},
      {"field": "created_at", "type": "string", "optional": false}
    ]
  },
  "payload": {
    "id": 1,
    "customer_id": "cust-17",
    "amount": 642.31,
    "status": "created",
    "created_at": "2026-05-01T12:00:00Z"
  }
}
```

Размер тут отдельная боль. Schema-блок дублируется в каждом сообщении. На 1М сообщений это десятки лишних мегабайт. Поэтому в production'е такие пайплайны делают на Avro: schema хранится в SR, в Kafka летит magic byte + schema_id плюс компактный binary payload. Это лекция [Schema Registry](../../../../05-contracts/05-03-schema-registry/i18n/ru/README.md) / [Эволюция схем](../../../../05-contracts/05-04-schema-evolution/i18n/ru/README.md) — тут сознательно простой формат.

## Как запустить

Стандартная последовательность:

```sh
make up                  # Postgres из override.yml
make db-init             # CREATE TABLE orders
make topic-create        # Kafka topic с 3 партициями
make connect-plugin-check
make connector-create    # POST /connectors
make connector-status    # должен быть RUNNING/RUNNING

make run-producer COUNT=200
sleep 2
make db-count            # 200
```

Если `connect-plugin-check` показал, что плагина нет — installed-set Connect-образа `cp-kafka-connect:8.0.0` от стенда к стенду чуть меняется, и иногда JDBC бандлится, иногда нет. В Makefile в сообщении ошибки есть готовая команда `confluent-hub install`, прямо внутри контейнера. После установки — `docker compose restart kafka-connect`, потом ещё раз check.

Повторный запуск с теми же id показывает поведение upsert'а:

```sh
make run-producer COUNT=200 START_ID=1
make db-count    # снова 200, но строки обновились (см. statuses в БД)
```

Если `make connector-status` вернул `tasks[0].state=FAILED`, поле `trace` содержит exception от sink'а. Самые частые причины:

- таблицы в БД нет (`db-init` забыт)
- типы в схеме не совпадают с типами в таблице (`amount=string` против `DOUBLE PRECISION`)
- Postgres недоступен по этому hostname'у (override.yml не привязан к `kafka-net`)
- драйвера PostgreSQL JDBC нет в плагин-директории (редкая, но бывает после ручных манипуляций с `connect-plugins/`)

## SMT в одну строку, чтобы было

SMT (Single Message Transforms) — отдельный слой между конвертером и коннектором. По дороге можно переименовать поле, выкинуть, замаскировать, привести тип. Конфигурируется так:

```json
"transforms": "rename,mask",
"transforms.rename.type": "org.apache.kafka.connect.transforms.ReplaceField$Value",
"transforms.rename.renames": "customer_id:user_id",
"transforms.mask.type": "org.apache.kafka.connect.transforms.MaskField$Value",
"transforms.mask.fields": "amount"
```

В нашей лекции SMT не используется — таблица и payload сделаны под друг друга. SMT всплывут позже: полное погружение в use case [Postgres → ClickHouse с анонимизацией](../../../../09-use-cases/03-pg-to-clickhouse/i18n/ru/README.md), там они нужны для outbox event router.

## Что осталось за кадром

Эта лекция намеренно тонкая. За кадром:

- Distributed mode с несколькими worker-нодами и автоматическим shifting задач.
- Дедупликация и exactly-once на стороне sink'а (для Postgres работает за счёт UPSERT'а; для S3/Elasticsearch там свои механики).
- DLQ (`errors.deadletterqueue.topic.name`) и стратегии обработки плохих записей.
- Connect-метрики (lag, throughput, error rate, restart-count). На них надо вешать Grafana.
- Source-коннекторы. Их концептуально проще понять после лекции [Debezium CDC](../../../07-04-debezium-cdc/i18n/ru/README.md) — там как раз source.

Если хочется глубже именно по JDBC sink — конфигурационный справочник Confluent очень подробный, у каждого поля документировано поведение в edge-cases. Параметров там штук пятьдесят.

## Дальше

В [Debezium CDC](../../../07-04-debezium-cdc/i18n/ru/README.md) разбираем CDC через Debezium. Это другой коннектор — source, не sink. Он подписывается на Postgres logical replication slot и стримит каждое изменение строки в Kafka в виде события `before`/`after`. После этого пара «Debezium → JDBC sink» даёт асинхронную репликацию между БД, и весь use case [Postgres → ClickHouse с анонимизацией](../../../../09-use-cases/03-pg-to-clickhouse/i18n/ru/README.md) ровно эту пару и собирает.
