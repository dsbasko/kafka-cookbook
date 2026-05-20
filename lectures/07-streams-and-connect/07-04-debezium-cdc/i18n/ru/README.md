# 07-04 — Debezium CDC

CDC — это «change data capture». Идея простая: вместо того чтобы периодически опрашивать таблицу (`SELECT * FROM users WHERE updated_at > $1`), мы читаем сами изменения. Каждый INSERT, UPDATE и DELETE — отдельное событие. С точностью до строки, со старым и новым значением, в порядке коммита транзакций.

Откуда берётся такая роскошь? Из журнала самой БД. У Postgres'а это WAL — write-ahead log, в который БД пишет всё, что собирается сделать с данными, ещё до того как сделает. WAL нужен для крэш-рекавери и репликации между мастером и репликой. Если научиться его читать со стороны — получим поток изменений «как есть», без вытаскивания самой таблицы.

Кто это умеет читать на практике — Debezium. Это набор Kafka Connect connector'ов, по одному на каждый поддерживаемый движок (Postgres, MySQL, MongoDB, SQL Server, Oracle, ещё пара). В этой лекции — только Postgres вариант.

## Зачем CDC вообще

Сценариев, где он спасает, четыре:

1. **Аналитика.** Postgres отлично для OLTP, но строить отчёты по нему на терабайтах больно. CDC → Kafka → ClickHouse / BigQuery / Snowflake. Бизнес-БД остаётся чистой, аналитика работает на отдельном движке.
2. **Поиск.** Postgres → Elasticsearch. Каждая правка строки → переиндексация документа. Без CDC пришлось бы либо batch'ом переливать всю базу раз в час, либо прибивать в код приложения двойную запись (а двойная запись без транзакции = боль).
3. **Микросервисы.** Старая монолитная БД, новый микросервис, которому нужны её данные. Подписали на CDC — он живёт своей жизнью на свежих данных, не дёргая исходный сервис синхронно.
4. **Outbox pattern.** Из лекции [Outbox-паттерн](../../../../04-reliability/04-03-outbox-pattern/i18n/ru/README.md) ты помнишь: транзакционный outbox решает проблему «БД-write + Kafka-publish атомарно». Но там publisher — это poller, который SELECT'ит outbox каждые 100ms. Дороже, чем хочется. С CDC publisher не нужен совсем — Debezium читает WAL и сам публикует.

Четвёртый пункт — это финальная форма outbox'а, и в этой лекции мы её собираем.

## Как Debezium читает WAL Postgres'а

Тут зарыто несколько слоёв. Разберёмся последовательно.

WAL Postgres'а пишется в физическом формате — байтовое представление изменения страниц на диске, без SQL-операторов вроде «INSERT INTO users VALUES (...)». Снаружи такой поток почти бесполезен. Чтобы достать из него логические изменения, Postgres с версии 10 умеет «logical replication» — поверх WAL'а работает декодер, который превращает физические записи в логические события (INSERT/UPDATE/DELETE с набором колонок).

Декодер выбирается через параметр `plugin.name`. Базовый встроенный — `pgoutput`. Раньше нужно было ставить `wal2json`, но Debezium с 2.0 поддерживает pgoutput из коробки, и установка плагинов больше не нужна.

Доступ к потоку идёт через два объекта:

- **publication** — это SQL-объект, который перечисляет таблицы для стриминга. По сути — список «что слушаем».
- **replication slot** — это позиция в WAL'е. Postgres держит для каждого слота указатель на самую старую запись WAL'а, которую слот ещё не подтвердил. Пока слот существует и от него нет ack'ов — Postgres хранит весь WAL начиная с этой позиции, не вычищая его.

Это критично. Replication slot — мощная штука и одновременно ловушка. Если ты создал слот, потом удалил коннектор без drop'а слота, а потом про него забыл — Postgres будет копить WAL вечно. Диск заполнится, БД встанет колом. У нас в `make connector-delete-all` есть явный `pg_drop_replication_slot` — без него легко закопаться. В production это закрывают мониторингом `pg_replication_slots.confirmed_flush_lsn` и алёртами на отстающие слоты.

## Структура события

Что Debezium кладёт в Kafka, когда в users случается UPDATE? Вот скелет:

```json
{
  "before": {"id": 42, "email": "old@x.com", "status": "active",  "full_name": "User 42"},
  "after":  {"id": 42, "email": "old@x.com", "status": "blocked", "full_name": "User 42"},
  "source": {"version": "3.5.0.Final", "ts_ms": 1714723200000, "lsn": 281474976710732, "table": "users"},
  "op": "u",
  "ts_ms": 1714723200123
}
```

`op` — символ операции:

- `c` — create (INSERT)
- `u` — update
- `d` — delete (after будет null)
- `r` — read (строка из начального snapshot'а — Debezium при первом запуске прочитает всю таблицу через SELECT и пометит каждую строку как `r`)
- `t` — truncate

`before` для UPDATE/DELETE приходит только если у таблицы выставлена `REPLICA IDENTITY FULL`. По дефолту Postgres пишет в WAL только PK строки — этого хватает для физической репликации, но для CDC ты получаешь обрубок: `{"id": 42}` без остальных полей. У нас в `db/init.sql` руками выставлено `REPLICA IDENTITY FULL` — за это платишь чуть большим объёмом WAL'а.

Tombstone — отдельная штука. Когда строка удалена и `tombstones.on.delete=true`, Debezium после события `op=d` шлёт ещё одно сообщение в тот же ключ с `value=null`. Это нужно для compact-топиков: log compaction удаляет все версии, у которых ключ совпал с tombstone'ом. Если CDC-топик настроен на `cleanup.policy=compact` (что часто), tombstone'ы — единственный способ выжать удалённые строки из истории.

## Конвенция имён топиков

Debezium для каждой таблицы создаёт топик с именем `<topic.prefix>.<schema>.<table>`. У нас `topic.prefix=cdc`, схема `public`, таблица `users` — итого `cdc.public.users`.

Если коннектор подписан на 10 таблиц — будет 10 топиков. Каждый со своим набором партиций (по дефолту 1, для production обычно поднимают). Ключ сообщения — primary key таблицы (как JSON). Это даёт стабильное партиционирование: все события одной строки летят в одну партицию, порядок сохраняется.

## Snapshot и потом

Когда коннектор стартует первый раз с `snapshot.mode=initial`, он делает:

1. Берёт снимок WAL-позиции (`pg_current_wal_lsn()`).
2. Делает `SELECT *` со всех таблиц из `table.include.list` и шлёт каждую строку как `op=r`.
3. После snapshot'а переключается на чтение WAL'а с зафиксированной позиции и идёт дальше как стрим.

Это даёт согласованную картину: подписался — сначала залил всю текущую базу в Kafka, дальше стримит инкрементальные изменения. Без потерь, без гонок.

`snapshot.mode` бывает разный — `initial` (наш дефолт), `no_data` (только новые изменения, без исторических данных; в Debezium 2.x этот режим назывался `never`), `initial_only` (snapshot и стоп), `when_needed`. Для аналитики обычно `initial`. Для outbox-таблицы — `no_data`, исторический outbox обычно не нужен.

## Outbox event router

В обычном CDC у тебя топик `cdc.public.outbox` — и в нём свалка из строк outbox-таблицы. Полезность ноль: на стороне потребителя пришлось бы парсить структуру outbox'а, доставать `aggregate_type` и решать, что это вообще за событие.

Debezium это делает за тебя через SMT (Single Message Transform) под названием `EventRouter`. Конфигурируется так:

- `route.by.field=aggregate_type` — берём имя из этой колонки.
- `route.topic.replacement=events.${routedByValue}` — подставляем в шаблон.
- `table.field.event.payload=payload` — value сообщения берётся из этой колонки.
- `table.field.event.key=aggregate_id` — ключ сообщения.
- `table.fields.additional.placement=type:header:eventType,...` — лишние колонки уходят в headers.

В итоге вместо одного `cdc.public.outbox` получается набор топиков `events.user`, `events.order`, `events.payment` (по тому, что в колонке `aggregate_type`), и каждое сообщение уже имеет нормальный бизнес-ключ и payload без обёртки. Потребитель подписывается на `events.user` и не знает, что внутри был outbox.

Этот SMT — финальная форма outbox-паттерна. Атомарность БД↔Kafka даёт транзакция на стороне сервиса (он пишет в `users` и `outbox` в одной TX), а доставку обеспечивает Debezium через WAL. Никакого poller'а в бизнес-сервисе.

## Что в нашем стенде

Postgres висит отдельным контейнером в той же docker-сети, что и kafka-connect — Connect ходит до него по hostname `lecture-07-04-postgres`. Параметры нужные для logical replication выставлены в `command:`:

```yaml
command: >
  postgres
    -c wal_level=logical
    -c max_replication_slots=4
    -c max_wal_senders=4
```

Без `wal_level=logical` pgoutput не запустится, выдаст ошибку при создании слота. С default'ным `replica` мы получаем только физическую репликацию.

Init-скрипт создаёт две таблицы и одну publication:

```sql
CREATE TABLE users (id BIGINT PRIMARY KEY, email TEXT, full_name TEXT, status TEXT, updated_at TIMESTAMPTZ);
ALTER TABLE users REPLICA IDENTITY FULL;

CREATE TABLE outbox (id UUID PRIMARY KEY, aggregate_type TEXT, aggregate_id TEXT, type TEXT, payload JSONB, created_at TIMESTAMPTZ);
ALTER TABLE outbox REPLICA IDENTITY FULL;

CREATE PUBLICATION dbz_publication FOR TABLE users, outbox;
```

Publication создаём сами с `publication.autocreate.mode=disabled` в коннекторе — так понятнее, какие таблицы на самом деле стримятся, и нет соблазна добавить таблицу через ALTER без понимания.

## Два коннектора

В этой лекции их два, и у каждого своя задача.

Первый — `lecture-07-04-debezium-pg-source`. Сырое CDC на таблицу `users`, без SMT. Каждое изменение летит в `cdc.public.users` в формате with before/after/op. Это тот случай, когда потребитель сам разбирает структуру — например, аналитический pipeline, которому нужны все детали.

Второй — `lecture-07-04-debezium-outbox`. CDC на таблицу `outbox` плюс EventRouter SMT. На выходе — топики `events.user`, `events.order` (зависит от того, что в `aggregate_type`). Это outbox-доставка для бизнес-событий.

Обрати внимание: оба коннектора подключены к одной БД, но через **разные replication slot'ы**. Каждый слот идёт по WAL'у независимо, со своей позицией. Это нормальная практика: слот — это «подписчик», и для разных назначений нужны разные подписчики.

## Демо-программа

`db-loader` — генератор изменений в Postgres'е. Он вставляет N юзеров, потом обновляет половину, потом удаляет четверть. Каждое изменение — в одной транзакции с записью в outbox.

Транзакция — это единственный способ гарантировать атомарность. Если в users записали, а в outbox упали — Debezium увидит INSERT в users без соответствующего outbox-события, и потребитель события не получит. Вот ядро вставки:

```go
return pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error {
    _, err := tx.Exec(ctx, `
        INSERT INTO users (id, email, full_name, status, updated_at)
        VALUES ($1, $2, $3, 'active', NOW())
    `, id, email, fullName)
    if err != nil {
        return err
    }
    payload := fmt.Sprintf(`{"id":%d,"email":%q,"full_name":%q}`, id, email, fullName)
    _, err = tx.Exec(ctx, `
        INSERT INTO outbox (id, aggregate_type, aggregate_id, type, payload)
        VALUES ($1, 'user', $2, 'user.created', $3::jsonb)
    `, uuid.New(), fmt.Sprintf("%d", id), payload)
    return err
})
```

`pgx.BeginFunc` — это helper, который сам сделает commit при `nil` и rollback при error из лямбды. Никакого ручного `tx.Commit()` или `defer tx.Rollback()` — закрытая абстракция.

Второй процесс — `cdc-consumer`. Подписывается одновременно на `cdc.public.users` и на все топики `events.*` — для этого франзу включаем regex-режим:

```go
cl, err := kafka.NewClient(
    kgo.ConsumerGroup(defaultGroup),
    kgo.ConsumeRegex(),
    kgo.ConsumeTopics(`^cdc\.public\.users$|^events\..+$`),
    kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
)
```

С `ConsumeRegex()` каждая строка в `ConsumeTopics` интерпретируется как регэксп. Удобно: топиков `events.<aggregate_type>` сколько угодно появится по мере того, как `db-loader` будет писать новые типы — подписка их подхватит автоматически.

Печатаем человекочитаемо: для CDC'шных — op + before/after, для outbox-роутер'ных — headers + payload как есть.

## Запуск

```sh
# из корня репо: убедиться, что Debezium plugin стоит
make connect-install-plugins

# из этой директории:
make up                       # Postgres
make db-init                  # users + outbox + publication
make connect-plugin-check     # убедиться, что Debezium виден через REST
make connector-create-all     # source + outbox connectors

# в одном терминале:
make run-cdc-consumer

# в другом:
make run-loader COUNT=10
```

В терминале с консьюмером сначала прилетит snapshot users (`op=r` на каждую строку), потом INSERT'ы (`op=c`), потом UPDATE'ы (`op=u`), потом DELETE'ы (`op=d` плюс tombstone). Параллельно — события в `events.user` через outbox-роутер: с aggregate_id в ключе, типом события в headers и чистым payload'ом без CDC-обёртки.

Посмотреть состояние слотов:

```sh
make slot-status
```

Увидишь два активных слота — `lecture_07_04_users_slot` и `lecture_07_04_outbox_slot`, у каждого свой `confirmed_flush_lsn`.

## Гарантии и подводные камни

Debezium даёт **at-least-once**. Никаких exactly-once тут нет — потребитель должен быть idempotent. Если коннектор перезапустился между fetch'ем из WAL'а и публикацией в Kafka — событие может прийти дважды. На стороне потребителя обычно дедупим по (topic, partition, offset) или по бизнес-ключу из payload'а (см. лекцию [Гарантии обработки](../../../../03-consumer/03-03-processing-guarantees/i18n/ru/README.md)).

Порядок гарантирован per-key, но не глобально. Все события одной строки (по PK) попадают в одну партицию и сохраняют порядок коммитов. События разных строк могут перемешаться, и это нормально — если порядок нужен глобальный, придётся ставить partitions=1 (с потерей масштабирования).

WAL накапливается, пока самый медленный slot не подтвердит позицию. Если коннектор сдох и его не починили — место кончится. Это не теория: реальные инциденты «у нас БД встала из-за брошенного debezium-slot'а» бывают регулярно. Мониторь `pg_replication_slots`.

Snapshot долгий. Если таблица на терабайт — initial snapshot тоже на терабайт, и пока он не закончится, инкрементальный стриминг не начнётся. Для огромных таблиц используют `incremental snapshot` (флаг `signal.data.collection`) — это отдельная Debezium-фича, в этой лекции мы её не трогаем.

Изменения схемы (DDL) Debezium на Postgres ловит автоматически — добавил колонку, она появится в новых событиях. Удалил колонку — её не будет в `after`. Но `before` со старыми событиями уже опубликован, так что потребитель должен быть толерантен к схемам — снова Protobuf / Avro со Schema Registry помогают, как обсуждали в модуле 05.

## Что дальше

Эта лекция — последняя в модуле 07. Дальше — модуль 08 про эксплуатацию (мониторинг, retention, sizing, troubleshooting), и в use case'ах модуля 09 этот же Debezium встретится дважды:

- [Postgres → ClickHouse с анонимизацией](../../../../09-use-cases/03-pg-to-clickhouse/i18n/ru/README.md) — Postgres → ClickHouse через Debezium + Go-анонимизатор + ClickHouse Sink
- [Postgres → Elasticsearch](../../../../09-use-cases/04-pg-to-elasticsearch/i18n/ru/README.md) — Postgres → Elasticsearch через Debezium + ES Sink (без Go вообще, declarative ETL)

Здесь же — концептуальная база. Если уловил, как WAL → slot → connector → топик складываются в стек — дальше use case'ы будут вариациями на эту тему.
