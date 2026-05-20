# 08-04 — Troubleshooting Runbook

Восемь модулей курса прошли — пора собрать всё в одну заметку, к которой возвращаешься среди ночи, когда в Slack капают алёрты. Эта лекция — runbook. Список типовых инцидентов, по которому идёшь сверху вниз: симптом → диагностика → действие. Не философия. Прикладной чек-лист.

Идея простая. Когда что-то горит, читать длинные доки некогда. Хочется быструю табличку «вижу X — смотри Y — крути Z». Дальше — двенадцать таких блоков, и три маленьких программы, которые показывают пару проблем глазами клиента.

## Как читать этот runbook

Каждая запись — три абзаца. Симптом (что именно ты видишь — алёрт, метрика, поведение пользователей). Диагностика (откуда брать факты: kafka-ui, kadm, JMX, логи брокера). Действие (что крутить и в каком порядке). Если действие требует osmotr на своём кластере — внутри блока есть ссылка на программу из этой лекции или на лекцию из курса, где это разбиралось.

Ничего нового тут не появляется. Это сборник того, что уже было в модулях 02–08. Просто в формате «увидел — сделал».

## 1. Under-replicated partitions

Симптом. Метрика `kafka.server:type=ReplicaManager,name=UnderReplicatedPartitions` поднялась с нуля. На кластерном дашборде красный счётчик «UR=N». Алёрт «replica out of sync».

Диагностика. Самое первое — проверить, сколько брокеров в metadata. Если три из трёх — значит, один брокер живой по сети, но fetcher не успевает. Если два из трёх — ясно, кого нет (`docker ps`, JMX consumer, наш `under-replicated-watch`). Дальше — `kafka-topics.sh --describe --under-replicated-partitions` или `ListTopics` через kadm, чтобы понять список затронутых топиков. Если UR живёт минутами и не уходит — лезь в логи брокера-фоловера: чаще всего там `OutOfMemoryError`, диск 100%, или GC-паузы.

Действие. Брокер упал — поднимаешь его (`docker start kafka-2` на нашем стенде). Брокер живой, но не догоняет — смотришь `replica.fetch.max.bytes`, диск, сеть. Если UR < ISR_min при `acks=all` — продьюсеры начнут получать `NOT_ENOUGH_REPLICAS`, это уже инцидент с записью, см. блок 4.

## 2. High consumer lag

Симптом. Lag по группе растёт линейно или скачком. Бизнес жалуется «новые события не появляются на UI» или «стейт устарел на 10 минут». Алёрт «consumer lag > N».

Диагностика. Сначала — стабильный рост или плато? Растёт линейно — продьюсер пишет быстрее, чем консьюмер успевает. Плато на одном уровне — консьюмер мёртв, новых записей не делает. Смотришь `kadm.Lag(group)` или `kafka-consumer-groups.sh --describe`. Получаешь lag per-partition. Если перекос — одна партиция растёт, остальные нулевые — это hot partition, см. блок 8. Если все равномерно — нагрузка превысила throughput воркеров. Если один консьюмер ушёл и партиции висят без owner'а — ребаланс не доделан, смотри блок 3.

Действие. Воркеров мало — добавляешь копии (или партиций больше — но это уже про планирование, не runbook). Один поток обработки тяжёлый — смотришь, можно ли распараллелить per-key через worker pool из лекции [Конкурентность и lag](../../../../03-consumer/03-05-concurrency-and-lag/i18n/ru/README.md). Внешняя система (БД, HTTP) тормозит — проверяешь её, поднимаешь backpressure через `cl.PauseFetchPartitions` (см. [Доставка во внешние системы](../../../../04-reliability/04-05-external-delivery/i18n/ru/README.md)), а не молча копишь in-flight.

## 3. Frequent rebalances

Симптом. Логи консьюмеров пестрят `Revoking ... Assigning`. Метрика `kafka.consumer.coordinator.rebalance-rate` ненулевая. Lag прыгает скачками — после каждого ребаланса выбрасывается часть кешей и repartition'ится state.

Диагностика. Что вызывает ребаланс? Три обычных причины. (1) Воркер не успевает делать `poll` за `max.poll.interval.ms` — координатор считает его мёртвым. (2) `session.timeout.ms` слишком короткий, GC-пауза дольше — то же самое. (3) Деплой постоянно поднимает/опускает копии, ребалансы — следствие штатного scaling'а.

Действие. (1) — поднимаешь `max.poll.interval.ms` или ускоряешь обработку (то же `cl.PauseFetchPartitions`, разделение тяжёлой работы на воркеров). (2) — поднимаешь `session.timeout.ms`, разбираешься с GC. (3) — переходишь на `cooperative-sticky` (`kgo.Balancers(kgo.CooperativeStickyBalancer())`), чтобы при ребалансе двигались только перераспределяемые партиции, а не все. Это всё было в [Группы и ребалансы](../../../../03-consumer/03-01-groups-and-rebalance/i18n/ru/README.md), тут только напоминалка.

## 4. Producer error rate ↑

Симптом. На дашборде producer'а растёт count ошибок. В логах `NotEnoughReplicas`, `RequestTimedOut`, `RecordTooLargeException`, `UnknownTopicOrPartition`, `InvalidProducerEpoch`. Бизнес — «у меня заказы пропадают».

Диагностика. Выбираешь по классу ошибки.
- `NotEnoughReplicas` — ISR упал ниже `min.insync.replicas`. Идёшь в блок 1.
- `RequestTimedOut` — брокер не успел ответить за `request.timeout.ms`. Брокер перегружен или сеть деградировала.
- `RecordTooLargeException` — клиент шлёт больше `max.message.bytes`. Не retriable, retry не поможет. Смотри payload, рассмотри вынос blob во внешнее хранилище.
- `UnknownTopicOrPartition` — топик удалили или auto-create не настроен и продьюсер пишет в несуществующий. Создавай идемпотентно через kadm.
- `InvalidProducerEpoch` — кто-то ещё стартанул с тем же `transactional.id`. Это zombie fencing, см. [Транзакции и EOS](../../../../04-reliability/04-01-transactions-and-eos/i18n/ru/README.md).

Действие. Сначала классифицируй retriable/non-retriable. Retriable — franz-go сам ретраит до `RetryTimeout`, тебе обычно достаточно дождаться. Non-retriable — фиксить код или конфиг, ретраи бесполезны. И главное правило: НЕ глушить ошибки в callback'е. Ошибка продьюсера = факт записи неизвестен. Молча пропустить — потерять данные.

## 5. Disk growing

Симптом. На брокере уровень `du` идёт вверх. Алёрт «kafka data dir > 80%».

Диагностика. Что копится? Три варианта.
- Топик с `retention.ms=-1` (compact или просто бесконечность). Размер сегментов — нормальная картина для CDC-state, но за ней тоже надо следить (см. профили в [Sizing и tuning](../../../08-03-sizing-and-tuning/i18n/ru/README.md)).
- Retention настроен, но не срабатывает — `segment.ms` слишком большой, активный сегмент не закрывается, retention его не трогает (см. [Retention и compaction](../../../08-02-retention-and-compaction/i18n/ru/README.md)). Вижу 70% диска под одним сегментом — обычно это.
- Tombstone'ы не дочищаются — `min.cleanable.dirty.ratio` высокий, log cleaner мало работает. Tail остаётся.

`kafka-log-dirs.sh --describe` в kafka-1 даст разбивку по топикам. Дальше — на проблемный топик `kafka-configs.sh --entity-type topics --describe` и сверка с тем, что ожидаешь.

Действие. Топик в норме, но шлют слишком много — сокращаешь retention или поднимаешь диск. Sегмент огромный — режешь `segment.ms`/`segment.bytes`, ждёшь ротацию. Compaction отстаёт — снижаешь `min.cleanable.dirty.ratio` до 0.1. Если совсем горит — удалять старые партиции вручную можно только остановив брокер, и это последняя мера.

## 6. Controller bouncing

Симптом. В KRaft кластере controller-узел постоянно меняется. Метрика `kafka.controller:type=KafkaController,name=ActiveControllerCount` дёргается. Создание/удаление топиков подвисает.

Диагностика. Логи controller-нод — там обычно видно: или потеря кворума (`__cluster_metadata` не собирает majority), или GC на одном из controller'ов вышибает его из quorum'а. На нашем sandbox-стенде combined-mode — broker и controller на одной JVM, поэтому если broker нагружен — страдает и controller. На production стоит разделять роли.

Действие. Если кворум не сходится — проверь, что все controller-ноды живые и видят друг друга по сети (порт 9093 у нас на стенде). Если одна нода тормозит из-за GC — heap, JVM-флаги, в крайнем случае рестарт. Без active controller'а DDL-операции (CreateTopic, DescribeConfigs альтер) висят.

## 7. Broker won't start

Симптом. После рестарта брокер не поднимается. В логах ловишь `RuntimeException`, `Failed to recover`, `Inconsistent log directory`.

Диагностика. Самое частое — повреждённый сегмент после грубого `kill -9` или OOM. Лог брокера обычно прямо говорит, какой файл не открылся. Второе — конфликт node.id: после `docker compose down -v` volumes пересоздались, а meta.properties в дата-каталоге осталось от старой инсталляции (если volumes не выбили). Третье — порт занят другим процессом.

Действие. Повреждённый сегмент — `LogManager` сам пытается восстановиться при старте; если не вышло — перенести файл в сторону, дать брокеру стартовать, реплики перельются с других. Конфликт meta.properties — понятно. Порт — `lsof -i :9092` и убрать конфликт. На нашем стенде просто `docker compose down && up` решает 90% проблем (включая ситуации, когда логи прокомпилировались некорректно).

## 8. Hot partition

Симптом. Lag растёт только на одной партиции, остальные нулевые. Throughput на топик уперся в потолок одного воркера, добавление воркеров не помогает (новые сидят без работы — все партиции уже распределены).

Диагностика. На какой партиции концентрация? `ListEndOffsets` до и после короткого окна нагрузки даёт `delta` per-partition. Если 80% записей в одну — это hot key. Если перекос помельче (20–30% разница) — нормальный шум murmur2 на маленьких объёмах, не паникуй.

Эту картину как раз делает `cmd/hot-partition-demo` из этой лекции. Один ключ `hot` пишет 1000/сек, десять обычных user-ключей — по 10/сек. Через 10 секунд видно, как партиция, куда `murmur2('hot')` попал, получает 85+% всего объёма.

Действие. Composite key — `cmd/composite-key-fix`. Берём бывший hot-ключ и докидываем суффикс `:bucket-N`, где `N = hash(payload_id) % buckets`. Логически это всё ещё «горячий поток одного типа», но физически он размазан по `buckets` партициям. После замены — перекос исчезает. Цена — теряем `one-key-one-partition` гарантию для hot-ключа, и если порядок per-key важен, надо внутри bucket'а сохранять группировку по дочернему ключу. Если порядок неважен — composite key решает hot partition «бесплатно».

## 9. Partition reassignment stuck

Симптом. Запустил `kafka-reassign-partitions.sh --execute`, проверка через `--verify` висит на `... still in progress`. Часами. UR partitions не уходят.

Диагностика. Кто пересылается медленно? Сравнение fetch-метрик per-partition или `kafka-replica-verification.sh` (помечен deprecated в 4.x, но команда ещё работает). Самая частая причина — при `--execute` выставили throttle в 10 MB/сек, а перетащить нужно терабайты. Throttle хранится в broker-уровневых `leader.replication.throttled.rate` / `follower.replication.throttled.rate` (bytes/sec) и в topic-уровневых списках `leader.replication.throttled.replicas` / `follower.replication.throttled.replicas`. Не путать с `replica.alter.log.dirs.io.max.bytes.per.second` — тот про JBOD-переезд между дисками одного брокера, к cross-broker reassignment он отношения не имеет.

Действие. Поднять throttle через `kafka-reassign-partitions.sh --additional --throttle <bytes-per-sec>` (это перепишет `leader.replication.throttled.rate` / `follower.replication.throttled.rate` атомарно). Проверить, что нет фоновых задач, отъедающих диск (compaction, large segment rotation). Если переезд идёт нормально, но топик гигантский — это просто долго, метрика прогресса есть в JSON-плане.

## 10. Topic deletion stuck

Симптом. `kafka-topics.sh --delete --topic foo` отработал без ошибки, но топик продолжает появляться в `--list`, файлы сегментов в data-каталоге брокера (`/var/lib/kafka/data/foo-*`) остаются. (Старая пометка `_marked_for_deletion` была ZK-эпохи — в KRaft её больше нет, топик либо есть, либо удалён.)

Диагностика. У брокера выставлен `delete.topic.enable=false` (по умолчанию `true`, но мало ли). Или — active controller недоступен (см. блок 6), и DDL висит. Или — один из брокеров с репликой топика лежит, и пока он не подтвердит удаление сегментов, операция не завершится.

Действие. Проверь `delete.topic.enable=true` на всех брокерах. Подними упавшие узлы, убедись, что есть active controller (`kafka-metadata-quorum.sh --bootstrap-server ... describe --status`). Если совсем заклинило (редкий corner case) — рестарт active controller-узла.

## 11. Schema Registry rejects

Симптом. Producer пишет, в SR запрос на регистрацию новой версии схемы. SR отвечает 409 Conflict с body `Schema being registered is incompatible with an earlier schema for subject "X"`.

Диагностика. Подключаешь `buf breaking --against` локально (если Protobuf). Видишь, что именно сломал: removed field, changed type, не reserved'нул tag. Если совместимость на subject'е стоит `BACKWARD` — нельзя удалять обязательные поля. Если `FORWARD` — нельзя добавлять обязательные. Если `FULL` — нельзя ни то, ни другое. См. [Эволюция схем](../../../../05-contracts/05-04-schema-evolution/i18n/ru/README.md).

Действие. Откатить изменение схемы. Поправить `proto`-файл — добавить новое поле как `optional` (или с дефолтом), не трогать существующие tag'и, не менять типы. Перевыпустить. Если прямо сейчас надо разлить старое — менять компилятивность subject'а только осознанно (понимая, что чужие consumer'ы могут начать ронять `Unmarshal`).

## 12. Connector failed

Симптом. На kafka-connect через REST `/connectors/<name>/status` возвращается `state: FAILED`. В trace — exception. Источник или sink не пишет.

Диагностика. Самые частые причины. (1) credentials — Postgres пароль изменили, Debezium слот не открывается. (2) plugin not found — kafka-connect стартовал без нужного класса в `plugin.path`, см. блок 34.5 про установку. (3) source не получает изменения — Postgres слот «упал» или WAL переполняется. (4) sink не пишет в downstream — ClickHouse/ES недоступны, в трейсе HTTP 5xx.

`docker logs kafka-connect | tail -200` обычно даёт всю историю. Дальше — точечно по причине.

Действие. (1) и (3) — фикс на стороне Postgres. (2) — переустановить plugin (см. 34.5). (4) — поправить downstream и `restart` коннектора через REST. Если коннектор «застрял в FAILED» — `pause` → `resume`, иногда `delete` + `create` (если данные не критичны).

## Что показывают наши программы

В этой лекции три коротких бинаря.

### hot-partition-demo

Создаёт топик из четырёх партиций, RF=3. Параллельно крутит два генератора — один с ключом `hot` на 1000 сообщений/сек, второй с десятью user-ключами по 10/сек. На выходе — таблица распределения по партициям с долями и баром. Видно, что 80+% всего объёма уехало в одну партицию, и остальные простаивают.

Сам цикл записи — это `cl.Produce` с callback'ом. Темп задаётся `time.Ticker`'ом:

```go
tick := time.Second / time.Duration(rate)
t := time.NewTicker(tick)
for {
    select {
    case <-ctx.Done():
        return sent
    case <-t.C:
        for _, k := range keys {
            rec := &kgo.Record{
                Topic: topic,
                Key:   []byte(k),
                Value: []byte("event"),
            }
            cl.Produce(ctx, rec, func(_ *kgo.Record, err error) { ... })
            sent++
        }
    }
}
```

Замеры — через `kadm.ListEndOffsets` до и после окна. Разница и есть delta per-partition:

```go
ends, err := admin.ListEndOffsets(rpcCtx, topic)
ends.Each(func(o kadm.ListedOffset) {
    if o.Err != nil { return }
    out[o.Partition] = o.Offset
})
```

Это надёжнее, чем считать в памяти на стороне продьюсера: нам важно «что реально лежит в партициях», а не «что отправили». Если бы мы считали только `sent`, перекос был бы не виден из-за того, что producer'ные callback'и могли ещё не отработать.

### composite-key-fix

Тот же сценарий, но вместо одного `hot` пишет четыре composite-ключа: `hot:bucket-0`, `hot:bucket-1`, `hot:bucket-2`, `hot:bucket-3`. `murmur2` раскидывает их по партициям, и поток размазывается. Суммарный темп `hot-rate` делится на `buckets`, чтобы сравнение с предыдущим бинарём было честным:

```go
hotPerKey := o.hotRate / o.buckets
if hotPerKey < 1 { hotPerKey = 1 }
hotKeys := make([]string, o.buckets)
for i := 0; i < o.buckets; i++ {
    hotKeys[i] = fmt.Sprintf("hot:bucket-%d", i)
}
```

В реальном коде bucket-индекс считается как `hash(payload_id) % buckets` — чтобы один и тот же логический объект всегда попадал в один и тот же bucket. Тогда per-object порядок сохраняется, а перекос всё равно ушёл. В нашей демке мы просто крутим все bucket'ы по очереди — для иллюстрации этого хватает.

### under-replicated-watch

Дашборд кластера в одном цикле. Каждые `interval` дёргает `ListBrokers` и `ListTopics`, считает under-replicated partitions, печатает summary и таблицу проблемных партиций.

Ядро — простая проверка через `len`:

```go
for _, t := range td {
    if t.Err != nil { continue }
    for _, p := range t.Partitions {
        if len(p.ISR) < len(p.Replicas) {
            urParts++
        }
    }
}
```

Это та же формула, что у JMX-метрики `UnderReplicatedPartitions` на брокере. Просто наблюдаем глазами клиента, без подключения к JMX. Работает, пока мы можем достучаться хотя бы до одного брокера — `ListTopics` это metadata-запрос, его обслужит любой живой брокер, franz-go сам выберет доступного.

Сценарий лекции — запустить `make run-watch` в одном терминале, в другом — `make kill-broker`. На следующем тике видно, что брокер пропал из `BROKERS`, и часть партиций (где он был в `Replicas`) ушла в UR. После `make restore-broker` — обратно зелёное.

## Запуск

```sh
make help                 # шпаргалка
make run-hot              # hot-partition-demo, видим перекос на одну партицию
make run-fixed            # composite-key-fix, видим выравнивание
make run-watch            # дашборд кластера, обновляется каждые 3s
make run-watch-once       # один тик и выход (для тестов)
make kill-broker          # docker stop kafka-2 — спровоцировать UR
make restore-broker       # docker start kafka-2 — вернуть в строй
```

Параметры:

```sh
HOT_RATE=2000 NORMAL_RATE=20 DURATION=20s make run-hot
BUCKETS=8 make run-fixed                                 # больше bucket'ов — лучше выравнивание
WATCH_INTERVAL=1s make run-watch                          # быстрее тики (нагружает Connect-API на стенде)
BROKER=kafka-3 make kill-broker                           # завалить другую ноду
```

Заодно посмотри на kafka-ui (http://localhost:8080) во время `kill-broker` — на главном экране тоже виден UR-счётчик, и у каждого топика на странице partitions цветится «out of sync». Часть инцидентов из runbook'а удобнее ловить там, чем командной строкой.

## Шпаргалка

| Симптом | Первая команда | Куда идти, если совсем горит |
|---------|---------------|------------------------------|
| UR partitions ↑ | `make run-watch-once` | блок 1 |
| Lag растёт | `kafka-consumer-groups.sh --describe` | блок 2, 3, 8 |
| Frequent rebalance | `grep -i revoking` в логах | блок 3 |
| Producer errors ↑ | classify по message ошибки | блок 4 |
| Disk ↑ | `kafka-log-dirs.sh --describe` | блок 5 |
| Controller bouncing | `ActiveControllerCount` в JMX | блок 6 |
| Broker won't start | `docker logs kafka-N` | блок 7 |
| Hot partition | `make run-hot` (видишь баланс?) | блок 8 |
| Reassignment stuck | `kafka-reassign-partitions.sh --verify` | блок 9 |
| Delete stuck | `--list` смотри `_marked_` | блок 10 |
| SR rejects | `buf breaking --against` локально | блок 11 |
| Connector failed | `docker logs kafka-connect` | блок 12 |

Этот runbook покрывает базовый набор «что встретишь в первый месяц жизни кластера». Полный перечень того, что может сломаться, гораздо длиннее. Чем дольше живёшь с Kafka — тем длиннее становится твой собственный runbook. Этот — стартовая точка.
