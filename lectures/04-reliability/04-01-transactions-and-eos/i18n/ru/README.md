# 04-01 — Transactions & EOS

В прошлом модуле мы научились коммитить offset'ы и обрабатывать сообщения at-least-once. Дубли. Идемпотентные обработчики. Dedup-таблица в Postgres. Это рабочая схема, но у неё есть прямой пробел: если на каждое входящее сообщение мы пишем одно или несколько новых сообщений в Kafka — гарантировать, что эта пачка либо вся появится, либо ни одной, обычными commit-ами нельзя.

Допустим, обработчик `orders` должен породить запись в `payments` и запись в `shipments`. Сначала пишем платёж — успех. Между двумя `Produce` процесс падает. На рестарте offset не закоммичен. Обработчик начинает заказ заново. Опять пишет платёж — теперь уже второй раз. Потом и отгрузку. На этот раз без падения. Даже если оба `Produce` были идемпотентны через producer-id, на рестарте мы получили нового продьюсера с новым id — идемпотентность не помогла. Состояние системы расщеплено.

Транзакции в Kafka про это. Они дают atomic multi-partition write — группу `Produce` запросов, которая либо вся видна потребителю, либо вся отбракована, плюс способ привязать к этой группе commit consumer-offset'а (это уже следующая лекция, [Consume-process-produce](../../../04-02-consume-process-produce/i18n/ru/README.md)). Здесь — про сами транзакции и фундамент: transactional.id, producer epoch, control records, изоляция чтения.

## TransactionalID и producer epoch

Идемпотентный продьюсер из лекции [Идемпотентный продьюсер](../../../../02-producer/02-03-idempotent-producer/i18n/ru/README.md) сам по себе — это `producer-id` плюс per-partition sequence numbers. Брокер видит «привет, я вот тот же продьюсер, не дублируй мою запись» в рамках одной сессии. Но `producer-id` живёт только пока живёт клиент. Перезапустился процесс — новый id, никакого «memory» о предыдущей сессии нет.

Транзакции добавляют поверх этого `transactional.id` — стабильный человеко-читаемый идентификатор, который ты задаёшь сам. Для одного сервиса с N инстансами обычно делают `<service>-<instance-id>` или просто `<service>-<consumer-group>-<partition>`. Главное — он стабильный между рестартами и уникальный per «логическая роль».

При первом `BeginTransaction` клиент идёт к транзакционному координатору (это специальный broker, по hash от `transactional.id`) и просит выдать ему `producer-id` и `epoch`. Координатор записывает «вот этот transactional.id принадлежит epoch=N», и пока есть одно соединение — всё чисто. Но что если процесс «ушёл в GC pause» на 30 секунд, мы решили, что он мёртв, и подняли новый? Новый процесс с тем же `transactional.id` дёргает координатор → координатор инкрементит epoch до N+1. Если старый, очнувшись, попробует написать что-то под старым epoch'ом — координатор вернёт `InvalidProducerEpoch` (или `ProducerFenced`). Любая запись и любой `EndTransaction` старого продьюсера — отказаны. Он zombie. Ничего сломанного не дольёт.

Это и есть zombie fencing. Без него exactly-once выглядел бы так: «у нас есть гарантии, пока никто не перезапускался». Бесполезно.

```
producer A      coord                  producer B
   |    pid=42, epoch=1                    |
   |---- BeginTxn -------->                |
   |    OK, epoch=1                        |
   |    [GC pause]                         |
   |                       <----- pid=42, epoch=2 (B стартует)
   |                                       |
   |    [очнулся, пишет]                   |
   |---- Produce(pid=42, e=1) ->           |
   |    InvalidProducerEpoch ❌            |
```

Демо в `cmd/zombie-fence/main.go`. Запускаешь два процесса с `-transactional-id=lecture-04-01-zombie` подряд, первый получает `FENCED` после старта второго и завершается.

Сам цикл первого процесса — голый Begin → Produce → EndTransaction:

```go
if err := cl.BeginTransaction(); err != nil {
    return fmt.Errorf("BeginTransaction: %w", err)
}

results := cl.ProduceSync(ctx, &kgo.Record{
    Topic: o.topic,
    Key:   []byte(o.role),
    Value: []byte(fmt.Sprintf(`{"role":%q,"attempt":%d}`, o.role, attempt)),
})
if produceErr := results.FirstErr(); produceErr != nil {
    _ = cl.EndTransaction(ctx, kgo.TryAbort)
    return produceErr
}

return cl.EndTransaction(ctx, kgo.TryCommit)
```

Что искать в выводе — строку `FENCED` у первого после того, как стартует второй. Real-world ошибка — это либо `ProducerFenced`, либо `InvalidProducerEpoch`; клиент франца возвращает её сразу из ProduceSync или из EndTransaction. Мы ловим обе:

```go
func isFenced(err error) bool {
    return errors.Is(err, kerr.ProducerFenced) ||
        errors.Is(err, kerr.InvalidProducerEpoch)
}
```

Дальше у zombie два честных пути — упасть с алёртом или тихо завершиться (в продакшене обычно первое, чтобы оркестратор не оставил процесс крутиться вхолостую).

## Atomic multi-partition write

Базовый сценарий — пишем в N топиков (или в N партиций одного топика, неважно), и нам нужна гарантия «всё или ничего». Без транзакций атомарности нет: каждое `Produce` отдельный network round-trip. Между ними может умереть процесс, может умереть брокер партиции, может закончиться timeout, может прорезаться сетевая партиция между клиентом и брокером.

Внутри транзакции это работает так. На первый `Produce` в новую партицию клиент шлёт координатору `AddPartitionsToTxn` — «эта партиция теперь часть моей транзакции с epoch=N». Координатор запоминает. Дальше идёт обычный `Produce` к лидеру партиции. Записи долетают на диск, как обычные. Внешне отличить транзакционную запись от нетранзакционной по самим данным нельзя.

Решающий шаг — `EndTransaction`. Координатор берёт список всех партиций, которые он насобирал через `AddPartitionsToTxn` для этого epoch, и шлёт каждой из них control record — особый служебный батч с маркером `COMMIT` или `ABORT`. Эти маркеры записываются в обычный лог партиции, у них есть свой offset. Их нельзя прочитать как пользовательские записи — fetch фильтрует control records наружу, но место в логе они занимают.

Демо — `cmd/transactional-producer/main.go`. На каждой попытке шлёт три связанные записи в три топика:

1. `tx-orders` — сам заказ (статус «created»);
2. `tx-payments` — платёжное поручение на тот же `order_id`;
3. `tx-shipments` — отгрузочное задание.

Дальше кидает монетку: commit или abort. В конце печатает счётчики.

Ядро попытки:

```go
if err := cl.BeginTransaction(); err != nil {
    return false, fmt.Errorf("BeginTransaction: %w", err)
}

orderID := strconv.Itoa(attempt)
produceErr := produceTriple(ctx, cl, orderID)

wantCommit := rand.Float64() < commitProb
if produceErr != nil {
    wantCommit = false // commit в этом состоянии всё равно бы не прошёл
}

commit := kgo.TryAbort
if wantCommit {
    commit = kgo.TryCommit
}
return wantCommit, cl.EndTransaction(ctx, commit)
```

И сам `produceTriple` — три записи разом через `ProduceSync`:

```go
results := cl.ProduceSync(ctx,
    &kgo.Record{Topic: topicOrders,    Key: []byte(orderID), Value: orderJSON},
    &kgo.Record{Topic: topicPayments,  Key: []byte(orderID), Value: paymentJSON},
    &kgo.Record{Topic: topicShipments, Key: []byte(orderID), Value: shipmentJSON},
)
return results.FirstErr()
```

Запускаешь:

```sh
make topic-create-all
make run-tx-producer ATTEMPTS=20 COMMIT_PROB=0.7
```

В выводе — список `[#XX] commit ✓` и `[#XX] abort ✗`. И финальная сводка с дельтой end-offset по каждому топику. Если было 14 commit'ов и 6 abort'ов — end-offset суммарно сдвинется примерно на (20 × 3 records) + (20 × 3 control markers) = 120 на все три топика. Реальные «полезные» записи увидит только read_committed клиент, и их будет ровно 14 × 3 = 42.

## TransactionTimeout

Координатор не верит продьюсеру вечно. Если продьюсер начал транзакцию и пропал, координатор сам аборнет её через таймаут, который клиент передал на старте. У franz-go v1.21.0 дефолт `TransactionTimeout` - 40 секунд (`pkg/kgo/config.go:603`); на стенде подняли до минуты через `kgo.TransactionTimeout(60*time.Second)` в коде продьюсера, чтобы три записи в три топика плюс маркеры укладывались с запасом. Таймаут на координаторе - страховка от лишней блокировки read_committed читателей: они ждут commit или abort marker, и без него застряли бы навсегда.

Если ты долго делаешь работу внутри транзакции (читаешь, обогащаешь, пишешь обратно) - увеличь `kgo.TransactionTimeout`. Только не путай с broker-side `transaction.max.timeout.ms` - тот ограничивает сверху то, что клиент имеет право попросить. По дефолту 15 минут (Kafka 4.2.0 на стенде показывает `transaction.max.timeout.ms=900000`).

## Изоляция: read_committed vs read_uncommitted

У консьюмера есть `isolation.level`. Дефолт в Kafka — `read_uncommitted`: читай всё, что есть в логе, как только оно туда долетело. Никакого ожидания marker'ов. Транзакционные записи отдаются сразу же, как только продьюсер их положил — даже если потом транзакция аборнется. Этот уровень — для случаев, когда транзакции тебя не волнуют.

`read_committed` — другое. Брокер при fetch'е отдаёт consumer'у только те транзакционные батчи, у которых уже есть commit marker. Aborted batch'и пропадают полностью (offset'ы как бы «съедаются» — с точки зрения клиента их в потоке нет). Записи pending-транзакции (commit ещё не пришёл) тоже не отдаются — fetch отдаёт всё до так называемого LSO (last stable offset), а это минимальный offset любой ещё открытой транзакции. То есть один зависший продьюсер может «застопорить» всю партицию для read_committed читателей до своего timeout'а. Это плата за гарантии.

Демо — `cmd/read-committed/main.go`, переключается флагом `-isolation`:

```go
opts := []kgo.Opt{
    kgo.ConsumerGroup(o.group),
    kgo.ConsumeTopics(o.topics...),
    kgo.FetchIsolationLevel(level), // ReadCommitted() или ReadUncommitted()
    kgo.ClientID("lecture-04-01-rc"),
    kgo.DisableAutoCommit(),
}
```

Сценарий «потрогать руками»:

```sh
make topic-create-all
# терминал 1 — produce 20 транзакций, 70% commit
make run-tx-producer ATTEMPTS=20 COMMIT_PROB=0.7

# терминал 2 — то, что увидит read_committed
make run-rc-consumer COUNT=100 IDLE=3s

# терминал 3 — то, что увидит read_uncommitted
make run-ru-consumer COUNT=100 IDLE=3s
```

В терминале 2 насчитаешь ~14 × 3 = 42 записи (по committed транзакциям). В терминале 3 — все 60, потому что для uncommitted нет разницы между «commit прошёл» и «потом аборнулось». На той же кластерной картинке — два разных видения мира. Это и есть смысл isolation level'а.

## Что транзакции НЕ дают

Когда говорят «exactly-once в Kafka», полезно понимать, до какой границы это работает.

1. **End-to-end EOS — только Kafka↔Kafka**. Если consumer читает топик, что-то делает, пишет в другой топик — да, транзакция (плюс `SendOffsetsToTransaction` из [Consume-process-produce](../../../04-02-consume-process-produce/i18n/ru/README.md)) даёт атомарный акт «прочитать → записать → закоммитить offset». Но если в середине ты дёргаешь HTTP API или пишешь в Postgres без outbox-паттерна — транзакция Kafka про эту внешнюю запись ничего не знает. Внешние стороны нужны отдельные механизмы (outbox в [Outbox-паттерн](../../../04-03-outbox-pattern/i18n/ru/README.md), idempotent receivers в [Доставка во внешние системы](../../../04-05-external-delivery/i18n/ru/README.md)).

2. **Транзакция != «не упадёт»**. Транзакция падает штатно: либо коммитится, либо аборнется. Если процесс умер посреди транзакции до `EndTransaction`, координатор аборнет её сам через timeout. Никакая магия не «дозальёт» половину записей. Чтобы сценарий был корректным, твой код должен уметь повторить попытку с теми же входными данными — то есть быть идемпотентным на уровне бизнес-логики.

3. **Throughput цена ощутимая**. Каждый `EndTransaction` — round-trip к координатору, далее write маркеров на каждую партицию, fsync'и под маркеры, плюс синхронизация с участниками. По нагрузочным тестам Confluent — обычно 3–10% overhead против чистого acks=all. Не катастрофа, но и не бесплатно.

4. **На стороне consumer'а pending-транзакции замораживают read_committed**. Если у тебя один продьюсер уходит в долгую транзакцию (или просто завис), все read_committed читатели партиций, в которые он успел добавиться, увидят паузу. На метриках это смотрится как lag, который не падает. Лечится либо коротким `transaction.timeout.ms`, либо мониторингом `LastStableOffset`.

5. **`transactional.id` живёт дольше процесса**. Если ты выбрал `transactional.id = "service-instance-7"`, и твой инстанс 7 умер навсегда, его id остаётся в координаторе с открытой транзакцией до timeout'а. Поэтому id обычно деривят от логической роли (частый трюк — партиция входящего топика). Привязка к pod-id из k8s даёт зомби-id на каждый рестарт пода — так лучше не делать.

## Подводка к [Consume-process-produce](../../../04-02-consume-process-produce/i18n/ru/README.md)

Сейчас мы умеем атомарно писать в N партиций. Но классический паттерн шире: «прочитал → обработал → записал → закоммитил offset», и нам нужно вписать commit consumer-offset'а тоже внутрь транзакции. Иначе в окно «уже записал, ещё не закоммитил» влезает рестарт, и мы получаем дубль. Эта связка — `SendOffsetsToTransaction` плюс read_committed на consumer'е downstream — называется consume-process-produce и разбирается в следующей лекции. Здесь были кирпичи, дальше — стенка.

## Файлы лекции

- `cmd/transactional-producer/main.go` — Begin → 3× Produce → End со случайным commit/abort и сводкой по end-offset.
- `cmd/zombie-fence/main.go` — два процесса с одним и тем же `transactional.id`; первый ловит fence после старта второго.
- `cmd/read-committed/main.go` — consumer на три транзакционных топика, переключается между read_committed и read_uncommitted флагом.
- `Makefile` — `topic-create-all`, `run-tx-producer`, `run-zombie-1`/`run-zombie-2`, `run-rc-consumer`/`run-ru-consumer`, `clean`.

## Команды для прогона

```sh
# Подготовка
make topic-create-all

# 1. Atomic multi-partition write
make run-tx-producer ATTEMPTS=20 COMMIT_PROB=0.7
# в отдельном терминале — что увидит read_committed
make run-rc-consumer COUNT=100 IDLE=3s
# и что увидит read_uncommitted
make run-ru-consumer COUNT=100 IDLE=3s

# 2. Zombie fencing — два терминала с одним txn-id
make run-zombie-1     # терминал A
# через 3-5 секунд:
make run-zombie-2     # терминал B
# терминал A должен поймать FENCED и завершиться

# Уборка
make clean
```
