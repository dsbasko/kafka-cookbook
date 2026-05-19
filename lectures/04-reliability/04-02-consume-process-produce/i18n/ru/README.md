# 04-02 — Consume-Process-Produce

В прошлой лекции была транзакционная запись в несколько топиков как одна атомарная операция. Это половина решения. Вторая половина — что делать, когда мы ещё и читаем из Kafka. Прочитал → обработал → записал → закоммитил offset. Этот цикл живёт в каждом втором сервисе, и тут всё хитрее, чем кажется.

Допустим, есть consumer на топике `orders`. Он читает заказ, обогащает его (lookup в БД, подмешивает customer profile, расчёт скидки, проверка fraud-score). Дальше пишет результат в `orders-enriched`. После этого хочет сказать брокеру «я обработал offset N, можно дальше». Между этими шагами — три точки потенциального крушения, и каждая порождает свой тип расхождения.

Вариант первый: упали ПОСЛЕ produce, ДО commit offset. Перезапустились — offset не сдвинулся, читаем тот же заказ, обогащаем заново, пишем во второй раз. На выходе дубль. Идемпотентный продьюсер из [Идемпотентный продьюсер](../../../../02-producer/02-03-idempotent-producer/i18n/ru/README.md) тут не спасает: producer-id у нового процесса другой, sequence numbers начинаются с нуля.

Вариант второй: упали ПОСЛЕ commit offset, ДО produce. Перезапустились — offset уже за этим заказом, к нему не вернёмся. На выходе пропуск.

Вариант третий: успели и produce, и commit, но в разном порядке относительно крушения — и тогда мы получим дубль или пропуск в зависимости от того, что было раньше. Без атомарной связки между «отдал записи в output» и «продвинул offset во входе» — ровно один раз не получается. Это at-least-once с дубликатами или at-most-once с потерями. Третьего не дано. До тех пор, пока не вытащим оба шага в одну транзакцию.

В транзакции в [Транзакции и EOS](../../../04-01-transactions-and-eos/i18n/ru/README.md) было `BeginTransaction → Produce → EndTransaction(Commit)`. Тут добавляется ещё один участник — group offset commit. Кафка умеет писать его внутрь той же транзакции через специальный запрос `TxnOffsetCommit`. Если транзакция коммитится, оба эффекта (records в output и offset в `__consumer_offsets`) становятся видимыми атомарно. Если аборнется — обоих нет. На уровне output read_committed-консьюмер их не увидит, на уровне offset группа осталась там, где была. Перезапустились — те же входные записи прочитаются заново. Заново обогатятся. Заново попадут в output. Снаружи всё выглядит так, как будто обработка случилась ровно один раз.

## GroupTransactSession

На уровне wire-протокола Kafka это `TxnOffsetCommit` request на координатор группы внутри открытой транзакции, плюс корректная обработка ребалансов. В Java-клиенте под это есть `producer.sendOffsetsToTransaction(offsets, groupMetadata)`. franz-go свой эквивалент (`commitTransactionOffsets` в `pkg/kgo/txn.go:939`) намеренно не экспортирует - в комментарии прямо написано «gigantic footgun if not done properly». Единственный публичный путь к EOS-консьюмеру в franz-go v1.21.0 - обёртка `kgo.GroupTransactSession`, которая делает три полезные вещи:

1. Берёт текущие consumer-offset'ы из своего группового состояния и кладёт их в транзакцию через `TxnOffsetCommitRequest`.
2. Заворачивает обработку ребалансов в свою логику. Если во время транзакции пришёл revoke — `End(TryCommit)` сам вернёт `committed=false` и аборнет транзакцию, чтобы не закоммитить offset на партиции, которой мы уже не владеем. Это критично: без этой защиты на пути двух consumer'ов, играющих один и тот же partition, появляются дубли.
3. Делает Flush до End на commit-пути, чтобы все Produce'ы добежали до брокера.

Сам цикл выглядит почти как обычный consume + produce, только с Begin/End вокруг батча:

```go
for {
    fetches := sess.PollFetches(pollCtx)
    if fetches.Empty() { continue }

    if err := sess.Begin(); err != nil {
        return fmt.Errorf("Begin: %w", err)
    }

    fetches.EachRecord(func(r *kgo.Record) {
        enriched := enrich(r)
        sess.Produce(ctx, &kgo.Record{
            Topic: o.output,
            Key:   r.Key,
            Value: enriched,
            Headers: []kgo.RecordHeader{
                {Key: "source.topic", Value: []byte(r.Topic)},
                {Key: "source.partition", Value: []byte(fmt.Sprintf("%d", r.Partition))},
                {Key: "source.offset", Value: []byte(fmt.Sprintf("%d", r.Offset))},
            },
        }, /* promise */)
    })

    committed, err := sess.End(ctx, kgo.TryCommit)
}
```

`End(TryCommit)` атомарно делает три шага:

1. flush producer-буфера, чтобы все Produce'ы добежали до брокера
2. `TxnOffsetCommit` для текущих позиций группы — записывает их в координатора как часть нашей транзакции
3. `EndTxnRequest(commit)` на координатор — после этого запроса изменения становятся видимыми для read_committed-консьюмеров

Если хотя бы один шаг не получился — возвращается `committed=false`, и снаружи это значит «начни с того же offset'а».

## Конфигурация на стороне consumer'а

Для EOS нужны два важных флага.

`kgo.FetchIsolationLevel(kgo.ReadCommitted())` — читать только из закоммиченных транзакций. Это касается того, какие записи отдаст брокер. На запись наш pipeline и так EOS-овский, но если ВХОДНОЙ топик пишется другим транзакционным продьюсером — без этого флага мы прочитаем записи из ещё не закоммиченных транзакций, попробуем их обработать, и если та upstream-транзакция аборнется — у нас в output будут записи, которых на входе никогда не было. Классический антипаттерн.

`RequireStableFetchOffsets` — раньше был отдельный флаг, в franz-go 1.21 он включён по умолчанию навсегда (см. config.go: «Deprecated: now permanently enabled»). Он отвечает за то, чтобы fetch не возвращал записи, для которых coordinator группы пока «не уверен» — то есть offset commit ещё в полёте у параллельной транзакции. Без этого механизма две группы, читающие один топик, могли бы временно расходиться по позиции, и одна из них прочитывала бы ту же запись дважды.

И ещё один момент — `TransactionalID`. Стабильный per-роль идентификатор, который переживает рестарты. Если у тебя два инстанса одного и того же consumer'а, у каждого должен быть свой `transactional.id`, и обычно его привязывают к `<service>-<member-id>` или к partition assignment. Если оба возьмут одинаковый id — второй выгонит первого по zombie fencing (см. [Транзакции и EOS](../../../04-01-transactions-and-eos/i18n/ru/README.md)), и одна из ролей перестанет работать.

## Что показывает наш код

В директории два бинарника — `cmd/cpp-pipeline` и `cmd/downstream-rc`. Pipeline читает `cpp-orders`, обогащает каждую запись (mock — добавляем `vip` по префиксу ключа) и пишет результат в `cpp-orders-enriched`. Downstream — простой read_committed-консьюмер на output. Считает уникальные ключи, проверяет дубли.

Главное место в pipeline — настройка `GroupTransactSession`:

```go
opts := []kgo.Opt{
    kgo.SeedBrokers(seeds...),
    kgo.TransactionalID(o.txnID),
    kgo.TransactionTimeout(60 * time.Second),
    kgo.ConsumerGroup(o.group),
    kgo.ConsumeTopics(o.input),
    kgo.FetchIsolationLevel(kgo.ReadCommitted()),
    kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
}
sess, err := kgo.NewGroupTransactSession(opts...)
```

`NewGroupTransactSession` — это `NewClient` плюс правильно подвешенные хуки `OnPartitionsRevoked` / `OnPartitionsLost`, чтобы `End` мог понять, что «нас выкинули из группы посреди транзакции» и вернуть `committed=false`.

Симуляция краха прячется между `Flush` и `End`. Идея — записи уже в логе output, но commit marker ещё не написан. Координатор по таймауту аборнет «нашу» транзакцию, и записи окажутся «осиротевшими» в логе:

```go
if err := sess.Client().Flush(ctx); err != nil {
    return fmt.Errorf("flush: %w", err)
}

if o.crashProb > 0 && rand.Float64() < o.crashProb && batchOut > 0 {
    fmt.Fprintf(os.Stderr, "💥 crash перед End: %d записей уже в логе output, ...\n", batchOut)
    os.Exit(2)
}

committed, err := sess.End(ctx, kgo.TryCommit)
```

Без явного `Flush` Produce был бы асинхронным батчингом — записи не успели бы дойти до брокера к моменту `os.Exit`, и read_uncommitted не увидел бы «следов» аборта. Для нашего демо нужен видимый эффект, поэтому мы форсим запись на брокер.

## Демо

Подними стенд, создай топики, налей 30 заказов на вход, запусти pipeline с гарантированным крашем, чтобы первая транзакция точно аборнулась.

```sh
make topic-create-all
make seed SEED_COUNT=30
make run-pipeline-crash CRASH_PROB=1.0   # упадёт перед End на первой транзакции
```

Что увидим: pipeline прочитал N записей (где N — записи первой партиции, которая попалась первой), отдал их в output через `Flush`, написал «💥 crash» и завершился `os.Exit(2)`. На output сейчас есть данные — без commit marker'а.

Сразу же (пока transaction timeout не истёк) запусти оба консьюмера. Сначала read_committed:

```sh
make run-downstream
```

Покажет 0 записей. Брокер удерживает их на стороне fetch — они за last stable offset, у транзакции нет ещё ни abort, ни commit marker'а.

```sh
make run-downstream-ru
```

Покажет ровно те N записей, что pipeline успел отправить до `os.Exit`. Это и есть смысл аборнутой транзакции в логе: данные физически записаны, но логически не существуют для read_committed.

Теперь подождём 60 секунд (наш `TransactionTimeout`), чтобы координатор аборнул осиротевшую транзакцию. Можно и не ждать — следующий запуск pipeline с тем же `transactional.id` сам всё ускорит через zombie fencing. Второй инстанс перебивает epoch первого. Координатор сразу же пишет abort marker для осиротевшей транзакции, и остаток входа становится читаемым без ожидания.

```sh
make run-pipeline-crash CRASH_PROB=0     # пройдёт без крашей, доберёт остаток
```

Pipeline стартует, видит, что для своей группы committed offset стоит после первого батча первой партиции (тот, что успешно committed раньше — если был; на свежем демо его нет). Дочитывает оставшиеся 30-N записей. Обрабатывает их и коммитит транзакции. На выходе — 30 уникальных ключей.

Проверь снова:

```sh
make run-downstream      # 30 записей, 30 уникальных ключей, 0 дублей
make run-downstream-ru   # 30 + N (записи аборнутой транзакции остаются в логе)
```

Это и есть EOS на консьюмер-стороне для downstream. Аборнутые записи физически в логе остались, занимают offset'ы, но read_committed-клиент их не отдаст никогда. До log retention'а.

## Ограничения

EOS, которое мы тут построили — это про Kafka↔Kafka. Если pipeline кроме Kafka трогает что-то ещё (запись в БД или вызов в downstream-сервис) — внешняя сторона не участвует в транзакции. Она может выполниться, а Kafka-транзакция аборнется. На рестарте pipeline её повторит. Если внешняя сторона не идемпотентна — двойной email. EOS Kafka тут уже не помогает. Помогают другие подходы — outbox-паттерн (про него следующая лекция, [Outbox-паттерн](../../../04-03-outbox-pattern/i18n/ru/README.md)), идемпотентные хендлеры на стороне внешнего получателя. XA-транзакции тоже теоретически закрывают вопрос, но на практике их применяют редко — слишком много операционных издержек.

Второе ограничение — fetch-offset reset. Если pipeline-консьюмер впервые приходит на топик, и при этом в input идёт активный транзакционный продьюсер с долгими in-flight транзакциями — наш fetch будет упираться в last stable offset и стоять. Лечится либо коротким `TransactionTimeout` у источника, либо стартом с конкретной известной позиции вместо ожидания LSO.

И последнее — `TransactionTimeout`. У нас выставлено явно 60 секунд (`pkg/kgo/config.go:603` — дефолт franz-go v1.21.0 это 40 секунд, мы его перетёрли, чтобы совпасть с Java-клиентским дефолтом `transaction.timeout.ms=60000`). Если обработка батча займёт больше, координатор сам аборнет транзакцию изнутри, и `End(TryCommit)` вернёт `InvalidTxnState`. Брокерный потолок — `transaction.max.timeout.ms`, дефолт 15 минут (`kafka-configs.sh --describe` на стенде Kafka 4.2.0). Если обработка тяжёлая (модель ML, большой DB-batch), таймаут надо поднимать вместе с `delivery.timeout.ms` у downstream'а — и не выше брокерного потолка.

## Запуск целиком

```sh
make topic-create-all
make seed SEED_COUNT=100

# гоняй с разными crash-prob, перезапускай — пока не идле
make run-pipeline-crash CRASH_PROB=0.3
make run-pipeline-crash CRASH_PROB=0.3
make run-pipeline-crash CRASH_PROB=0       # последний — без крашей, доберёт остаток

make run-downstream                         # 100 записей, 100 уникальных
```

Для понимания механики дополнительно полезно:

- `make group-describe` — committed offset группы pipeline'а после серии крашей. Должен совпадать с end-offset входа.
- `make end-offsets` — увидеть «лишние» records в output (аборнутые) и control records (commit/abort markers).
- `make verify` — короткий sanity-check: сравнить count input и read_committed-count output. Должны быть равны.
