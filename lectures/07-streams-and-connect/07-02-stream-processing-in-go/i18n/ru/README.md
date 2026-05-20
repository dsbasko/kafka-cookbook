# 07-02 — Stream Processing in Go (franz-go + Pebble)

В [Stream processing: концепции](../../../07-01-stream-processing-concepts/i18n/ru/README.md) мы говорили про идеи: event-time, окна, watermark, KStream/KTable. Тут пора потрогать. Stream processing'у нужно state'е — счётчики где-то живут между записями. И state этот надо переживать рестарты, иначе любая аналитика рассыпается на первой же `kill -9`.

Беда в том, что для Go нативного Kafka Streams нет. В Java — есть библиотека, прямо от Confluent. В Go — пусто. Самые близкие штуки (Watermill, например) — это про message routing, не про stateful streams. Так что собираем руками: kafka-клиент + локальный embedded KV-store + changelog topic для durability.

В нашем случае это `franz-go` + `Pebble` + compacted-топик `word-count-changelog`. Получается упрощённая копия модели Kafka Streams: state живёт на диске, обновления параллельно копируются в Kafka, при потере диска state восстанавливается из changelog'а с beginning'а. Без watermark'ов, без окон по времени, без сложной топологии — просто чтобы увидеть три ключевые механики на работающем коде.

## Зачем нам state

Stateless-обработка: пришла запись, сделал с ней что-то, записал куда-то — забыл. `map`, `filter`, `flatMap`. Перезапустил процесс, ничего не потерял.

Stateful — другое дело. Считаем `count`, `sum`, `top-N`, `unique users за час`. Вторая запись зависит от того, что мы видели в первой. Память где-то надо держать. Варианты на пальцах.

1. **In-memory-only.** Просто `map[string]int` в горутине. Быстро, ноль зависимостей, после `kill -9` всё обнулилось. Подходит ровно для демо-скриптов.
2. **Внешняя БД.** Postgres, Redis, любой KV. Накладные на каждый инкремент — сетевой round-trip. На потоке 50k msg/sec уже больно.
3. **Embedded store + changelog.** Пишем в локальный LSM (Pebble/RocksDB), параллельно копию изменений отправляем в compacted-топик Kafka. Производительность как у локальной БД (миллисекундные сетевые round-trip'ы пропадают), durability — кафочного уровня. Это и есть «как делает Kafka Streams».

Третий вариант мы и собираем. Pebble тут — потому что чистый Go, без CGo (RocksDB через CGo — отдельная боль на сборках). Pebble — это LSM-движок CockroachDB, на нём же построено их собственное хранилище — для нашего sandbox'а более чем достаточно.

## Pebble в двух словах

LSM-дерево, embedded, key-value. API очень простой: `Set`, `Get`, `Delete`, итерация. Хранит на диск (по дефолту в указанную директорию), периодически сбрасывает memtable на диск. По принципам — родственник RocksDB.

Что нам важно из API:

- `pebble.Open(dir, opts)` — открыть/создать БД на диске.
- `db.Set(key, value, sync)` — записать.
- `db.Get(key)` → `(value, closer, err)` — прочитать (`closer.Close()` обязателен после использования).
- `db.NewIter(opts)` → итератор по всему диапазону.
- `db.Flush()` — форсировать сброс memtable на диск.

Опция `pebble.Sync` против `pebble.NoSync` решает про fsync. В нашем коде мы складываем все Set'ы одного батча в `*pebble.Batch` без sync'а, а потом коммитим батч с `pebble.Sync` — fsync один раз на батч, а не на каждую запись. На проде в комбинации с changelog'ом часто берут `NoSync` даже на коммит батча плюс периодический `Flush`: durability обеспечивает Kafka, локальный диск нужен только для скорости.

## Архитектура нашего word-count

Три топика и одна локальная директория.

- `lecture-07-02-text-events` — input. Любые строки, мы режем их на слова и считаем.
- `lecture-07-02-word-count-changelog` — compacted-топик. На каждое обновление счётчика пишем `(word, current_count)`. Compaction в Kafka гарантирует, что для каждого ключа сохранится только последнее значение, размер не растёт линейно.
- `lecture-07-02-word-counts` — output. Раз в `flush` секунд (5 по умолчанию) эмитим текущий top-N как снэпшот.

И директория `./state/` — туда Pebble складывает свой LSM. Удалил директорию — потерял локальный state. Запустил `cmd/changelog-restorer` — восстановил с changelog'а.

Поток в одну сторону, без петель:

```
text-events ──> [word-count] ──┬──> word-count-changelog (compact)
                               ├──> word-counts (top-N snapshot)
                               └──> ./state/ (Pebble)
```

И обратное направление, только для рестарта state'а:

```
word-count-changelog ──> [changelog-restorer] ──> ./state/
```

## Цикл word-count'а

Самое важное — порядок трёх долговечных операций в одном цикле polling'а: produce в changelog, batch в Pebble и commit offset'а. Если их перепутать, можно либо потерять инкременты при краше, либо словить дубли при рестарте.

Правильный порядок: **changelog → Pebble → commit offset'а**. У каждого шага свой смысл.

Сначала накапливаем инкременты в in-memory overlay (без записи в Pebble) и собираем соответствующие changelog-записи:

```go
overlay := make(map[string]uint64)
var produces []*kgo.Record

fetches.EachRecord(func(rec *kgo.Record) {
    words := tokenize(string(rec.Value))
    for _, word := range words {
        cur, ok := overlay[word]
        if !ok {
            cur, _ = readUint64(w.store, []byte(word))
        }
        cur++
        overlay[word] = cur
        produces = append(produces, &kgo.Record{
            Topic: w.changelogTopic,
            Key:   []byte(word),
            Value: encodeUint64(cur),
        })
    }
})
```

Overlay нужен потому, что в одном батче одно и то же слово может встретиться несколько раз, и каждой changelog-записи нужен текущий бегущий счётчик, а не устаревшее значение из Pebble.

Дальше публикуем changelog одним `ProduceSync`, фиксируем overlay в Pebble одним batch'ем и только после этого коммитим offset'ы:

```go
if err := w.client.ProduceSync(rpcCtx, produces...).FirstErr(); err != nil {
    return fmt.Errorf("changelog produce: %w", err)
}

batch := w.store.NewBatch()
for word, count := range overlay {
    _ = batch.Set([]byte(word), encodeUint64(count), nil)
}
if err := batch.Commit(pebble.Sync); err != nil {
    return fmt.Errorf("pebble batch commit: %w", err)
}

if err := w.client.CommitUncommittedOffsets(commitCtx); err != nil {
    return fmt.Errorf("commit offsets: %w", err)
}
```

Почему такой порядок. Если бы мы сначала закоммитили offset'ы, потом писали changelog и в этой щели нас прибило бы — после рестарта word-count считал бы себя успешно прошедшим этот батч, но в changelog'е изменений нет. Если потом потеряем Pebble и попробуем восстановиться — счётчики уедут вниз. Хуже всего, что эта потеря — тихая: никто не оповестит про счётчик, который молча занижает.

Почему changelog раньше Pebble. Если краш произойдёт между ними, в changelog уже лежат новые значения, а Pebble остался со старыми. На рестарте offset не закоммичен, поэтому тот же входной батч переобрабатывается, в changelog уезжают те же новые значения (compaction схлопнет дубликаты по ключу), и Pebble догоняется. Конечное состояние согласовано. Если бы мы писали Pebble первым и крашнулись до changelog'а, restorer из changelog'а дал бы значения ниже того, что уже лежит в Pebble — а сам Pebble на replay-е переинкрементировался бы, потому что overlay стартует с того, что уже есть в Pebble, и счётчик уехал бы на один батч вперёд.

Вся цепочка всё равно даёт **at-least-once**, не exactly-once. Краш между commit'ом Pebble и commit'ом offset'а на рестарте приведёт к переобработке батча — Pebble переинкрементирует, потому что overlay видит уже обновлённые значения, и changelog получит завышенные счётчики. Чтобы это убрать, нужны транзакционные семантики продьюсера на весь блок: `kgo.NewGroupTransactSession` плюс `Begin/End(TryCommit)` — лекция [Consume-process-produce](../../../../04-reliability/04-02-consume-process-produce/i18n/ru/README.md). Для word-count'а завышение на один-два после редкого краша — приемлемая цена.

## Output: top-N снэпшот

Раз в `flush` секунд фоновая горутина проходит по Pebble и эмитит текущий top-N. Печать в stdout — для глаз, запись в `word-counts` — чтобы downstream-процесс мог это потреблять.

```go
func (w *wordCounter) flushTopN(ctx context.Context) error {
    rows, err := w.collectAll()
    // ... сортировка по count убыванию ...
    if len(rows) > w.topN {
        rows = rows[:w.topN]
    }
    // печать в stdout
    // ProduceSync top-N в outputTopic
}
```

Запись в outputTopic тут — Produce без транзакции, без commit'а offset'а вместе с ним. Снэпшот публикуется «как есть» — если он пропадёт, через 5 секунд будет следующий. Это нормальная семантика для метрических снэпшотов. Если downstream не переваривает дубли (мы могли отправить top-N и успеть сделать новый flush до того, как прошлый дошёл) — клади idempotency-key с timestamp'ом и отбрасывай старьё на consumer'е.

## Compacted changelog: что и почему

`word-count-changelog` — топик с `cleanup.policy=compact`. Что это значит. Обычный топик хранит все записи до retention'а. Compacted — для каждого ключа гарантирует наличие как минимум последней записи. Старые версии того же ключа со временем удаляются compaction'ом (фоновая работа в брокере).

Зачем нам это. Word-count видел слово `kafka` тысячу раз — и тысячу раз дописал в changelog. После compaction'а в физическом логе из этой тысячи останется только одна-две последних записи (точнее зависит от тайминга и `min.cleanable.dirty.ratio`). Размер changelog'а растёт **линейно с числом уникальных слов**, не с числом инкрементов.

Это и есть способ держать в Kafka «материализованную view» на state. По аналогии с KTable — у нас compacted-топик плюс local store, и они согласованы по последнему значению на ключ.

Топик создаётся со специальными конфигами:

```sh
docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka-1:9092 --create \
  --topic lecture-07-02-word-count-changelog \
  --config cleanup.policy=compact \
  --config segment.ms=60000 \
  --config min.cleanable.dirty.ratio=0.01
```

`segment.ms=60000` плюс `min.cleanable.dirty.ratio=0.01` — параметры, чтобы compaction случался часто на маленьком объёме. На проде они обычно сильно больше: compaction не дешёвый.

## Restore: с нуля из changelog'а

Сценарий «диск умер, Pebble пропал». Запускаем `cmd/changelog-restorer`. Он читает `word-count-changelog` с beginning'а, кладёт пары в Pebble, останавливается на high-watermark'е каждой партиции.

Сначала узнаём, докуда читать:

```go
end, err := admin.ListEndOffsets(rpcCtx, topic)
// ...
end.Each(func(o kadm.ListedOffset) {
    if o.Offset > 0 {
        out[o.Partition] = o.Offset
    }
})
```

Дальше читаем без consumer-group (нам не нужен committed offset, нужен снэпшот целого compacted-лога), отслеживаем максимальный offset вручную и сравниваем:

```go
fetches.EachRecord(func(rec *kgo.Record) {
    if rec.Offset+1 > maxOffsets[rec.Partition] {
        maxOffsets[rec.Partition] = rec.Offset + 1
    }
    if len(rec.Value) == 0 {
        // tombstone — ключа больше нет
        _ = store.Delete(rec.Key, pebble.NoSync)
        return
    }
    // ... pebble.Set(key, value)
})

if reachedEnd(maxOffsets, endOffsets) {
    break
}
```

Tombstone — запись с `value=nil` в compacted-логе. Семантически означает «удали этот ключ, для меня его больше нет». В нашем word-count мы tombstone никогда не пишем (счётчик может только расти), но restorer всё равно их корректно обрабатывает — на случай ручных правок или будущих эволюций модели.

После того как все партиции дочитаны до end-offset'а, делаем `Flush()` — Pebble сбрасывает накопленное на диск. После этого можно стартовать word-count со стандартным `make run` — он найдёт state на месте и продолжит с точки, в которой changelog был на момент restore'а.

Один нюанс: между моментом restore'а и моментом старта word-count'а в changelog могли уже прилететь новые записи (если кто-то параллельно ещё пишет). Это нормально. Word-count при старте подхватит свой last committed offset из consumer-group'ы, начнёт читать `text-events` с того же места — и заодно догонит changelog в части новых обновлений. Самосогласованность сохраняется.

## Запуск

Стенд должен быть поднят (`docker compose up -d` из корня).

Один раз создать топики:

```sh
make topic-create-all
```

В одном терминале — заливать input:

```sh
make seed-text
```

Цикл из десятка фраз идёт в `text-events` секунду в секунду. Можно подкинуть свой текст через `kafka-console-producer.sh` руками — формат любой, мы режем по словам.

В другом терминале — word-count:

```sh
make run
```

Каждые 5 секунд он печатает top-10 слов и текущее число обработанных событий. Глянь, как растут счётчики. Поубивай его (`Ctrl+C`), запусти снова — счётчики продолжаются с того же значения, потому что Pebble остался на диске.

Хочешь увидеть restore — снеси директорию state и восстанови из changelog'а:

```sh
rm -rf ./state
make restore
make run
```

После `make restore` директория `./state/` снова заполнена, и word-count при старте найдёт свои счётчики.

Прибрать после лекции:

```sh
make topic-delete-all
rm -rf ./state
```

## Куда расти

То, что мы собрали — модель stateful processing'а на минималках. Не хватает массы вещей, и про каждую полезно знать, что её здесь нет.

- **Time windows.** Word-count'у не нужен event-time — он считает «всё за всё время». Реальные стримы почти всегда хотят окна (см. [Stream processing: концепции](../../../07-01-stream-processing-concepts/i18n/ru/README.md)). На основе нашей схемы это делается так: ключ Pebble не `word`, а `<word>:<window-start>`, плюс отдельный процесс закрывает окна по watermark'у и удаляет старые ключи.
- **Joins.** Stream-stream и stream-table join'ы — отдельная большая тема. Базово: нужно репартиционировать обе стороны по join-ключу, потом держать local cache (KTable-side) в Pebble.
- **Backpressure.** В нашем коде `flushLoop` идёт независимо от обработки. Если поток входящих сообщений сильно опережает скорость flush'а в Kafka — буфер растёт. Для production'а: `cl.PauseFetchPartitions` при перегрузе outputTopic'а (паттерн из [Доставка во внешние системы](../../../../04-reliability/04-05-external-delivery/i18n/ru/README.md)).
- **Exactly-once.** Чтобы избавиться от дублей при крашах, нужны транзакции producer'а вокруг блока «changelog produce + Pebble update + offset commit». В franz-go v1.21.0 публичная точка входа — `kgo.NewGroupTransactSession`, паттерн из [Consume-process-produce](../../../../04-reliability/04-02-consume-process-produce/i18n/ru/README.md).
- **Шардинг state'а.** При большом числе партиций input'а одна нода с одним Pebble — bottleneck. Kafka Streams делит state по партициям ключа, каждая нода держит свой shard. Тут — один процесс, один state. Расширяется через consumer-group: каждый member берёт свои партиции, держит свой Pebble; changelog'ом всё равно делятся.
- **Метрики и наблюдаемость.** Lag входного топика, размер state'а, lag changelog-publish'а, latency flush'а top-N. Это [Мониторинг и метрики](../../../../08-operations/08-01-monitoring-and-metrics/i18n/ru/README.md).

Всё перечисленное — поверх той же базы. Pebble + changelog + грамотный порядок «changelog → state → commit». Меняется обвязка, не суть.

## Что унести

- **Stateful streams без state store'а — это иллюзия.** В памяти всё работает, пока не упадёт; нужно либо внешнее хранилище (медленно), либо embedded + changelog (быстрее и durable).
- **Pebble + compacted changelog topic — рабочая схема для Go.** Не Kafka Streams, но достаточно для большинства практических задач.
- **Порядок операций важнее, чем кажется.** Changelog → state → commit. Любая перестановка даёт неприятную семантику (потеря или несогласованный счётчик), и эту неприятность ты заметишь сильно позже первого продакшн-инцидента.
- **Compacted topic — это материализованный snapshot, не лог.** Все рассуждения про retention к нему не применимы; размер ограничен числом уникальных ключей, не числом записей.

В [Kafka Connect](../../../07-03-kafka-connect/i18n/ru/README.md) уйдём в другую сторону — Kafka Connect и декларативный ETL без своего кода. Для тех случаев, где Pebble + Go — overkill.
