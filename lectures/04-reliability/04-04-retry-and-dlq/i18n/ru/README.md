# 04-04 — Retry & DLQ Deep Dive

В лекции [Обработка ошибок](../../../../03-consumer/03-04-error-handling/i18n/ru/README.md) уже разбирали error handling на стороне consumer'а: in-place retry для transient ошибок и DLQ для всего, что не получилось. Это работает на масштабе одного consumer-loop'а. Тут мы пойдём дальше. Появится несколько retry-топиков с задержкой, у DLQ — отдельная судьба, и отдельный CLI для повторной отправки.

Зачем вообще усложнять? Сейчас разберём.

## Почему in-place retry перестаёт хватать

Главная проблема in-place retry — он сидит в poll-loop'е. Пока ты пять раз пытаешься достучаться до сломанного даунстрима, consumer не дёргает poll. В franz-go v1.21.0 heartbeat-loop работает независимо от обработки, поэтому длинный backoff сам по себе из группы не выкинет — координатор считает клиента живым, пока его сетевой heartbeat в порядке. Стреляет всё в момент ребаланса (новый член зашёл, лидер сменился, broker упал): если в этот момент handler сидит в backoff'е, у него есть только `RebalanceTimeout` (`rebalance.timeout.ms`, дефолт 60 секунд в franz-go v1.21.0) чтобы свернуться и переджойниться. Не успел — координатор кикает клиента, партиция уезжает к другому, и тот возьмётся за тот же offset с той же ошибкой. В Java-клиенте механика жёстче: между `poll()` вызовами действует `max.poll.interval.ms` (дефолт 5 минут), и его превышение сразу кикает консьюмера, без привязки к ребалансу.

Это первый аргумент. Второй — головы́ блокируются. У тебя в одной партиции 1000 сообщений, среди них одно битое. На него тратится тридцать секунд. Все 999 за ним ждут. Так получается hot-line из-за единственного мусорного record'а.

Третий — про длину паузы. Если даунстрим лежит, нет смысла стучать чаще, чем он проснётся. Минута, пять минут, час. Спать на это время прямо в poll-loop'е нельзя по причине номер один. Параллельно работать тоже не получится — порядок сломается, offset нельзя коммитить, пока «висит» record (см. [Конкурентность и lag](../../../../03-consumer/03-05-concurrency-and-lag/i18n/ru/README.md)).

Вывод. Если ретраи нужны не «тут же ещё разок», а «через 30 секунд / 5 минут / час» — нужен другой механизм. Тот, что не блокирует основной consumer.

## Идея retry-топиков

Решение простое и наглядное. Делаем отдельный топик на каждый интервал ожидания:

- `payments` — основной;
- `payments-retry-30s` — упало в основном, перекинули сюда;
- `payments-retry-5m` — упало в `retry-30s`, перекинули сюда;
- `payments-retry-1h` — последний шанс;
- `payments-dlq` — финальная остановка.

Один consumer слушает все четыре топика (main и три retry). Когда из main приходит record и handle падает — мы его пакуем с дополнительными headers и шлём в `retry-30s`. Дальше он лежит там как обычное сообщение Kafka. Тот же consumer его рано или поздно прочитает. И вот тут трюк: перед обработкой смотрим на `record.Timestamp` и ждём, пока пройдёт нужный интервал. Если запись пришла секунду назад, а ждать надо тридцать — спим 29 секунд. Потом снова handle. Получилось — commit и едем дальше. Не получилось — `retry-5m`. Сценарий повторяется на каждой ступени.

Получаем то, что хотели:

- ретраи не блокируют основной поток. main-партиции всегда обрабатываются с тем же темпом, что без ошибок;
- между попытками — реальные интервалы ожидания, а не «как успеет poll-loop»;
- история движения по pipeline'у видна в headers (`error.message`, `previous.topic`, `retry.count`) — оператор разберёт инцидент по headers DLQ-сообщения, не лазая в логи.

Минус: я всё равно блокирую poll-loop ровно на retry-топиках, пока «отлёживаю» record. На лекционной нагрузке это нормально. На production-нагрузке делают по-другому — отдельный consumer на каждый retry-топик, либо `PauseFetchPartitions` плюс отложенный `ResumeFetchPartitions` (это тема [Доставка во внешние системы](../../../04-05-external-delivery/i18n/ru/README.md)). Для понимания паттерна важна сама эскалация, остальное — детали реализации.

## Headers как протокол

Каждая ступень pipeline'а оставляет следы. Соглашение в нашей лекции:

| Header | Кто ставит | Что значит |
| --- | --- | --- |
| `error.class` | каждая ступень | `permanent` или `transient` (последняя классификация) |
| `error.message` | каждая ступень | строка ошибки |
| `error.timestamp` | каждая ступень | когда упало (RFC3339Nano UTC) |
| `retry.count` | каждая ступень | счётчик эскалаций (0 → 1 → 2 → 3 → DLQ) |
| `previous.topic` | каждая ступень | откуда переехали (для DLQ это последняя retry-ступень) |
| `original.topic` | первая эскалация | где record родился (никогда не меняется) |
| `original.partition` / `original.offset` | первая эскалация | координаты первого появления |

Соглашение специально консервативное. Headers — это пары байтов, ничего самовалидирующегося там нет. Мы решаем сами, что и как туда класть. Если выбор полей понятный — DLQ можно разбирать без access'а к коду processor'а: открыл headers, прочитал error.class и retry.count, и уже видишь картину.

`previous.topic` отдельно — он удобный для replay'а. Когда оператор ловит DLQ-инцидент и хочет понять, на какой именно ступени окончательно сдалось — `previous.topic` отвечает. `original.topic` нужен другому: чтобы понять, где «дом» этого payload'а. После replay из DLQ обратно в main — `original.topic` остаётся прежним, его мы при replay'е не перетираем. Получается стабильный идентификатор «места рождения» record'а, удобный для трейсинга.

## Что показывает наш processor

Главное — таблица ступеней. Я их завёл явно, потому что это контракт лекции:

```go
stages := []stage{
    {topic: *mainTopic, delay: 0, nextTopic: *retry30},
    {topic: *retry30, delay: *delay30s, nextTopic: *retry5m},
    {topic: *retry5m, delay: *delay5m, nextTopic: *retry1h},
    {topic: *retry1h, delay: *delay1h, nextTopic: ""},
}
```

Пустой `nextTopic` на последней retry-ступени — флажок «дальше эскалировать некуда». `forwardOrDLQ` увидит пустую строку и пошлёт record в DLQ с `reason=exhausted`. Если бы поставили `*dlq` напрямую, в логе печатался бы `reason=next-retry`, и три случая (`next-retry` / `permanent` / `exhausted`) не различались бы между собой.

Один consumer группы `lecture-04-04-processor` подписывается на все четыре топика. Перед `handle()` смотрим на `delay` ступени и, если он положительный, ждём до `record.Timestamp + delay`. Это сердце retry-механики:

```go
func waitUntilDue(ctx context.Context, recordTs time.Time, delay time.Duration) error {
    due := recordTs.Add(delay)
    wait := time.Until(due)
    if wait <= 0 {
        return nil
    }
    fmt.Printf("WAIT  due=%s (через %s)\n", due.UTC().Format(time.RFC3339), wait.Truncate(time.Second))
    select {
    case <-ctx.Done():
        return ctx.Err()
    case <-time.After(wait):
        return nil
    }
}
```

Дальше — решение, куда отправить упавший record. Три случая, каждый со своим target'ом:

```go
target := st.nextTopic
reason := "next-retry"
if isPermanent(cause) {
    target = dlqTopic
    reason = "permanent"
} else if target == "" {
    target = dlqTopic
    reason = "exhausted"
}
```

`permanent` — сразу в DLQ, минуя retry-ступени. Битый JSON (poison-pill) или отказ доменной валидации — повторять бесполезно, даже через час. `exhausted` — это transient, но мы уже на `retry-1h` и `nextTopic` пуст. Всё, что не вылечилось за час, считаем безнадёжным.

Headers собираются в `forwardWithHeaders`. Тонкий момент — `original.*` ставится только при первой эскалации:

```go
if _, ok := idx["original.topic"]; !ok {
    headers = appendOrReplace(headers, "original.topic", r.Topic)
    headers = appendOrReplace(headers, "original.partition", strconv.Itoa(int(r.Partition)))
    headers = appendOrReplace(headers, "original.offset", strconv.FormatInt(r.Offset, 10))
}
headers = appendOrReplace(headers, "previous.topic", r.Topic)
headers = appendOrReplace(headers, "retry.count", strconv.Itoa(nextRetries))
```

`appendOrReplace` важен: error-headers перетираются на каждой ступени (нам нужна последняя ошибка, не первая), а `original.*` пишутся один раз и держатся.

## DLQ как терминал

Когда record добрался до DLQ — это конец автоматического pipeline'а. Дальше его читает отдельный обработчик и в общем случае не возвращает в основной поток. Цели у DLQ-обработчика три:

1. Зафиксировать инцидент в долговременном хранилище (БД, append-only лог, S3) — чтобы можно было поднять глазами через неделю.
2. Дёрнуть алёрт — кто-то живой должен узнать, что сообщение умерло.
3. Не блокировать DLQ-партиции бесконечной обработкой — DLQ должен дочитываться быстро, иначе lag растёт и ты теряешь видимость.

В нашей лекции `cmd/dlq-processor` делает первое и второе. Алёрт mock'нут до stdout (в проде это webhook в Slack или PagerDuty). Хранилище — append-only JSON-файл `/tmp/lecture-04-04-incidents.jsonl`. По плану лекции там должна быть Postgres-таблица — паттерн идентичный, файл выбран, чтобы не тащить ещё один docker-compose. На прод — подменяешь `os.OpenFile` на `pgxpool.Exec(INSERT ...)`, и всё.

Структура incident-записи:

```go
type incident struct {
    DLQTopic         string `json:"dlq_topic"`
    DLQPartition     int32  `json:"dlq_partition"`
    DLQOffset        int64  `json:"dlq_offset"`
    Key              string `json:"key,omitempty"`
    OriginalTopic    string `json:"original_topic,omitempty"`
    OriginalPart     string `json:"original_partition,omitempty"`
    OriginalOffset   string `json:"original_offset,omitempty"`
    PreviousTopic    string `json:"previous_topic,omitempty"`
    RetryCount       string `json:"retry_count,omitempty"`
    ErrorClass       string `json:"error_class,omitempty"`
    ErrorMessage     string `json:"error_message,omitempty"`
    ErrorTimestamp   string `json:"error_timestamp,omitempty"`
    DLQRecordTime    string `json:"dlq_record_time"`
    PayloadByteCount int    `json:"payload_bytes"`
}
```

Намеренно нет поля `payload`. Идея — incident-лог должен быть лёгким и пригодным к индексированию (по error_class, по original_topic). Если payload надо посмотреть — это уже отдельная операция через `kafka-console-consumer` или dump через `replay-cli --dry-run`. В incident-лог копировать payload'ы — путь к терабайту жирных JSON'ов, по которым потом не найти ни одного нужного инцидента.

Алёрт в stdout простой:

```
[ALERT] #3  dlq=payments-dlq p=1 off=2 key=k-7
        original=payments/0/14 previous=payments-retry-1h retries=3
        class=transient message="exhausted retries: transient downstream blip on payment id=\"k-7\""
        payload=42 bytes
```

Этого хватит, чтобы понять: запись k-7 пришла из основного `payments`, прошла все три retry-ступени, упала по transient на каждой и в итоге сдалась после часа ожидания. В реальном алёрт-канале форматирование другое, поля те же.

## Replay

DLQ — это финал автоматики, но не приговор. Часть инцидентов после фикса даунстрима имеет смысл переиграть. Тот же `transient`: за час даунстрим починили, и теперь у нас в `payments-dlq` лежит 200 записей, которые могли бы пройти, если их подать снова.

`cmd/replay-cli` это умеет. Ключевые флаги:

- `-from-topic` — откуда читать, по умолчанию `payments-dlq`;
- `-to-topic` — куда переотправить, по умолчанию основной `payments`;
- `-since` — фильтр по времени DLQ-записи (берём всё новее `now() - since`);
- `-error-class` — опциональный фильтр по header'у; типичный случай — `transient`;
- `-dry-run` — посчитать совпадения, ничего не публикуя.

Перепаковка в новый record:

```go
func replayRecord(r *kgo.Record, toTopic string) *kgo.Record {
    headers := append([]kgo.RecordHeader(nil), r.Headers...)
    headers = setHeader(headers, "retry.count", "0")
    headers = setHeader(headers, "replay.from-dlq", r.Topic+"/"+strconv.Itoa(int(r.Partition))+"/"+strconv.FormatInt(r.Offset, 10))
    headers = setHeader(headers, "replay.timestamp", time.Now().UTC().Format(time.RFC3339Nano))
    return &kgo.Record{
        Topic:   toTopic,
        Key:     r.Key,
        Value:   r.Value,
        Headers: headers,
    }
}
```

Значимое здесь:

- `retry.count` обнуляется. Новый pipeline начинается с нуля — иначе DLQ-replay сразу попал бы под счётчик исчерпанных попыток предыдущей сессии и улетел обратно в DLQ.
- `replay.from-dlq` — координаты исходного record'а в DLQ. Если после replay снова упадём — в новом DLQ-инциденте по этому header'у видно, что текущий прогон уже второй.
- payload и key — нетронуты. Это важно: в тех системах, где consumer строит дедуп по бизнес-ключу payload'а, replay не должен ломать идемпотентность.

Что замолчал намеренно. Replay не дедуплицирует. Если запустить `make replay` дважды подряд — отправит дважды. Защиту от этого должен делать consumer (см. [Outbox-паттерн](../../../04-03-outbox-pattern/i18n/ru/README.md) про idempotency на dedup-таблице). Альтернатива — хранить ID уже сделанных replay'ев на стороне CLI, но тогда у нас стейт-полный CLI, что отдельная история.

## Метрики, на которые смотреть

Наблюдаемость pipeline'а строится на четырёх числах. Каждое из них имеет осмысленную цель:

- end-offset основного `payments`. Растёт пропорционально нагрузке. На него можно навесить алёрт «throughput упал».
- end-offset каждого retry-топика. На стабильно работающей системе они должны быть низкими и расти медленно. Резкий рост — сигнал «даунстрим деградировал». Идеал — все три retry-топика близки к нулю.
- end-offset DLQ. Любой ненулевой прирост — алёрт. На production это обычно `rate(messages_in_dlq_total[5m]) > 0` в Prometheus.
- consumer lag по группе processor'а. Лекция [Конкурентность и lag](../../../../03-consumer/03-05-concurrency-and-lag/i18n/ru/README.md) показывала `kadm.Lag` — для каждой ступени отдельный лаг, и если на main всё хорошо, а на retry-30s огромный — значит, мы захлёбываемся в ретраях.

Про DLQ есть отдельная мета-метрика — `error.class` distribution. Из incident-лога её снимаешь одной строкой: `jq -r '.error_class' /tmp/lecture-04-04-incidents.jsonl | sort | uniq -c`. Если 90% инцидентов — `transient`, значит, retry pipeline скорее всего слишком короткий: нужен ещё один уровень с большей задержкой, либо replay по расписанию.

## Демо

Стенд из корня репозитория должен быть поднят (`docker compose up -d` в корне). Дальше из директории лекции:

```sh
make topic-create-pipeline
make seed-with-failures SEED_MESSAGES=20
```

В `payments` лежит 20 mock-сообщений. Часть с `mode=ok` (прошли с первой попытки), часть `transient` (всё время падают, на каждой ступени уйдут дальше), часть `permanent` (битый JSON или явный reject — сразу в DLQ).

Запускаем processor с быстрыми задержками, чтобы пайплайн прошёл за полминуты, а не за час:

```sh
make run-processor-fast
```

В выводе видно, как records путешествуют. Что-то вида:

```
OK    [payments] p=0 off=3 key=k-3
FAIL  [payments] p=2 off=4 key=k-5 reason=next-retry err=transient ... → payments-retry-30s
FAIL  [payments] p=1 off=2 key=k-7 reason=permanent err=invalid json: ... → payments-dlq
WAIT  due=2026-05-01T12:30:15Z (через 1s)
FAIL  [payments-retry-30s] p=0 off=0 key=k-5 reason=next-retry err=transient ... → payments-retry-5m
```

После того как processor прокачает все 4 топика и встанет на «нет новых сообщений» — Ctrl+C. В соседнем терминале:

```sh
make run-dlq
```

DLQ-processor читает `payments-dlq`, печатает ALERT и пишет JSON-строки в `/tmp/lecture-04-04-incidents.jsonl`. Проверим:

```sh
make dlq-count
cat /tmp/lecture-04-04-incidents.jsonl | jq -r '[.error_class, .original_topic, .key] | @tsv'
```

В DLQ — все `permanent` (сразу) плюс все `transient` (после исчерпания трёх retry-ступеней).

Теперь replay. Допустим, мы починили downstream и хотим вернуть всё `transient` за последний час обратно в основной топик:

```sh
make replay REPLAY_CLASS=transient REPLAY_SINCE=1h
```

CLI читает `payments-dlq`, фильтрует по `error.class=transient`, упаковывает с обнулённым `retry.count` и шлёт в `payments`. После этого в основном топике появляются те же payload'ы заново — `payment.k-5`, `payment.k-9`. Если запустить processor снова, они пойдут по pipeline'у с нуля. На лекционных моках они опять упадут (моки не лечатся), но зато в логе processor'а у новых retry-сообщений будет header `replay.from-dlq` со ссылкой на исходный DLQ-offset. По нему оператор поймёт: текущий прогон уже второй, первая жизнь записи закончилась в DLQ.

`make replay-dry` делает то же самое без ProduceSync — полезно убедиться, что фильтр захватывает то, что ожидаешь, до реального трафика.

## Рамки паттерна

Несколько границ, которые легко упустить.

Pipeline retry-топиков сам по себе не делает доставку гарантированной — это тот же at-least-once, что был в [Обработка ошибок](../../../../03-consumer/03-04-error-handling/i18n/ru/README.md). Ровно те же грабли «упали между produce и commit» работают и тут. Если processor упал между «сделали ProduceSync в `retry-5m`» и «сделали CommitRecords для `retry-30s`» — на рестарте `retry-30s` отдаст этот record снова, и он попадёт в `retry-5m` повторно. Дубль в `retry-5m`. Идемпотентность handler'а — единственная защита.

Длительные ожидания в `retry-1h` (час) на одной партиции блокируют все остальные record'ы в этой же партиции. Это тонкое место. Один способ обойти — partitioning по бизнес-ключу: если `key=k-5` залип на час, остальные ключи лежат в других партициях и обрабатываются как ни в чём не бывало. Если же все retry-сообщения летят в одну партицию (например, ключ — это user_id, а у одного user'а сразу 100 сообщений) — pipeline захлёбывается. Решение — либо уменьшать `retry-1h` до меньшей задержки, либо параллелить через worker pool с per-key affinity (см. [Конкурентность и lag](../../../../03-consumer/03-05-concurrency-and-lag/i18n/ru/README.md)), либо разделять retry-pipeline на больше партиций, чем у основного топика.

Replay — ручная операция, и это нормально. Автоматический replay из DLQ обратно в main без понимания причины инцидента — путь к infinite loop'у. Если фикс не выкатили или причина была не в downstream, а в самом payload'е — record снова упадёт по тому же сценарию, и DLQ начнёт расти. Поэтому replay инициирует человек или routine, который проверил, что причина устранена.

И последнее. Retry-pipeline не подходит для случаев, где порядок важнее всего. Когда payment k-5 ушёл в `retry-30s`, а payment k-6 (с тем же ключом, но более поздний) проскочил по основному пути — мы нарушили order per-key. Если бизнес-логика терпит inversions, это нормально. Если строго запрещены — нужна другая архитектура, например, парковка всей партиции через `PauseFetchPartitions` до восстановления downstream'а ([Доставка во внешние системы](../../../04-05-external-delivery/i18n/ru/README.md)).

## Запуск целиком

```sh
make topic-create-pipeline
make seed-with-failures SEED_MESSAGES=50

# терминал 1
make run-processor-fast

# терминал 2 (как только processor отработает)
make run-dlq

# терминал 3
make dlq-count
make replay REPLAY_CLASS=transient REPLAY_SINCE=24h

make clean       # снести группы, топики и incident-лог
```

Полезные sanity-check'и: `make main-count` (общее число записей в основном топике с учётом replay'ев), `make dlq-count` (сколько умерло), `wc -l /tmp/lecture-04-04-incidents.jsonl` (сколько алёртов сгенерировалось — должно совпадать с DLQ).
