# 03-04 — Error Handling

В прошлой лекции мы поставили дедуп на стороне БД и закрыли вопрос с дублями. Каждое сообщение из Kafka один раз попадало в `messages`, повторы молча проглатывались. Что мы там не показали — handle всегда возвращал `nil`. Будто бизнес-обработчик не падает. На реальной системе это иллюзия.

Падает регулярно. Сеть моргнула — TCP-таймаут до downstream'а. Платёжный шлюз отвалился на 5 минут — 503. Аналитический пайплайн поставил rate-limit — 429. Прилетел payload, в котором поле `amount` — это строка `"$100"`, а вы ждёте float — JSON unmarshal падает. И вот ваш consumer стоит перед записью, которая не обрабатывается. Что делать?

## Четыре варианта решения

Тут нет единого правильного ответа, есть выбор стратегии под класс ошибки.

1. **Skip** — записать ошибку в лог или метрику, пропустить сообщение, закоммитить offset. Всё, поехали дальше. Сообщение потеряно для бизнес-логики, но pipeline не встал. Подходит для «не критично, бывает» — клик по кнопке, аналитический трекинг низкого приоритета.
2. **Retry in-place** — попробовать ту же запись ещё раз тут же, в этом же worker-цикле, с backoff'ом. Один-два-три раза. Если успех — закоммитить, поехали дальше. Если выгорело — переходим в одну из стратегий ниже. Подходит для коротких сбоев: TCP-моргание, мгновенный rate-limit.
3. **Retry-topic** — отправить сообщение в отдельный топик `*-retry-30s`, у которого consumer ждёт до timestamp+30s, потом обрабатывает. Не обработалось — следующий retry-топик с большим окном (`*-retry-5m`, `*-retry-1h`). Так удерживаем основной consumer быстрым, а поломанные записи разносим по отдельным «полкам» с возрастающей задержкой. Это уже [Retry и DLQ deep dive](../../../../04-reliability/04-04-retry-and-dlq/i18n/ru/README.md), тут показываем только идею.
4. **DLQ** — dead-letter queue. Отдельный топик `*-dlq`, куда отправляется сообщение вместе с диагностикой (что упало, на каком offset'е, сколько раз пробовали). Дальше — отдельная обработка: алёрт, разбор инцидента, replay.

В жизни эти варианты обычно комбинируются. Транзиентная ошибка — retry in-place; не помогло — retry-topic с задержкой; не помогло после нескольких прогонов — DLQ. Вечного «попробуй ещё раз» не бывает: если за разумное количество попыток обработка не пошла, проблема либо в данных, либо в downstream'е, и сидеть в poll-цикле уже бессмысленно.

## Классификация ошибки — это половина дела

Прежде чем выбрать стратегию, надо понять класс ошибки. Их два:

- **transient** — что-то временное; есть смысл подождать и повторить. Сетевые сбои, таймауты HTTP, rate-limit, недоступность downstream'а на короткое время.
- **permanent** — что-то невозвратное; повтор ничего не изменит. Битый JSON, схема не совпала, бизнес-правило отвергло запрос (платеж заблокирован комплаенсом — повтор тот же самый платёж не пройдёт), несуществующий ключ в БД.

Граница не всегда чёткая. Тот же 500 от downstream'а может быть transient (дернёт админ — оживёт) или permanent (баг в downstream-сервисе, починят через неделю). Тут уже начинается inженерное решение: после стольких-то попыток считаем permanent. Магической константы не существует.

В нашем коде классификация — через тип Go-ошибки. Есть собственный тип `permError`, всё, что не он — transient:

```go
type permError struct{ msg string }

func (e *permError) Error() string { return e.msg }

func permErrorf(format string, args ...any) error {
    return &permError{msg: fmt.Sprintf(format, args...)}
}

func isPermanent(err error) bool {
    var p *permError
    return errors.As(err, &p)
}
```

`errors.As` хорошо работает с обёрнутыми ошибками — если внутри пайплайна кто-то завернёт permError через `fmt.Errorf("...: %w", err)`, проверка всё равно его найдёт.

## Poison-pill problem

Особый случай permanent ошибки — отравленная пилюля. Это сообщение, которое **намертво** ломает наивный consumer: каждый poll возвращает его, обработка падает, offset не коммитится, на следующий poll снова это же сообщение, опять падает. Pipeline стоит. Lag растёт. Алерт срабатывает.

Самые типичные poison-pills:

- неполный или битый JSON (особенно если producer и consumer катались разными командами и producer накатил breaking change без согласования);
- сообщение, написанное по другой схеме (например, версия v3, которую consumer ждёт как v1, и unmarshal делает совсем не то, что ожидает обработчик дальше);
- невалидный enum value (consumer переключается switch'ом и попадает в default, который панизит);
- payload, у которого поле — `null`, а код делает `pmt.Items[0]`.

Решение — **обнаружить и отвести в сторону**. Не зацикливаться на этой записи, не паниковать на uncaught panic, но и не глотать молча через goroutine, которая делает recover и продолжает. Правильный ход — поймать, упаковать в DLQ с пометкой что именно упало, закоммитить offset и читать дальше.

В нашем `handle` это первая же проверка после прихода record'а:

```go
var p payment
if err := json.Unmarshal(r.Value, &p); err != nil {
    return permErrorf("invalid json: %v", err)
}
```

Если payload не парсится — это permanent сразу. Никакой backoff не починит «вот эту строку с неправильной кавычкой». В DLQ её, и дальше.

## Что делает наш код

В лекции — два бинарника. Они работают параллельно, но смотрят на разные топики.

`cmd/multi-strategy/main.go` — основной processor. Читает `payments`, решает что делать с ошибкой:

- mode=ok → обработка успешна → коммит после батча;
- mode=transient → in-place retry до `max-retries` раз с экспоненциальным backoff'ом; если за это время «исцелилось» (так сделано в моке — после двух попыток мок начинает возвращать nil) → коммит; если не исцелилось → DLQ как exhausted retries;
- mode=permanent → сразу DLQ;
- битый JSON → сразу DLQ как poison-pill.

Сам цикл retry — без всяких хитрых очередей, просто `for` с backoff'ом:

```go
for attempt := 1; attempt <= o.maxRetries; attempt++ {
    backoff := o.baseBackoff * (1 << (attempt - 1))
    select {
    case <-ctx.Done():
        return ctx.Err()
    case <-time.After(backoff):
    }

    err = handle(r, attempts, key)
    if err == nil {
        // ok, едем дальше
        return nil
    }
    if isPermanent(err) {
        // во время retry оказалось permanent — в DLQ
        return forwardToDLQ(ctx, cl, o.dlqTopic, r, err, usedAttempts)
    }
}
```

`baseBackoff * (1 << (attempt - 1))` — это `200ms * 2^(attempt-1)` для дефолта: 200ms, 400ms, 800ms. Для трёх попыток суммарное окно retry на одну запись — около 1.4 секунды, потом либо исцелилось, либо в DLQ.

Когда сообщение едет в DLQ, мы прикладываем headers с диагностикой:

```go
headers = append(headers,
    kgo.RecordHeader{Key: "error.class", Value: []byte(errClass(cause))},
    kgo.RecordHeader{Key: "error.message", Value: []byte(cause.Error())},
    kgo.RecordHeader{Key: "original.topic", Value: []byte(r.Topic)},
    kgo.RecordHeader{Key: "original.partition", Value: []byte(strconv.Itoa(int(r.Partition)))},
    kgo.RecordHeader{Key: "original.offset", Value: []byte(strconv.FormatInt(r.Offset, 10))},
    kgo.RecordHeader{Key: "retry.count", Value: []byte(strconv.Itoa(attempts))},
    kgo.RecordHeader{Key: "dlq.timestamp", Value: []byte(time.Now().UTC().Format(time.RFC3339Nano))},
)
```

Эти headers — единственный мост от ошибки к человеку, который потом будет разбирать DLQ. Без них в DLQ окажется голый payload, и непонятно, чем именно эта запись плоха.

`cmd/dlq-reader/main.go` — отдельный процесс, который сидит на `payments-dlq` и красиво печатает каждую запись с её headers. В реальной системе на этом месте — alerter, тикет в jira, метрика, индекс по incident-ID. У нас демонстрационный stdout: видно, что в DLQ приехали разные классы ошибок, и каждый сохранил контекст.

## Pause / Resume партиций — отдельный инструмент

Иногда in-place retry не подходит, а retry-topic кажется перебором. Например, downstream API упал на пять минут. Длинный retry-цикл с экспоненциальным backoff'ом из группы сам по себе не выкинет: franz-go heartbeat'ит независимо от обработки, поэтому coordinator считает клиента живым, пока его сетевой heartbeat-loop в порядке. Проблема стрельнёт в момент ребаланса (новый член зашёл, лидер сменился, broker упал). Если в этот момент handler сидит в backoff'е, у него есть только `RebalanceTimeout` (`rebalance.timeout.ms`, дефолт 60 секунд в franz-go v1.21.0) чтобы свернуть работу, закоммитить offset и переджойниться. Не успел — coordinator кикает клиента, partition уезжает другому члену, тот берётся за ту же работу и тоже упадёт. Pingpong на уровне всей группы. Плохо.

В franz-go есть `cl.PauseFetchPartitions` и `cl.ResumeFetchPartitions`. Это другой механизм: партиция остаётся **назначенной** consumer'у (heartbeat'ы продолжают ходить, group считает нас живыми), но `PollFetches` перестаёт отдавать новые записи с этой партиции. Можно поставить partition на паузу, делать в фоне HTTP-проверку «жив ли downstream», когда жив — снять паузу.

В этой лекции code этим не пользуется ради простоты, но знать про него надо. Мы вернёмся к pause/resume в [Доставка во внешние системы](../../../../04-reliability/04-05-external-delivery/i18n/ru/README.md), где circuit breaker естественным образом ложится на эту пару вызовов: CB перешёл в open → пауза партиций → CB вернулся в half-open → resume.

## Дубли в DLQ — это нормально

Тут тонкий момент. После того как мы записали в DLQ, мы коммитим offset основного топика. Между этими двумя действиями есть микро-окно, в которое можно упасть.

```
ProduceSync(DLQ)     ✓  ← запись уже в DLQ
[crash here]
CommitRecords         ✗  ← committed offset не сдвинулся
```

На рестарте основной топик отдаст ту же запись заново, она пойдёт по тому же пути и попадёт в DLQ второй раз. У DLQ нет защиты от дублей — ничего страшного, но обработчик DLQ должен это учитывать. Либо дедуп по `(original.topic, original.partition, original.offset)` (которые мы кладём в headers), либо просто принять, что DLQ-инциденты иногда случаются дублями.

Решается это тем же transactional outbox или Kafka transactions ([Транзакции и EOS](../../../../04-reliability/04-01-transactions-and-eos/i18n/ru/README.md)) — но только если DLQ-производство и main-consumer commit идут через одну Kafka-транзакцию. Это уже сложнее, и не каждый случай требует таких гарантий.

## Tradeoffs

Любой подход к error handling — это компромисс. Здесь они такие.

In-place retry **блокирует poll-loop**. Пока крутится backoff на одну запись, другие партиции этого consumer'а тоже стоят. Если retry длинный, отстают все. Защита — небольшое окно retry (пара секунд), а если нужно долго — переезжаем в retry-topic.

DLQ **прячет ошибки**. Если на DLQ нет alert'а и человек туда не заглядывает, через неделю там сидят 50 тысяч записей, и никто не в курсе. DLQ без operational обвязки — это «потерял и забыл». Алерт по росту lag'а DLQ-топика обязателен.

Permanent классификация **может быть ошибочной**. Если processor поторопился пометить как permanent и отправил в DLQ — restoration уже руками: replay-CLI читает DLQ и переотправляет в основной ([Retry и DLQ deep dive](../../../../04-reliability/04-04-retry-and-dlq/i18n/ru/README.md)). Поэтому permanent = «я уверен, что повтор не поможет», а не «я устал ретраить».

Headers в DLQ — **не контракт по умолчанию**. У разных команд разные конвенции по именам. У нас здесь `error.class` / `error.message` / `original.*` / `retry.count` / `dlq.timestamp`. У Confluent в их connector framework — `__error.class.name` и компания. Когда команд несколько, это надо договорить заранее, иначе один пишет, другой читает — и не находит.

## Прогон руками

Сначала топики и сид:

```sh
make topic-create-all      # payments + payments-dlq
make seed                  # 30 сообщений: 50% ok, 30% transient, 20% permanent
```

`seed` льёт три категории сообщений случайным образом по заданным процентам (`SEED_MESSAGES`, `TRANSIENT_PCT`, `PERMANENT_PCT` — переменные Makefile). Внутри permanent-доли половина — невалидный JSON (poison-pill), половина — валидный JSON с `mode=permanent`.

Дальше processor:

```sh
make run-processor
```

В выводе будут строки `OK`, `RETRY`, `PERM`, `EXH` — по каждой записи видно, что произошло. После каждого батча — counter'ы вида `ok=N retried=N dlq-perm=N dlq-exh=N`.

Параллельно (или после Ctrl+C) — DLQ reader:

```sh
make run-dlq-reader
```

Покажет каждую запись из DLQ с headers. Видно, что у permanent — `error.class=permanent`, у битого JSON — тоже permanent, но `error.message` про unmarshal. У exhausted retries (если они появятся при низком `max-retries`) — `error.class=transient`, в `error.message` будет `exhausted retries: ...`.

Сколько в DLQ всего:

```sh
make dlq-count             # суммарно по всем партициям payments-dlq
```

Очистка:

```sh
make clean                 # удаляем committed offset'ы и оба топика
```

## Что ещё попробовать

- увеличь `TRANSIENT_PCT=80` — большая часть будет крутиться по retry, но всё равно «исцелится» после 2-х попыток (это в моке захардкожено, см. константу `transientFails`); итог — 80% OK с retry, 20% в DLQ;
- поставь `MAX_RETRIES=1` и ту же `TRANSIENT_PCT=80` — большинство transient не успевает «исцелиться» за одну попытку и уходит в DLQ как exhausted; в headers увидишь `error.class=transient`, `error.message: exhausted retries: ...`;
- увеличь `BASE_BACKOFF=2s` и смотри, как замедляется обработка: на retry-цикл одной transient-записи теперь уходит около 14 секунд (`2s + 4s + 8s`); видно, как блокируется poll-loop;
- запусти processor в двух копиях с одним и тем же `GROUP` — partition'ы поделятся, retry будут идти параллельно по партициям, но **внутри партиции** обработка по-прежнему последовательная (это про concurrency, лекция [Конкурентность и lag](../../../03-05-concurrency-and-lag/i18n/ru/README.md));
- запусти `make seed PERMANENT_PCT=100` — все записи в DLQ, processor стабильно «работает» (всё коммитится), но реальный бизнес-эффект нулевой; это и есть DLQ-flood — alert должен сработать на росте lag'а DLQ.

## Дальше

Тут мы научились отделять transient от permanent и разносить их по разным маршрутам. Но retry-цикл всё ещё внутри poll-loop'а — длинные backoff'ы тут невозможны. Лекция [Retry и DLQ deep dive](../../../../04-reliability/04-04-retry-and-dlq/i18n/ru/README.md) развязывает это через цепочку retry-топиков с растущими задержками: `*-retry-30s` → `*-retry-5m` → `*-retry-1h` → `*-dlq`. Принцип такой же, диспетчер другой.

Лекция [Конкурентность и lag](../../../03-05-concurrency-and-lag/i18n/ru/README.md) — про конкурентность. Если retry на одной записи блокирует все партиции — может, стоит обрабатывать партиции параллельно? Или per-key worker pool? Будет про lag, ordering и tradeoff'ы между throughput и гарантиями порядка.

И ещё — в [Доставка во внешние системы](../../../../04-reliability/04-05-external-delivery/i18n/ru/README.md) появится `PauseFetchPartitions`. Тут мы про него только упомянули, там будет полноценный circuit breaker, который рулит этим переключателем.
