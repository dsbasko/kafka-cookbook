# 04-05 — External Delivery

В [Retry и DLQ deep dive](../../../04-04-retry-and-dlq/i18n/ru/README.md) мы разобрали retry и DLQ внутри Kafka — где причиной отказа была наша же логика обработки или соседний сервис, в который тоже пишем через Kafka. Тут другой сценарий. Из топика приходит уведомление, его надо доставить во внешний HTTP-приёмник: партнёрский webhook, push-провайдер, чужая платформа. Задача звучит просто. На практике именно тут чаще всего разваливаются end-to-end гарантии, потому что внешний даунстрим живёт по своим правилам — у него собственный rate limit, свои таймауты, свои правила безопасности и совершенно свой график отказов.

## Чем отличается «доставка наружу»

Первое — у тебя нет инвестиции во владельца receiver'а. Если ты пишешь в свою же Kafka, проблемы видны: лежит брокер, лажает producer, истекает retention. Здесь чужой стек. У него лежит app server. У него стоит nginx, который отдаёт 502. У них кончилось окно rate-limit'а, и они шлют 429 на каждый запрос. Кратчайший путь — слепо ретраить — заканчивается тем, что мы добиваем уже лежащий downstream.

Второе — у HTTP другая модель ошибок. Сетевые таймауты и обрывы — это одно. 5xx от сервера — другое (он жив, но не справляется). 4xx — третье (он жив, ты не прав). Слепо считать всё это «failed» нельзя, потому что 4xx ретраить бессмысленно — receiver уже сказал, что наш payload плохой. Хоть полгода стучись, не лучше станет.

Третье — поверхность атаки. Webhook принимает запрос откуда угодно. Чтобы receiver не получал чужие уведомления (и чтобы мы не съели чужое), вводим подписи. Чтобы при ретрае receiver не повторял ту же операцию — вводим идемпотентность. Это не «дополнительный сахар», это элементарная гигиена интеграции.

Четвёртое — backpressure. Если receiver лежит, а сообщений в Kafka сыпется тысячами в секунду — мы не должны делать тысячи попыток в секунду. Должны притормозить. И вот тут впервые становится полезен `PauseFetchPartitions`.

Дальше по делу — каждое из четырёх по очереди.

## Exponential backoff с jitter

Стандартный паттерн ретраев: первый — почти сразу, второй — через 200 мс, третий — 400, четвёртый — 800. Удвоение, пока не упрёшься в потолок (например, 5 секунд). Зачем удваивать — чтобы не добивать живой, но медленный downstream. Если он не успевает за 200 мс, дай ему секунду.

Но удвоение в чистом виде создаёт другую проблему. Представь, что тысяча наших инстансов одновременно увидели один и тот же сбой. Все они начнут ретраить через 200 мс. Через 400. Через 800. Получается синхронизированная стая — она бьёт даунстрим волнами. Лекарство — jitter. Вместо «спать ровно backoff'» — «спать случайное время в диапазоне `[0, backoff]`» (это full-jitter по AWS). Стая распыляется, нагрузка размазывается во времени.

В нашем courier это выглядит так:

```go
backoff := c.initialBackoff
for attempt := 1; attempt <= c.maxAttempts; attempt++ {
    status, err := c.send(ctx, r)
    if err == nil {
        return ..., nil
    }
    if errors.Is(err, errPermanent) {
        return ..., err
    }
    sleep := time.Duration(rand.Int64N(int64(backoff)))
    select {
    case <-ctx.Done():
        return ..., ctx.Err()
    case <-time.After(sleep):
    }
    backoff *= 2
    if backoff > c.maxBackoff {
        backoff = c.maxBackoff
    }
}
```

Тут важна одна деталь — у нас `rand.Int64N(int64(backoff))`, не `time.Sleep(backoff)`. Никакого фиксированного интервала. Каждая попытка спит случайное время в окне до текущего backoff'а. Если ты прогонишь это в тысячу параллельных инстансов, увидишь равномерное распределение пауз — без волн.

## Circuit breaker

Ретраи решают проблему «стучи разумно для одного сообщения». Они не решают «вообще перестань стучать, когда downstream лежит». Если у нас 100 сообщений в очереди, и каждое из них проходит четыре retry-попытки — это четыреста бесполезных запросов к лежащему серверу. Из них 399 заведомо лишние.

Тут заходит circuit breaker. Идея древняя — она пришла из обычной электрики. Если в цепи слишком много неуспехов подряд, размыкаем контакт. Дальше всё, что попадает в `Execute()`, мгновенно отскакивает с `ErrOpenState` без сетевого вызова. Через какое-то время (cooldown) переключаемся в Half-Open и пускаем одну пробную попытку. Получилось — закрываемся, всё снова идёт. Не получилось — снова Open на следующий cooldown.

Состояний три: Closed, Open, Half-Open. И между ними допустимо четыре перехода:

1. Closed → Open. Подряд столько-то неуспехов, размыкаемся. У нас по умолчанию пять подряд.
2. Open → Half-Open. Прошёл cooldown (`Timeout` в Settings). Попробуем одну пробу.
3. Half-Open → Closed. Проба прошла, замыкаемся.
4. Half-Open → Open. Проба упала, снова на cooldown.

Берём `sony/gobreaker/v2`, потому что он тривиальный, без зависимостей и с дженериками. Settings, которые имеют смысл:

```go
c.cb = gobreaker.NewCircuitBreaker[deliveryResult](gobreaker.Settings{
    Name:        "courier-webhook",
    MaxRequests: 1,                    // в Half-Open — ровно одна проба
    Timeout:     *cbOpenTimeout,       // сколько сидит в Open до Half-Open
    ReadyToTrip: func(counts gobreaker.Counts) bool {
        return counts.ConsecutiveFailures >= uint32(*cbConsecutive)
    },
    OnStateChange: c.onStateChange,
})
```

`MaxRequests=1` — пускаем строго одну пробу. Если разрешить десяток, в Half-Open вернётся пачка одновременных запросов на ещё-не-прогретый сервер. Это плохой UX. Один запрос — один сигнал «работает / не работает».

`ReadyToTrip` тут на consecutive-failures. Можно использовать ratio (например, 50% от 20 запросов), но для лекции consecutive нагляднее. У нас прогон ровный, неуспехи идут подряд — пять подряд = размыкаемся.

CB обнимает не каждый retry, а целиком одну доставку с её внутренними ретраями:

```go
func (c *courier) deliver(ctx context.Context, r *kgo.Record) (deliveryResult, error) {
    return c.cb.Execute(func() (deliveryResult, error) {
        return c.deliverWithRetries(ctx, r)
    })
}
```

Один Execute = одно событие для CB. Сколько внутри было retry-итераций, ему всё равно. Это удобно — у CB своя метрика «сообщений целиком провалилось». Если бы мы считали отдельные HTTP-неуспехи, CB размыкался бы от одного сложного сообщения, которому понадобилось три retry перед успехом.

## Backpressure в Kafka через PauseFetchPartitions

CB сам по себе не останавливает consumer. Он только режет HTTP-вызовы. Что делает наш poll-loop, пока CB Open? Он продолжает фетчить новые сообщения из Kafka, гонять их через CB и ловить `ErrOpenState`. Это бессмысленная активность — fetch-буфер пухнет, сообщения накапливаются, мы их «обрабатываем» (мгновенно отбивая), не коммитим, потом следующая итерация снова поднимает их же. Гонка.

Решение — на уровне Kafka-клиента сказать «пока не подтягивай новых fetch'ей». В franz-go это `cl.PauseFetchPartitions(...)` или `cl.PauseFetchTopics(...)`. После вызова PollFetches возвращает по этим топикам пустоту, пока не будет `ResumeFetchTopics` (`pkg/kgo/consumer.go:655` — pause is independent от индивидуального `PauseFetchPartitions`). Новых FetchRequest'ов на брокер не уходит, уже забуференные записи для паузнутых топиков отфильтровываются на `takeBuffered(paused)` (`pkg/kgo/consumer.go:542`), in-flight ничего не растёт. При этом heartbeat-loop продолжает работать как обычно — партиции остаются за нами, ребаланса не вызываем.

Тут только один нюанс — паузить ровно по факту перехода CB в Open рискованно. CB, бывает, флапает: 5 подряд упало, открылся, через 15 секунд проверил пробу, проба прошла, закрылся, через 200 мс снова 5 подряд, снова открылся. Pause/Resume — чисто клиентское состояние, координатор группы про них не знает (`pkg/kgo/consumer.go:655` — никаких RPC, только внутренний atomic-флаг). Поэтому «шумность» здесь не сетевая, а исключительно в логах и в графиках лагов — fetch'и то идут, то не идут пилой. Чтобы не разводить эту пилу, ставим порог: паузим, только если CB пробыл Open дольше, чем `pause-after`.

Посмотри как это сшито. Сначала колбэк CB:

```go
func (c *courier) onStateChange(name string, from, to gobreaker.State) {
    c.logger.Warn("CB state change", "name", name, "from", from, "to", to)
    switch to {
    case gobreaker.StateOpen:
        c.openSince.Store(time.Now().UnixNano())
    case gobreaker.StateHalfOpen, gobreaker.StateClosed:
        c.openSince.Store(0)
        if c.paused.Swap(false) {
            c.cl.ResumeFetchTopics(c.topics...)
            c.logger.Info("partitions resumed", "topics", c.topics)
        }
    }
}
```

Тут только фиксируем время перехода в Open и резюмим, если выходим из него. Сама пауза дёргается из poll-loop'а:

```go
func (c *courier) maybePauseOnLongOpen() {
    since := c.openSince.Load()
    if since == 0 {
        return
    }
    if time.Since(time.Unix(0, since)) < c.pauseAfter {
        return
    }
    if c.paused.CompareAndSwap(false, true) {
        c.cl.PauseFetchTopics(c.topics...)
        c.logger.Warn("partitions paused — CB stayed Open too long", ...)
    }
}
```

`maybePauseOnLongOpen` зовётся в начале каждой итерации main loop'а перед PollFetches. Если CB давно Open — паузим, в следующий PollFetches возвращается пустота, мы не давим на даунстрим. Когда CB сам перейдёт в Half-Open и потом Closed, колбэк вызовет ResumeFetchTopics — fetch'и пойдут как обычно.

Получается две петли управления: HTTP-уровень (CB ловит большую часть мусорных вызовов) и Kafka-уровень (PauseFetchPartitions глушит источник, если CB долго не оправляется). Они не дублируют друг друга — они работают на разных временных горизонтах. CB — секунды. Kafka-pause — десятки секунд и больше.

## HMAC и Idempotency-Key

Receiver не верит нам по-умолчанию. Он сидит на публичном :8090, кто угодно может туда POST'ить. Чтобы он отличал нас от шумителей, добавляем подпись. Берём общий секрет (`HMAC_SECRET`), считаем HMAC-SHA256 от тела запроса, кладём в header `X-Signature`. Receiver делает то же самое со своей копией секрета и сравнивает. Если совпало — это мы. Если нет — выкидывает.

```go
mac := hmac.New(sha256.New, c.hmacKey)
mac.Write(body)
signature := hex.EncodeToString(mac.Sum(nil))
req.Header.Set("X-Signature", signature)
```

Нюанс — секрет общий. Это симметрично, никакой PKI. Для интеграции с одним партнёром норм. Если партнёров много и не хочется им раздавать один секрет — вместо HMAC берут асимметричную подпись (Ed25519 или RS256 в стиле JWS). Но это уже сильно дороже, и для лекции про доставку — оверкилл.

Idempotency-Key — отдельная история. Receiver получит наш запрос, успешно его выполнит, попробует ответить — а у нас уже истёк http-timeout. Мы видим failure, ретраим. Receiver получает второй такой же запрос. По-хорошему он должен опознать, что это повтор, и не выполнять операцию заново. Для этого мы кладём стабильный идентификатор в header:

```go
idem := fmt.Sprintf("%s:%d:%d", r.Topic, r.Partition, r.Offset)
req.Header.Set("Idempotency-Key", idem)
```

Ключ из `topic:partition:offset` — стабильный навсегда. Тот же record при ретрае даёт тот же ключ. Receiver хранит таблицу обработанных ключей с TTL (день, неделя — зависит от бизнеса). Видит повтор — возвращает тот же ответ, не дёргая бэкенд.

В нашем mock-webhook мы только логируем `Idempotency-Key`, дедупликацию не делаем — лекция не про receiver'ов. Но в реальности это самая важная часть end-to-end exactly-once с внешним даунстримом. Без неё ретраи превращаются в дубли заказов.

## Классификация HTTP-кодов

Это короткое, но важное место. Не все HTTP-неуспехи одинаково ретраебельны:

- 2xx — успех, коммитим offset.
- 4xx (кроме 408 и 429) — receiver сказал «это плохой запрос». Сколько ни стучи, лучше не станет. Это **permanent** для нас. Не ретраим, отдельным маркером отдаём наружу, в нашем случае — коммитим (запись «потеряна», но мы хотя бы не блокируем партицию).
- 408 (Request Timeout), 429 (Too Many Requests) — формально 4xx, но семантика «попробуй позже». Ретраебельны.
- 5xx — сервер не справился. Ретраебельно.
- network error / timeout — всё, что из транспорта (`net.Error`, обрыв, таймаут http.Client). Ретраебельно.

В коде это выглядит так:

```go
switch {
case resp.StatusCode >= 200 && resp.StatusCode < 300:
    return resp.StatusCode, nil
case resp.StatusCode == http.StatusRequestTimeout,
    resp.StatusCode == http.StatusTooManyRequests,
    resp.StatusCode >= 500:
    return resp.StatusCode, fmt.Errorf("retriable status %d", resp.StatusCode)
default:
    return resp.StatusCode, fmt.Errorf("status %d: %w", resp.StatusCode, errPermanent)
}
```

Внутреннее правило: всё, что мы пометили `errPermanent`, run() распознаёт через `errors.Is` и спокойно коммитит — даже без доставки. Мусорное сообщение не должно блокировать партицию вечно. Если потерять его недопустимо — рядом обязательно нужен retry-pipeline и DLQ из [Retry и DLQ deep dive](../../../04-04-retry-and-dlq/i18n/ru/README.md), чтобы permanent-сообщение хотя бы попадало в DLQ-инцидент-лог. В этой лекции мы это упростили до коммита-без-доставки, чтобы не рассыпать тему.

## Что делает mock-webhook

`cmd/mock-webhook/main.go` — обычный HTTP-сервер на стандартной библиотеке, без зависимостей. Принимает POST `/deliver`. По дайс-роллу решает: 200, 503 или «зависнуть» (имитация таймаута). Доли отказов — `FAIL_RATE_503` и `FAIL_RATE_TIMEOUT`. Ещё есть `/health` и `/stats` — health для healthcheck'а в docker, stats для просмотра текущих счётчиков.

Самая полезная часть для отладки — `/stats`:

```go
mux.HandleFunc("/stats", func(w http.ResponseWriter, _ *http.Request) {
    s := stats.snapshot()
    fmt.Fprintf(w, `{"total":%d,"ok":%d,"fail_503":%d,"fail_timeout":%d}\n`, ...)
})
```

После прогона `make seed` + courier видно: сколько запросов мы реально отправили, сколько попали в 503, сколько повисли в таймаут. Если у тебя `FAIL_RATE_503=0.5` и в `/stats` написано `total=120 ok=60 fail_503=60` — всё ровно.

Mock не валидирует HMAC и не дедупит по Idempotency-Key. Только логирует оба header'а. Этого хватает, чтобы убедиться визуально, что courier их кладёт. В реальном receiver'е валидация подписи — обязательна; дедуп — крайне рекомендована.

## Как это запустить

Стенд из корня репозитория должен быть поднят (`docker compose up -d`). Дальше из директории лекции.

```sh
make topic-create        # создать topic notifications
make up-mock             # mock-webhook на :8090, без отказов
make seed                # 100 уведомлений в notifications

# в другом терминале:
make run-courier         # courier подписывается, доставляет, печатает logs
```

Сценарий с отказами — это самое интересное:

```sh
# 50% запросов падают с 503
make chaos-fail-50

# в логах courier'а пойдут retry, потом CB state change → Open,
# потом partitions paused — CB stayed Open too long.
# через cb-open-timeout → Half-Open → Closed → partitions resumed.

make chaos-clear         # вернуть mock в нормальный режим
```

Если хочется поиграть с CB-чувствительностью:

```sh
make run-courier CB_TRIP_AFTER=3 PAUSE_AFTER=3s CB_OPEN_TIMEOUT=5s
```

Так CB размыкается уже от трёх подряд, пауза партиций включается через 3 секунды Open, и cooldown — 5 секунд. На демо это куда нагляднее, чем дефолтные 5/10/15.

## Что вынести с лекции

Доставка наружу — последнее звено reliability. Из Kafka мы можем гарантировать exactly-once у себя. До границы с внешним даунстримом гарантия превращается в at-least-once. Чтобы это at-least-once не разломало receiver'а — нужен идемпотентный приёмник через Idempotency-Key. Без него любой наш retry превращается в дубль заказа / уведомления / списания.

Сами retry должны быть с backoff и jitter — иначе мы добиваем уже лежащий downstream. Сверху retry — circuit breaker, который защищает от бесполезных вызовов целыми пакетами. Сверху CB — Kafka-уровневый pause, который не даёт fetch-буферу пухнуть, пока CB долго в Open. И всё это сшивается через `OnStateChange` callback — он мост между CB и `PauseFetchTopics`.

Эта лекция закрывает модуль 04 (надёжность). Дальше — модуль 05 про контракты: Protobuf, Schema Registry, эволюция схем. Там уже не про «как доставить», а про «что именно мы доставляем и как менять формат, не ломая получателей».
