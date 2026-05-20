# 09-02 — Push Notifications

Use case про доставку уведомлений в три внешних канала. Один Kafka-топик на входе, три канала-получателя, у каждого свой retry-пайплайн и DLQ. Это сборка из четырёх лекций сразу — outbox-паттерн ([Outbox-паттерн](../../../../04-reliability/04-03-outbox-pattern/i18n/ru/README.md)) забыли, но retry/DLQ ([Retry и DLQ deep dive](../../../../04-reliability/04-04-retry-and-dlq/i18n/ru/README.md)), CB и HMAC ([Доставка во внешние системы](../../../../04-reliability/04-05-external-delivery/i18n/ru/README.md)), микросервисная гарантия с dedup ([Коммуникация микросервисов](../../../01-microservices-comm/i18n/ru/README.md)) и Protobuf ([Protobuf в Go](../../../../05-contracts/05-02-protobuf-in-go/i18n/ru/README.md)) тут все по делу.

## Что собираем

Сценарий простой. Какой-то upstream (продьюсер) шлёт `Notification` в `notification-events`. У сообщения есть поле `channel`. Допустимых значений три:

1. `firebase` — push в Android-приложение.
2. `apns` — push в iOS-приложение.
3. `webhook` — HTTP-вызов на сторонний URL клиента.

Дальше сервис должен дойти до соответствующего внешнего получателя по HTTP, переживать падения с retry и backoff, не падать целиком при долгом отказе одного канала, не дублировать доставку, держать историю по каждому уведомлению.

Внутри получается так:

```
notification-events
       │
       ▼
 notification-router (consumer)
       │
       ├──► notification-firebase ──► firebase-sender ──► mock-firebase
       │           │
       │           ├──► notification-firebase-retry-30s
       │           ├──► notification-firebase-retry-5m
       │           └──► notification-firebase-dlq ──► firebase-dlq-consumer
       │
       ├──► notification-apns     ──► apns-sender     ──► mock-apns
       │           └── (тот же retry/dlq)
       │
       └──► notification-webhook  ──► webhook-sender  ──► mock-webhook
                   └── (тот же retry/dlq)
```

14 топиков. Звучит много. На каждый канал — 4 (main + retry-30s + retry-5m + dlq), один общий entry и один router-DLQ для записей, у которых `channel` вообще не выставлен (proto3-дефолт = `CHANNEL_UNSPECIFIED`) либо незнакомое значение. Если бы каналов было 10, вырос бы только список топиков, не код.

## Зачем разные топики на каждый канал

Можно было бы держать один `notifications` и фильтровать в каждом sender'е по `channel`. Так делать не надо. Причины:

- consumer-группы каналов скейлятся независимо. Если firebase тормозит, не хочется, чтобы apns ждал;
- retry-задержки у каналов могут отличаться. Webhook'у пять минут не страшно, а push в банковское приложение лучше попробовать через тридцать секунд;
- DLQ читать удобнее, когда видно сразу: `notification-firebase-dlq` — у нас лежит проблема в одном канале, остальные живы;
- лимит топиков в Kafka — десятки тысяч на брокер. Лишние десять штук погоды не сделают.

Цена — лишний форвард в router'е. Терпимо.

## Что показывает наш код

Сначала router. Тонкий consumer на `notification-events`. Достаёт `Notification` из protobuf-payload и форвардит byte-в-byte в нужный канал-топик по полю `channel`. Без лишней обработки и без прикладной логики.

```go
out = append(out, &kgo.Record{
    Topic:   dest,
    Key:     r.Key,
    Value:   r.Value,
    Headers: appendRouterHeaders(r.Headers, o.NodeID, n.GetChannel().String()),
})
```

`destinationFor` — switch по enum-каналу, без чудес. Headers докладываем `router-node` и `channel` — для трейсинга. Дальше — `ProduceSync` всем накопленным батчем, потом `CommitRecords` входному. Та же at-least-once гарантия, что в outbox: между produce и commit окно для дублей, ловит его dedup в sender'е.

Если у записи `channel=CHANNEL_UNSPECIFIED` (proto3-дефолт у кривого продьюсера) или enum, который router не знает, она уходит в `notification-events-dlq` — отдельный router-DLQ-топик. Молча дропать нельзя: при `proto3` забыть выставить поле — лёгкое, поэтому разделяем «правильно сроутили» и «не нашли куда» на уровне топика, а не молчания.

Главное в этом куске — не появилось доменной логики. Router тупой и быстрый. Сложность вся в sender'ах.

### Sender — один на канал

Каждый канал-sender — это один процесс с тонкой обвязкой (вариантов канала три, sender по штуке на каждый). Подписан на свой main-топик и две retry-ступени:

```go
stages := []Stage{
    {Topic: *mainTopic,    Delay: 0,             NextTopic: *retry30Topic},
    {Topic: *retry30Topic, Delay: *delay30,      NextTopic: *retry5mTopic},
    {Topic: *retry5mTopic, Delay: *delay5m,      NextTopic: ""},
}
```

Одна consumer-группа на все три топика. На retry-ступенях перед обработкой ждём, пока `record.Timestamp + stage.Delay` не наступит — записал записал в retry-30s в 12:00:00, ждём до 12:00:30. Это блокирует poll-loop, но по делу: пайплайн должен быть нагляден. В проде делают изящнее (отдельный поток на каждый retry-топик или `PauseFetchPartitions` — последнее в [Доставка во внешние системы](../../../../04-reliability/04-05-external-delivery/i18n/ru/README.md)).

```go
if st.Delay > 0 {
    if err := waitUntilDue(ctx, r.Timestamp, st.Delay); err != nil {
        return err
    }
}
```

Дальше — попытка доставки. Под защитой circuit breaker:

```go
result, err := s.cb.Execute(func() (deliveryResult, error) {
    return s.deliverWithRetries(ctx, &n)
})
```

Внутри `deliverWithRetries` — обычный backoff с jitter, до `MaxAttempts` раз внутри одной ступени. CB смотрит снаружи: если N подряд `Execute()`-ов вернули ошибку, переходит в Open и режет дальнейшие звонки. Половина [Доставка во внешние системы](../../../../04-reliability/04-05-external-delivery/i18n/ru/README.md) оказалась тут как есть — паттерн универсальный.

### Куда уходит record после неудачи

Три варианта:

1. **Успех** — пишем `notifications_log(status='delivered', ...)` плюс `processed_events(consumer, notification_id)` в одной транзакции. Коммитим offset в Kafka.
2. **Permanent ошибка** (4xx кроме 408/429, или невалидный protobuf) — сразу в DLQ, мимо retry. Коммитим (можно).
3. **Transient ошибка** (5xx, 408, 429, network, timeout) и retries исчерпаны — форвардим в `nextTopic` ступени. Коммитим.

```go
target := st.NextTopic
reason := "next-retry"
if permanent {
    target = s.opts.DLQTopic
    reason = "permanent"
} else if target == "" {
    target = s.opts.DLQTopic
    reason = "exhausted"
}
```

Пустой `NextTopic` у последней ступени — сигнал «дальше уже DLQ». Headers у форварда докладываются: `retry.count`, `error.class`, `error.message`, `original.topic`, `previous.topic`, `forward.reason`. В DLQ потом видно весь маршрут — кто упал, где упал, сколько раз.

### Dedup и эффективно-однократность

Между `cb.Execute()` (там может быть несколько HTTP-попыток) и записью в Postgres есть окно. Если процесс упадёт после успешной доставки, но до commit'а offset'а — при рестарте sender прочитает то же сообщение снова. Receiver к этому моменту уже видел уведомление по `Idempotency-Key` (это `notification_id`) — он-то не задвоит. А вот наша таблица `notifications_log` могла бы:

```go
err = pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error {
    consumer := string(s.opts.Channel) + "-sender"
    tag, err := tx.Exec(ctx, dedupSQL, consumer, n.GetId())
    if err != nil { return fmt.Errorf("dedup: %w", err) }
    if tag.RowsAffected() == 0 {
        return nil // уже обрабатывали — выходим без INSERT'а в notifications_log
    }
    _, err = tx.Exec(ctx, insertHistorySQL, ...)
    return err
})
```

Гейт и бизнес-вставка в одной транзакции — иначе между ними мог бы случиться краш, и при рестарте гейт скажет «уже обработано», а лог так и не появится. Тот же приём, что в [Коммуникация микросервисов](../../../01-microservices-comm/i18n/ru/README.md).

### HMAC и идемпотентность снаружи

Каждый HTTP-запрос идёт с двумя ключевыми headers:

```go
req.Header.Set("Idempotency-Key", n.GetId())
req.Header.Set("X-Signature", sig) // hex(HMAC-SHA256(secret, body))
```

`Idempotency-Key` — `notification_id`. Тот же id у всех ретраев одного уведомления, значит receiver на стороне Firebase / APNs / webhook'а понимает: «о, это я уже видел, не пуляю в пользователя второй раз». В моках мы это только логгируем, в проде там настоящая логика receiver'а.

`X-Signature` — HMAC-SHA256 от тела с общим секретом. Receiver проверяет — кто-то посторонний с того же IP не насыпет ему левых push-ов.

### DLQ как отдельный потребитель

DLQ-топик — терминал. Туда летит то, что прошло все ступени и не доставилось, или то, что прилетело сразу как `permanent`. Sender в DLQ сам не пишет — там отдельный процесс с режимом `-mode=dlq`:

```go
case "dlq":
    err := RunDLQ(ctx, DLQOpts{
        NodeID:    *nodeID,
        Channel:   d.Channel,
        DLQTopic:  *dlqTopic,
        Group:     *dlqGroup,
        DSN:       dsn,
        FromStart: *fromStart,
    })
```

Этот процесс пишет `notifications_log(status='dlq', last_error=..., attempts=...)` — историю с указанием, что доставить не получилось. В реальной системе сюда же лепили бы алёрт в Slack, метрику в Prometheus, страничку в админке для ручного replay, тред в on-call канале.

## Mock-сервисы

Три stdlib-only HTTP-хендлера. Один и тот же код, разный порт и имя:

- `cmd/mock-firebase/main.go` на :8091
- `cmd/mock-apns/main.go` на :8092
- `cmd/mock-webhook/main.go` на :8093

Каждый принимает POST `/send`. По `FAIL_RATE_503` отвечает 503, по `FAIL_RATE_TIMEOUT` зависает на N секунд, есть пинг `/health` для healthcheck'а, в остальных случаях отдаёт 200. Это копия паттерна из [Доставка во внешние системы](../../../../04-reliability/04-05-external-delivery/i18n/ru/README.md), размноженная на три канала.

```go
case dice < fail503:
    w.Header().Set("Retry-After", "1")
    http.Error(w, ..., http.StatusServiceUnavailable)
case dice < fail503+failTimeout:
    select {
    case <-time.After(time.Duration(timeoutHangSec) * time.Second):
        w.WriteHeader(http.StatusGatewayTimeout)
    case <-r.Context().Done():
        return
    }
default:
    w.WriteHeader(http.StatusOK)
```

Каждый mock — самостоятельный go-модуль, собирается отдельным Dockerfile через `go mod init` на лету. Это сделано осознанно: чтобы образы не тащили половину workspace курса.

Для интеграционного теста — отдельная история. Там mock'и поднимаются прямо в тесте через `httptest.NewServer` с импортом `internal/mockserver`. Без docker, на свободных портах. Один и тот же handler-template, разное окружение.

## Запуск вручную

Стенд из корневого `docker-compose.yml` уже работает. Дальше:

```sh
make up                  # Postgres :15441 + три mock'а в docker
make db-init             # таблицы notifications_log + processed_events
make topic-create-all    # 14 топиков, P=6 RF=3
make run-router &        # форварды по каналам
make run-firebase-sender &
make run-firebase-dlq &  # отдельно — записывает в notifications_log status='dlq'
# то же для apns и webhook (run-apns-sender / run-apns-dlq / run-webhook-sender / run-webhook-dlq)
make seed                # 100 уведомлений в notification-events
make db-history          # видим разбивку по channel/status
```

В норме всё ляжет в `delivered`. Чтобы посмотреть retry и DLQ:

```sh
make chaos-fail-50       # перезапуск mock'ов с FAIL_RATE_503=0.5
make seed
make db-history          # появятся строки status='dlq' (если retry не помогли)
make mock-stats          # видим, сколько mock'и реально отдали 503
```

Дефолтные параметры sender'а — `delay-30s=30s`, `delay-5m=5m`. Для интерактивной демонстрации это слишком долго: send'у в retry-30s ждать полминуты, чтобы попробовать снова. Поэтому есть флаги `-delay-30s` и `-delay-5m` — поставь, например, `2s` и `5s`, чтобы пайплайн прокручивался на глазах.

## Интеграционный тест

Самая интересная часть. Файл `test/integration_test.go` под build-tag `integration`. Запускается через `make test-integration` и требует, чтобы Kafka и Postgres стояли.

Что делает:

1. Поднимает три `httptest.Server` с `FAIL_RATE_503=0.7` (агрессивно — чтобы хотя бы часть исчерпала retry и попала в DLQ).
2. Стартует router, по одному sender'у на канал в `deliver`-режиме и по одному в `dlq`-режиме — все семь штук как горутины внутри теста.
3. Шлёт 200 уведомлений в `notification-events` round-robin'ом по каналам.
4. Ждёт, пока `notifications_log.delivered + notifications_log.dlq == 200`. Это и есть критерий «пайплайн отработал».
5. Если `dlq > 0` — переключает mock'и на `FAIL_RATE_503=0`, ждёт стабилизации, потом перечитывает DLQ-топики и публикует записи обратно в main с новым `notification_id` (`replay-*`). Это и есть DLQ replay.
6. Проверяет, что после replay `delivered` вырос хотя бы на половину переотправленных.
7. Останавливает все ноды, проверяет, что никто не упал по необработанной ошибке.

```go
if lastSnap.dlq > 0 {
    fbCfg.set(0.0, 0.0, 5)   // liveMockHandler читает атомарно,
    apnsCfg.set(0.0, 0.0, 5) // смена видна на следующем запросе
    whCfg.set(0.0, 0.0, 5)
    replayed, err := replayDLQ(root, bootstrap)
    threshold := baseline.delivered + replayed/2
    // ждём, пока delivered ≥ threshold
}
```

Здесь есть нюанс. `mockserver.Handler(cfg, stats)` замораживает `cfg` в замыкании, поэтому в тесте поверх лежит тонкая обёртка `liveMockHandler`: она держит `mockConfig` с `atomic.Value` полями и читает их при каждом запросе. Смена fail rate через `cfg.set(...)` срабатывает на ближайшем входящем запросе, перевешивать `http.Handler` не нужно — `http.Server.Handler` обычное поле без atomic, race с in-flight запросами поймал бы `go test -race`.

200 уведомлений вместо 5000 из изначального плана — для скорости прогона на dev-машине. Логика та же, тест отрабатывает за 12-15 секунд. Если хочется реальной нагрузки — поменяй `totalNotifications` константу.

## Где это в курсе

Use case собирает в одно:

- [Retry и DLQ deep dive](../../../../04-reliability/04-04-retry-and-dlq/i18n/ru/README.md) — retry-топики с задержкой и DLQ как терминал
- [Доставка во внешние системы](../../../../04-reliability/04-05-external-delivery/i18n/ru/README.md) — circuit breaker, HMAC, exponential backoff с jitter, мок-webhook паттерн
- [Protobuf в Go](../../../../05-contracts/05-02-protobuf-in-go/i18n/ru/README.md) — Protobuf для wire-формата
- [Коммуникация микросервисов](../../../01-microservices-comm/i18n/ru/README.md) — at-least-once + dedup на consumer'е через `processed_events`

Чего тут осознанно нет:

- Schema Registry. Здесь byte-в-byte protobuf без `magic byte` + `schema_id`. Отдельная лекция ([Schema Registry](../../../../05-contracts/05-03-schema-registry/i18n/ru/README.md)) показывает, как добавить SR — паттерн ортогональный, в этот use case встраивается без изменений в логике.
- [Outbox-паттерн](../../../../04-reliability/04-03-outbox-pattern/i18n/ru/README.md). Выходное сообщение в `notification-events` пишется напрямую — мы не моделируем write-side с базой. Если бы делали, добавили бы `outbox` таблицу и publisher, но это удлинило бы пример без новых уроков.
- gRPC API. Этот use case — про async-доставку. gRPC-фронт показан в [Коммуникация микросервисов](../../../01-microservices-comm/i18n/ru/README.md) и [Гибрид gRPC + Kafka](../../../../06-communication-patterns/06-04-hybrid-grpc-and-kafka/i18n/ru/README.md).
- Реальные APNs / Firebase credentials. Cert-flow для APNs и FCM HTTP v1 token-exchange — отдельная история, в курсе она out of scope. Архитектура канала тут демонстрируется на mock'ах.

## Что попробовать руками

После того как пайплайн заработает, есть несколько хороших экспериментов:

- сделать `make chaos-fail-50` с агрессивным `FAIL_RATE_503=0.9` и посмотреть, как CB sender'а тормозит при затяжном отказе. В логах видно `CB ...: closed → open`.
- остановить `firebase-sender` посреди нагрузки. После рестарта sender добирает с того же offset'а — записи не теряются. На стороне Postgres дублей нет — dedup-гейт держит.
- запустить два sender'а одного канала с разным `-node-id`. Они подцепятся в одну consumer-группу, поделят партиции пополам. Скейл — без изменений в коде.
- вручную замерджить retry-30s и retry-5m в один топик с задержкой 1 минута и посмотреть, как меняется поведение. Подсказка: в `Stage{}` это меняется в одном месте.

## Файлы

```
.
├── README.md                          # вот этот текст
├── Makefile                           # все команды
├── docker-compose.override.yml        # Postgres :15441 + 3 mock'а
├── db/init.sql                        # notifications_log + processed_events
├── proto/notifications/v1/            # Notification + Channel enum
├── gen/                               # сгенерированный Go-код
├── buf.yaml / buf.gen.yaml            # конфиг buf
├── cmd/
│   ├── notification-router/           # consumer на notification-events → каналы
│   ├── firebase-sender/               # тонкая обёртка над sender.Main
│   ├── apns-sender/
│   ├── webhook-sender/
│   ├── mock-firebase/                 # HTTP mock + Dockerfile, stdlib-only
│   ├── mock-apns/
│   ├── mock-webhook/
│   └── seed-tool/                     # make seed
├── internal/
│   ├── router/router.go               # логика router'а
│   ├── sender/sender.go               # retry + CB + HMAC + БД (общий код всех каналов)
│   ├── sender/cmdmain.go              # CmdDefaults + flags для cmd-обёрток
│   └── mockserver/server.go           # handler-фабрика для теста
└── test/integration_test.go           # end-to-end тест с DLQ replay
```
