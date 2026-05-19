# 06-05 — Saga: Choreography vs Orchestration

Распределённая транзакция между сервисами. Постгресовый `BEGIN/COMMIT` тут не работает — у каждого сервиса своя БД, общего двухфазного коммита нет, никто не подпишется на 2PC поверх Kafka. Поэтому мы строим бизнес-транзакцию иначе: цепочкой локальных шагов с компенсациями. Это и есть сага.

В лекции — два способа собрать сагу. Choreography: сервисы общаются событиями через Kafka, никто не «дирижирует». Orchestration: один сервис с saga_state в Postgres ведёт каждую сагу за руку. Один и тот же бизнес-сценарий — разная архитектура. Разные плюсы и минусы. Цель — увидеть это руками, через код, который реально запускается.

## Сценарий

Покупатель оформил заказ на `N` копеек. Чтобы заказ доехал, нужно три шага:

1. **Payment** — авторизовать оплату.
2. **Inventory** — зарезервировать товар на складе.
3. **Shipment** — назначить курьера и поехать.

Если на любом шаге что-то ломается — откатывать предыдущие. Деньги списали, товара нет — рефанд. Зарезервировали, курьеров не нашлось — отпустить резерв и тот же рефанд. Откаты не пытаются «вернуть всё назад атомарно»: каждое отменяющее действие — отдельный шаг, со своим успехом или провалом, и оно публикуется как такое же событие, как и прямое.

Это и значит «компенсация»: противоположное действие, выполненное явным шагом.

## Где между ними разница

В саге всегда есть две роли. Кто-то эти шаги выполняет (исполнители), и кто-то знает порядок (координация). Choreography размазывает координацию по исполнителям: каждый сервис подписан на «свой» upstream-эвент и публикует «свой» downstream. Никто не видит сагу целиком.

Orchestration отделяет координацию в отдельный сервис — оркестратор. Он один знает, кто после кого, у него saga_state в БД, он шлёт исполнителям команды и ждёт ответа.

Сравни на одном уровне:

| | choreography | orchestration |
|---|---|---|
| Кто знает порядок шагов | размазано | один сервис |
| Где состояние саги | нигде целиком (в каждом сервисе своя часть) | в saga_state в Postgres |
| Связность сервисов | низкая (только через топики) | средняя (исполнители знают про cmd/reply контракт) |
| Добавить новый шаг | подписаться на нужный эвент и публиковать новый | поправить state-машину оркестратора |
| Видимость хода саги | только через лог топиков | один SELECT |
| Риск зацикливания эвентов | реальный, нужно следить | нулевой, оркестратор не зациклится сам с собой |
| Дебаг сложного флоу | тяжело | проще |

Главный аргумент за choreography — низкая связность. Главный аргумент против — нет одного места, где видно всю сагу. На простых сценариях из 2–3 шагов выигрывает choreography. На длинных и сложных — orchestration. Маркер «сложный» — больше четырёх шагов или ветвистая логика «после X иди в Y или в Z в зависимости от...».

## Choreography

Топики (`saga-choreo.<service>-<verb>`):

```
order-requested      ─→ payment-completed ─→ inventory-reserved ─→ shipment-scheduled  (счастливый путь)
                       └ payment-failed                                                  (терминал FAILED)
                                              └ inventory-failed ─→ payment-refunded   (откат деньгами)
                                                                  └ shipment-failed     ─→ inventory-released ─→ payment-refunded
```

Никаких `*-cmd` и `*-reply` тут нет. Только факты о произошедшем: «оплата завершилась», «резерв упал», «курьер не нашёлся». Сервис, у которого есть что компенсировать, подписан на «фейловые» эвенты ниже по цепочке.

Кто на что подписан:

- **payment-service** слушает `order-requested`, `inventory-failed`, `inventory-released`. На `order-requested` — тянется к платёжке (в учебке — фейк через `FAIL_RATE`). На два других — публикует `payment-refunded`.
- **inventory-service** слушает `payment-completed`, `shipment-failed`. На первое — резервирует. На второе — отпускает резерв.
- **shipment-service** слушает `inventory-reserved`. Назначает доставку или валит её.
- **order-service-choreo** слушает все девять топиков и собирает в памяти таймлайн каждой саги. Это observability-наблюдатель, добавлен только чтобы было видно ход саги в одном терминале. В реальной системе эту функцию закрывает трейсинг, отдельного сервиса для неё не заводят.

Главный момент, в который надо поверить: каскад компенсации сам по себе — тоже цепочка эвентов. `shipment-failed` вызывает действие у `inventory-service`, тот публикует `inventory-released`, его уже ловит `payment-service` и идёт рефанд. Никто не звонит «по списку откатить всех». Каждое звено реагирует на своё.

Сам обработчик внутри `payment-service` — обычная диспатч-обёртка по топику. Вот она целиком:

```go
case sagaio.TopicChoreoOrderRequested:
    var evt sagav1.OrderRequested
    if err := sagaio.Unmarshal(r, &evt); err != nil {
        return err
    }
    now := timestamppb.New(time.Now().UTC())
    if shouldFail(failRate) {
        return sagaio.Produce(ctx, cl, sagaio.TopicChoreoPaymentFailed, evt.GetSagaId(),
            &sagav1.PaymentFailed{
                SagaId: evt.GetSagaId(), Reason: "card declined", OccurredAt: now,
            })
    }
    paymentID := "pay-" + uuid.NewString()[:8]
    return sagaio.Produce(ctx, cl, sagaio.TopicChoreoPaymentCompleted, evt.GetSagaId(),
        &sagav1.PaymentCompleted{
            SagaId: evt.GetSagaId(), PaymentId: paymentID,
            AmountCents: evt.GetAmountCents(), Currency: evt.GetCurrency(),
            OccurredAt: now,
        })
```

На что смотреть: payment ничего не знает про следующий шаг. Он ловит запрос, тянется к платёжке, публикует факт. Inventory — это уже не его проблема. И ровно так же он реагирует на `inventory-failed`: публикует `payment-refunded` и забывает.

### Что в этом неприятного

Подписей много, никто не видит сагу целиком. Если три месяца спустя добавить «после shipment ещё нотификация» — это не правка одного файла, это разобраться, кто на что подписан, не зацикливается ли новый эвент на старого слушателя, не сломаются ли существующие саги в полёте.

И — важный момент — `payment-refunded` приходит из двух разных причин: после `inventory-failed` и после `inventory-released`. Сервис должен быть готов к этому. Если он сделает `if reason == "shipment-cascade" return refund` — упадёт на втором сценарии. Идемпотентность по `saga_id` тут обязательна, иначе сага дважды забирает деньги обратно.

## Orchestration

Тот же сценарий, но в центре — `orchestrator`. У него своя БД на `15435`, в ней таблица `saga_state` с одной строчкой на одну сагу. Топики поделены на пары `cmd/reply`:

```
saga-orch.place-order        ─ оркестратор слушает (вход)
saga-orch.payment-cmd        ─ слушает payment-service[orch]
saga-orch.payment-reply      ─ слушает оркестратор
saga-orch.inventory-cmd      ─ слушает inventory-service[orch]
saga-orch.inventory-reply    ─ слушает оркестратор
saga-orch.shipment-cmd       ─ слушает shipment-service[orch]
saga-orch.shipment-reply     ─ слушает оркестратор
```

Шесть исполнительских топиков плюс один входной — три сервиса по паре `cmd/reply` плюс топик-вход. Можно посчитать. Это не магия.

Сервисы исполнители тут проще, чем в choreography. Они обрабатывают `<X>Command`, делают своё дело (в учебке — псевдо), отвечают `<X>Reply`. Не знают, что было до них и что будет после. Не знают про компенсации в смысле «правильного порядка». Им просто прилетит `payment-cmd` с `action=REFUND` — они и сделают рефанд.

Логика саги — целиком в оркестраторе. Это конечный автомат. Шаги называются `current_step` в `saga_state`:

```
                place-order
                    │
                    ▼
            AWAITING_PAYMENT
              │           │
       ok=true            ok=false
              │              │
              ▼              ▼
       AWAITING_INVENTORY  DONE/FAILED
         │             │
    ok=true          ok=false
         │             │
         ▼             ▼
   AWAITING_SHIPMENT  COMPENSATING_PAYMENT (refund)
     │            │
ok=true          ok=false
     │            │
     ▼            ▼
DONE/SUCCESS  COMPENSATING_INVENTORY (release)
                  │
                  ▼
               COMPENSATING_PAYMENT (refund)
                  │
                  ▼
               DONE/FAILED
```

Каждый узел графа — состояние строки в `saga_state`. Каждое ребро — приход reply-эвента, ведущий к UPDATE этой строки и публикации следующего command'а.

Реальный код перехода после успешного `payment.AUTHORIZE` — вот он:

```go
if rep.GetOk() {
    pid := rep.GetPaymentId()
    if err := updateSaga(ctx, pool, rep.GetSagaId(),
        stepAwaitingInventory, statusRunning, "payment.authorized",
        &pid, nil, nil, nil); err != nil {
        return err
    }
    return sagaio.Produce(ctx, cl, sagaio.TopicOrchInventoryCmd, rep.GetSagaId(),
        &sagav1.InventoryCommand{
            SagaId:      rep.GetSagaId(),
            Action:      sagav1.InventoryAction_INVENTORY_ACTION_RESERVE,
            CustomerId:  row.customerID,
            AmountCents: row.amountCents,
        })
}
```

Что тут видно: сначала UPDATE состояния, потом ProduceSync следующего command'а. Тот же паттерн на каждом ребре графа — поэтому весь оркестратор укладывается в три обработчика плюс entry-point. И — внимательно — тут есть слабое место.

### Где саге больно даже в orchestration

UPDATE прошёл, потом крашнулся процесс перед ProduceSync. saga_state говорит «AWAITING_INVENTORY», а команды никто не послал. Сага зависла. В production это закрывают transactional outbox'ом (см. `04-03`) — UPDATE и INSERT в outbox в одной TX, отдельный publisher шлёт в Kafka и помечает запись как published. В этой лекции мы намеренно делаем проще, чтобы фокус был на state-машине, а не на инфраструктуре. Запомни: самостоятельные UPDATE + Produce — это at-least-once с риском подвисания, и лечится outbox'ом.

Второе слабое место — повторные сообщения от самого исполнителя. Reply может прийти дважды (рестарт consumer'а до commit'а offset'а). В коде лекции защиты от этого нет: `UPDATE saga_state` (см. `cmd/orchestrator/main.go:52`) фильтрует только по `saga_id`, без проверки `current_step`. Дубликат `<X>-reply` спокойно откатит шаг назад и выпустит следующий command второй раз. Идемпотентен только `place-order` через `INSERT ... ON CONFLICT DO NOTHING`. В production это закрывают либо `WHERE current_step = $expected` в UPDATE, либо таблицей `processed_events` с offset'ом reply-партиции. На сервисы-исполнители всё равно ложится требование «обрабатывай команду идемпотентно по `saga_id` и `action`», потому что оркестратор спокойно перешлёт ту же команду повторно.

## Кто, когда и зачем

Choreography хороша, когда:

- Шагов мало (2–4).
- Команда расщеплена по микросервисам и не хочет делить «общий» оркестратор.
- Цена связности высока — например, команды сервисов в разных языках, и тащить общий контракт командного протокола не получится.

Orchestration хороша, когда:

- Шагов много или ветвящаяся логика.
- Нужна видимость состояния саги — селектом, дашбордом, runbook'ом.
- Бизнесу нужны метрики типа «сколько саг сейчас в COMPENSATING_PAYMENT» — в choreography этого нигде нет.

Промежуточный режим: оркестрация для критичных саг и choreography для всего остального. Не надо делать «один правильный выбор на всю систему» — это разные инструменты.

## Запуск

Сначала подними Postgres и создай топики:

```sh
make up
make db-init
make topic-create-all
```

### Choreography

Четыре терминала. Каждый — отдельный сервис. Порядок старта не важен, можно в любом.

```sh
make run-payment-choreo      # терминал 1
make run-inventory-choreo    # терминал 2
make run-shipment-choreo     # терминал 3
make run-order-choreo        # терминал 4 — observability
```

Триггерим сагу:

```sh
make run-place-order MODE=choreo COUNT=3
```

В четвёртом терминале (`run-order-choreo`) увидишь таймлайн каждой саги по мере прохождения шагов. Счастливый путь — четыре события, заканчивается на `shipment.scheduled`.

Чтобы посмотреть компенсацию, запусти shipment-service с принудительным фейлом:

```sh
make chaos-fail-shipment    # вместо обычного run-shipment-choreo
```

И снова `make run-place-order MODE=choreo COUNT=1`. В таймлайне увидишь полный каскад: `order-requested → payment-completed → inventory-reserved → shipment-failed → inventory-released → payment-refunded`. Шесть событий вместо четырёх — это и есть стоимость отката: два дополнительных шага на возврат резерва и денег.

### Orchestration

Те же четыре терминала, но теперь с флагом `-mode=orch`:

```sh
make run-orchestrator        # терминал 1 — нужен Postgres из make up
make run-payment-orch        # терминал 2
make run-inventory-orch      # терминал 3
make run-shipment-orch       # терминал 4
```

Триггерим:

```sh
make run-place-order MODE=orch COUNT=3
```

Состояние саг живёт в `saga_state`. Глянуть текущую картину:

```sh
make saga-list
```

Увидишь по строке на сагу с `current_step`, `status` и id'шниками платежа/резерва/доставки. Сравни с choreography: там состояние нигде не лежит цельно, оно размазано по логам сервисов.

Компенсация в orchestration — тот же `chaos-fail-shipment`, но запускаешь shipment-сервис в orch-режиме:

```sh
SHIPMENT_FAIL_RATE=1 make run-shipment-orch
make run-place-order MODE=orch COUNT=1
make saga-list   # увидишь DONE/FAILED, failure_reason заполнен
```

Здесь ты сразу читаешь итог саги из таблицы. В choreography пришлось бы пробежаться по логам всех сервисов или подцепиться к `run-order-choreo` обсёрвером.

## С чем сравнить из курса

`04-01` (transactions and EOS) и `04-03` (outbox pattern) — рядом по теме. Saga решает другую проблему: она не делает атомарной локальную транзакцию (это outbox), и не делает атомарной запись в N топиков одного сервиса (это EOS). Saga делает «целостность» бизнес-операции, размазанной между сервисами, через явные шаги-компенсации. Outbox и EOS — кирпичики, на которых саге становится легче. Без них она работает с at-least-once и риском зависания, как у нас в лекции.

`06-04` (hybrid grpc + kafka) — соседняя лекция, где gRPC + outbox + один топик. Сага — это естественное расширение оттуда: больше топиков, больше сервисов, больше состояний. По сути, в orchestration оркестратор живёт по той же схеме «команда → событие → следующая команда», только координированно.

## Файлы

- `cmd/place-order/main.go` — триггер саги, общий для choreo/orch.
- `cmd/order-service-choreo/main.go` — observability-наблюдатель choreography.
- `cmd/payment-service/main.go` — оплата, два режима.
- `cmd/inventory-service/main.go` — резерв, два режима.
- `cmd/shipment-service/main.go` — доставка, два режима.
- `cmd/orchestrator/main.go` — state-машина в orchestration с saga_state в Postgres.
- `proto/saga/v1/saga.proto` — события choreography и пары command/reply orchestration.
- `db/init.sql` — таблица saga_state и индекс по статусу.
- `docker-compose.override.yml` — Postgres на 15435.

И — finальная мысль. Сага не убирает сложность распределённой транзакции. Она её перемещает: с «найти алгоритм 2PC» на «правильно описать каждый шаг и его компенсацию, и сделать оба идемпотентными». Когда эта мысль становится естественной — большая часть инфраструктурных решений в распределённой системе складывается сама.
