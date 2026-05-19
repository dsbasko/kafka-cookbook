# 05-04 — Schema Evolution

В [Schema Registry](../../../05-03-schema-registry/i18n/ru/README.md) мы научили producer и consumer договариваться через Schema Registry: один регистрирует схему, другой по schema_id её достаёт. Пока схема одна — всё тихо. Но контракт живёт. Через месяц приходит запрос: «давайте добавим валюту в Order». Через полгода — «теперь ещё адрес доставки». Через год кто-то предложит поменять `amount_cents` на string, потому что фронту удобнее. И вот тут начинается интересное.

Эта лекция — про дисциплину изменений. Что в Protobuf можно менять безопасно, что — никогда. Какие compatibility-режимы умеет SR. Что делает `buf breaking` и зачем он нужен в CI. Как это всё ложится на rolling deployment, когда producer-3 и consumer-1 одновременно живут в проде.

## Четыре режима совместимости

Schema Registry хранит per-subject настройку `compatibility`. Это правило, по которому SR разрешает или запрещает регистрировать новую версию схемы под существующим subject'ом. Вариантов четыре:

- **NONE** — не проверять. Любая схема пройдёт. Дальше как повезёт.
- **BACKWARD** (дефолт в Confluent SR) — новая схема должна уметь читать данные, написанные старой. Это про апгрейд consumer'ов: catch up к новой версии можно постепенно, потому что новый код понимает старые сообщения.
- **FORWARD** — старая схема должна уметь читать данные, написанные новой. Это про апгрейд producer'ов: новый код пишет, старые consumer'ы читают.
- **FULL** — и то и другое. Самый строгий режим, при нём evolution идёт совсем мелкими шагами.

В реальности 90% сред выбирают BACKWARD: catch up consumer'ов проще, чем катить новый producer и держать старых клиентов вечно. Но если у тебя десятки команд читают один топик и обновляются на разной скорости, FORWARD или FULL — это страховка от ситуации «выкатили producer с новым полем, и все читатели разом легли».

В нашем стенде compat по умолчанию глобальный (`/config`), но переопределяется per-subject (`/config/<subject>`). Лекция явно ставит BACKWARD на subject — без этого все попытки «зарегистрировать v4» будут зависеть от глобального настройки конкретного запуска.

## Что Protobuf считает совместимым

Protobuf на wire-уровне — это пары `(tag, value)`. Tag — это `field_number << 3 | wire_type`. Никаких имён в payload'е нет — имя поля живёт только в схеме, на проводе уезжает только номер. Из этого вытекают правила.

Безопасные изменения:

- **Добавить новое поле с новым номером.** Старые consumer'ы не знают тэга, складывают байты в unknown fields. Новые видят значение. BACKWARD ✅, FORWARD ✅.
- **Удалить поле, которое больше никто не пишет.** Старые читатели его не увидят (получат default), новые писатели его не отправят. Обычно safer — пометить `reserved` на номер, чтобы случайно не переиспользовать. BACKWARD ✅.
- **Переименовать поле без смены номера.** Имя живёт только в схеме, wire format не меняется. BACKWARD ✅, FORWARD ✅.

Опасные изменения:

- **Сменить тип поля.** Был `int64`, стал `string` — wire type разный (varint vs length-delimited), payload не разберётся. BACKWARD ❌.
- **Сменить номер поля.** Тэг другой — старые байты не найдутся. BACKWARD ❌.
- **Удалить поле и переиспользовать его номер под другим типом.** Никогда. Используй `reserved`.
- **Изменить enum: добавить запрещено только в редких компиляторах**, чаще безопасно. Удалить значение — опасно, старые сообщения с этим тэгом разберутся в `0` (UNSPECIFIED).

В нашей лекции v1 → v2 → v3 — это серия безопасных шагов: каждый раз добавляются поля. v4_breaking меняет тип поля 3 и переносит поле 4 на номер 7. SR это поймает, `buf breaking` поймает, любая адекватная CI поймает.

## Что лежит в `proto/`

Четыре `.proto`-файла. Структура такая:

```
proto/orders/
├── v1/order.proto              # 3 поля
├── v2/order.proto              # +currency
├── v3/order.proto              # +shipping_address (+ Address)
└── v4_breaking/order.proto     # сломанная попытка v3
```

v1, v2, v3 — это нормальные версии с отдельными package'ами `orders.v1`, `orders.v2`, `orders.v3`. Каждая порождает свой Go-пакет в `gen/`. v4_breaking хитрее — он специально объявляет `package orders.v3`, потому что только при совпадающем fully-qualified name `buf breaking` будет сравнивать. Чтобы main buf-модуль не упал на «Address declared multiple times», v4_breaking исключён из модуля через buf.yaml:

```yaml
modules:
  - path: proto
    excludes:
      - proto/orders/v4_breaking
```

Go-кода для v4_breaking, понятно, не генерируется — мы и не хотим, чтобы кто-то по случайности этим типом пользовался. Файл нужен ровно для двух демонстраций: `make proto-breaking-check` и `make try-register-v4`.

## Subject и proto-package: тонкое место

Confluent SR проверяет совместимость в рамках одного subject'а. Внутри subject'а у всех версий схем должен совпадать proto-package — иначе compat-check отвергает регистрацию с ошибкой `PACKAGE_CHANGED`. Это важная деталь.

В лекции у v1 пакет `orders.v1`, у v3 — `orders.v3`. Это сделано ради чистоты Go-кода: каждая версия порождает свой Go-пакет (`gen/orders/v1`, `gen/orders/v3`), и producer-v1 с producer-v3 работают с разными `Order` типами. Из-за разных пакетов SR не пустит обе версии в один subject. Поэтому лекция работает с двумя subject'ами:

- `lecture-05-04-orders-v1-value` — туда регистрируется v1 (пакет `orders.v1`).
- `lecture-05-04-orders-v3-value` — туда регистрируется v3 (пакет `orders.v3`), и туда же `make try-register-v4` пытается встать второй версией.

В реальной жизни такое не делают: по-нормальному `.proto`-файл живёт в одном пакете и эволюционирует через добавление полей. Версии — это git-коммиты, не разные пакеты. В лекции отдельные пакеты появились только чтобы у учебных бинарников были разные Go-типы для иллюстрации forward compatibility на wire-уровне.

## buf breaking — gate в CI

`buf breaking` сравнивает два состояния схемы и репортит несовместимости по выбранному набору правил. У нас в `buf.yaml` стоит `breaking: use: FILE` — это самый строгий набор у buf'а (FILE ⊃ PACKAGE ⊃ WIRE_JSON ⊃ WIRE), ловит и изменение wire-формата, и переименования полей, и удаление полей, и смену имени файла. Проверяет тип, номер, обязательность, наличие — всё по списку правил, который buf публикует в своих доках.

В живом проекте обычно сравнивают «текущий PR» и «main». В лекции инфраструктуры с git-ref'ами нет, поэтому Makefile делает это руками: копирует `proto/orders/v3/order.proto` и `proto/orders/v4_breaking/order.proto` в tmp-каталог, собирает их в buf-image'ы и натравливает breaking друг на друга:

```makefile
proto-breaking-check:
	@tmpdir=$$(mktemp -d); \
	  trap 'rm -rf $$tmpdir' EXIT; \
	  mkdir -p $$tmpdir/v3 $$tmpdir/v4; \
	  cp proto/orders/v3/order.proto $$tmpdir/v3/; \
	  cp proto/orders/v4_breaking/order.proto $$tmpdir/v4/; \
	  ( cd $$tmpdir && \
	      buf build v3 -o v3.bin && \
	      buf build v4 -o v4.bin && \
	      buf breaking v4.bin --against v3.bin ); \
	  rc=$$?; \
	  ...
```

Запуск выдаёт примерно такое:

```
order.proto:32:1:Previously present field "4" with name "currency" on message "Order" was deleted.
order.proto:35:3:Field "3" with name "amount_cents" on message "Order" changed type from "int64" to "string".

OK: buf корректно зарепортил несовместимость v3 → v4_breaking
```

Логика в Makefile инвертирует код возврата: buf breaking возвращает 100 при найденных нарушениях, а в нашем демо это и есть желаемый исход. Если buf вернул 0 — значит мы случайно поменяли v4_breaking так, что он стал совместимым, и тест демо сломан. Сообщение в обе стороны.

В реальном CI `buf breaking` ставят отдельным шагом до push, обычно `buf breaking --against '.git#branch=main'`. На push в main, либо на PR — если ломается, PR не мерджится. Это дешёвая страховка ровно от того, что SR ловит на runtime.

## SR и compat check

Когда buf breaking-check мы прогнали локально, дальше идём в SR. Там тоже compatibility-проверка — но другой природы. Buf смотрит на абстрактные правила («тип поля изменился»), SR смотрит, что проходит реальные ограничения сериализатора Confluent (про Protobuf — оно близко к buf'овским FILE, но не один-в-один).

Workflow для v3-subject'а в Makefile разложен явно:

1. `make register-v3` — регистрируем v3 в `lecture-05-04-orders-v3-value`. Получаем version 1.
2. `make sr-set-compat-backward-v3` — фиксируем режим. Без этого глобальный дефолт может сыграть на нас, лекция этого не хочет.
3. `make try-register-v4` — отправляем v4_breaking в тот же subject. SR применяет compat-check, видит изменение типа `amount_cents` int64 → string, отвечает 409:

```json
{
  "error_code": 409,
  "message": "Schema being registered is incompatible with an earlier schema for subject \"lecture-05-04-orders-v3-value\", details: [{errorType:\"FIELD_SCALAR_KIND_CHANGED\", description:\"The kind of a SCALAR field at path '#/Order/3' in the new schema does not match its kind in the old schema\"}, ...]"
}
```

Это и есть то, что мы хотим увидеть. Subject продолжает жить с версией 1 (v3), v4_breaking в реестр не попал, никакой producer не сможет получить под него `schema_id`.

Если поставить `make sr-set-compat-none-v3` — тот же `try-register-v4` пройдёт. SR тогда не проверяет ничего, и мир получает «версию 2, которая разъехалась с предыдущими версиями». На этом обычно и горят проды, в которые забыли заглянуть в compat-настройки.

`make register-v1` живёт отдельно — он регистрирует v1 в своём subject'е (`lecture-05-04-orders-v1-value`). В лекции это нужно ровно для того, чтобы subject существовал, и под ним работал producer-v1. К compat-демонстрации register-v1 не относится.

## Sliding deployment в живую

В лекции три бинарника: `producer-v1`, `producer-v3`, `consumer-v1`. У каждого producer'а свой топик и свой subject. Сценарий, ради которого всё затевалось:

1. Запускаем `producer-v3` — пишет 5 Order'ов в `lecture-05-04-orders-v3` со всеми пятью полями.
2. Запускаем `consumer-v1` (по умолчанию подписан на тот же топик, `-topic=lecture-05-04-orders-v3`).
3. consumer-v1 читает сообщения и видит первые три поля. Currency и shipping_address уезжают в unknown fields, программа не падает.

Producer-v3 регистрирует v3-схему в SR (получает свой schema_id) и пишет в Confluent wire format с этим id. Consumer-v1 — намеренно «глупый», он не зовёт SR, срезает первые 5 байт заголовка плюс protobuf message-index, а остаток скармливает `proto.Unmarshal` в `*ordersv1.Order`:

```go
schemaID, payload, err := stripWireFormatHeader(rec.Value)
// ...
var order ordersv1.Order
if err := proto.Unmarshal(payload, &order); err != nil {
    logger.Error("unmarshal v1", "err", err)
    return
}
```

И вот тут проявляется forward compatibility Protobuf'а. Consumer не знает про новые поля, ему всё равно. proto-runtime аккуратно складывает байты неизвестных тэгов в unknown_fields структуры. Программа работает, поля из v1 раскладываются как раньше:

```
--- lecture-05-04-orders-v3/2@1 key=ord-v3-00003 schema_id=15 ---
  id           = ord-v3-00003
  customer_id  = cus-052
  amount_cents = 12345
  unknown      = 47 bytes (поля v3, которые v1 не знает)
```

Schema_id в логе виден — он показывает, что под этим id в SR живёт уже v3. Но consumer-v1 им не пользовался для разбора.

Это и есть rolling deployment: producer обновили, consumer'ы обновятся когда смогут. Никто не лежит. Когда дойдут руки — выкатят consumer-v3, который начнёт читать новые поля. До тех пор данные не теряются: они в логах Kafka, новый код их прочтёт когда появится.

В обратную сторону — producer-v1 пишет, consumer-v3 читает — тоже работает. Consumer-v3 увидит первые три поля в полях, currency будет пустой строкой, shipping_address — nil. Это и есть default values при отсутствии тэга в payload'е.

## Что важно держать в голове

Compat в SR — это runtime gate. Он спасает от того, что кто-то случайно зарегистрировал кривую схему. Но он не заставит твой Go-код помнить про unknown fields, не научит твоё приложение работать с пропусками, не починит логику. Schema Registry даёт совместимость на уровне сериализации, не на уровне семантики.

Buf breaking — это compile-time gate. Он быстрее, дешевле и ставится в CI до того, как новая схема вообще доехала до SR. Хорошая практика — оба шага: buf breaking в CI плюс SR-compat в проде. Один ловит ошибки до merge'а, другой — на регистрации.

Если у тебя evolution частая (раз в неделю и чаще) — стоит подумать про FORWARD или FULL compat-режим, особенно если читателей много и они на разных циклах деплоя. Если редкая (раз в квартал) — BACKWARD достаточно.

И последнее. Если уж зашёл туда, где schema sliding ломается — обычно правильный путь не «как протащить мимо compat», а новый subject. `orders-v2-value` рядом с `orders-v1-value`, два топика, два набора consumer'ов, миграция на стороне приложений. Это дороже, но честнее: сломанная совместимость в одном subject'е — это тихая бомба, которая рванёт где-нибудь в середине ночи.

## Файлы

- `proto/orders/v1/order.proto` — стартовая версия, 3 поля
- `proto/orders/v2/order.proto` — +currency
- `proto/orders/v3/order.proto` — +shipping_address (вложенный Address)
- `proto/orders/v4_breaking/order.proto` — сломанная вариация v3
- `cmd/producer-v1/main.go` — пишет Order'ы по схеме v1
- `cmd/producer-v3/main.go` — пишет Order'ы по схеме v3
- `cmd/consumer-v1/main.go` — читает топик в `*v1.Order`, демонстрирует unknown fields
- `buf.yaml`, `buf.gen.yaml` — настройка модуля, lint, breaking-check, codegen
- `Makefile` — все цели для запуска

## Запуск

Стенд из корня репозитория должен быть поднят (`docker compose up -d`).

```sh
make proto-gen                    # сгенерировать gen/orders/{v1,v2,v3}
make proto-lint                   # buf lint
make proto-breaking-check         # сверить v3 и v4_breaking, ожидать репорт buf'а

make topic-create-v1
make topic-create-v3

# SR-compat демо в subject'е v3
make register-v3                  # положить v3 как версию 1
make sr-set-compat-backward-v3    # зафиксировать compat-режим
make try-register-v4              # v4_breaking — отбой 409
make sr-list-versions-v3          # увидеть, что в subject'е лежит только v3 (одна версия)

# Wire-level forward compat демо
make register-v1                  # регистрация v1 в своём subject'е
make run-consumer-v1              # стартовать consumer'а (подписан на topic v3)
make run-producer-v3              # 5 Order'ов по v3 (consumer-v1 читает их и видит unknown fields)
make run-producer-v1              # для контраста: 5 Order'ов по v1 в свой топик

make clean                        # удалить топики, subject'ы и gen/
```

## Соседние лекции

- [Schema Registry](../../../05-03-schema-registry/i18n/ru/README.md) — wire format и базовая регистрация
- [Protobuf в Go](../../../05-02-protobuf-in-go/i18n/ru/README.md) — `.proto`, buf, кодген
- [Зачем контракты и wire-форматы](../../../05-01-why-contracts-and-wire-formats/i18n/ru/README.md) — зачем вообще схемы
