# 08-01 — Monitoring & Metrics

Система без метрик — это система, которая молчит до того момента, пока что-то не упадёт. Особенно с Kafka. Снаружи всё выглядит как обычный TCP-сокет на 9092: коннект жив, ничего не отвечает «ошибкой». А внутри уже сутки растёт consumer lag, диск ползёт к 90%, один из брокеров на полчаса вылетел из ISR, и ребалансы у consumer-группы случаются каждые тридцать секунд. Без графиков ты этого не увидишь, пока пользователь не позвонит.

Эта лекция — про то, как поднять минимальный наблюдательный стек поверх sandbox-стенда и какие метрики смотреть в первую очередь.

## Что подцепляем поверх стенда

Стек простой. Kminion работает exporter'ом — превращает Kafka API в Prometheus-метрики. Prometheus их собирает с интервалом 15 секунд. Grafana рисует. Всё это поднимается через `docker-compose.override.yml` лекции, садится в ту же сеть, что и kafka-1/2/3.

```
                 +----------+
                 |  kminion |  scrape Kafka API → Prom метрики
                 |   :8080  |
                 +----+-----+
                      ^
                      | scrape every 15s
                      |
+------------+   +----+-----+        +----------+
|  kafka-1/2/3|<--|prometheus|<------|  grafana |
|  :9092     |   |  :9090   |        |   :3000  |
+------------+   +----------+        +----------+
                                          |
                                          v
                                  http://localhost:3000
                                  → дашборд kminion-overview
```

Запуск:

```sh
make up                # три контейнера встанут разом
make topic-create      # отдельный топик для нагрузки
make run-load          # producer + slow consumer
open http://localhost:3000
```

Через минуту все таргеты в Prometheus станут UP. Grafana подцепит автопровиженный datasource, дашборд `Kafka — kminion overview` появится сам.

## Откуда берутся метрики Kafka

Тут есть слой, в котором новички часто застревают. У Kafka нет встроенного `/metrics` эндпоинта в формате Prometheus. Брокер экспортит метрики через JMX — это Java-стандарт, удобный внутри JVM-мира и инородный снаружи. Чтобы Prometheus их забрал, нужен мост.

Мостов исторически два:

1. **JMX exporter** (Prometheus jmx_exporter). Java-агент, который цепляется к JVM брокера через `-javaagent` и поднимает свой HTTP-эндпоинт. На этом эндпоинте отдаётся всё, что есть в JMX-дереве брокера, переведённое в формат Prometheus, — а в JMX-дереве у Kafka их сотни.
2. **kminion**. Отдельный сервис, написанный на Go (Cloudhut, потом Redpanda Data). Он не цепляется к JVM. Вместо этого ходит в Kafka как обычный клиент через Kafka API: запрашивает метаданные кластера, описывает топики и партиции, читает offsets consumer-групп, считает lag. Из этого собирает свой набор метрик и отдаёт их в /metrics.

В sandbox мы берём kminion. Причин у такого выбора три:

1. **Меньше возни с образом Kafka.** JMX exporter требует поднять Java-agent внутри образа. У нас образ apache/kafka:4.2.0, и пихать туда дополнительный jar — это либо своя build-стадия в Dockerfile, либо volume-mount с конфигом. Для учебного стенда — лишний слой.
2. **Lag из коробки.** Kminion умеет lag через тот же Kafka API, что и `kafka-consumer-groups.sh --describe`. А это та метрика, ради которой обычно всё и затевают.
3. **Понятный namespace.** Метрики приходят с префиксом `kminion_kafka_*`, без необходимости копировать pattern-файлы для маппинга MBean'ов.

В production выбор обычно другой. JMX exporter ставят рядом с брокерами для broker-side метрик: request rate per type, под-капотные тайминги отдельных стадий запроса, network threads, replica fetcher state. Kminion (или его аналог `kafka-exporter` от danielqsj) поднимают рядом для consumer lag и topic stats. Эти инструменты не конкурируют — они закрывают разные слои.

Если будешь дальше копать — pattern-файл для JMX exporter под Kafka лежит [в репо jmx_exporter](https://github.com/prometheus/jmx_exporter/blob/main/example_configs/kafka-2_0_0.yml), оттуда же видно, насколько богаче набор метрик у JMX-варианта.

## Что показывает наш стенд

Дашборд `Kafka — kminion overview` собран на kminion-метриках и закрывает четыре зоны: здоровье кластера, скорость записи, lag и диск.

Ключевые метрики, которые тут стоит запомнить:

- `kminion_kafka_cluster_info` — gauge со значением 1, у него на labels висит вся «карточка» кластера: количество брокеров, id контроллера, версия и id кластера. Если broker_count просел с 3 до 2 — проблема.
- `kminion_kafka_topic_high_water_mark_sum` — сумма high water marks по партициям топика. `rate(...[1m])` — это скорость записи в топик в сообщениях/сек.
- `kminion_kafka_topic_low_water_mark_sum` — то же для earliest offset. Разность с HWM показывает, сколько сообщений сейчас хранится в топике.
- `kminion_kafka_topic_log_dir_size_total_bytes` — размер на диске (per-topic, через `DescribeLogDirs`).
- `kminion_kafka_consumer_group_topic_lag` — lag группы по конкретному топику, суммарный по партициям. Самая важная метрика для алёртов.
- `kminion_kafka_consumer_group_topic_partition_lag` — то же, но per-partition. Помогает увидеть hot partition (одна партиция отстаёт сильнее остальных).

Полный список — `make kminion-metrics`, выдаст первые полсотни строк с префиксом `kminion_kafka_`.

## Что показывает наша программа

Лекция приходит со своим нагрузчиком — `cmd/load-generator/main.go`. Один процесс делает две вещи параллельно: producer пишет в `lecture-08-01-events` со скоростью `-rate msg/sec`, consumer той же лекции читает топик группой `lecture-08-01-slow` с искусственной задержкой `-consume-delay` на каждое сообщение.

Цель — создать видимое расхождение между скоростью записи и скоростью чтения. На дашборде это сразу видно: «Скорость записи» рисует ровную линию, «Lag по группам» начинает плавно расти.

Сам цикл producer'а — голый `Produce` с тикером:

```go
ticker := time.NewTicker(interval)
defer ticker.Stop()

var seq int64
for {
    select {
    case <-ctx.Done():
        cl.Flush(context.Background())
        return nil
    case <-ticker.C:
        seq++
        rec := &kgo.Record{
            Topic: topic,
            Key:   []byte(fmt.Sprintf("k-%d", seq%32)),
            Value: payload,
        }
        cl.Produce(ctx, rec, func(_ *kgo.Record, err error) {
            if err == nil {
                produced.Add(1)
            }
        })
    }
}
```

Consumer симметрично простой. Цикл `PollFetches`, на каждой записи делается sleep:

```go
fetches.EachRecord(func(_ *kgo.Record) {
    select {
    case <-ctx.Done():
        return
    case <-time.After(delay):
    }
    consumed.Add(1)
})
```

Запусти `make run-load`, открой Grafana — через минуту-две на панели «Lag по группам» увидишь свою группу `lecture-08-01-slow` с восходящим графиком.

Хочешь увидеть, как lag не растёт? Уменьши задержку:

```sh
CONSUME_DELAY=1ms make run-load
```

Или наоборот — раздуй продьюсер, чтобы посмотреть, как растёт `kminion_kafka_topic_log_dir_size_total_bytes`:

```sh
RATE=2000 PAYLOAD_KB=4 CONSUME_DELAY=100ms make run-load
```

## Какие метрики смотреть в первую очередь

В реальной операционке у тебя нет времени смотреть на сотни графиков. Полезнее держать в голове короткий список — то, что должно быть на дежурном дашборде и в алёртах.

**На уровне кластера:**

- `under_replicated_partitions > 0` дольше 5 минут — алёрт. Партиция, у которой ISR меньше replication factor, потеряла одну из реплик. Если это совпадает с min.insync.replicas — продьюсеры уже получают `NotEnoughReplicas`.
- `offline_partitions > 0` — алёрт критический. Партиция без leader'а, в неё нельзя писать и из неё нельзя читать.
- `active_controller_count != 1` — у кластера должно быть ровно по одному активному контроллеру (в KRaft — один из quorum-нод). Если 0 или 2 — что-то не так с координацией.

**На уровне топика:**

- размер на диске. Если retention настроен правильно, размер должен быть стационарным. Если растёт линейно — retention не отрабатывает. Если резко прыгает — где-то всплеск нагрузки.
- скорость записи. Если внезапно стала нулевой — продьюсеры остановились или потеряли connectivity. Если выросла на порядок — кто-то что-то сделал.

**На уровне consumer-группы:**

- lag. Главная метрика. Растущий lag = consumer не успевает. Причина обычно одна из нескольких: медленный handler, мало instance'ов в группе, partition skew (одна партиция нагружена сильнее остальных), долгий downstream-вызов на каждое сообщение.
- количество members в группе. Резкое падение — рестарт деплоя или crash. Резкий рост — кто-то выкатил больше инстансов, чем партиций (избыточные простаивают).
- частота rebalance'ов. Группа, которая ребалансится каждые 30 секунд — это группа, которая ничего не успевает обработать. Обычно симптом `max.poll.interval.ms < время обработки batch'а`.

**На уровне продьюсера** (если у тебя свои метрики из приложения):

- error-rate продьюсера. Готовой метрики с таким именем у franz-go нет (это имя из Java-клиента, `record-error-rate`). Собирают сами через хук `HookProduceRecordUnbuffered` — он на каждую запись отдаёт ошибку её promise'а. Растёт — посмотри классы ошибок (retriable vs non-retriable).
- request latency P99. Тоже собирается своим кодом — через `HookBrokerWrite` / `HookBrokerRead` (или `HookBrokerE2E` для оценки полного round-trip). Растёт — проблема либо у брокера, либо в сети.

В sandbox-дашборде я сделал минимум — четыре зоны (общая статистика, скорость записи, lag, диск). Больше пока не нужно. Цель — показать, как стек устроен. Reference-дашборд для production собирается отдельно, под конкретные SLO.

## Дашборд провижится сам

Grafana-провижининг устроен так: при старте Grafana читает `/etc/grafana/provisioning/datasources/*.yml` и `/etc/grafana/provisioning/dashboards/*.yml`. Datasource из этих файлов создаются автоматически, dashboards подтягиваются из путей, прописанных в provider'е.

У нас два файла. `grafana-provisioning/datasources/prometheus.yml` объявляет datasource с именем `Prometheus` и `uid: prometheus` (UID важен — на него ссылается JSON дашборда). `grafana-provisioning/dashboards/dashboards.yml` объявляет провайдер, который смотрит в `/var/lib/grafana/dashboards` — туда mount'ом проброшен `grafana-dashboard.json`.

Если меняешь дашборд через UI и сохраняешь — Grafana запишет в свою БД, но при следующем рестарте провижининг перетрёт обратно из файла. Чтобы изменения зафиксировались, надо отредактировать сам JSON. Это ровно то поведение, которого ты хочешь в IaC-подходе: dashboard живёт как файл в репо, не как мутируемая запись в БД.

## Когда стек поднялся, но метрик нет

Типичная отладочная цепочка, если открыл Grafana и видишь пустоту:

1. `make prometheus-targets` — должно быть `health: up` для job=`kminion`. Если нет — kminion не стартанул или сеть кривая.
2. `make kminion-metrics` — kminion должен отдавать метрики прямо. Если ответ пустой или 500 — kminion не подключился к kafka-1/2/3 (проверь `docker logs lecture-08-01-kminion`, ищи `failed to connect`).
3. В Grafana открой Explore. Выбери Prometheus datasource и набери `kminion_kafka_cluster_info`. Если данные приходят — значит, scrape работает, и проблема в JSON дашборда (вероятно, не совпал uid datasource).

Чаще всего ломается шаг 3. UID я вшил константу `prometheus`, но если ты решишь переименовать datasource в provisioning — поправь и в `grafana-dashboard.json` (поле `"uid": "prometheus"` в каждом таргете).

## Что вне scope

Алёртинг (Alertmanager или Grafana Alerting) — отдельная тема, тут не разбирается. Принцип «список метрик, на которые надо алёртить» — выше. Сами правила в Prometheus или Grafana пишутся ровно так, как любые другие.

JMX exporter — упомянут как production-альтернатива, но в sandbox не поднимаем.

Distributed tracing (Jaeger, Tempo) — это вообще другой инструмент. Метрики говорят «что происходит в кластере», traces говорят «куда уходит этот конкретный request». В сложных gRPC + Kafka-системах ты захочешь оба, но это не предмет этой лекции.

Дальше — [Retention и compaction](../../../08-02-retention-and-compaction/i18n/ru/README.md) на практике.
