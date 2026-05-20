# 08-01 — Monitoring & Metrics

A system without metrics is a system that stays silent until something breaks. Especially with Kafka. From the outside everything looks like a normal TCP socket on 9092: connection is alive, nothing returns an "error". But inside, consumer lag has been growing for a day, disk is creeping toward 90%, one of the brokers dropped out of ISR for half an hour, and rebalances in the consumer group happen every thirty seconds. Without graphs you won't see any of this until a user calls.

This lecture covers how to bring up a minimal observability stack on top of the sandbox and which metrics to check first.

## What we hook up on top of the sandbox

The stack is simple. Kminion works as an exporter — converts the Kafka API into Prometheus metrics. Prometheus scrapes them every 15 seconds. Grafana renders them. Everything starts via the lecture's `docker-compose.override.yml` and joins the same network as kafka-1/2/3.

```
                 +----------+
                 |  kminion |  scrape Kafka API → Prom metrics
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
                                  → kminion-overview dashboard
```

Startup:

```sh
make up                # three containers come up together
make topic-create      # a separate topic for the load
make run-load          # producer + slow consumer
open http://localhost:3000
```

After a minute all targets in Prometheus will be UP. Grafana picks up the auto-provisioned datasource, and the `Kafka — kminion overview` dashboard appears automatically.

## Where Kafka metrics come from

This is a layer where beginners often get stuck. Kafka has no built-in `/metrics` endpoint in Prometheus format. The broker exports metrics via JMX — a Java standard that works well inside the JVM world but is foreign outside it. To let Prometheus scrape them, you need a bridge.

Historically there are two bridges:

1. **JMX exporter** (Prometheus jmx_exporter). A Java agent that attaches to the broker's JVM via `-javaagent` and exposes its own HTTP endpoint. That endpoint serves everything in the broker's JMX tree, translated into Prometheus format — and Kafka's JMX tree has hundreds of metrics.
2. **kminion**. A standalone service written in Go (Cloudhut, then Redpanda Data). It does not attach to the JVM. Instead it connects to Kafka as a regular client via the Kafka API: requests cluster metadata, describes topics and partitions, reads consumer group offsets, calculates lag. From this it builds its own metric set and exposes it at /metrics.

In the sandbox we use kminion. Three reasons for this choice:

1. **Less friction with the Kafka image.** JMX exporter requires starting a Java agent inside the image. Our image is apache/kafka:4.2.0, and injecting an extra jar there means either a custom Dockerfile build stage or a volume mount with a config. For a teaching sandbox — an unnecessary layer.
2. **Lag out of the box.** Kminion gets lag via the same Kafka API as `kafka-consumer-groups.sh --describe`. That is the metric most setups are built around.
3. **A clear namespace.** Metrics arrive with the prefix `kminion_kafka_*`, with no need to copy pattern files for MBean mappings.

In production the choice is usually different. JMX exporter is deployed alongside brokers for broker-side metrics: request rate per type, under-the-hood timings for individual request stages, network threads, replica fetcher state. Kminion (or its analogue `kafka-exporter` by danielqsj) is deployed separately for consumer lag and topic stats. These tools do not compete — they cover different layers.

If you want to dig further — the pattern file for JMX exporter under Kafka is in [the jmx_exporter repo](https://github.com/prometheus/jmx_exporter/blob/main/example_configs/kafka-2_0_0.yml), and it shows how much richer the JMX-based metric set is.

## What our sandbox shows

The `Kafka — kminion overview` dashboard is built on kminion metrics and covers four zones: cluster health, write throughput, lag, and disk.

Key metrics worth remembering here:

- `kminion_kafka_cluster_info` — a gauge with value 1; its labels carry the full cluster "card": broker count, controller id, version, and cluster id. If broker_count dropped from 3 to 2 — there is a problem.
- `kminion_kafka_topic_high_water_mark_sum` — the sum of high water marks across a topic's partitions. `rate(...[1m])` is the write throughput to the topic in messages/sec.
- `kminion_kafka_topic_low_water_mark_sum` — the same for the earliest offset. The difference from the HWM shows how many messages are currently stored in the topic.
- `kminion_kafka_topic_log_dir_size_total_bytes` — on-disk size (per-topic, via `DescribeLogDirs`).
- `kminion_kafka_consumer_group_topic_lag` — the group's lag for a specific topic, summed across partitions. The most important metric for alerts.
- `kminion_kafka_consumer_group_topic_partition_lag` — the same, but per partition. Helps surface a hot partition (one partition lagging more than the others).

Full list — `make kminion-metrics`, outputs the first ~50 lines with the `kminion_kafka_` prefix.

## What our program shows

The lecture ships with its own load generator — `cmd/load-generator/main.go`. A single process does two things in parallel: a producer writes to `lecture-08-01-events` at `-rate msg/sec`, and a consumer for the same lecture reads the topic in group `lecture-08-01-slow` with an artificial `-consume-delay` per message.

The goal is to create a visible gap between write speed and read speed. On the dashboard this is immediately visible: "Write throughput" draws a flat line, "Group lag" starts rising steadily.

The producer loop itself — bare `Produce` with a ticker:

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

The consumer is symmetrically simple. A `PollFetches` loop, with a sleep on each record:

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

Run `make run-load`, open Grafana — within a minute or two the "Group lag" panel will show your group `lecture-08-01-slow` with a rising graph.

Want to see lag not growing? Reduce the delay:

```sh
CONSUME_DELAY=1ms make run-load
```

Or go the other way — inflate the producer to watch `kminion_kafka_topic_log_dir_size_total_bytes` grow:

```sh
RATE=2000 PAYLOAD_KB=4 CONSUME_DELAY=100ms make run-load
```

## Which metrics to check first

In real operations you don't have time to watch hundreds of graphs. It's more useful to keep a short list in your head — what belongs on the on-call dashboard and in alerts.

**At the cluster level:**

- `under_replicated_partitions > 0` for more than 5 minutes — alert. A partition whose ISR is smaller than the replication factor has lost one of its replicas. If this coincides with min.insync.replicas — producers are already getting `NotEnoughReplicas`.
- `offline_partitions > 0` — critical alert. A partition with no leader; you cannot write to it or read from it.
- `active_controller_count != 1` — the cluster must have exactly one active controller (in KRaft — one of the quorum nodes). If 0 or 2 — something is wrong with coordination.

**At the topic level:**

- disk size. If retention is configured correctly, size should be stable. If it grows linearly — retention is not kicking in. If it spikes sharply — there is a load surge somewhere.
- write throughput. If it suddenly hits zero — producers have stopped or lost connectivity. If it jumped by an order of magnitude — someone did something.

**At the consumer group level:**

- lag. The primary metric. Rising lag = consumer is not keeping up. The cause is usually one of: slow handler, too few instances in the group, partition skew (one partition more heavily loaded than the rest), slow downstream call per message.
- number of members in the group. A sudden drop — deploy restart or crash. A sudden spike — someone rolled out more instances than partitions (the excess ones sit idle).
- rebalance frequency. A group that rebalances every 30 seconds is a group that processes nothing. Usually a symptom of `max.poll.interval.ms < batch processing time`.

**At the producer level** (if you have your own application metrics):

- producer error-rate. franz-go has no ready-made metric under this name (the Java client calls it `record-error-rate`). You build it yourself via the `HookProduceRecordUnbuffered` hook — it fires on each record with the error its promise will be called with. If rising — check error classes (retriable vs non-retriable).
- request latency P99. Also built from your own code — via `HookBrokerWrite` / `HookBrokerRead` (or `HookBrokerE2E` for a full round-trip estimate). If rising — the problem is either at the broker or in the network.

In the sandbox dashboard I built the minimum — four zones (overall stats, write throughput, lag, disk). Nothing more is needed for now. The goal is to show how the stack is structured. A reference dashboard for production is assembled separately, against specific SLOs.

## The dashboard provisions itself

Grafana provisioning works like this: on startup Grafana reads `/etc/grafana/provisioning/datasources/*.yml` and `/etc/grafana/provisioning/dashboards/*.yml`. Datasources from these files are created automatically, dashboards are loaded from paths declared in the provider.

We have two files. `grafana-provisioning/datasources/prometheus.yml` declares a datasource named `Prometheus` with `uid: prometheus` (the UID matters — the dashboard JSON references it). `grafana-provisioning/dashboards/dashboards.yml` declares a provider that watches `/var/lib/grafana/dashboards` — where `grafana-dashboard.json` is mounted.

If you change the dashboard in the UI and save — Grafana writes to its database, but on the next restart provisioning overwrites it from the file. To persist changes, edit the JSON directly. That is exactly the behavior you want with an IaC approach: the dashboard lives as a file in the repo, not as a mutable record in a database.

## When the stack is up but there are no metrics

A typical debugging chain if you open Grafana and see nothing:

1. `make prometheus-targets` — should show `health: up` for job=`kminion`. If not — kminion did not start or the network is misconfigured.
2. `make kminion-metrics` — kminion should serve metrics directly. If the response is empty or 500 — kminion failed to connect to kafka-1/2/3 (check `docker logs lecture-08-01-kminion`, look for `failed to connect`).
3. In Grafana open Explore. Select the Prometheus datasource and type `kminion_kafka_cluster_info`. If data comes back — scraping works and the problem is in the dashboard JSON (likely the datasource uid does not match).

Step 3 breaks most often. I hardcoded the UID `prometheus`, but if you rename the datasource in provisioning — fix it in `grafana-dashboard.json` too (the field `"uid": "prometheus"` in every target).

## What is out of scope

Alerting (Alertmanager or Grafana Alerting) is a separate topic and is not covered here. The "list of metrics to alert on" principle — see above. The actual rules in Prometheus or Grafana are written exactly like any others.

JMX exporter is mentioned as a production alternative but is not brought up in the sandbox.

Distributed tracing (Jaeger, Tempo) is a different tool entirely. Metrics tell you "what is happening in the cluster", traces tell you "where this specific request is going". In complex gRPC + Kafka systems you will want both, but that is not the subject of this lecture.

Next — [Retention and compaction](../../../08-02-retention-and-compaction/i18n/en/README.md) in practice.
