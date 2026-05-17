.PHONY: help run topic-create topic-describe topic-delete consume-cli build

KAFKA_BOOTSTRAP ?= localhost:19092,localhost:19093,localhost:19094
TOPIC ?= brew.orders.v1
PARTITIONS ?= 3
RF ?= 3
MESSAGES ?= 10

help:
	@echo "make run            - записать $(MESSAGES) OrderPlaced в $(TOPIC) через ProduceSync"
	@echo "make topic-create   - то же самое (alias для run; топик создаётся идемпотентно)"
	@echo "make topic-describe - kafka-topics.sh --describe для $(TOPIC)"
	@echo "make consume-cli    - kafka-console-consumer изнутри kafka-1, читает с начала"
	@echo "make topic-delete   - удалить $(TOPIC) после лекции"
	@echo "make build          - go build ./..."

run:
	KAFKA_BOOTSTRAP=$(KAFKA_BOOTSTRAP) go run ./cmd/producer \
	  -topic=$(TOPIC) -partitions=$(PARTITIONS) -rf=$(RF) -messages=$(MESSAGES)

topic-create: run

topic-describe:
	docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh \
	  --bootstrap-server kafka-1:9092 --describe --topic $(TOPIC)

consume-cli:
	docker exec -it kafka-1 /opt/kafka/bin/kafka-console-consumer.sh \
	  --bootstrap-server kafka-1:9092 --topic $(TOPIC) \
	  --from-beginning --property print.key=true --property print.partition=true \
	  --property print.offset=true --property key.separator=" | "

topic-delete:
	docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh \
	  --bootstrap-server kafka-1:9092 --delete --topic $(TOPIC)

build:
	go build ./...
