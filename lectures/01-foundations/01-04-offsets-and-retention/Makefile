.PHONY: help run topic-create-short-retention topic-describe topic-delete build

KAFKA_BOOTSTRAP ?= localhost:19092,localhost:19093,localhost:19094
TOPIC ?= brew.orders.retention-demo
PARTITIONS ?= 3
RF ?= 3
MESSAGES ?= 100
INTERVAL ?= 10s
RETENTION ?= 60s
SEGMENT ?= 10s
RECREATE ?= false

help:
	@echo "make run                          - наполнить топик и наблюдать earliest/latest каждые $(INTERVAL)"
	@echo "make run RECREATE=true            - удалить топик перед запуском (обнуляет offset'ы)"
	@echo "make topic-create-short-retention - то же самое (alias для run; имя из плана)"
	@echo "make topic-describe               - kafka-topics.sh --describe + конфиги топика"
	@echo "make topic-delete                 - удалить топик после лекции"
	@echo "make build                        - go build ./..."

run:
	KAFKA_BOOTSTRAP=$(KAFKA_BOOTSTRAP) go run ./cmd/load-and-watch \
	  -topic=$(TOPIC) -partitions=$(PARTITIONS) -rf=$(RF) \
	  -messages=$(MESSAGES) -interval=$(INTERVAL) \
	  -retention=$(RETENTION) -segment=$(SEGMENT) \
	  -recreate=$(RECREATE)

topic-create-short-retention: run

topic-describe:
	docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh \
	  --bootstrap-server kafka-1:9092 --describe --topic $(TOPIC)
	@echo
	docker exec kafka-1 /opt/kafka/bin/kafka-configs.sh \
	  --bootstrap-server kafka-1:9092 --entity-type topics --entity-name $(TOPIC) --describe

topic-delete:
	docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh \
	  --bootstrap-server kafka-1:9092 --delete --topic $(TOPIC)

build:
	go build ./...
