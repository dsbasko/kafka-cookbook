.PHONY: help run topic-create topic-describe topic-recreate topic-delete build

KAFKA_BOOTSTRAP ?= localhost:19092,localhost:19093,localhost:19094
TOPIC ?= brew.orders.v1
PARTITIONS ?= 3
RF ?= 3

help:
	@echo "make run              - создать brew-топик идемпотентно и распечатать leader/replicas/ISR per-partition"
	@echo "make topic-create     - то же самое, явная цель (alias для run)"
	@echo "make topic-describe   - описать brew-топик через kafka-topics.sh внутри kafka-1 для сравнения"
	@echo "make topic-recreate   - удалить и создать заново (видно перераспределение leader'ов)"
	@echo "make topic-delete     - удалить brew-топик через kafka-topics.sh"
	@echo "make build            - go build ./..."

run:
	KAFKA_BOOTSTRAP=$(KAFKA_BOOTSTRAP) go run ./cmd/inspect \
	  -topic=$(TOPIC) -partitions=$(PARTITIONS) -rf=$(RF)

topic-create: run

topic-recreate:
	KAFKA_BOOTSTRAP=$(KAFKA_BOOTSTRAP) go run ./cmd/inspect \
	  -topic=$(TOPIC) -partitions=$(PARTITIONS) -rf=$(RF) -recreate=true

topic-describe:
	docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh \
	  --bootstrap-server kafka-1:9092 --describe --topic $(TOPIC)

topic-delete:
	docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh \
	  --bootstrap-server kafka-1:9092 --delete --topic $(TOPIC)

build:
	go build ./...
