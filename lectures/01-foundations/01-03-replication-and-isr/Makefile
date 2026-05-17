.PHONY: help run kill-broker restore-broker topic-describe topic-delete build

KAFKA_BOOTSTRAP ?= localhost:19092,localhost:19093,localhost:19094
TOPIC ?= brew.orders.v1
PARTITIONS ?= 3
RF ?= 3
INTERVAL ?= 2s
BROKER ?= kafka-2

help:
	@echo "make run             - запустить watch-isr (создаст топик идемпотентно и опрашивает каждые $(INTERVAL))"
	@echo "make kill-broker     - остановить $(BROKER) (по умолчанию kafka-2), чтобы посмотреть, как сжимается ISR"
	@echo "make restore-broker  - запустить $(BROKER) обратно и наблюдать, как ISR восстанавливается"
	@echo "make topic-describe  - описание топика через kafka-topics.sh внутри kafka-1"
	@echo "make topic-delete    - удалить топик после лекции"
	@echo "make build           - go build ./..."

run:
	KAFKA_BOOTSTRAP=$(KAFKA_BOOTSTRAP) go run ./cmd/watch-isr \
	  -topic=$(TOPIC) -partitions=$(PARTITIONS) -rf=$(RF) -interval=$(INTERVAL)

kill-broker:
	docker stop $(BROKER)

restore-broker:
	docker start $(BROKER)

topic-describe:
	docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh \
	  --bootstrap-server kafka-1:9092 --describe --topic $(TOPIC)

topic-delete:
	docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh \
	  --bootstrap-server kafka-1:9092 --delete --topic $(TOPIC)

build:
	go build ./...
