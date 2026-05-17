.PHONY: help run run-2nd run-fresh group-describe group-delete build

KAFKA_BOOTSTRAP ?= localhost:19092,localhost:19093,localhost:19094
TOPIC ?= brew.orders.v1
GROUP ?= brew.kitchen-service
FROM_START ?= true
MEMBER_ID ?= 1

help:
	@echo "make run             - запустить kitchen-service (member=$(MEMBER_ID), from-start=$(FROM_START))"
	@echo "make run-2nd         - то же самое с MEMBER_ID=2 (для второго терминала, демонстрирует rebalance)"
	@echo "make run-fresh       - запустить со свежей group.id (видно сообщения, даже если предыдущий запуск их закоммитил)"
	@echo "make group-describe  - kafka-consumer-groups.sh --describe для $(GROUP)"
	@echo "make group-delete    - удалить committed offset'ы группы (осторожно: на следующем запуске опять читаем с earliest)"
	@echo "make build           - go build ./..."

run:
	KAFKA_BOOTSTRAP=$(KAFKA_BOOTSTRAP) MEMBER_ID=$(MEMBER_ID) go run ./cmd/consumer \
	  -topic=$(TOPIC) -group=$(GROUP) -from-start=$(FROM_START)

run-2nd:
	KAFKA_BOOTSTRAP=$(KAFKA_BOOTSTRAP) MEMBER_ID=2 go run ./cmd/consumer \
	  -topic=$(TOPIC) -group=$(GROUP) -from-start=$(FROM_START)

run-fresh:
	KAFKA_BOOTSTRAP=$(KAFKA_BOOTSTRAP) MEMBER_ID=$(MEMBER_ID) go run ./cmd/consumer \
	  -topic=$(TOPIC) -group=$(GROUP)-fresh-$$$$ -from-start=true

group-describe:
	docker exec kafka-1 /opt/kafka/bin/kafka-consumer-groups.sh \
	  --bootstrap-server kafka-1:9092 --describe --group $(GROUP)

group-delete:
	docker exec kafka-1 /opt/kafka/bin/kafka-consumer-groups.sh \
	  --bootstrap-server kafka-1:9092 --delete --group $(GROUP)

build:
	go build ./...
