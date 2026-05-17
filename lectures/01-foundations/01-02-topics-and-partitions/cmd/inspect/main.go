// Программа inspect - утилита Brew для лекции 01-02. Создаёт топик
// brew.orders.v1 идемпотентно и печатает per-partition таблицу с leader,
// replicas и ISR, чтобы видеть, как Kafka раскидывает партиции по нодам
// стенда. После пятничного промо «бесплатный кофе по пятницам» Brew
// пересоздал брокер-топик с 12 партициями вместо одной - эта программа
// показывает, что именно изменилось в metadata.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"text/tabwriter"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"

	"github.com/dsbasko/kafka-sandbox/lectures/internal/kafka"
	"github.com/dsbasko/kafka-sandbox/lectures/internal/log"
	"github.com/dsbasko/kafka-sandbox/lectures/internal/runctx"
)

const (
	defaultTopic       = "brew.orders.v1"
	defaultPartitions  = 3
	defaultReplication = 3
)

func main() {
	logger := log.New()

	topic := flag.String("topic", defaultTopic, "brew-топик, который создаём и описываем (по умолчанию brew.orders.v1)")
	partitions := flag.Int("partitions", defaultPartitions, "число партиций при создании")
	rf := flag.Int("rf", defaultReplication, "replication factor при создании")
	recreate := flag.Bool("recreate", false, "удалить топик перед созданием")
	flag.Parse()

	rootCtx, cancel := runctx.New()
	defer cancel()

	if err := run(rootCtx, *topic, int32(*partitions), int16(*rf), *recreate); err != nil {
		logger.Error("inspect failed", "err", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, topic string, partitions int32, rf int16, recreate bool) error {
	admin, err := kafka.NewAdmin()
	if err != nil {
		return fmt.Errorf("kafka.NewAdmin: %w", err)
	}
	defer admin.Close()

	rpcCtx, rpcCancel := context.WithTimeout(ctx, 15*time.Second)
	defer rpcCancel()

	if recreate {
		if err := deleteTopic(rpcCtx, admin, topic); err != nil {
			return fmt.Errorf("delete topic: %w", err)
		}
		fmt.Printf("brew-topic %q удалён\n", topic)
	}

	created, err := ensureTopic(rpcCtx, admin, topic, partitions, rf)
	if err != nil {
		return fmt.Errorf("ensure topic: %w", err)
	}
	if created {
		fmt.Printf("brew-topic %q создан: partitions=%d rf=%d\n", topic, partitions, rf)
	} else {
		fmt.Printf("brew-topic %q уже существует - описываем\n", topic)
	}

	details, err := admin.ListTopics(rpcCtx, topic)
	if err != nil {
		return fmt.Errorf("ListTopics: %w", err)
	}
	td, ok := details[topic]
	if !ok {
		return fmt.Errorf("topic %q отсутствует в metadata-ответе", topic)
	}
	if td.Err != nil {
		return fmt.Errorf("topic %q load error: %w", topic, td.Err)
	}

	printTopic(td)
	return nil
}

func ensureTopic(ctx context.Context, admin *kadm.Client, topic string, partitions int32, rf int16) (bool, error) {
	resp, err := admin.CreateTopic(ctx, partitions, rf, nil, topic)
	if err == nil && resp.Err == nil {
		return true, nil
	}

	cause := err
	if cause == nil {
		cause = resp.Err
	}
	if errors.Is(cause, kerr.TopicAlreadyExists) {
		return false, nil
	}
	return false, cause
}

func deleteTopic(ctx context.Context, admin *kadm.Client, topic string) error {
	resp, err := admin.DeleteTopic(ctx, topic)
	if err == nil && resp.Err == nil {
		return nil
	}
	cause := err
	if cause == nil {
		cause = resp.Err
	}
	if errors.Is(cause, kerr.UnknownTopicOrPartition) {
		return nil
	}
	return cause
}

func printTopic(td kadm.TopicDetail) {
	fmt.Printf("\nTopic:       %s\n", td.Topic)
	fmt.Printf("TopicID:     %s\n", td.ID)
	fmt.Printf("Partitions:  %d\n", len(td.Partitions))
	fmt.Println()

	parts := td.Partitions.Sorted()

	tw := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "PARTITION\tLEADER\tREPLICAS\tISR\tOFFLINE")
	for _, p := range parts {
		offline := fmt.Sprintf("%v", p.OfflineReplicas)
		if len(p.OfflineReplicas) == 0 {
			offline = "-"
		}
		fmt.Fprintf(tw, "%d\t%d\t%v\t%v\t%s\n", p.Partition, p.Leader, p.Replicas, p.ISR, offline)
	}
	_ = tw.Flush()
}
