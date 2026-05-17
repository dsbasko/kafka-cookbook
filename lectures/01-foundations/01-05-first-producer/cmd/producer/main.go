// Утилита первого продьюсера для лекции 01-05.
//
// Контекст Brew: order-service пишет в brew.orders.v1 первые OrderPlaced
// после миграции с RabbitMQ на Kafka. Сценарий учебный - 10 записей через
// ProduceSync с ключом order-N, без батчинга и compression. Цель - увидеть
// пары (partition, offset), которые брокер возвращает в kgo.ProduceResults,
// и сверить их с end-offsets через kadm.ListEndOffsets.
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
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/dsbasko/kafka-sandbox/lectures/internal/kafka"
	"github.com/dsbasko/kafka-sandbox/lectures/internal/log"
	"github.com/dsbasko/kafka-sandbox/lectures/internal/runctx"
)

const (
	defaultTopic       = "brew.orders.v1"
	defaultPartitions  = 3
	defaultReplication = 3
	defaultMessages    = 10
)

func main() {
	logger := log.New()

	topic := flag.String("topic", defaultTopic, "топик, в который пишем")
	partitions := flag.Int("partitions", defaultPartitions, "число партиций при создании")
	rf := flag.Int("rf", defaultReplication, "replication factor при создании")
	messages := flag.Int("messages", defaultMessages, "сколько сообщений записать")
	flag.Parse()

	rootCtx, cancel := runctx.New()
	defer cancel()

	if err := run(rootCtx, runOpts{
		topic:      *topic,
		partitions: int32(*partitions),
		rf:         int16(*rf),
		messages:   *messages,
	}); err != nil {
		logger.Error("brew-producer failed", "err", err)
		os.Exit(1)
	}
}

type runOpts struct {
	topic      string
	partitions int32
	rf         int16
	messages   int
}

func run(ctx context.Context, o runOpts) error {
	cl, err := kafka.NewClient()
	if err != nil {
		return fmt.Errorf("kafka.NewClient: %w", err)
	}
	defer cl.Close()
	admin := kadm.NewClient(cl)

	if err := ensureTopic(ctx, admin, o); err != nil {
		return fmt.Errorf("ensure topic: %w", err)
	}

	fmt.Printf("пишем %d OrderPlaced в топик %q через ProduceSync\n\n", o.messages, o.topic)

	tw := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "N\tKEY\tVALUE\tPARTITION\tOFFSET\tBROKER-TS")

	for i := 0; i < o.messages; i++ {
		if err := ctx.Err(); err != nil {
			_ = tw.Flush()
			return err
		}
		key := fmt.Sprintf("order-%d", i)
		val := fmt.Sprintf("OrderPlaced order_id=order-%d", i)
		rec := &kgo.Record{
			Topic: o.topic,
			Key:   []byte(key),
			Value: []byte(val),
		}

		rpcCtx, rpcCancel := context.WithTimeout(ctx, 10*time.Second)
		res := cl.ProduceSync(rpcCtx, rec)
		rpcCancel()
		if err := res.FirstErr(); err != nil {
			_ = tw.Flush()
			return fmt.Errorf("produce %d: %w", i, err)
		}
		got := res[0].Record
		fmt.Fprintf(tw, "%d\t%s\t%s\t%d\t%d\t%s\n",
			i, key, val, got.Partition, got.Offset, got.Timestamp.Format("15:04:05.000"))
	}
	_ = tw.Flush()

	fmt.Println()
	fmt.Println("готово. Смотрим ту же картину со стороны лога:")
	if err := printEndOffsets(ctx, admin, o.topic); err != nil {
		return fmt.Errorf("print end offsets: %w", err)
	}
	return nil
}

func ensureTopic(ctx context.Context, admin *kadm.Client, o runOpts) error {
	rpcCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()

	resp, err := admin.CreateTopic(rpcCtx, o.partitions, o.rf, nil, o.topic)
	if err == nil && resp.Err == nil {
		fmt.Printf("brew-topic %q создан: partitions=%d rf=%d\n\n", o.topic, o.partitions, o.rf)
		return nil
	}
	cause := err
	if cause == nil {
		cause = resp.Err
	}
	if errors.Is(cause, kerr.TopicAlreadyExists) {
		fmt.Printf("brew-topic %q уже существует - пишем как есть\n\n", o.topic)
		return nil
	}
	return cause
}

func printEndOffsets(ctx context.Context, admin *kadm.Client, topic string) error {
	rpcCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	ends, err := admin.ListEndOffsets(rpcCtx, topic)
	if err != nil {
		return fmt.Errorf("ListEndOffsets: %w", err)
	}
	tw := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "PARTITION\tLATEST")
	var total int64
	ends.Each(func(o kadm.ListedOffset) {
		if o.Err != nil {
			return
		}
		fmt.Fprintf(tw, "%d\t%d\n", o.Partition, o.Offset)
		total += o.Offset
	})
	fmt.Fprintf(tw, "TOTAL\t%d\n", total)
	return tw.Flush()
}
