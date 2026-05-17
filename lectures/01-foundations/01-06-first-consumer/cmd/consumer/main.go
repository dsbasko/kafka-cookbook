// Утилита первого консьюмера для лекции 01-06.
//
// Контекст Brew: kitchen-service на кофейне читает brew.orders.v1, чтобы
// бариста видели новые OrderPlaced. Сценарий учебный - один процесс в группе
// brew.kitchen-service с auto-commit, корректный shutdown по SIGINT. Без
// manual commit, без DLQ, без retry-топиков. Цель - увидеть пары
// (partition, offset) на стороне чтения и сверить их с тем, что вернул
// ProduceSync в лекции 01-05.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"text/tabwriter"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/dsbasko/kafka-sandbox/lectures/internal/config"
	"github.com/dsbasko/kafka-sandbox/lectures/internal/kafka"
	"github.com/dsbasko/kafka-sandbox/lectures/internal/log"
	"github.com/dsbasko/kafka-sandbox/lectures/internal/runctx"
)

const (
	defaultTopic = "brew.orders.v1"
	defaultGroup = "brew.kitchen-service"
)

func main() {
	logger := log.New()

	topic := flag.String("topic", defaultTopic, "брокер-топик, из которого читаем")
	group := flag.String("group", defaultGroup, "group.id консьюмер-группы")
	fromStart := flag.Bool("from-start", true,
		"при первом старте группы читать с earliest (на втором запуске committed offset уже есть и этот флаг ни на что не влияет)")
	flag.Parse()

	memberID := config.EnvOr("MEMBER_ID", "1")

	rootCtx, cancel := runctx.New()
	defer cancel()

	if err := run(rootCtx, runOpts{
		topic:     *topic,
		group:     *group,
		memberID:  memberID,
		fromStart: *fromStart,
	}); err != nil && !errors.Is(err, context.Canceled) {
		logger.Error("brew-consumer failed", "err", err)
		os.Exit(1)
	}
}

type runOpts struct {
	topic     string
	group     string
	memberID  string
	fromStart bool
}

func run(ctx context.Context, o runOpts) error {
	opts := []kgo.Opt{
		kgo.ConsumerGroup(o.group),
		kgo.ConsumeTopics(o.topic),
		kgo.ClientID(fmt.Sprintf("kitchen-service-%s", o.memberID)),
		kgo.OnPartitionsAssigned(func(_ context.Context, _ *kgo.Client, m map[string][]int32) {
			fmt.Fprintf(os.Stderr, "[member=%s] assigned: %v\n", o.memberID, m)
		}),
		kgo.OnPartitionsRevoked(func(_ context.Context, _ *kgo.Client, m map[string][]int32) {
			fmt.Fprintf(os.Stderr, "[member=%s] revoked:  %v\n", o.memberID, m)
		}),
	}
	if o.fromStart {
		opts = append(opts, kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	}

	cl, err := kafka.NewClient(opts...)
	if err != nil {
		return fmt.Errorf("kafka.NewClient: %w", err)
	}
	defer cl.Close()

	fmt.Printf("kitchen-service запущен: brew-topic=%q group=%q member=%s from-start=%v\n",
		o.topic, o.group, o.memberID, o.fromStart)
	fmt.Println("читаем brew-orders; Ctrl+C - выход.")
	fmt.Println()

	tw := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "MEMBER\tPARTITION\tOFFSET\tKEY\tVALUE\tBROKER-TS")

	for {
		fetches := cl.PollFetches(ctx)
		if fetches.IsClientClosed() {
			return nil
		}
		if errs := fetches.Errors(); len(errs) > 0 {
			for _, e := range errs {
				if errors.Is(e.Err, context.Canceled) {
					_ = tw.Flush()
					fmt.Println()
					fmt.Println("kitchen-service остановлен по сигналу, offset'ы коммитятся в Close().")
					return nil
				}
				return fmt.Errorf("fetch %s/%d: %w", e.Topic, e.Partition, e.Err)
			}
		}

		fetches.EachRecord(func(r *kgo.Record) {
			fmt.Fprintf(tw, "%s\t%d\t%d\t%s\t%s\t%s\n",
				o.memberID, r.Partition, r.Offset,
				string(r.Key), string(r.Value),
				r.Timestamp.Format("15:04:05.000"),
			)
		})
		_ = tw.Flush()
	}
}
