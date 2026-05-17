// Утилита load-and-watch для лекции 01-04. Готовит маленькую песочницу
// retention'а на отдельном тренировочном топике brew.orders.retention-demo:
// создаёт его с короткими retention.ms и segment.ms, заливает 100 сообщений-
// «заказов» и потом каждые 10 секунд печатает earliest/latest/retained
// per-partition. Настоящий brew.orders.v1 на стенде Brew живёт с retention
// 30 дней и не трогается этой утилитой - демо использует отдельное имя
// специально, чтобы лекции 01-05 и 01-06 потом работали с чистым
// brew.orders.v1 без риска что записи испарятся через минуту.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"sort"
	"strconv"
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
	defaultTopic       = "brew.orders.retention-demo"
	defaultPartitions  = 3
	defaultReplication = 3
	defaultMessages    = 100
	defaultInterval    = 10 * time.Second
	defaultRetention   = 60 * time.Second
	defaultSegment     = 10 * time.Second
)

func main() {
	logger := log.New()

	topic := flag.String("topic", defaultTopic, "топик, который наполняем и наблюдаем")
	partitions := flag.Int("partitions", defaultPartitions, "число партиций при создании")
	rf := flag.Int("rf", defaultReplication, "replication factor при создании")
	messages := flag.Int("messages", defaultMessages, "сколько сообщений записать на старте")
	interval := flag.Duration("interval", defaultInterval, "пауза между опросами offset'ов и heartbeat-записями")
	retention := flag.Duration("retention", defaultRetention, "retention.ms топика")
	segment := flag.Duration("segment", defaultSegment, "segment.ms топика - после этого сегмент закрывается и может быть удалён по retention")
	recreate := flag.Bool("recreate", false, "удалить топик перед созданием - обнуляет offset'ы и счётчик")
	flag.Parse()

	rootCtx, cancel := runctx.New()
	defer cancel()

	if err := run(rootCtx, runOpts{
		topic:      *topic,
		partitions: int32(*partitions),
		rf:         int16(*rf),
		messages:   *messages,
		interval:   *interval,
		retention:  *retention,
		segment:    *segment,
		recreate:   *recreate,
	}); err != nil {
		logger.Error("load-and-watch failed", "err", err)
		os.Exit(1)
	}
}

type runOpts struct {
	topic      string
	partitions int32
	rf         int16
	messages   int
	interval   time.Duration
	retention  time.Duration
	segment    time.Duration
	recreate   bool
}

func run(ctx context.Context, o runOpts) error {
	cl, err := kafka.NewClient()
	if err != nil {
		return fmt.Errorf("kafka.NewClient: %w", err)
	}
	defer cl.Close()
	admin := kadm.NewClient(cl)

	if o.recreate {
		dropCtx, dropCancel := context.WithTimeout(ctx, 15*time.Second)
		if err := deleteTopic(dropCtx, admin, o.topic); err != nil {
			dropCancel()
			return fmt.Errorf("delete topic: %w", err)
		}
		dropCancel()
		fmt.Printf("brew-topic %q удалён (recreate=true)\n", o.topic)
	}

	if err := ensureTopic(ctx, admin, o); err != nil {
		return fmt.Errorf("ensure topic: %w", err)
	}

	if err := loadInitial(ctx, cl, o.topic, o.messages); err != nil {
		return fmt.Errorf("load initial: %w", err)
	}
	fmt.Printf("brew-orders: записано %d сообщений в топик %q\n\n", o.messages, o.topic)

	fmt.Printf("watching offsets каждые %s (Ctrl+C - выход)\n", o.interval)
	fmt.Printf("retention.ms=%s, segment.ms=%s - старые сегменты должны уходить через retention+интервал retention-checker'а\n\n",
		o.retention, o.segment)

	t := time.NewTicker(o.interval)
	defer t.Stop()

	hb := 0
	if err := tick(ctx, admin, o.topic, hb); err != nil {
		fmt.Fprintf(os.Stderr, "tick failed: %v\n", err)
	}
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
			hb++
			if err := writeHeartbeat(ctx, cl, o.topic, hb); err != nil {

				fmt.Fprintf(os.Stderr, "heartbeat write failed: %v\n", err)
			}
			if err := tick(ctx, admin, o.topic, hb); err != nil {
				fmt.Fprintf(os.Stderr, "tick failed: %v\n", err)
			}
		}
	}
}

func ensureTopic(ctx context.Context, admin *kadm.Client, o runOpts) error {
	rpcCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()

	configs := map[string]*string{
		"retention.ms":   kadm.StringPtr(strconv.FormatInt(o.retention.Milliseconds(), 10)),
		"segment.ms":     kadm.StringPtr(strconv.FormatInt(o.segment.Milliseconds(), 10)),
		"cleanup.policy": kadm.StringPtr("delete"),
	}

	resp, err := admin.CreateTopic(rpcCtx, o.partitions, o.rf, configs, o.topic)
	if err == nil && resp.Err == nil {
		fmt.Printf("brew-topic %q создан: partitions=%d rf=%d retention.ms=%d segment.ms=%d\n",
			o.topic, o.partitions, o.rf, o.retention.Milliseconds(), o.segment.Milliseconds())
		return nil
	}
	cause := err
	if cause == nil {
		cause = resp.Err
	}
	if !errors.Is(cause, kerr.TopicAlreadyExists) {
		return cause
	}

	fmt.Printf("brew-topic %q уже существует - подгоняем retention/segment\n", o.topic)
	alters := []kadm.AlterConfig{
		{Op: kadm.SetConfig, Name: "retention.ms", Value: configs["retention.ms"]},
		{Op: kadm.SetConfig, Name: "segment.ms", Value: configs["segment.ms"]},
		{Op: kadm.SetConfig, Name: "cleanup.policy", Value: configs["cleanup.policy"]},
	}
	alterResp, err := admin.AlterTopicConfigs(rpcCtx, alters, o.topic)
	if err != nil {
		return fmt.Errorf("AlterTopicConfigs: %w", err)
	}
	for _, r := range alterResp {
		if r.Err != nil {
			return fmt.Errorf("alter %s: %w", r.Name, r.Err)
		}
	}
	return nil
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

func loadInitial(ctx context.Context, cl *kgo.Client, topic string, messages int) error {
	rpcCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	// Изображаем шквал заказов на промо «бесплатный кофе по пятницам»:
	// 100 OrderPlaced подряд с ключом order_id, чтобы наполнить
	// лог и потом наблюдать как retention его выкашивает.
	for i := 0; i < messages; i++ {
		rec := &kgo.Record{
			Topic: topic,
			Key:   []byte(fmt.Sprintf("order-%d", i)),
			Value: []byte(fmt.Sprintf("OrderPlaced order_id=order-%d", i)),
		}
		if err := cl.ProduceSync(rpcCtx, rec).FirstErr(); err != nil {
			return fmt.Errorf("produce %d: %w", i, err)
		}
	}
	return nil
}

func writeHeartbeat(ctx context.Context, cl *kgo.Client, topic string, n int) error {
	rpcCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	// Heartbeat нужен, чтобы активный сегмент закрывался по segment.ms:
	// retention не трогает активный сегмент, без heartbeat'ов лог застрял бы
	// в одном незакрытом сегменте и старые записи никогда не удалились бы.
	rec := &kgo.Record{
		Topic: topic,
		Key:   []byte(fmt.Sprintf("hb-%d", n)),
		Value: []byte(fmt.Sprintf("heartbeat-%d", n)),
	}
	return cl.ProduceSync(rpcCtx, rec).FirstErr()
}

func tick(ctx context.Context, admin *kadm.Client, topic string, hb int) error {
	rpcCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	starts, err := admin.ListStartOffsets(rpcCtx, topic)
	if err != nil {
		return fmt.Errorf("ListStartOffsets: %w", err)
	}
	ends, err := admin.ListEndOffsets(rpcCtx, topic)
	if err != nil {
		return fmt.Errorf("ListEndOffsets: %w", err)
	}

	type row struct {
		partition int32
		earliest  int64
		latest    int64
	}
	var rows []row
	starts.Each(func(o kadm.ListedOffset) {
		if o.Err != nil {
			return
		}
		rows = append(rows, row{partition: o.Partition, earliest: o.Offset})
	})
	for i := range rows {
		if eo, ok := ends.Lookup(topic, rows[i].partition); ok && eo.Err == nil {
			rows[i].latest = eo.Offset
		} else {
			rows[i].latest = -1
		}
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].partition < rows[j].partition })

	fmt.Printf("[%s]  heartbeats=%d\n", time.Now().Format("15:04:05"), hb)
	tw := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "PARTITION\tEARLIEST\tLATEST\tRETAINED")
	var totalEarliest, totalLatest int64
	for _, r := range rows {
		retained := r.latest - r.earliest
		fmt.Fprintf(tw, "%d\t%d\t%d\t%d\n", r.partition, r.earliest, r.latest, retained)
		totalEarliest += r.earliest
		totalLatest += r.latest
	}
	fmt.Fprintf(tw, "TOTAL\t%d\t%d\t%d\n", totalEarliest, totalLatest, totalLatest-totalEarliest)
	_ = tw.Flush()
	fmt.Println("---")
	return nil
}
