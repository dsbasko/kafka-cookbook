package main

import (
	"context"
	"encoding/binary"
	"errors"
	"flag"
	"fmt"
	"os"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/dsbasko/kafka-sandbox/lectures/internal/kafka"
	"github.com/dsbasko/kafka-sandbox/lectures/internal/log"
	"github.com/dsbasko/kafka-sandbox/lectures/internal/runctx"
)

const (
	defaultChangelogTopic = "lecture-07-02-word-count-changelog"
	defaultStateDir       = "./state"
)

func main() {
	logger := log.New()

	changelogTopic := flag.String("changelog", defaultChangelogTopic, "compacted-топик с changelog state'а")
	stateDir := flag.String("state", defaultStateDir, "директория Pebble state'а")
	clean := flag.Bool("clean", true, "удалить директорию state перед заливкой")

	flag.Parse()

	rootCtx, cancel := runctx.New()
	defer cancel()

	if *clean {
		if err := os.RemoveAll(*stateDir); err != nil {
			logger.Error("clean state failed", "err", err)
			os.Exit(1)
		}
		fmt.Printf("state cleared: %s\n", *stateDir)
	}

	store, err := pebble.Open(*stateDir, &pebble.Options{})
	if err != nil {
		logger.Error("pebble open failed", "err", err)
		os.Exit(1)
	}
	defer store.Close()

	if err := restore(rootCtx, store, *changelogTopic); err != nil && !errors.Is(err, context.Canceled) {
		logger.Error("restore failed", "err", err)
		os.Exit(1)
	}
}

func restore(ctx context.Context, store *pebble.DB, topic string) error {
	endOffsets, err := readEndOffsets(ctx, topic)
	if err != nil {
		return fmt.Errorf("read end offsets: %w", err)
	}
	if len(endOffsets) == 0 {
		fmt.Printf("changelog topic %q пустой — восстанавливать нечего\n", topic)
		return nil
	}

	cl, err := kafka.NewClient(
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		return err
	}
	defer cl.Close()

	var consumed atomic.Int64
	var keys atomic.Int64
	deadline := time.Now().Add(60 * time.Second)
	maxOffsets := map[int32]int64{}
	var writeErr error

	fmt.Printf("restore starting: topic=%q target-offsets=%v\n", topic, endOffsets)

	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("restore timed out (60s); seen %d records", consumed.Load())
		}
		fetches := cl.PollFetches(ctx)
		if errs := fetches.Errors(); len(errs) > 0 {
			for _, e := range errs {
				if errors.Is(e.Err, context.Canceled) {
					return nil
				}
				return fmt.Errorf("fetch %s/%d: %w", e.Topic, e.Partition, e.Err)
			}
		}
		fetches.EachRecord(func(rec *kgo.Record) {
			if writeErr != nil {
				return
			}
			if rec.Offset+1 > maxOffsets[rec.Partition] {
				maxOffsets[rec.Partition] = rec.Offset + 1
			}
			if len(rec.Key) == 0 {
				return
			}
			if len(rec.Value) == 0 {

				if err := store.Delete(rec.Key, pebble.NoSync); err != nil {
					writeErr = fmt.Errorf("pebble delete %q: %w", rec.Key, err)
				}
				return
			}
			if _, ok := decodeUint64(rec.Value); !ok {
				return
			}
			if err := store.Set(rec.Key, rec.Value, pebble.NoSync); err != nil {
				writeErr = fmt.Errorf("pebble set %q: %w", rec.Key, err)
				return
			}
			keys.Add(1)
			consumed.Add(1)
		})
		if writeErr != nil {
			return writeErr
		}

		if reachedEnd(maxOffsets, endOffsets) {
			break
		}
	}

	if err := store.Flush(); err != nil {
		return fmt.Errorf("pebble flush: %w", err)
	}

	fmt.Printf("restore done: records=%d unique-keys≈%d\n",
		consumed.Load(), keys.Load())
	return nil
}

func readEndOffsets(ctx context.Context, topic string) (map[int32]int64, error) {
	admin, err := kafka.NewAdmin()
	if err != nil {
		return nil, err
	}
	defer admin.Close()

	rpcCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	end, err := admin.ListEndOffsets(rpcCtx, topic)
	if err != nil {
		return nil, err
	}

	out := map[int32]int64{}
	var firstErr error
	end.Each(func(o kadm.ListedOffset) {
		if o.Err != nil && firstErr == nil {
			firstErr = fmt.Errorf("ListEndOffsets %s/%d: %w", o.Topic, o.Partition, o.Err)
			return
		}
		if o.Offset > 0 {
			out[o.Partition] = o.Offset
		}
	})
	if firstErr != nil {
		return nil, firstErr
	}
	return out, nil
}

func reachedEnd(progress, end map[int32]int64) bool {
	for partition, target := range end {
		off, ok := progress[partition]
		if !ok {
			return false
		}
		if off < target {
			return false
		}
	}
	return true
}

func decodeUint64(b []byte) (uint64, bool) {
	if len(b) != 8 {
		return 0, false
	}
	return binary.BigEndian.Uint64(b), true
}
