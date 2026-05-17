// Утилита quorum-status - то, чем Brew проверяет здоровье KRaft-кворума на
// свежеподнятом стенде. Печатает: ClusterID, список брокеров,
// MetadataControllerProxy (возврат от BrokerMetadata - proxy-маршрут до
// контроллера; в KRaft он совпадает с активным Raft-лидером только в спокойном
// состоянии кластера) и RaftLeader (возврат от DescribeQuorum по системному
// топику __cluster_metadata - реально активный контроллер кворума).
//
// Зачем разделять. MetadataControllerProxy показывает, через какую ноду клиент
// сходит за метаданными. RaftLeader показывает, кто сейчас держит лидерство в
// Raft-группе. На спокойном кластере значения совпадают. На переходных режимах
// (перевыборы, обслуживание контроллера) расходятся. Если смотреть только на
// BrokerMetadata, можно принять proxy за лидера и долго ловить «кластер вроде
// живой, а почему-то тормозит».
package main

import (
	"context"
	"fmt"
	"os"
	"sort"
	"text/tabwriter"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/dsbasko/kafka-sandbox/lectures/internal/kafka"
	"github.com/dsbasko/kafka-sandbox/lectures/internal/log"
	"github.com/dsbasko/kafka-sandbox/lectures/internal/runctx"
)

const metadataTopic = "__cluster_metadata"

func main() {
	logger := log.New()

	rootCtx, cancel := runctx.New()
	defer cancel()

	if err := run(rootCtx); err != nil {
		logger.Error("quorum-status failed", "err", err)
		os.Exit(1)
	}
}

func run(ctx context.Context) error {
	cl, err := kafka.NewClient()
	if err != nil {
		return fmt.Errorf("kafka.NewClient: %w", err)
	}
	defer cl.Close()

	admin := kadm.NewClient(cl)

	rpcCtx, rpcCancel := context.WithTimeout(ctx, 15*time.Second)
	defer rpcCancel()

	md, err := admin.BrokerMetadata(rpcCtx)
	if err != nil {
		return fmt.Errorf("BrokerMetadata: %w", err)
	}

	leaderID, voters, qErr := describeQuorumLeader(rpcCtx, cl)

	brokers := append([]kadm.BrokerDetail(nil), md.Brokers...)
	sort.Slice(brokers, func(i, j int) bool {
		return brokers[i].NodeID < brokers[j].NodeID
	})

	fmt.Printf("ClusterID:               %s\n", md.Cluster)
	fmt.Printf("Brokers:                 %d\n", len(brokers))
	fmt.Printf("MetadataControllerProxy: %d  (BrokerMetadata.Controller; в KRaft - proxy, не Raft-leader)\n", md.Controller)
	if qErr != nil {
		fmt.Printf("RaftLeader:              <недоступно: %v>\n", qErr)
	} else {
		fmt.Printf("RaftLeader:              %d  (DescribeQuorum по %s; это активный controller)\n", leaderID, metadataTopic)
		fmt.Printf("CurrentVoters:           %v\n", voters)
	}
	fmt.Println()

	tw := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "NODE\tHOST\tPORT\tRACK\tROLE")
	for _, b := range brokers {
		rack := "-"
		if b.Rack != nil && *b.Rack != "" {
			rack = *b.Rack
		}
		role := "broker"
		if qErr == nil && b.NodeID == leaderID {
			role = "broker + active controller"
		} else if containsInt32(voters, b.NodeID) {
			role = "broker + voter"
		}
		fmt.Fprintf(tw, "%d\t%s\t%d\t%s\t%s\n", b.NodeID, b.Host, b.Port, rack, role)
	}
	return tw.Flush()
}

func describeQuorumLeader(ctx context.Context, cl kmsg.Requestor) (int32, []int32, error) {
	req := kmsg.NewPtrDescribeQuorumRequest()
	topic := kmsg.NewDescribeQuorumRequestTopic()
	topic.Topic = metadataTopic
	part := kmsg.NewDescribeQuorumRequestTopicPartition()
	part.Partition = 0
	topic.Partitions = []kmsg.DescribeQuorumRequestTopicPartition{part}
	req.Topics = []kmsg.DescribeQuorumRequestTopic{topic}

	resp, err := req.RequestWith(ctx, cl)
	if err != nil {
		return -1, nil, err
	}
	if len(resp.Topics) == 0 || len(resp.Topics[0].Partitions) == 0 {
		return -1, nil, fmt.Errorf("empty DescribeQuorum response")
	}
	p := resp.Topics[0].Partitions[0]
	if p.ErrorCode != 0 {
		msg := ""
		if p.ErrorMessage != nil {
			msg = *p.ErrorMessage
		}
		return -1, nil, fmt.Errorf("DescribeQuorum partition error code=%d %s", p.ErrorCode, msg)
	}
	voters := make([]int32, 0, len(p.CurrentVoters))
	for _, v := range p.CurrentVoters {
		voters = append(voters, v.ReplicaID)
	}
	sort.Slice(voters, func(i, j int) bool { return voters[i] < voters[j] })
	return p.LeaderID, voters, nil
}

func containsInt32(xs []int32, x int32) bool {
	for _, v := range xs {
		if v == x {
			return true
		}
	}
	return false
}
