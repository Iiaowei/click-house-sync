package cmd

import (
	kadmin "click-house-sync/internal/kafka"
	"fmt"
	"sort"

	"github.com/spf13/cobra"
)

var kafkaCmd = &cobra.Command{
	Use:   "kafka",
	Short: "Kafka 相关子命令",
}

var kafkaTopicsCountCmd = &cobra.Command{
	Use:   "topics-count",
	Short: "统计主题数量",
	RunE: func(cmd *cobra.Command, args []string) error {
		brokers := brokersList()
		names, err := kadmin.ListTopicNames(brokers)
		if err != nil {
			return err
		}
		sort.Strings(names)
		printJSON(map[string]any{
			"command": "kafka topics-count",
			"brokers": brokers,
			"topics":  len(names),
		})
		return nil
	},
}

var kafkaTopicsListCmd = &cobra.Command{
	Use:   "topics",
	Short: "列举主题名称",
	RunE: func(cmd *cobra.Command, args []string) error {
		brokers := brokersList()
		names, err := kadmin.ListTopicNames(brokers)
		if err != nil {
			return err
		}
		sort.Strings(names)
		printJSON(map[string]any{
			"command": "kafka topics",
			"brokers": brokers,
			"topics":  names,
			"count":   len(names),
		})
		return nil
	},
}

var kafkaTopicInfoCmd = &cobra.Command{
	Use:   "topic-info",
	Short: "查看主题信息",
	RunE: func(cmd *cobra.Command, args []string) error {
		topic, _ := cmd.Flags().GetString("topic")
		if topic == "" {
			return fmt.Errorf("缺少 --topic")
		}
		brokers := brokersList()
		parts, err := kadmin.ReadTopicPartitions(brokers, topic)
		if err != nil {
			return err
		}
		rf := 0
		leaders := map[int]struct{}{}
		var pinfo []map[string]any
		for _, p := range parts {
			if len(p.Replicas) > rf {
				rf = len(p.Replicas)
			}
			leaders[p.Leader.ID] = struct{}{}
			pinfo = append(pinfo, map[string]any{
				"id":       p.ID,
				"leader":   p.Leader.ID,
				"replicas": p.Replicas,
				"isr":      p.Isr,
			})
		}
		var leaderIDs []int
		for id := range leaders {
			leaderIDs = append(leaderIDs, id)
		}
		sort.Ints(leaderIDs)
		printJSON(map[string]any{
			"command":            "kafka topic-info",
			"topic":              topic,
			"brokers":            brokers,
			"partitions":         len(parts),
			"replication_factor": rf,
			"leaders":            leaderIDs,
			"partition_info":     pinfo,
		})
		return nil
	},
}

var kafkaTopicsInfoCmd = &cobra.Command{
	Use:   "topics-info",
	Short: "查看所有主题信息",
	RunE: func(cmd *cobra.Command, args []string) error {
		brokers := brokersList()
		names, err := kadmin.ListTopicNames(brokers)
		if err != nil {
			return err
		}
		sort.Strings(names)
		var items []map[string]any
		for _, topic := range names {
			parts, err := kadmin.ReadTopicPartitions(brokers, topic)
			if err != nil {
				return err
			}
			rf := 0
			leaders := map[int]struct{}{}
			var pinfo []map[string]any
			for _, p := range parts {
				if len(p.Replicas) > rf {
					rf = len(p.Replicas)
				}
				leaders[p.Leader.ID] = struct{}{}
				pinfo = append(pinfo, map[string]any{
					"id":       p.ID,
					"leader":   p.Leader.ID,
					"replicas": p.Replicas,
					"isr":      p.Isr,
				})
			}
			var leaderIDs []int
			for id := range leaders {
				leaderIDs = append(leaderIDs, id)
			}
			sort.Ints(leaderIDs)
			items = append(items, map[string]any{
				"topic":              topic,
				"partitions":         len(parts),
				"replication_factor": rf,
				"leaders":            leaderIDs,
				"partition_info":     pinfo,
			})
		}
		printJSON(map[string]any{
			"command": "kafka topics-info",
			"brokers": brokers,
			"topics":  len(names),
			"items":   items,
		})
		return nil
	},
}

func init() {
	rootCmd.AddCommand(kafkaCmd)
	kafkaCmd.AddCommand(kafkaTopicsListCmd)
	kafkaCmd.AddCommand(kafkaTopicsCountCmd)
	kafkaCmd.AddCommand(kafkaTopicInfoCmd)
	kafkaCmd.AddCommand(kafkaTopicsInfoCmd)
	kafkaTopicInfoCmd.Flags().String("topic", "", "主题名称（必填）")
}
