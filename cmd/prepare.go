// cmd 包包含资源预创建的命令。
package cmd

import (
	"click-house-sync/internal/clickhouse"
	kadmin "click-house-sync/internal/kafka"
	"fmt"
	"strings"

	"github.com/spf13/cobra"
)

// prepareCmd 为单表创建 Kafka Topic、Kafka 引擎表与物化视图。
var prepareCmd = &cobra.Command{
	Use:   "prepare",
	Short: "创建Kafka表、Topic、物化视图",
	Long:  "为单表创建 Kafka Topic、Kafka 引擎表、物化视图（MV）。不导出数据，适合预创建资源。",
	RunE: func(cmd *cobra.Command, args []string) error {
		// 读取源表参数并校验
		table, _ := cmd.Flags().GetString("table")
		if table == "" {
			return fmt.Errorf("缺少 --table")
		}
		// 连接 ClickHouse
		db, err := clickhouse.Connect(chHost, chPort, chUser, chPassword, chDatabase, chSecure)
		if err != nil {
			return err
		}
		defer db.Close()
		// 查询表级配置（current/target database、brokers、group 等）
		tconf, err := lookupTableConfig(table)
		if err != nil {
			return err
		}
		// 确认源库（默认全局 ch-database，或取表级 current_database）
		srcDB := chDatabase
		if !cmd.Root().PersistentFlags().Changed("ch-database") && tconf != nil && tconf.CurrentDatabase != "" {
			srcDB = tconf.CurrentDatabase
		}
		// 生成默认 topic 名：<srcDB>_<table>
		if kafkaTopic == "" {
			kafkaTopic = srcDB + "_" + table
		}
		if targetDatabase == "" {
			if tconf != nil && tconf.TargetDatabase != "" {
				targetDatabase = tconf.TargetDatabase
			} else {
				targetDatabase = chDatabase
			}
		}
		// 估算分区数：根据表行数与 rows_per_partition 折算
		n, err := clickhouse.CountTableRows(db, srcDB, table)
		if err != nil {
			return err
		}
		rpp := rowsPerPartition
		if tconf != nil && tconf.RowsPerPartition > 0 {
			rpp = tconf.RowsPerPartition
		}
		p := clickhouse.PartitionsForRows(n, rpp)
		var brokers []string
		if cmd.Root().PersistentFlags().Changed("kafka-brokers") {
			brokers = brokersList()
		} else if tconf != nil && len(tconf.Brokers) > 0 {
			brokers = tconf.Brokers
		} else {
			brokers = brokersList()
		}
		// 创建 Kafka Topic
		recreateTopic, _ := cmd.Flags().GetBool("recreate-topic")
		if recreateTopic {
			_ = kadmin.DeleteTopic(brokers, kafkaTopic)
		}
		if err := kadmin.CreateTopic(brokers, kafkaTopic, p, replicationFactor); err != nil {
			return err
		}
		var group string
		if cmd.Root().PersistentFlags().Changed("group-name") && strings.TrimSpace(groupName) != "" {
			group = groupName
		} else if tconf != nil && strings.TrimSpace(tconf.GroupName) != "" {
			group = tconf.GroupName
		} else {
			group = groupName + "-" + table
		}
		// Kafka 引擎表与推送型 MV 保持与源库一致，确保跨库不引发不稳定行为
		kafkaDB := targetDatabase
		if err := clickhouse.CreateDatabaseIfNotExists(db, kafkaDB); err != nil {
			return err
		}
		// 创建 Kafka 引擎表（字段结构与源表一致）
		if err := clickhouse.CreateKafkaTableFromSource(db, srcDB, table, kafkaDB, brokers, kafkaTopic, group, "JSONEachRow", 1, kafkaMaxBlockSize, kafkaAutoOffsetReset); err != nil {
			return err
		}
		if queryableMV {
			if err := clickhouse.CreateMaterializedViewOwn(db, kafkaDB, table, targetDatabase); err != nil {
				return err
			}
			printJSON(map[string]any{
				"command":            "prepare",
				"database":           srcDB,
				"table":              table,
				"topic":              kafkaTopic,
				"partitions":         p,
				"replication_factor": replicationFactor,
				"group":              group,
				"kafka_table":        strings.Join([]string{kafkaDB, "kafka_" + table + "_sink"}, "."),
				"materialized_view":  strings.Join([]string{targetDatabase, "mv_from_kafka_" + table}, "."),
				"source":             strings.Join([]string{srcDB, table}, "."),
			})
		} else {
			if err := clickhouse.CreateMaterializedViewToKafka(db, srcDB, table, kafkaDB); err != nil {
				return err
			}
			printJSON(map[string]any{
				"command":                    "prepare",
				"database":                   srcDB,
				"table":                      table,
				"topic":                      kafkaTopic,
				"partitions":                 p,
				"replication_factor":         replicationFactor,
				"group":                      group,
				"kafka_table":                strings.Join([]string{kafkaDB, "kafka_" + table + "_sink"}, "."),
				"materialized_view_to_kafka": strings.Join([]string{kafkaDB, "mv_to_kafka_" + table}, "."),
				"source":                     strings.Join([]string{srcDB, table}, "."),
			})
		}
		return nil
	},
}

func init() {
	rootCmd.AddCommand(prepareCmd)
	// 源表名：用于生成 Kafka 表字段，并作为推送 MV 的数据来源
	prepareCmd.Flags().String("table", "", "源表名（必填）")
	prepareCmd.Flags().Bool("recreate", false, "删除并重建 Kafka 表与物化视图")
	prepareCmd.Flags().Bool("recreate-topic", false, "删除并重建 Kafka 主题")
}
