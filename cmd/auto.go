// cmd 包包含一键创建与导出的 auto 命令。
package cmd

import (
	"click-house-sync/internal/clickhouse"
	kadmin "click-house-sync/internal/kafka"
	"fmt"
	"strings"

	"github.com/spf13/cobra"
)

// autoCmd 一键创建 Topic/表/MV，并将数据导出到 Kafka。
var autoCmd = &cobra.Command{
	Use:   "auto",
	Short: "输入源表名自动完成全部步骤",
	Long:  "一键为单表创建 Topic、Kafka 表、物化视图，并导出数据到 Kafka。可结合全局与表级配置（order/key/cursor）。",
	RunE: func(cmd *cobra.Command, args []string) error {
		// 读取源表参数
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
		// 查找表级配置
		tconf, err := lookupTableConfig(table)
		if err != nil {
			return err
		}
		// 源库与默认 topic
		srcDB := chDatabase
		if !cmd.Root().PersistentFlags().Changed("ch-database") && tconf != nil && tconf.CurrentDatabase != "" {
			srcDB = tconf.CurrentDatabase
		}
		if kafkaTopic == "" {
			kafkaTopic = srcDB + "_" + table
		}
		// 目标库（Kafka 表所在库）
		if targetDatabase == "" {
			if tconf != nil && tconf.TargetDatabase != "" {
				targetDatabase = tconf.TargetDatabase
			} else {
				targetDatabase = chDatabase
			}
		}
		tgtTable := targetTable
		if strings.TrimSpace(tgtTable) == "" {
			if tconf != nil && strings.TrimSpace(tconf.TargetTable) != "" {
				tgtTable = tconf.TargetTable
			} else {
				tgtTable = table
			}
		}
		// 估算分区并创建 Topic
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
		if err := kadmin.CreateTopic(brokers, kafkaTopic, p, replicationFactor); err != nil {
			return err
		}
		// 创建 Kafka 表与推送型 MV
		var group string
		if cmd.Root().PersistentFlags().Changed("group-name") && strings.TrimSpace(groupName) != "" {
			group = groupName
		} else if tconf != nil && strings.TrimSpace(tconf.GroupName) != "" {
			group = tconf.GroupName
		} else {
			group = groupName + "-" + table
		}
		kafkaDB := targetDatabase
		if err := clickhouse.CreateDatabaseIfNotExists(db, kafkaDB); err != nil {
			return err
		}
		eng := mvEngine
		if tconf != nil && strings.TrimSpace(tconf.MVEngine) != "" {
			eng = tconf.MVEngine
		}
		verCol := versionColumn
		if tconf != nil && strings.TrimSpace(tconf.VersionColumn) != "" {
			verCol = tconf.VersionColumn
		}
		sCol := signColumn
		if tconf != nil && strings.TrimSpace(tconf.SignColumn) != "" {
			sCol = tconf.SignColumn
		}
		extras := map[string]string{}
		switch strings.ToLower(strings.TrimSpace(eng)) {
		case "replacing":
			if strings.TrimSpace(verCol) != "" {
				extras[verCol] = "UInt64"
			}
		case "collapsing":
			if strings.TrimSpace(sCol) != "" {
				extras[sCol] = "Int8"
			}
		case "versioned_collapsing":
			if strings.TrimSpace(sCol) != "" {
				extras[sCol] = "Int8"
			} else {
				extras["sign"] = "Int8"
			}
			if strings.TrimSpace(verCol) != "" {
				extras[verCol] = "UInt64"
			} else {
				extras["version"] = "UInt64"
			}
		}
		if err := clickhouse.CreateKafkaTableFromSource(db, srcDB, table, kafkaDB, brokers, kafkaTopic, group, "JSONEachRow", 1, kafkaMaxBlockSize, kafkaAutoOffsetReset, extras); err != nil {
			return err
		}
		if err := clickhouse.CreateTargetTableLikeSource(db, srcDB, table, targetDatabase, tgtTable, "tuple()", ""); err != nil {
			return err
		}
		if err := clickhouse.CreateMaterializedView(db, kafkaDB, table, targetDatabase, tgtTable); err != nil {
			return err
		}
		sourceCols, err := clickhouse.GetColumns(db, srcDB, table)
		if err != nil {
			return err
		}
		targetCols, err := clickhouse.GetColumns(db, targetDatabase, tgtTable)
		if err != nil {
			return err
		}
		typeDiffs := clickhouse.AnalyzeTypeDiff(sourceCols, targetCols)
		// 回补历史数据：导出到 Kafka（可按游标/排序/键配置）
		bs := batchSize
		if tconf != nil && tconf.BatchSize > 0 {
			bs = tconf.BatchSize
		}
		ord := exportOrderBy
		if tconf != nil && strings.TrimSpace(tconf.ExportOrderBy) != "" {
			ord = tconf.ExportOrderBy
		}
		keycol := exportKeyColumn
		if tconf != nil && strings.TrimSpace(tconf.ExportKeyColumn) != "" {
			keycol = tconf.ExportKeyColumn
		}
		curCol := cursorColumn
		curStart := cursorStart
		curEnd := cursorEnd
		if tconf != nil {
			if strings.TrimSpace(tconf.CursorColumn) != "" {
				curCol = tconf.CursorColumn
			}
			if strings.TrimSpace(tconf.CursorStart) != "" {
				curStart = tconf.CursorStart
			}
			if strings.TrimSpace(tconf.CursorEnd) != "" {
				curEnd = tconf.CursorEnd
			}
		}
		vtCol := strings.TrimSpace(versionTimeColumn)
		if tconf != nil && strings.TrimSpace(tconf.VersionTimeColumn) != "" {
			vtCol = strings.TrimSpace(tconf.VersionTimeColumn)
		}
		if strings.TrimSpace(curCol) == "" && strings.TrimSpace(vtCol) != "" {
			curCol = vtCol
		}
		if strings.TrimSpace(ord) == "" && strings.TrimSpace(vtCol) != "" {
			ord = vtCol
		}
		if err := exportTableToKafka(db, srcDB, table, brokers, kafkaTopic, bs, ord, keycol, curCol, curStart, curEnd, 1, 1, n); err != nil {
			return err
		}
		// 输出执行结果
		printJSON(map[string]any{
			"command":           "auto",
			"database":          srcDB,
			"table":             table,
			"topic":             kafkaTopic,
			"partitions":        p,
			"replication_factor": replicationFactor,
			"group":             group,
			"kafka_table":       strings.Join([]string{kafkaDB, "kafka_" + table + "_sink"}, "."),
			"materialized_view": strings.Join([]string{targetDatabase, "mv_from_kafka_" + table}, "."),
			"target_table":      strings.Join([]string{targetDatabase, tgtTable}, "."),
			"type_diffs":        typeDiffs,
			"source":            strings.Join([]string{srcDB, table}, "."),
		})
		return nil
	},
}

func init() {
	rootCmd.AddCommand(autoCmd)
	// 源表名：用于生成 Kafka 表字段，并作为推送 MV 的数据来源
	autoCmd.Flags().String("table", "", "源表名（必填）")
}
