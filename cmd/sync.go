// cmd 包包含多表同步的命令。
package cmd

import (
	"click-house-sync/internal/clickhouse"
	"click-house-sync/internal/config"
	kadmin "click-house-sync/internal/kafka"
	"fmt"
	"strings"

	"github.com/spf13/cobra"
)

// syncCmd 从 tables.yaml 读取多表，顺序创建资源并可选择执行导出。
var syncCmd = &cobra.Command{
	Use:   "sync",
	Short: "从配置文件批量准备或同步多表",
	Long:  "读取 tables.yaml 批量为多表创建 Topic、Kafka 引擎表、物化视图，并可按配置导出数据到 Kafka（支持表级 order/key/cursor 配置）。",
	RunE: func(cmd *cobra.Command, args []string) error {
		// 读取执行选项：是否仅准备资源、遇错是否继续、指定表列表
		prepareOnly, _ := cmd.Flags().GetBool("prepare-only")
		continueOnError, _ := cmd.Flags().GetBool("continue-on-error")
		fullExport, _ := cmd.Flags().GetBool("full-export")
		recreate, _ := cmd.Flags().GetBool("recreate")
		recreateTopic, _ := cmd.Flags().GetBool("recreate-topic")
		sourceMVToKafka, _ := cmd.Flags().GetBool("source-mv-to-kafka")
		kafkaDatabaseFlag, _ := cmd.Flags().GetString("kafka-database")
		if fullExport {
			prepareOnly = false
		}
		tablesCSV, _ := cmd.Flags().GetString("tables")
		var names []string
		if tablesCSV != "" {
			names = splitCSV(tablesCSV)
		}
		if tablesFile == "" {
			tablesFile = "tables.yaml"
		}
		// 建立 ClickHouse 连接
		db, err := clickhouse.Connect(chHost, chPort, chUser, chPassword, chDatabase, chSecure)
		if err != nil {
			return err
		}
		defer db.Close()

		// 读取批量表配置
		tlist, err := config.LoadTablesFile(tablesFile)
		if err != nil {
			return err
		}
		var targetList []config.Table
		if len(names) > 0 {
			for _, n := range names {
				for _, t := range tlist {
					if t.Name == n {
						targetList = append(targetList, t)
					}
				}
			}
		} else {
			targetList = tlist
		}
		if len(targetList) == 0 {
			return fmt.Errorf("tables_file 无表项或未匹配到指定表")
		}
		var results []map[string]any
		dbset := map[string]struct{}{}
		for i, t := range targetList {
			// 决定源/目标库：优先使用表级配置，其次全局参数
			srcDB := chDatabase
			if !cmd.Root().PersistentFlags().Changed("ch-database") && t.CurrentDatabase != "" {
				srcDB = t.CurrentDatabase
			}
			dbset[srcDB] = struct{}{}
			tgtDB := chDatabase
			if cmd.Root().PersistentFlags().Changed("target-database") {
				tgtDB = targetDatabase
			} else if t.TargetDatabase != "" {
				tgtDB = t.TargetDatabase
			}
			tgtTable := targetTable
			if strings.TrimSpace(tgtTable) == "" {
				if strings.TrimSpace(t.TargetTable) != "" {
					tgtTable = t.TargetTable
				} else {
					tgtTable = t.Name
				}
			}

			// 构造资源参数：topic、brokers、replicas、分区估算、批量大小、group
			topic := srcDB + "_" + t.Name
			if cmd.Root().PersistentFlags().Changed("kafka-topic") && strings.TrimSpace(kafkaTopic) != "" {
				topic = kafkaTopic
			}
			var brokers []string
			if cmd.Root().PersistentFlags().Changed("kafka-brokers") {
				brokers = brokersList()
			} else if len(t.Brokers) > 0 {
				brokers = t.Brokers
			} else {
				brokers = brokersList()
			}
			rep := replicationFactor
			rowsPer := t.RowsPerPartition
			if rowsPer <= 0 {
				rowsPer = rowsPerPartition
			}
			bsize := t.BatchSize
			if bsize <= 0 {
				bsize = batchSize
			}
			var group string
			if cmd.Root().PersistentFlags().Changed("group-name") && strings.TrimSpace(groupName) != "" {
				group = groupName
			} else if strings.TrimSpace(t.GroupName) != "" {
				group = t.GroupName
			} else {
				group = groupName + "-" + t.Name
			}
			// 推送模式：不创建目标 MergeTree 表
			n, err := clickhouse.CountTableRows(db, srcDB, t.Name)
			if err != nil {
				results = append(results, map[string]any{"table": t.Name, "error": err.Error()})
				if continueOnError {
					continue
				}
				return err
			}
			p := clickhouse.PartitionsForRows(n, rowsPer)
			if recreateTopic {
				_ = kadmin.DeleteTopic(brokers, topic)
			}
			if err := kadmin.CreateTopic(brokers, topic, p, rep); err != nil {
				results = append(results, map[string]any{"table": t.Name, "error": err.Error()})
				if continueOnError {
					continue
				}
				return err
			}
			// Kafka 引擎表与物化视图所在库，默认跟随 target-database，可通过 --kafka-database 显式指定
			kafkaDB := tgtDB
			if strings.TrimSpace(kafkaDatabaseFlag) != "" {
				kafkaDB = strings.TrimSpace(kafkaDatabaseFlag)
			}
			if err := clickhouse.CreateDatabaseIfNotExists(db, kafkaDB); err != nil {
				results = append(results, map[string]any{"table": t.Name, "error": err.Error()})
				if continueOnError {
					continue
				}
				return err
			}
			if recreate {
				_ = clickhouse.DropMaterializedViewIfExists(db, kafkaDB, "mv_"+t.Name)
				_ = clickhouse.DropMaterializedViewIfExists(db, kafkaDB, "mv_from_kafka_"+t.Name)
				_ = clickhouse.DropMaterializedViewIfExists(db, kafkaDB, "mv_to_kafka_"+t.Name)
				_ = clickhouse.DropTableIfExists(db, kafkaDB, "kafka_"+t.Name)
				_ = clickhouse.DropTableIfExists(db, kafkaDB, "kafka_"+t.Name+"_sink")
			}
			// 创建 Kafka 引擎表（字段结构对齐源表）
			// decide MV engine and extras for Kafka sink schema per table
			eng := mvEngine
			if strings.TrimSpace(t.MVEngine) != "" {
				eng = t.MVEngine
			}
			mvOrd := mvOrderBy
			if strings.TrimSpace(t.MVOrderBy) != "" {
				mvOrd = t.MVOrderBy
			}
			mvPart := mvPartitionBy
			if strings.TrimSpace(t.MVPartitionBy) != "" {
				mvPart = t.MVPartitionBy
			}
			verCol := versionColumn
			if strings.TrimSpace(t.VersionColumn) != "" {
				verCol = t.VersionColumn
			}
			if strings.ToLower(strings.TrimSpace(eng)) == "replacing" && strings.TrimSpace(verCol) == "" {
				verCol = "version"
			}
			sCol := signColumn
			if strings.TrimSpace(t.SignColumn) != "" {
				sCol = t.SignColumn
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
			if err := clickhouse.CreateKafkaTableFromSource(db, srcDB, t.Name, kafkaDB, brokers, topic, group, "JSONEachRow", 1, kafkaMaxBlockSize, kafkaAutoOffsetReset, extras); err != nil {
				results = append(results, map[string]any{"table": t.Name, "error": err.Error()})
				if continueOnError {
					continue
				}
				return err
			}
			typeDiffs := []clickhouse.TypeDiff{}
			if sourceMVToKafka {
				if err := clickhouse.CreateMaterializedViewToKafka(db, srcDB, t.Name, kafkaDB); err != nil {
					results = append(results, map[string]any{"table": t.Name, "error": err.Error()})
					if continueOnError {
						continue
					}
					return err
				}
			} else if queryableMV {
				td := mvTTLDays
				tc := mvTTLColumn
				if t.MVTTLDays > 0 {
					td = t.MVTTLDays
				}
				if strings.TrimSpace(t.MVTTLColumn) != "" {
					tc = t.MVTTLColumn
				}
				if err := clickhouse.CreateMaterializedViewOwn(db, kafkaDB, srcDB, t.Name, tgtDB, eng, mvOrd, mvPart, verCol, sCol, td, tc, mvMaxPartitionsPerInsertBlock); err != nil {
					results = append(results, map[string]any{"table": t.Name, "error": err.Error()})
					if continueOnError {
						continue
					}
					return err
				}
			} else {
				if err := clickhouse.CreateTargetTableLikeSource(db, srcDB, t.Name, tgtDB, tgtTable, "tuple()", ""); err != nil {
					results = append(results, map[string]any{"table": t.Name, "error": err.Error()})
					if continueOnError {
						continue
					}
					return err
				}
				sourceCols, err := clickhouse.GetColumns(db, srcDB, t.Name)
				if err != nil {
					results = append(results, map[string]any{"table": t.Name, "error": err.Error()})
					if continueOnError {
						continue
					}
					return err
				}
				targetCols, err := clickhouse.GetColumns(db, tgtDB, tgtTable)
				if err != nil {
					results = append(results, map[string]any{"table": t.Name, "error": err.Error()})
					if continueOnError {
						continue
					}
					return err
				}
				typeDiffs = clickhouse.AnalyzeTypeDiff(sourceCols, targetCols)
				if err := clickhouse.CreateMaterializedView(db, kafkaDB, t.Name, tgtDB, tgtTable); err != nil {
					results = append(results, map[string]any{"table": t.Name, "error": err.Error()})
					if continueOnError {
						continue
					}
					return err
				}
			}
			if !prepareOnly {
				// 可选：回补历史数据到 Kafka（使用导出参数）
				ord := exportOrderBy
				if strings.TrimSpace(t.ExportOrderBy) != "" {
					ord = t.ExportOrderBy
				}
				keycol := exportKeyColumn
				if strings.TrimSpace(t.ExportKeyColumn) != "" {
					keycol = t.ExportKeyColumn
				}
				curCol := cursorColumn
				curStart := cursorStart
				curEnd := cursorEnd
				if strings.TrimSpace(t.CursorColumn) != "" {
					curCol = t.CursorColumn
				}
				if strings.TrimSpace(t.CursorStart) != "" {
					curStart = t.CursorStart
				}
				if strings.TrimSpace(t.CursorEnd) != "" {
					curEnd = t.CursorEnd
				}
				vtCol := strings.TrimSpace(versionTimeColumn)
				if strings.TrimSpace(t.VersionTimeColumn) != "" {
					vtCol = strings.TrimSpace(t.VersionTimeColumn)
				}
				if strings.TrimSpace(curCol) == "" && strings.TrimSpace(vtCol) != "" {
					curCol = vtCol
				}
				if strings.TrimSpace(ord) == "" && strings.TrimSpace(vtCol) != "" {
					ord = vtCol
				}
				if cursorStartFromTarget && strings.TrimSpace(curCol) != "" {
					var src string
					if queryableMV {
						src = qualified(tgtDB, "mv_from_kafka_"+t.Name)
					} else {
						tgtTbl := targetTable
						if strings.TrimSpace(tgtTbl) == "" {
							if strings.TrimSpace(t.TargetTable) != "" {
								tgtTbl = t.TargetTable
							} else {
								tgtTbl = t.Name
							}
						}
						src = qualified(tgtDB, tgtTbl)
					}
					q := fmt.Sprintf("SELECT max(%s) FROM %s", quoteIdent(curCol), src)
					var v any
					if err := db.QueryRow(q).Scan(&v); err == nil && v != nil {
						curStart = fmt.Sprint(v)
					}
				}
				if fullExport {
					curCol = ""
					curStart = ""
					curEnd = ""
				}
				printJSON(map[string]any{"event": "export_start", "database": srcDB, "table": t.Name, "topic": topic})
				if err := exportTableToKafka(db, srcDB, t.Name, brokers, topic, bsize, ord, keycol, curCol, curStart, curEnd, len(targetList), i+1, n); err != nil {
					results = append(results, map[string]any{"table": t.Name, "error": err.Error()})
					if continueOnError {
						continue
					}
					return err
				}
			}
			// 汇总输出，便于审计与回溯
			m := map[string]any{
				"table":              t.Name,
				"topic":              topic,
				"partitions":         p,
				"replication_factor": rep,
				"group":              group,
				"kafka_table":        fmt.Sprintf("%s.%s", kafkaDB, "kafka_"+t.Name+"_sink"),
				"type_diffs":         typeDiffs,
				"source":             fmt.Sprintf("%s.%s", srcDB, t.Name),
			}
			if sourceMVToKafka {
				m["materialized_view_to_kafka"] = fmt.Sprintf("%s.%s", kafkaDB, "mv_to_kafka_"+t.Name)
			} else {
				m["target_table"] = fmt.Sprintf("%s.%s", tgtDB, tgtTable)
				m["materialized_view"] = fmt.Sprintf("%s.%s", kafkaDB, "mv_from_kafka_"+t.Name)
			}
			results = append(results, m)
		}

		out := map[string]any{"command": "sync", "results": results}
		if len(dbset) == 1 {
			for k := range dbset {
				out["database"] = k
			}
		} else {
			var dblist []string
			for k := range dbset {
				dblist = append(dblist, k)
			}
			out["databases"] = dblist
		}
		printJSON(out)
		return nil
	},
}

func init() {
	rootCmd.AddCommand(syncCmd)
	// 仅创建资源：Topic/Kafka 表/物化视图，不执行历史导出
	syncCmd.Flags().Bool("prepare-only", false, "仅创建资源（Topic/Kafka表/MV），不导出数据")
	// 遇错继续：跳过失败的表，处理剩余表
	syncCmd.Flags().Bool("continue-on-error", false, "遇到错误继续执行下一表（跳过错误表）")
	// 指定表清单：逗号分隔，覆盖 tables.yaml 中的配置
	syncCmd.Flags().String("tables", "", "仅处理指定表（逗号分隔），覆盖 tables.yaml 中配置")
	// 全量导出：创建资源后，对匹配的所有表执行一次性全量导出到 Kafka
	syncCmd.Flags().Bool("full-export", false, "创建资源后对所有表执行全量导出到Kafka（覆盖 prepare-only，忽略表级/全局游标过滤）")
	syncCmd.Flags().Bool("recreate", false, "删除并重建 Kafka 表与物化视图（应用新的 brokers/offset/reset 设置）")
	syncCmd.Flags().Bool("recreate-topic", false, "按 tables.yaml 重新创建 Kafka 主题（先删除旧主题再创建）")
	syncCmd.Flags().Bool("source-mv-to-kafka", false, "在源库创建 mv_to_kafka_<table>（实时写入 Kafka），不创建目标落库 MV")
	syncCmd.Flags().String("kafka-database", "", "Kafka 引擎表与 MV 所在库（默认跟随 target-database）")
}

// splitCSV 将逗号分隔的字符串拆分并去除空格。
func splitCSV(s string) []string {
	s = strings.TrimSpace(s)
	if s == "" {
		return []string{}
	}
	parts := strings.Split(s, ",")
	var out []string
	for _, p := range parts {
		t := strings.TrimSpace(p)
		if t != "" {
			out = append(out, t)
		}
	}
	return out
}
