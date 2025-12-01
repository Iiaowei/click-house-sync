package cmd

import (
	"click-house-sync/internal/clickhouse"
	"fmt"

	"github.com/spf13/cobra"
)

// createTargetCmd 负责在目标库创建与源表列结构一致的 MergeTree 表。
// 使用场景：当需要在 B 库预先创建用于写入/查询的目标表时，可以通过该命令指定排序与分区策略。
var createTargetCmd = &cobra.Command{
	Use:   "create-target",
	Short: "创建目标MergeTree表",
	Long:  "在目标数据库创建与源表列结构一致的 MergeTree 表。可指定 ORDER BY 与 PARTITION BY 表达式，用于优化写入与查询性能。",
	RunE: func(cmd *cobra.Command, args []string) error {
		// 读取必需参数：源表名
		table, _ := cmd.Flags().GetString("table")
		if table == "" {
			return fmt.Errorf("缺少 --table")
		}
		// 读取可选参数：排序与分区表达式
		orderBy, _ := cmd.Flags().GetString("order-by")
		partitionBy, _ := cmd.Flags().GetString("partition-by")
		// 建立到 ClickHouse 的连接（基于全局连接参数）
		db, err := clickhouse.Connect(chHost, chPort, chUser, chPassword, chDatabase, chSecure)
		if err != nil {
			return err
		}
		defer db.Close()
		// 从 tables.yaml 查找该表的附加配置（current/target database、target table）
		tconf, err := lookupTableConfig(table)
		if err != nil {
			return err
		}
		// 决定源库：优先使用 tables.yaml 的 current_database，否则使用全局 ch-database
		srcDB := chDatabase
		if !cmd.Root().PersistentFlags().Changed("ch-database") && tconf != nil && tconf.CurrentDatabase != "" {
			srcDB = tconf.CurrentDatabase
		}
		// 决定目标库：优先使用 --target-database，其次 tables.yaml 的 target_database，否则回退到全局 ch-database
		tgtDB := chDatabase
		if cmd.Root().PersistentFlags().Changed("target-database") && targetDatabase != "" {
			tgtDB = targetDatabase
		} else if tconf != nil && tconf.TargetDatabase != "" {
			tgtDB = tconf.TargetDatabase
		}
		// 决定目标表名：优先使用 --target-table 或 tables.yaml 的 target_table，否则默认源表名后缀 _replica
		tgtTable := targetTable
		if tgtTable == "" {
			if tconf != nil && tconf.TargetTable != "" {
				tgtTable = tconf.TargetTable
			} else {
				tgtTable = table + "_replica"
			}
		}
		// 基于源表结构创建目标 MergeTree 表，并应用指定的 ORDER/PARTITION 表达式
		if err := clickhouse.CreateTargetTableLikeSource(db, srcDB, table, tgtDB, tgtTable, orderBy, partitionBy); err != nil {
			return err
		}
		// 输出执行结果，便于在日志中追踪
		printJSON(map[string]any{
			"command":      "create-target",
			"source":       fmt.Sprintf("%s.%s", srcDB, table),
			"target":       fmt.Sprintf("%s.%s", tgtDB, tgtTable),
			"engine":       "MergeTree",
			"order_by":     orderBy,
			"partition_by": partitionBy,
		})
		return nil
	},
}

func init() {
	rootCmd.AddCommand(createTargetCmd)
	// 源表名：从该表读取列结构，作为目标表的列定义
	createTargetCmd.Flags().String("table", "", "源表名（必填）")
	// ORDER BY：用于目标表的主排序键，默认使用 tuple()（无排序键）
	createTargetCmd.Flags().String("order-by", "", "ORDER BY 表达式（默认 tuple()）")
	// PARTITION BY：用于目标表的分区键（可选）
	createTargetCmd.Flags().String("partition-by", "", "PARTITION BY 表达式（可选）")
}
