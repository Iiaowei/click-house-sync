package cmd

import (
	"click-house-sync/internal/clickhouse"
	"click-house-sync/internal/config"
	"fmt"
	"strings"

	"github.com/spf13/cobra"
)

var schemaDiffBatchCmd = &cobra.Command{
	Use:   "schema-diff-batch",
	Short: "批量比较源/目标表结构差异",
	RunE: func(cmd *cobra.Command, args []string) error {
		result := map[string]any{
			"command":       "schema-diff-batch",
			"tables_file":   "",
			"total_count":   0,
			"correct_count": 0,
			"error_count":   0,
			"error_tables":  []map[string]any{},
			"forced_output": true,
		}
		fail := func(msg string) error {
			result["fatal_error"] = msg
			printJSON(result)
			return nil
		}
		tablesPath, _ := cmd.Flags().GetString("tables-file")
		if strings.TrimSpace(tablesPath) == "" {
			tablesPath = tablesFile
		}
		if strings.TrimSpace(tablesPath) == "" {
			tablesPath = "tables.yaml"
		}
		result["tables_file"] = tablesPath
		targetHost, _ := cmd.Flags().GetString("target-host")
		targetPort, _ := cmd.Flags().GetInt("target-port")
		targetUser, _ := cmd.Flags().GetString("target-user")
		targetPassword, _ := cmd.Flags().GetString("target-password")
		targetSecure, _ := cmd.Flags().GetBool("target-secure")
		targetDBFlag, _ := cmd.Flags().GetString("target-db")

		if strings.TrimSpace(targetHost) == "" {
			targetHost = chHost
		}
		if targetPort <= 0 {
			targetPort = chPort
		}
		if strings.TrimSpace(targetUser) == "" {
			targetUser = chUser
		}

		srcConn, err := clickhouse.Connect(chHost, chPort, chUser, chPassword, chDatabase, chSecure)
		if err != nil {
			return fail(err.Error())
		}
		defer srcConn.Close()

		tgtConn, err := clickhouse.Connect(targetHost, targetPort, targetUser, targetPassword, chDatabase, targetSecure)
		if err != nil {
			return fail(err.Error())
		}
		defer tgtConn.Close()

		tlist, err := config.LoadTablesFile(tablesPath)
		if err != nil {
			return fail(err.Error())
		}
		if len(tlist) == 0 {
			return fail("tables_file 无表项")
		}

		total := 0
		okCount := 0
		errCount := 0
		errorTables := make([]map[string]any, 0)

		for _, t := range tlist {
			srcDB := chDatabase
			if strings.TrimSpace(t.CurrentDatabase) != "" {
				srcDB = t.CurrentDatabase
			}
			tgtDB := targetDBFlag
			if strings.TrimSpace(tgtDB) == "" {
				if strings.TrimSpace(t.TargetDatabase) != "" {
					tgtDB = t.TargetDatabase
				} else if strings.TrimSpace(targetDatabase) != "" {
					tgtDB = targetDatabase
				} else {
					tgtDB = chDatabase
				}
			}
			tgtTable := t.Name
			if strings.TrimSpace(t.TargetTable) != "" {
				tgtTable = t.TargetTable
			}
			total++
			srcCols, e1 := clickhouse.GetColumns(srcConn, srcDB, t.Name)
			if e1 != nil {
				errCount++
				errorTables = append(errorTables, map[string]any{
					"table":  t.Name,
					"source": fmt.Sprintf("%s.%s", srcDB, t.Name),
					"target": fmt.Sprintf("%s.%s", tgtDB, tgtTable),
					"error":  e1.Error(),
				})
				continue
			}
			tgtCols, e2 := clickhouse.GetColumns(tgtConn, tgtDB, tgtTable)
			if e2 != nil {
				errCount++
				errorTables = append(errorTables, map[string]any{
					"table":  t.Name,
					"source": fmt.Sprintf("%s.%s", srcDB, t.Name),
					"target": fmt.Sprintf("%s.%s", tgtDB, tgtTable),
					"error":  e2.Error(),
				})
				continue
			}

			typeDiffs := clickhouse.AnalyzeTypeDiff(srcCols, tgtCols)
			srcPos := map[string]uint64{}
			tgtPos := map[string]uint64{}
			for _, c := range srcCols {
				srcPos[c.Name] = c.Position
			}
			for _, c := range tgtCols {
				tgtPos[c.Name] = c.Position
			}
			orderDiffs := make([]map[string]any, 0)
			for name, sPos := range srcPos {
				if tPos, ok := tgtPos[name]; ok && sPos != tPos {
					orderDiffs = append(orderDiffs, map[string]any{
						"column":          name,
						"source_position": sPos,
						"target_position": tPos,
					})
				}
			}

			if len(typeDiffs) == 0 && len(orderDiffs) == 0 {
				okCount++
				continue
			}
			errCount++
			errorTables = append(errorTables, map[string]any{
				"table":       t.Name,
				"source":      fmt.Sprintf("%s.%s", srcDB, t.Name),
				"target":      fmt.Sprintf("%s.%s", tgtDB, tgtTable),
				"type_diffs":  typeDiffs,
				"order_diffs": orderDiffs,
			})
		}

		result["total_count"] = total
		result["correct_count"] = okCount
		result["error_count"] = errCount
		result["error_tables"] = errorTables
		result["matched_exact"] = errCount == 0 && total > 0
		printJSON(result)
		return nil
	},
}

func init() {
	rootCmd.AddCommand(schemaDiffBatchCmd)
	schemaDiffBatchCmd.Flags().String("tables-file", "", "tables.yaml 路径（默认 --tables-file）")
	schemaDiffBatchCmd.Flags().String("target-db", "", "目标库名（优先级高于 tables.yaml 的 target_database）")
	schemaDiffBatchCmd.Flags().String("target-host", "", "目标 ClickHouse host（默认 --ch-host）")
	schemaDiffBatchCmd.Flags().Int("target-port", 0, "目标 ClickHouse 端口（默认 --ch-port）")
	schemaDiffBatchCmd.Flags().String("target-user", "", "目标 ClickHouse 用户（默认 --ch-user）")
	schemaDiffBatchCmd.Flags().String("target-password", "", "目标 ClickHouse 密码（默认空）")
	schemaDiffBatchCmd.Flags().Bool("target-secure", false, "目标 ClickHouse 是否启用 TLS")
}
