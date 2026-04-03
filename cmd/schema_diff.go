package cmd

import (
	"click-house-sync/internal/clickhouse"
	"fmt"
	"strings"

	"github.com/spf13/cobra"
)

var schemaDiffCmd = &cobra.Command{
	Use:   "schema-diff",
	Short: "比较源表与目标表结构差异",
	RunE: func(cmd *cobra.Command, args []string) error {
		table, _ := cmd.Flags().GetString("table")
		if strings.TrimSpace(table) == "" {
			return fmt.Errorf("缺少 --table")
		}
		targetTableName, _ := cmd.Flags().GetString("target-table")
		if strings.TrimSpace(targetTableName) == "" {
			targetTableName = table
		}
		sourceDBName, _ := cmd.Flags().GetString("source-database")
		if strings.TrimSpace(sourceDBName) == "" {
			sourceDBName = chDatabase
		}
		targetDBName, _ := cmd.Flags().GetString("target-db")
		if strings.TrimSpace(targetDBName) == "" {
			if strings.TrimSpace(targetDatabase) != "" {
				targetDBName = targetDatabase
			} else {
				targetDBName = chDatabase
			}
		}
		targetHost, _ := cmd.Flags().GetString("target-host")
		targetPort, _ := cmd.Flags().GetInt("target-port")
		targetUser, _ := cmd.Flags().GetString("target-user")
		targetPassword, _ := cmd.Flags().GetString("target-password")
		targetSecure, _ := cmd.Flags().GetBool("target-secure")

		if strings.TrimSpace(targetHost) == "" {
			targetHost = chHost
		}
		if targetPort <= 0 {
			targetPort = chPort
		}
		if strings.TrimSpace(targetUser) == "" {
			targetUser = chUser
		}

		srcConn, err := clickhouse.Connect(chHost, chPort, chUser, chPassword, sourceDBName, chSecure)
		if err != nil {
			return err
		}
		defer srcConn.Close()

		tgtConn, err := clickhouse.Connect(targetHost, targetPort, targetUser, targetPassword, targetDBName, targetSecure)
		if err != nil {
			return err
		}
		defer tgtConn.Close()

		sourceCols, err := clickhouse.GetColumns(srcConn, sourceDBName, table)
		if err != nil {
			return err
		}
		targetCols, err := clickhouse.GetColumns(tgtConn, targetDBName, targetTableName)
		if err != nil {
			return err
		}

		typeDiffs := clickhouse.AnalyzeTypeDiff(sourceCols, targetCols)
		srcPos := map[string]uint64{}
		tgtPos := map[string]uint64{}
		for _, c := range sourceCols {
			srcPos[c.Name] = c.Position
		}
		for _, c := range targetCols {
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

		printJSON(map[string]any{
			"command":       "schema-diff",
			"source":        fmt.Sprintf("%s:%d/%s.%s", chHost, chPort, sourceDBName, table),
			"target":        fmt.Sprintf("%s:%d/%s.%s", targetHost, targetPort, targetDBName, targetTableName),
			"type_diffs":    typeDiffs,
			"order_diffs":   orderDiffs,
			"source_cols":   sourceCols,
			"target_cols":   targetCols,
			"diffs_count":   len(typeDiffs) + len(orderDiffs),
			"matched_exact": len(typeDiffs) == 0 && len(orderDiffs) == 0,
		})
		return nil
	},
}

func init() {
	rootCmd.AddCommand(schemaDiffCmd)
	schemaDiffCmd.Flags().String("table", "", "源表名（必填）")
	schemaDiffCmd.Flags().String("target-table", "", "目标表名（默认与源表同名）")
	schemaDiffCmd.Flags().String("source-database", "", "源库名（默认 --ch-database）")
	schemaDiffCmd.Flags().String("target-db", "", "目标库名（默认 --target-database）")
	schemaDiffCmd.Flags().String("target-host", "", "目标 ClickHouse host（默认 --ch-host）")
	schemaDiffCmd.Flags().Int("target-port", 0, "目标 ClickHouse 端口（默认 --ch-port）")
	schemaDiffCmd.Flags().String("target-user", "", "目标 ClickHouse 用户（默认 --ch-user）")
	schemaDiffCmd.Flags().String("target-password", "", "目标 ClickHouse 密码（默认空）")
	schemaDiffCmd.Flags().Bool("target-secure", false, "目标 ClickHouse 是否启用 TLS")
}
