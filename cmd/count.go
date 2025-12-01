package cmd

import (
	"click-house-sync/internal/clickhouse"
	"fmt"

	"github.com/spf13/cobra"
)

var countCmd = &cobra.Command{
	Use:   "count",
	Short: "统计行数（含物化视图）",
	RunE: func(cmd *cobra.Command, args []string) error {
		table, _ := cmd.Flags().GetString("table")
		withMV, _ := cmd.Flags().GetBool("with-mv")
		diff, _ := cmd.Flags().GetBool("diff")
		conn, err := clickhouse.Connect(chHost, chPort, chUser, chPassword, chDatabase, chSecure)
		if err != nil {
			return err
		}
		defer conn.Close()
		if table != "" {
			srcDB := chDatabase
			if !cmd.Root().PersistentFlags().Changed("ch-database") {
				tconf, _ := lookupTableConfig(table)
				if tconf != nil && tconf.CurrentDatabase != "" {
					srcDB = tconf.CurrentDatabase
				}
			}
			n, err := clickhouse.CountTableRows(conn, srcDB, table)
			if err != nil {
				return err
			}
			out := map[string]any{"command": "count", "database": srcDB, "table": table, "rows": n}
			tgtDB := chDatabase
			if cmd.Root().PersistentFlags().Changed("target-database") && targetDatabase != "" {
				tgtDB = targetDatabase
			} else {
				tconf, _ := lookupTableConfig(table)
				if tconf != nil && tconf.TargetDatabase != "" {
					tgtDB = tconf.TargetDatabase
				}
			}
			mvName := "mv_from_kafka_" + table
			out["materialized_view"] = tgtDB + "." + mvName
			var mvRows uint64
			var mvErr error
			if eng, e1 := clickhouse.GetTableEngine(conn, tgtDB, mvName); e1 == nil && eng == "MaterializedView" {
				q := fmt.Sprintf("SELECT count() FROM `%s`.`%s`", tgtDB, mvName)
				if err := conn.QueryRow(q).Scan(&mvRows); err != nil {
					mvErr = err
				}
			} else {
				if mvr, err := clickhouse.CountTableRows(conn, tgtDB, mvName); err == nil {
					mvRows = mvr
				} else {
					mvErr = err
				}
			}
			if mvErr != nil {
				out["mv_error"] = mvErr.Error()
			} else {
				out["mv_rows"] = mvRows
			}
			if diff && mvErr == nil && n != mvRows {
				printJSONWithRedKeys(out, []string{"rows", "mv_rows"})
			} else {
				printJSON(out)
			}
			return nil
		}
		rows, err := clickhouse.CountAllTablesRows(conn, chDatabase)
		if err != nil {
			return err
		}
		if !withMV {
			var total uint64
			var items []map[string]any
			for _, r := range rows {
				items = append(items, map[string]any{"table": r.Table, "rows": r.Rows})
				total += r.Rows
			}
			printJSON(map[string]any{"command": "count_all", "database": chDatabase, "tables": items, "total": total})
			return nil
		}
		tgtDB := chDatabase
		if cmd.Root().PersistentFlags().Changed("target-database") && targetDatabase != "" {
			tgtDB = targetDatabase
		}
		var total uint64
		var items []map[string]any
		for _, r := range rows {
			total += r.Rows
			mvName := "mv_from_kafka_" + r.Table
			var mvRows uint64
			var mvErr error
			if eng, e1 := clickhouse.GetTableEngine(conn, tgtDB, mvName); e1 == nil && eng == "MaterializedView" {
				q := fmt.Sprintf("SELECT count() FROM `%s`.`%s`", tgtDB, mvName)
				if err := conn.QueryRow(q).Scan(&mvRows); err != nil {
					mvErr = err
				}
			} else {
				if mvr, err := clickhouse.CountTableRows(conn, tgtDB, mvName); err == nil {
					mvRows = mvr
				} else {
					mvErr = err
				}
			}
			item := map[string]any{"table": r.Table, "rows": r.Rows, "materialized_view": tgtDB + "." + mvName}
			if mvErr != nil {
				item["mv_error"] = mvErr.Error()
			} else {
				item["mv_rows"] = mvRows
			}
			if diff && mvErr == nil && r.Rows != mvRows {
				printJSONWithRedKeys(item, []string{"rows", "mv_rows"})
			} else if diff {
				printJSON(item)
			} else {
				items = append(items, item)
			}
		}
		if !diff {
			printJSON(map[string]any{"command": "count_all_with_mv", "database": chDatabase, "target_database": tgtDB, "tables": items, "total": total})
		}
		return nil
	},
}

func init() {
	rootCmd.AddCommand(countCmd)
	countCmd.Flags().String("table", "", "表名")
	countCmd.Flags().Bool("with-mv", false, "同时查询物化视图行数")
	countCmd.Flags().Bool("diff", false, "比较源表与物化视图行数，数量不一致标红")
}
