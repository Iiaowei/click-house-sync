// cmd 包包含导出建表语句的 gen-ddl 命令。
package cmd

import (
	"click-house-sync/internal/clickhouse"
	"fmt"
	"os"
	"sort"
	"strings"

	"github.com/spf13/cobra"
)

// genDDLCmd 对库中每个表输出 SHOW CREATE TABLE，可覆盖库名。
var genDDLCmd = &cobra.Command{
	Use:   "gen-ddl",
	Short: "生成库中的建表语句",
	Long:  "遍历指定数据库，输出每个表的建表 DDL（SHOW CREATE TABLE）。支持过滤视图与 Kafka 表，按前/后缀过滤，并可用 --ddl-database 覆盖输出中的库名。",
	RunE: func(cmd *cobra.Command, args []string) error {
		output, _ := cmd.Flags().GetString("output")
		includeViews, _ := cmd.Flags().GetBool("include-views")
		prefix, _ := cmd.Flags().GetString("prefix")
		suffix, _ := cmd.Flags().GetString("suffix")
		ddlDB, _ := cmd.Flags().GetString("ddl-database")

		if output == "" {
			output = "create_tables.sql"
		}

		db, err := clickhouse.Connect(chHost, chPort, chUser, chPassword, chDatabase, chSecure)
		if err != nil {
			return err
		}
		defer db.Close()

		query := "SELECT name, engine FROM system.tables WHERE database = ?"
		rows, err := db.Query(query, chDatabase)
		if err != nil {
			return err
		}
		defer rows.Close()
		var names []string
		var engines []string
		for rows.Next() {
			var name, engine string
			if err := rows.Scan(&name, &engine); err != nil {
				return err
			}
			if !includeViews && (engine == "MaterializedView" || engine == "View" || engine == "Kafka") {
				continue
			}
			if len(prefix) > 0 && !hasPrefix(name, prefix) {
				continue
			}
			if len(suffix) > 0 && !hasSuffix(name, suffix) {
				continue
			}
			names = append(names, name)
			engines = append(engines, engine)
		}
		sort.Strings(names)

		var content []byte
		// 输出创建数据库语句（可覆盖库名）
		dbOut := chDatabase
		if ddlDB != "" {
			dbOut = ddlDB
		}
		content = append(content, []byte(fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s;\n\n", dbOut))...)
		for _, n := range names {
			var ddl string
			if err := db.QueryRow(fmt.Sprintf("SHOW CREATE TABLE %s.%s", chDatabase, n)).Scan(&ddl); err != nil {
				return err
			}
			if ddlDB != "" && ddlDB != chDatabase {
				orig := fmt.Sprintf("%s.%s", chDatabase, n)
				target := fmt.Sprintf("%s.%s", ddlDB, n)
				ddl = strings.ReplaceAll(ddl, orig, target)
			}
			content = append(content, []byte(ddl)...)
			if len(ddl) == 0 || ddl[len(ddl)-1] != ';' {
				content = append(content, ';')
			}
			content = append(content, []byte("\n\n")...)
		}
		tmp := output + ".tmp"
		if err := os.WriteFile(tmp, content, 0644); err != nil {
			return err
		}
		if err := os.Rename(tmp, output); err != nil {
			return err
		}
		out := map[string]any{"command": "gen-ddl", "database": chDatabase, "output": output, "tables": len(names)}
		if ddlDB != "" {
			out["ddl_database"] = ddlDB
		}
		printJSON(out)
		return nil
	},
}

func init() {
	rootCmd.AddCommand(genDDLCmd)
	genDDLCmd.Flags().String("output", "create_tables.sql", "输出 SQL 文件路径（默认 create_tables.sql）")
	genDDLCmd.Flags().Bool("include-views", false, "包含视图与 Kafka 引擎表")
	genDDLCmd.Flags().String("prefix", "", "仅包含指定前缀的表名")
	genDDLCmd.Flags().String("suffix", "", "仅包含指定后缀的表名")
	genDDLCmd.Flags().String("ddl-database", "", "覆盖输出 DDL 中的库名（例如将 demo 改写为 mv）")
}
