// cmd 包包含执行原生 SQL 的命令。
package cmd

import (
	"click-house-sync/internal/clickhouse"
	"database/sql"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/spf13/cobra"
)

var sqlCmd = &cobra.Command{
	Use:   "sql",
	Short: "执行原生 ClickHouse SQL",
	Long:  "执行原生 ClickHouse SQL，默认输出风格尽量贴近 clickhouse-client（不依赖 clickhouse-client 二进制）。",
	RunE: func(cmd *cobra.Command, args []string) error {
		sqlText, _ := cmd.Flags().GetString("sql")
		queryText, _ := cmd.Flags().GetString("query")
		file, _ := cmd.Flags().GetString("file")
		multiquery, _ := cmd.Flags().GetBool("multiquery")
		maxRows, _ := cmd.Flags().GetInt("max-rows")
		outFormat, _ := cmd.Flags().GetString("format")
		withNames, _ := cmd.Flags().GetBool("with-names")
		withTypes, _ := cmd.Flags().GetBool("with-types")
		showTime, _ := cmd.Flags().GetBool("time")

		sqlText = strings.TrimSpace(sqlText)
		queryText = strings.TrimSpace(queryText)
		file = strings.TrimSpace(file)
		if queryText != "" {
			if sqlText != "" {
				return fmt.Errorf("--sql 与 --query 不能同时指定")
			}
			sqlText = queryText
		}
		if sqlText == "" && file == "" {
			return fmt.Errorf("必须提供 --sql/--query 或 --file")
		}
		if sqlText != "" && file != "" {
			return fmt.Errorf("--sql/--query 与 --file 只能二选一")
		}
		if maxRows <= 0 {
			maxRows = 200
		}
		if strings.TrimSpace(outFormat) == "" {
			outFormat = "TabSeparated"
		}

		var stmts []string
		if sqlText != "" {
			if multiquery {
				stmts = splitSQLStatements([]byte(sqlText))
			} else {
				stmts = []string{sqlText}
			}
		} else {
			b, err := os.ReadFile(file)
			if err != nil {
				return err
			}
			stmts = splitSQLStatements(b)
			if len(stmts) == 0 {
				return fmt.Errorf("SQL 文件中没有可执行语句")
			}
		}

		db, err := clickhouse.Connect(chHost, chPort, chUser, chPassword, chDatabase, chSecure)
		if err != nil {
			// 当默认库不存在时，回退到 default，行为更接近 clickhouse-client。
			if isDatabaseNotExistError(err) && !strings.EqualFold(strings.TrimSpace(chDatabase), "default") {
				db, err = clickhouse.Connect(chHost, chPort, chUser, chPassword, "default", chSecure)
			}
			if err != nil {
				return err
			}
		}
		defer db.Close()

		executed := 0
		startAll := time.Now()
		for i, stmt := range stmts {
			stmt = strings.TrimSpace(stmt)
			if stmt == "" {
				continue
			}
			startStmt := time.Now()
			err := runSQLStatement(db, stmt, maxRows, outFormat, withNames, withTypes)
			if err != nil {
				if !multiquery {
					return err
				}
				fmt.Fprintf(os.Stderr, "SQL[%d] error: %v\n", i, err)
				continue
			}
			executed++
			if showTime {
				fmt.Fprintf(os.Stderr, "Elapsed: %.3f sec. SQL[%d]\n", time.Since(startStmt).Seconds(), i)
			}
		}
		if showTime {
			fmt.Fprintf(os.Stderr, "Total elapsed: %.3f sec. statements=%d executed=%d\n", time.Since(startAll).Seconds(), len(stmts), executed)
		}
		return nil
	},
}

func init() {
	rootCmd.AddCommand(sqlCmd)
	sqlCmd.Flags().String("sql", "", "要执行的原生 SQL 语句")
	sqlCmd.Flags().StringP("query", "q", "", "clickhouse-client 兼容别名，等价于 --sql")
	sqlCmd.Flags().String("file", "", "SQL 文件路径（支持多语句）")
	sqlCmd.Flags().Bool("multiquery", false, "允许执行多语句（类似 clickhouse-client --multiquery）")
	sqlCmd.Flags().String("format", "TabSeparated", "输出格式：TabSeparated|CSV|JSONEachRow")
	sqlCmd.Flags().Bool("with-names", false, "输出列名（TabSeparated/CSV）")
	sqlCmd.Flags().Bool("with-types", false, "输出列类型（TabSeparated/CSV）")
	sqlCmd.Flags().Bool("time", false, "在 stderr 输出耗时信息")
	sqlCmd.Flags().Int("max-rows", 200, "查询结果最大返回行数")
}

func runSQLStatement(db *sql.DB, stmt string, maxRows int, outFormat string, withNames bool, withTypes bool) error {
	mode := detectSQLMode(stmt)
	if mode == "exec" {
		return runExec(db, stmt)
	}
	if mode == "query" {
		return runQuery(db, stmt, maxRows, outFormat, withNames, withTypes)
	}
	rows, qerr := db.Query(stmt)
	if qerr == nil {
		rows.Close()
		return runQuery(db, stmt, maxRows, outFormat, withNames, withTypes)
	}

	if eerr := runExec(db, stmt); eerr != nil {
		return fmt.Errorf("query_error: %v ; exec_error: %v", qerr, eerr)
	}
	return nil
}

func runQuery(db *sql.DB, stmt string, maxRows int, outFormat string, withNames bool, withTypes bool) error {
	rows, err := db.Query(stmt)
	if err != nil {
		return err
	}
	defer rows.Close()
	cols, err := rows.Columns()
	if err != nil {
		return err
	}
	colTypes, _ := rows.ColumnTypes()
	if len(colTypes) == len(cols) {
		_ = printHeader(outFormat, cols, colTypes, withNames, withTypes)
	}
	rowCount := 0
	for rows.Next() {
		if rowCount >= maxRows {
			break
		}
		vals := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return err
		}
		if err := printDataRow(outFormat, cols, vals); err != nil {
			return err
		}
		rowCount++
	}
	if err := rows.Err(); err != nil {
		return err
	}
	return nil
}

func runExec(db *sql.DB, stmt string) error {
	res, err := db.Exec(stmt)
	if err != nil {
		return err
	}
	_, _ = res.RowsAffected()
	return nil
}

func detectSQLMode(stmt string) string {
	s := strings.TrimSpace(stmt)
	if s == "" {
		return "query"
	}
	head := strings.ToUpper(strings.Fields(s)[0])
	switch head {
	case "SELECT", "SHOW", "DESC", "DESCRIBE", "EXISTS", "WITH":
		return "query"
	case "INSERT", "UPDATE", "DELETE", "CREATE", "DROP", "ALTER", "TRUNCATE", "RENAME", "OPTIMIZE", "SYSTEM", "GRANT", "REVOKE", "USE":
		return "exec"
	default:
		return "auto"
	}
}

func isDatabaseNotExistError(err error) bool {
	if err == nil {
		return false
	}
	s := strings.ToLower(err.Error())
	return strings.Contains(s, "database") && strings.Contains(s, "does not exist")
}

func normalizeSQLValue(v any) any {
	switch t := v.(type) {
	case []byte:
		return string(t)
	case time.Time:
		return t.Format("2006-01-02 15:04:05")
	default:
		return t
	}
}

func printHeader(format string, cols []string, colTypes []*sql.ColumnType, withNames bool, withTypes bool) error {
	f := strings.ToLower(strings.TrimSpace(format))
	switch f {
	case "tabseparated":
		if withNames {
			fmt.Println(strings.Join(cols, "\t"))
		}
		if withTypes {
			var types []string
			for _, ct := range colTypes {
				types = append(types, ct.DatabaseTypeName())
			}
			fmt.Println(strings.Join(types, "\t"))
		}
		return nil
	case "csv":
		if withNames {
			fmt.Println(strings.Join(cols, ","))
		}
		if withTypes {
			var types []string
			for _, ct := range colTypes {
				types = append(types, ct.DatabaseTypeName())
			}
			fmt.Println(strings.Join(types, ","))
		}
		return nil
	case "jsoneachrow":
		return nil
	default:
		return fmt.Errorf("不支持的 --format: %s", format)
	}
}

func printDataRow(format string, cols []string, vals []any) error {
	f := strings.ToLower(strings.TrimSpace(format))
	switch f {
	case "tabseparated":
		items := make([]string, len(vals))
		for i, v := range vals {
			items[i] = escapeTSV(formatValue(v))
		}
		fmt.Println(strings.Join(items, "\t"))
		return nil
	case "csv":
		items := make([]string, len(vals))
		for i, v := range vals {
			items[i] = escapeCSV(formatValue(v))
		}
		fmt.Println(strings.Join(items, ","))
		return nil
	case "jsoneachrow":
		row := map[string]any{}
		for i, c := range cols {
			row[c] = normalizeSQLValue(vals[i])
		}
		printJSON(row)
		return nil
	default:
		return fmt.Errorf("不支持的 --format: %s", format)
	}
}

func formatValue(v any) string {
	if v == nil {
		return "\\N"
	}
	switch t := v.(type) {
	case []byte:
		return string(t)
	case time.Time:
		return t.Format("2006-01-02 15:04:05")
	default:
		return fmt.Sprint(t)
	}
}

func escapeTSV(s string) string {
	s = strings.ReplaceAll(s, "\\", "\\\\")
	s = strings.ReplaceAll(s, "\t", "\\t")
	s = strings.ReplaceAll(s, "\n", "\\n")
	return s
}

func escapeCSV(s string) string {
	needQuote := strings.ContainsAny(s, ",\"\n\r")
	if !needQuote {
		return s
	}
	return `"` + strings.ReplaceAll(s, `"`, `""`) + `"`
}
