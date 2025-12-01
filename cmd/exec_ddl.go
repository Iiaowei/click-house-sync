// cmd 包包含执行 SQL 文件的 exec-ddl 命令。
package cmd

import (
	"click-house-sync/internal/clickhouse"
	"os"
	"strings"

	"github.com/spf13/cobra"
)

// execDDLCmd 读取 SQL 文件并执行以分号分隔的语句。
var execDDLCmd = &cobra.Command{
	Use:   "exec-ddl",
	Short: "执行SQL文件",
	Long:  "读取并执行指定文件中的 SQL 语句（以分号分隔）。",
	RunE: func(cmd *cobra.Command, args []string) error {
		file, _ := cmd.Flags().GetString("file")
		cont, _ := cmd.Flags().GetBool("continue-on-error")
		if strings.TrimSpace(file) == "" {
			file = "create_tables.sql"
		}
		db, err := clickhouse.Connect(chHost, chPort, chUser, chPassword, chDatabase, chSecure)
		if err != nil {
			return err
		}
		defer db.Close()
		b, err := os.ReadFile(file)
		if err != nil {
			return err
		}
		stmts := splitSQLStatements(b)
		var results []map[string]any
		executed := 0
		for i, s := range stmts {
			if strings.TrimSpace(s) == "" {
				continue
			}
			if _, err := db.Exec(s); err != nil {
				results = append(results, map[string]any{"index": i, "error": err.Error()})
				if !cont {
					return err
				}
				continue
			}
			results = append(results, map[string]any{"index": i, "ok": true})
			executed++
		}
		printJSON(map[string]any{"command": "exec-ddl", "file": file, "statements": len(stmts), "executed": executed, "results": results})
		return nil
	},
}

func init() {
	rootCmd.AddCommand(execDDLCmd)
	execDDLCmd.Flags().String("file", "create_tables.sql", "SQL 文件路径（默认 create_tables.sql）")
	execDDLCmd.Flags().Bool("continue-on-error", false, "遇到错误继续执行下一条语句")
}

// splitSQLStatements 将 SQL 文本拆分为语句，跳过注释并正确处理引号。
func splitSQLStatements(b []byte) []string {
	s := string(b)
	var out []string
	var sb strings.Builder
	inSingle := false
	inDouble := false
	inBacktick := false
	inLineComment := false
	inBlockComment := false
	var prev byte
	for i := 0; i < len(s); i++ {
		ch := s[i]
		if inLineComment {
			if ch == '\n' {
				inLineComment = false
			}
			prev = ch
			continue
		}
		if inBlockComment {
			if ch == '*' && i+1 < len(s) && s[i+1] == '/' {
				inBlockComment = false
				i++
				prev = '/'
				continue
			}
			prev = ch
			continue
		}
		if !inSingle && !inDouble && !inBacktick {
			if ch == '-' && i+1 < len(s) && s[i+1] == '-' {
				inLineComment = true
				i++
				prev = '-'
				continue
			}
			if ch == '/' && i+1 < len(s) && s[i+1] == '*' {
				inBlockComment = true
				i++
				prev = '*'
				continue
			}
		}
		if ch == '\'' && !inDouble && !inBacktick {
			if !(inSingle && prev == '\\') {
				inSingle = !inSingle
			}
			sb.WriteByte(ch)
			prev = ch
			continue
		}
		if ch == '"' && !inSingle && !inBacktick {
			if !(inDouble && prev == '\\') {
				inDouble = !inDouble
			}
			sb.WriteByte(ch)
			prev = ch
			continue
		}
		if ch == '`' && !inSingle && !inDouble {
			inBacktick = !inBacktick
			sb.WriteByte(ch)
			prev = ch
			continue
		}
		if ch == ';' && !inSingle && !inDouble && !inBacktick {
			t := strings.TrimSpace(sb.String())
			if t != "" {
				out = append(out, t)
			}
			sb.Reset()
			prev = ch
			continue
		}
		sb.WriteByte(ch)
		prev = ch
	}
	t := strings.TrimSpace(sb.String())
	if t != "" {
		out = append(out, t)
	}
	return out
}
