// cmd 包包含导出建表语句的 gen-ddl 命令。
package cmd

import (
	"click-house-sync/internal/clickhouse"
	"fmt"
	"os"
	"sort"
	"strings"
	"unicode"

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
		tablesCSV, _ := cmd.Flags().GetString("tables")
		forceMergeTree, _ := cmd.Flags().GetBool("merge-tree")

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
		if strings.TrimSpace(tablesCSV) != "" {
			want := splitCSVLocal(tablesCSV)
			set := map[string]struct{}{}
			for _, w := range want {
				set[w] = struct{}{}
			}
			var filtered []string
			for _, n := range names {
				if _, ok := set[n]; ok {
					filtered = append(filtered, n)
				}
			}
			names = filtered
		}

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
			if forceMergeTree {
				ddl = ensureMergeTreeDDL(ddl)
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
	genDDLCmd.Flags().String("tables", "", "仅生成指定表 DDL（逗号分隔）")
	genDDLCmd.Flags().Bool("merge-tree", false, "将输出 DDL 的引擎统一改为 MergeTree")
}

func rewriteEngineToMergeTree(ddl string) string {
	s := ddl
	inSingle := false
	inDouble := false
	inBacktick := false
	var prev byte
	for i := 0; i < len(s); i++ {
		ch := s[i]
		if ch == '\'' && !inDouble && !inBacktick {
			if !(inSingle && prev == '\\') {
				inSingle = !inSingle
			}
			prev = ch
			continue
		}
		if ch == '"' && !inSingle && !inBacktick {
			if !(inDouble && prev == '\\') {
				inDouble = !inDouble
			}
			prev = ch
			continue
		}
		if ch == '`' && !inSingle && !inDouble {
			inBacktick = !inBacktick
			prev = ch
			continue
		}
		if inSingle || inDouble || inBacktick {
			prev = ch
			continue
		}
		if i+6 <= len(s) && strings.EqualFold(s[i:i+6], "ENGINE") {
			j := i + 6
			for j < len(s) && (s[j] == ' ' || s[j] == '\t' || s[j] == '\n' || s[j] == '\r') {
				j++
			}
			if j >= len(s) || s[j] != '=' {
				prev = ch
				continue
			}
			j++
			for j < len(s) && (s[j] == ' ' || s[j] == '\t' || s[j] == '\n' || s[j] == '\r') {
				j++
			}
			k := j
			for k < len(s) {
				r := rune(s[k])
				if !(unicode.IsLetter(r) || unicode.IsDigit(r) || r == '_') {
					break
				}
				k++
			}
			for k < len(s) && (s[k] == ' ' || s[k] == '\t' || s[k] == '\n' || s[k] == '\r') {
				k++
			}
			if k < len(s) && s[k] == '(' {
				depth := 0
				inS := false
				inD := false
				esc := false
				for k < len(s) {
					c := s[k]
					if inS {
						if c == '\\' && !esc {
							esc = true
						} else {
							if c == '\'' && !esc {
								inS = false
							}
							esc = false
						}
						k++
						continue
					}
					if inD {
						if c == '\\' && !esc {
							esc = true
						} else {
							if c == '"' && !esc {
								inD = false
							}
							esc = false
						}
						k++
						continue
					}
					if c == '\'' {
						inS = true
						k++
						continue
					}
					if c == '"' {
						inD = true
						k++
						continue
					}
					if c == '(' {
						depth++
						k++
						continue
					}
					if c == ')' {
						depth--
						k++
						if depth == 0 {
							break
						}
						continue
					}
					k++
				}
			}
			prefix := s[:i]
			suffix := s[k:]
			return prefix + "ENGINE = MergeTree()" + suffix
		}
		prev = ch
	}
	return s
}

func containsOrderBy(ddl string) bool {
	s := ddl
	inSingle := false
	inDouble := false
	inBacktick := false
	var prev byte
	for i := 0; i < len(s); i++ {
		ch := s[i]
		if ch == '\'' && !inDouble && !inBacktick {
			if !(inSingle && prev == '\\') {
				inSingle = !inSingle
			}
			prev = ch
			continue
		}
		if ch == '"' && !inSingle && !inBacktick {
			if !(inDouble && prev == '\\') {
				inDouble = !inDouble
			}
			prev = ch
			continue
		}
		if ch == '`' && !inSingle && !inDouble {
			inBacktick = !inBacktick
			prev = ch
			continue
		}
		if inSingle || inDouble || inBacktick {
			prev = ch
			continue
		}
		if i+5 <= len(s) && strings.EqualFold(s[i:i+5], "ORDER") {
			j := i + 5
			for j < len(s) && (s[j] == ' ' || s[j] == '\t' || s[j] == '\n' || s[j] == '\r') {
				j++
			}
			if j+2 <= len(s) && strings.EqualFold(s[j:j+2], "BY") {
				return true
			}
		}
		prev = ch
	}
	return false
}

func indexOfSettings(ddl string) int {
	s := ddl
	inSingle := false
	inDouble := false
	inBacktick := false
	var prev byte
	for i := 0; i < len(s); i++ {
		ch := s[i]
		if ch == '\'' && !inDouble && !inBacktick {
			if !(inSingle && prev == '\\') {
				inSingle = !inSingle
			}
			prev = ch
			continue
		}
		if ch == '"' && !inSingle && !inBacktick {
			if !(inDouble && prev == '\\') {
				inDouble = !inDouble
			}
			prev = ch
			continue
		}
		if ch == '`' && !inSingle && !inDouble {
			inBacktick = !inBacktick
			prev = ch
			continue
		}
		if inSingle || inDouble || inBacktick {
			prev = ch
			continue
		}
		if i+8 <= len(s) && strings.EqualFold(s[i:i+8], "SETTINGS") {
			return i
		}
		prev = ch
	}
	return -1
}

func ensureMergeTreeDDL(ddl string) string {
	s := rewriteEngineToMergeTree(ddl)
	if containsOrderBy(s) {
		return s
	}
	idx := indexOfSettings(s)
	if idx >= 0 {
		return s[:idx] + " ORDER BY tuple() " + s[idx:]
	}
	t := strings.TrimSpace(s)
	if len(t) > 0 && t[len(t)-1] == ';' {
		return strings.TrimSpace(t[:len(t)-1]) + " ORDER BY tuple();"
	}
	return s + " ORDER BY tuple()"
}
