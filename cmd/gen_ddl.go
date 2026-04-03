// cmd 包包含导出建表语句的 gen-ddl 命令。
package cmd

import (
	"click-house-sync/internal/clickhouse"
	"database/sql"
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
		withSyncCast, _ := cmd.Flags().GetBool("with-sync-cast")
		includeRollback, _ := cmd.Flags().GetBool("include-rollback")
		includeQualitySQL, _ := cmd.Flags().GetBool("include-quality-sql")
		castTargetDB, _ := cmd.Flags().GetString("cast-target-database")
		castTargetSuffix, _ := cmd.Flags().GetString("cast-target-suffix")

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
		dbOut := chDatabase
		if ddlDB != "" {
			dbOut = ddlDB
		}
		if withSyncCast && strings.TrimSpace(castTargetDB) != "" {
			dbOut = strings.TrimSpace(castTargetDB)
		}
		content = append(content, []byte(fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s;\n\n", dbOut))...)
		var details []map[string]any
		for _, n := range names {
			if withSyncCast {
				detail, upDDL, downDDL, qualitySQL, err := buildSyncCastDDL(db, chDatabase, dbOut, n, castTargetSuffix, includeQualitySQL)
				if err != nil {
					return err
				}
				details = append(details, detail)
				content = append(content, []byte(upDDL)...)
				content = append(content, []byte("\n\n")...)
				if includeRollback {
					content = append(content, []byte(downDDL)...)
					content = append(content, []byte("\n\n")...)
				}
				if includeQualitySQL && len(qualitySQL) > 0 {
					content = append(content, []byte(strings.Join(qualitySQL, "\n\n"))...)
					content = append(content, []byte("\n\n")...)
				}
				continue
			}
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
		if withSyncCast {
			out["mode"] = "sync_cast"
			out["details"] = details
			out["include_rollback"] = includeRollback
			out["include_quality_sql"] = includeQualitySQL
			out["target_database"] = dbOut
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
	genDDLCmd.Flags().Bool("with-sync-cast", false, "生成 String 中间层 + 严格 CAST 物化视图的重建 DDL")
	genDDLCmd.Flags().Bool("include-rollback", true, "在 with-sync-cast 模式下附加回滚 DDL")
	genDDLCmd.Flags().Bool("include-quality-sql", true, "在 with-sync-cast 模式下附加数据质量巡检 SQL")
	genDDLCmd.Flags().String("cast-target-database", "", "with-sync-cast 模式下目标库名（默认与 ddl-database 或 ch-database 一致）")
	genDDLCmd.Flags().String("cast-target-suffix", "", "with-sync-cast 模式下目标表后缀（默认空，即与源表同名）")
}

func buildSyncCastDDL(db *sql.DB, sourceDB string, targetDB string, table string, targetSuffix string, includeQualitySQL bool) (map[string]any, string, string, []string, error) {
	sourceCols, err := clickhouse.GetColumns(db, sourceDB, table)
	if err != nil {
		return nil, "", "", nil, err
	}
	targetTableName := table + targetSuffix
	targetCols, err := clickhouse.GetColumns(db, targetDB, targetTableName)
	if err != nil {
		targetCols = sourceCols
	}
	typeDiffs := clickhouse.AnalyzeTypeDiff(sourceCols, targetCols)
	kafkaTable := "kafka_" + table + "_sink"
	mvName := "mv_from_kafka_" + table
	topic := sourceDB + "_" + table
	group := groupName + "-" + table
	if strings.TrimSpace(groupName) == "" {
		group = "ch-sync-" + table
	}
	stringCols := buildColumnsDDL(sourceCols, true)
	targetColsDDL := buildColumnsDDL(targetCols, false)
	typedCols := buildColumnsDDL(sourceCols, false)
	selectExpr, err := clickhouse.BuildMvSelectWithCasts(sourceCols, targetCols)
	if err != nil {
		return nil, "", "", nil, err
	}
	up := []string{
		fmt.Sprintf("DROP VIEW IF EXISTS %s;", qualifiedDDL(targetDB, mvName)),
		fmt.Sprintf("DROP TABLE IF EXISTS %s;", qualifiedDDL(targetDB, kafkaTable)),
		fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (%s) ENGINE = Kafka SETTINGS kafka_broker_list = '%s', kafka_topic_list = '%s', kafka_group_name = '%s', kafka_format = 'JSONEachRow', kafka_num_consumers = %d, kafka_max_block_size = %d, kafka_auto_offset_reset = '%s';", qualifiedDDL(targetDB, kafkaTable), stringCols, kafkaBrokers, topic, group, 1, kafkaMaxBlockSize, kafkaAutoOffsetReset),
		fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (%s) ENGINE = MergeTree ORDER BY tuple();", qualifiedDDL(targetDB, targetTableName), targetColsDDL),
		fmt.Sprintf("CREATE MATERIALIZED VIEW IF NOT EXISTS %s TO %s AS SELECT %s FROM %s SETTINGS stream_like_engine_allow_direct_select=1, input_format_skip_unknown_fields=1, date_time_input_format='best_effort';", qualifiedDDL(targetDB, mvName), qualifiedDDL(targetDB, targetTableName), selectExpr, qualifiedDDL(targetDB, kafkaTable)),
	}
	down := []string{
		fmt.Sprintf("DROP VIEW IF EXISTS %s;", qualifiedDDL(targetDB, mvName)),
		fmt.Sprintf("DROP TABLE IF EXISTS %s;", qualifiedDDL(targetDB, kafkaTable)),
		fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (%s) ENGINE = Kafka SETTINGS kafka_broker_list = '%s', kafka_topic_list = '%s', kafka_group_name = '%s', kafka_format = 'JSONEachRow', kafka_num_consumers = %d, kafka_max_block_size = %d, kafka_auto_offset_reset = '%s';", qualifiedDDL(targetDB, kafkaTable), typedCols, kafkaBrokers, topic, group, 1, kafkaMaxBlockSize, kafkaAutoOffsetReset),
		fmt.Sprintf("CREATE MATERIALIZED VIEW IF NOT EXISTS %s TO %s AS SELECT * FROM %s SETTINGS stream_like_engine_allow_direct_select=1, input_format_skip_unknown_fields=1, date_time_input_format='best_effort';", qualifiedDDL(targetDB, mvName), qualifiedDDL(targetDB, targetTableName), qualifiedDDL(targetDB, kafkaTable)),
	}
	var qualitySQL []string
	if includeQualitySQL {
		qualitySQL = buildQualitySQL(sourceDB, table, targetDB, targetTableName, targetCols)
	}
	detail := map[string]any{
		"table":        table,
		"target_table": targetTableName,
		"type_diffs":   typeDiffs,
	}
	return detail, strings.Join(up, "\n"), strings.Join(down, "\n"), qualitySQL, nil
}

func buildColumnsDDL(cols []clickhouse.Column, asString bool) string {
	var out []string
	for _, c := range cols {
		t := c.Type
		if asString {
			t = "String"
		}
		out = append(out, fmt.Sprintf("%s %s", quoteDDL(c.Name), t))
	}
	return strings.Join(out, ",")
}

func quoteDDL(v string) string {
	return "`" + strings.ReplaceAll(v, "`", "``") + "`"
}

func qualifiedDDL(db string, table string) string {
	return quoteDDL(db) + "." + quoteDDL(table)
}

func buildQualitySQL(sourceDB string, sourceTable string, targetDB string, targetTable string, cols []clickhouse.Column) []string {
	var out []string
	out = append(out, fmt.Sprintf("SELECT '%s' AS check_name, (SELECT count() FROM %s) AS source_rows, (SELECT count() FROM %s) AS target_rows;", sourceTable, qualifiedDDL(sourceDB, sourceTable), qualifiedDDL(targetDB, targetTable)))
	for _, c := range cols {
		out = append(out, fmt.Sprintf("SELECT '%s' AS check_name, countIf(%s IS NULL) AS null_rows, round(countIf(%s IS NULL) / nullIf(count(), 0), 6) AS null_ratio FROM %s;", c.Name, quoteDDL(c.Name), quoteDDL(c.Name), qualifiedDDL(targetDB, targetTable)))
	}
	for _, c := range cols {
		n := strings.ToLower(c.Type)
		if strings.Contains(n, "int") || strings.Contains(n, "float") || strings.Contains(n, "decimal") || strings.HasPrefix(n, "date") || strings.HasPrefix(n, "datetime") {
			out = append(out, fmt.Sprintf("SELECT '%s' AS check_name, min(%s) AS min_value, max(%s) AS max_value FROM %s;", c.Name, quoteDDL(c.Name), quoteDDL(c.Name), qualifiedDDL(targetDB, targetTable)))
		}
	}
	return out
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
