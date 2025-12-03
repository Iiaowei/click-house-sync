// cmd 包包含生成 tables.yaml 的 gen-tables 命令。
package cmd

import (
	"click-house-sync/internal/clickhouse"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"gopkg.in/yaml.v3"
)

// genTablesFile 是包含 tables 数组的 YAML 封装。
type genTablesFile struct {
	Tables []genTableItem `yaml:"tables"`
}

// genTableItem 描述写入 tables.yaml 的单表配置。
type genTableItem struct {
	Name             string   `yaml:"name"`
	Brokers          []string `yaml:"brokers"`
	RowsPerPartition int      `yaml:"rows_per_partition"`
	BatchSize        int      `yaml:"batch_size"`
	GroupName        string   `yaml:"group_name"`
	TargetTable      string   `yaml:"target_table"`
	TargetDatabase   string   `yaml:"target_database"`
	CurrentDatabase  string   `yaml:"current_database"`
	ExportOrderBy    string   `yaml:"export_order_by"`
	ExportKeyColumn  string   `yaml:"export_key_column"`
	CursorColumn     string   `yaml:"cursor_column"`
	CursorStart      string   `yaml:"cursor_start"`
	CursorEnd        string   `yaml:"cursor_end"`
}

// genTablesCmd 扫描数据库的表并按默认值生成 tables.yaml 框架。
var genTablesCmd = &cobra.Command{
	Use:   "gen-tables",
	Short: "查询库内表并生成 tables.yaml",
	Long:  "扫描指定数据库中的表，生成包含每表的 Kafka/同步配置的 tables.yaml（可选包含视图与Kafka表，支持前后缀过滤、默认 order/key/cursor 字段）。",
	RunE: func(cmd *cobra.Command, args []string) error {
		output, _ := cmd.Flags().GetString("output")
		includeViews, _ := cmd.Flags().GetBool("include-views")
		prefix, _ := cmd.Flags().GetString("prefix")
		suffix, _ := cmd.Flags().GetString("suffix")
		_, _ = cmd.Flags().GetString("topic-prefix")
		_, _ = cmd.Flags().GetString("topic-suffix")
		exportOrderBy, _ := cmd.Flags().GetString("export-order-by")
		exportKeyColumn, _ := cmd.Flags().GetString("export-key-column")
		cursorColumn, _ := cmd.Flags().GetString("cursor-column")
		cursorStart, _ := cmd.Flags().GetString("cursor-start")
		cursorEnd, _ := cmd.Flags().GetString("cursor-end")
		includeColumns, _ := cmd.Flags().GetString("include-columns")
		excludeColumns, _ := cmd.Flags().GetString("exclude-columns")
		tablesCSV, _ := cmd.Flags().GetString("tables")

		if output == "" {
			output = "tables.yaml"
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
			if hasPrefix(name, "kafka_") || hasPrefix(name, "mv_") {
				continue
			}
			names = append(names, name)
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
		if strings.TrimSpace(includeColumns) != "" {
			required := splitCSVLocal(includeColumns)
			var filtered []string
			for _, n := range names {
				cols, err := clickhouse.GetColumns(db, chDatabase, n)
				if err != nil {
					continue
				}
				have := map[string]struct{}{}
				for _, c := range cols {
					have[c.Name] = struct{}{}
				}
				ok := true
				for _, r := range required {
					if _, exists := have[r]; !exists {
						ok = false
						break
					}
				}
				if ok {
					filtered = append(filtered, n)
				}
			}
			names = filtered
		}
		if strings.TrimSpace(excludeColumns) != "" {
			excluded := splitCSVLocal(excludeColumns)
			var filtered []string
			for _, n := range names {
				cols, err := clickhouse.GetColumns(db, chDatabase, n)
				if err != nil {
					filtered = append(filtered, n)
					continue
				}
				have := map[string]struct{}{}
				for _, c := range cols {
					have[c.Name] = struct{}{}
				}
				banned := false
				for _, r := range excluded {
					if _, exists := have[r]; exists {
						banned = true
						break
					}
				}
				if !banned {
					filtered = append(filtered, n)
				}
			}
			names = filtered
		}
		var out genTablesFile
		for _, n := range names {
			item := genTableItem{
				Name:             n,
				Brokers:          brokersList(),
				RowsPerPartition: rowsPerPartition,
				BatchSize:        batchSize,
				GroupName:        groupName + "-" + n,
				TargetTable:      n + "_replica",
				TargetDatabase:   targetDatabase,
				CurrentDatabase:  chDatabase,
				ExportOrderBy:    exportOrderBy,
				ExportKeyColumn:  exportKeyColumn,
				CursorColumn:     cursorColumn,
				CursorStart:      cursorStart,
				CursorEnd:        cursorEnd,
			}
			if item.TargetDatabase == "" {
				item.TargetDatabase = chDatabase
			}
			out.Tables = append(out.Tables, item)
		}
		b, err := yaml.Marshal(out)
		if err != nil {
			return err
		}
		tmp := output + ".tmp"
		if err := os.WriteFile(tmp, b, 0644); err != nil {
			return err
		}
		if err := os.Rename(tmp, output); err != nil {
			return err
		}
		printJSON(map[string]any{"command": "gen-tables", "output": output, "tables": len(out.Tables)})
		time.Sleep(10 * time.Millisecond)
		return nil
	},
}

func init() {
	rootCmd.AddCommand(genTablesCmd)
	genTablesCmd.Flags().String("output", "tables.yaml", "输出文件路径（默认 tables.yaml）")
	genTablesCmd.Flags().Bool("include-views", false, "包含视图与 Kafka 引擎表")
	genTablesCmd.Flags().String("prefix", "", "仅包含指定前缀的表名")
	genTablesCmd.Flags().String("suffix", "", "仅包含指定后缀的表名")
	genTablesCmd.Flags().String("topic-prefix", "", "为生成的 topic 加前缀（可选）")
	genTablesCmd.Flags().String("topic-suffix", "", "为生成的 topic 加后缀（可选）")
	genTablesCmd.Flags().String("export-order-by", "", "默认填充到 tables.yaml 的 export_order_by 字段")
	genTablesCmd.Flags().String("export-key-column", "", "默认填充到 tables.yaml 的 export_key_column 字段")
	genTablesCmd.Flags().String("cursor-column", "", "默认填充到 tables.yaml 的 cursor_column 字段")
	genTablesCmd.Flags().String("cursor-start", "", "默认填充到 tables.yaml 的 cursor_start 字段")
	genTablesCmd.Flags().String("cursor-end", "", "默认填充到 tables.yaml 的 cursor_end 字段")
	genTablesCmd.Flags().String("include-columns", "", "仅生成包含指定列的表（逗号分隔，全部匹配）")
	genTablesCmd.Flags().String("exclude-columns", "", "排除包含指定列的表（逗号分隔，任一匹配即排除）")
	genTablesCmd.Flags().String("tables", "", "仅生成指定表（逗号分隔）")
}

// hasPrefix 判断 s 是否以 p 为前缀，或 p 为空。
func hasPrefix(s, p string) bool { return len(p) == 0 || (len(s) >= len(p) && s[:len(p)] == p) }

// hasSuffix 判断 s 是否以 sf 为后缀，或 sf 为空。
func hasSuffix(s, sf string) bool {
	return len(sf) == 0 || (len(s) >= len(sf) && s[len(s)-len(sf):] == sf)
}

// splitCSVLocal 将逗号分隔的字符串拆分并去除空格与空项。
func splitCSVLocal(s string) []string {
	s = strings.TrimSpace(s)
	if s == "" {
		return nil
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
