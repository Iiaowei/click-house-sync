// cmd 包包含导出到 Kafka 的命令。
package cmd

import (
	"click-house-sync/internal/clickhouse"
	kadmin "click-house-sync/internal/kafka"
	kprod "click-house-sync/internal/kafka"
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/spf13/cobra"
)

// exportCmd 按批次从 ClickHouse 读取并写入 Kafka。
var exportCmd = &cobra.Command{
	Use:   "export",
	Short: "分批写入Kafka",
	Long:  "从源表按批次读取并写入 Kafka。支持 ORDER BY 稳定排序、消息键列保证分区内顺序，以及游标范围分页（cursor-column/cursor-start/cursor-end）。",
	RunE: func(cmd *cobra.Command, args []string) error {
		table, _ := cmd.Flags().GetString("table")
		if table == "" {
			return fmt.Errorf("缺少 --table")
		}
		db, err := clickhouse.Connect(chHost, chPort, chUser, chPassword, chDatabase, chSecure)
		if err != nil {
			return err
		}
		defer db.Close()
		tconf, err := lookupTableConfig(table)
		if err != nil {
			return err
		}
		srcDB := chDatabase
		if !cmd.Root().PersistentFlags().Changed("ch-database") && tconf != nil && tconf.CurrentDatabase != "" {
			srcDB = tconf.CurrentDatabase
		}
		if kafkaTopic == "" {
			kafkaTopic = srcDB + "_" + table
		}
		cols, err := clickhouse.GetColumns(db, srcDB, table)
		if err != nil {
			return err
		}
		var names []string
		for _, c := range cols {
			names = append(names, c.Name)
		}
		ord := exportOrderBy
		if tconf != nil && strings.TrimSpace(tconf.ExportOrderBy) != "" {
			ord = tconf.ExportOrderBy
		}
		keyCol := exportKeyColumn
		if tconf != nil && strings.TrimSpace(tconf.ExportKeyColumn) != "" {
			keyCol = tconf.ExportKeyColumn
		}
		curCol := cursorColumn
		curStart := cursorStart
		curEnd := cursorEnd
		if tconf != nil {
			if strings.TrimSpace(tconf.CursorColumn) != "" {
				curCol = tconf.CursorColumn
			}
			if strings.TrimSpace(tconf.CursorStart) != "" {
				curStart = tconf.CursorStart
			}
			if strings.TrimSpace(tconf.CursorEnd) != "" {
				curEnd = tconf.CursorEnd
			}
		}
		if watch && cursorStartFromTarget && strings.TrimSpace(curCol) != "" {
			// determine target location
			tgtDB := chDatabase
			if cmd.Root().PersistentFlags().Changed("target-database") {
				tgtDB = targetDatabase
			} else if tconf != nil && strings.TrimSpace(tconf.TargetDatabase) != "" {
				tgtDB = tconf.TargetDatabase
			}
			var src string
			if mvOwnTable {
				src = qualified(tgtDB, "mv_from_kafka_"+table)
			} else {
				tgtTbl := targetTable
				if strings.TrimSpace(tgtTbl) == "" {
					if tconf != nil && strings.TrimSpace(tconf.TargetTable) != "" {
						tgtTbl = tconf.TargetTable
					} else {
						tgtTbl = table
					}
				}
				src = qualified(tgtDB, tgtTbl)
			}
			q := fmt.Sprintf("SELECT max(%s) FROM %s", quoteIdent(curCol), src)
			var v any
			if err := db.QueryRow(q).Scan(&v); err == nil && v != nil {
				// start from max seen in target
				curStart = fmt.Sprint(v)
			}
		}
		keyIndex := -1
		if keyCol != "" {
			for i, n := range names {
				if n == keyCol {
					keyIndex = i
					break
				}
			}
		}
		bs := batchSize
		if tconf != nil && tconf.BatchSize > 0 {
			bs = tconf.BatchSize
		}
		var brokers []string
		if cmd.Root().PersistentFlags().Changed("kafka-brokers") {
			brokers = brokersList()
		} else if tconf != nil && len(tconf.Brokers) > 0 {
			brokers = tconf.Brokers
		} else {
			brokers = brokersList()
		}
		if err := kadmin.WaitTopicReady(brokers, kafkaTopic, 10*time.Second); err != nil {
			return err
		}
		w := kprod.NewWriter(brokers, kafkaTopic, bs)
		defer w.Close()
		ctx := context.Background()
		offset := 0
		total := 0
		var lastCursor any
		var cursorIdx = -1
		if strings.TrimSpace(curCol) != "" {
			for i, n := range names {
				if n == curCol {
					cursorIdx = i
					break
				}
			}
		}
		for {
			base := fmt.Sprintf("SELECT %s FROM %s", joinQuoted(names), qualified(srcDB, table))
			var where string
			if cursorIdx >= 0 {
				if lastCursor != nil {
					where = fmt.Sprintf(" WHERE %s > %s", quoteIdent(curCol), sqlLiteral(lastCursor))
				} else if strings.TrimSpace(curStart) != "" {
					where = fmt.Sprintf(" WHERE %s >= %s", quoteIdent(curCol), sqlLiteral(curStart))
				}
				if strings.TrimSpace(curEnd) != "" {
					if where == "" {
						where = fmt.Sprintf(" WHERE %s <= %s", quoteIdent(curCol), sqlLiteral(curEnd))
					} else {
						where += fmt.Sprintf(" AND %s <= %s", quoteIdent(curCol), sqlLiteral(curEnd))
					}
				}
			}
			base += where
			effOrder := ord
			if cursorIdx >= 0 {
				if strings.TrimSpace(effOrder) == "" {
					effOrder = quoteIdent(curCol)
				} else if !strings.Contains(effOrder, curCol) {
					effOrder = quoteIdent(curCol) + ", " + effOrder
				}
			}
			if o := normalizeOrderBy(effOrder, names); o != "" {
				base += fmt.Sprintf(" ORDER BY %s", o)
			}
			var query string
			if cursorIdx >= 0 {
				query = fmt.Sprintf("%s LIMIT %d", base, bs)
			} else {
				query = fmt.Sprintf("%s LIMIT %d, %d", base, offset, bs)
			}
			printJSON(map[string]any{"event": "export_query", "database": srcDB, "table": table, "query": query})
			rows, err := db.Query(query)
			if err != nil {
				printErrJSON(map[string]any{"query": query, "error": err.Error()})
				return err
			}
			n := 0
			var msgs []kprod.Message
			for rows.Next() {
				vals := make([]any, len(names))
				ptrs := make([]any, len(names))
				for i := range vals {
					ptrs[i] = &vals[i]
				}
				if err := rows.Scan(ptrs...); err != nil {
					rows.Close()
					return err
				}
				if cursorIdx >= 0 {
					lastCursor = vals[cursorIdx]
				}
				m := map[string]any{}
				for i, name := range names {
					v := vals[i]
					switch t := v.(type) {
					case []byte:
						m[name] = string(t)
					case time.Time:
						m[name] = t.Format("2006-01-02 15:04:05")
					default:
						m[name] = v
					}
				}
				var key []byte
				if keyIndex >= 0 {
					kv := vals[keyIndex]
					switch t := kv.(type) {
					case []byte:
						key = t
					default:
						key = []byte(fmt.Sprint(kv))
					}
				}
				msg, err := kprod.MessageFromMap(m, key)
				if err != nil {
					rows.Close()
					return err
				}
				msgs = append(msgs, msg)
				n++
			}
			rows.Close()
			if n == 0 {
				if watch {
					time.Sleep(time.Duration(pollInterval) * time.Second)
					continue
				}
				break
			}
			if err := kprod.WriteBatchWithRetry(ctx, brokers, kafkaTopic, w, msgs, 3); err != nil {
				return err
			}
			if cursorIdx < 0 {
				offset += n
			}
			total += n
			printJSON(map[string]any{"event": "batch_exported", "database": srcDB, "table": table, "offset": offset, "size": n})
			time.Sleep(10 * time.Millisecond)
		}
		printJSON(map[string]any{"command": "export", "database": srcDB, "table": table, "topic": kafkaTopic, "total": total})
		return nil
	},
}

func init() {
	rootCmd.AddCommand(exportCmd)
	exportCmd.Flags().String("table", "", "源表名（必填）")
}

// joinComma 用逗号拼接字符串切片。
func joinComma(s []string) string {
	if len(s) == 0 {
		return ""
	}
	var b []byte
	for i, v := range s {
		if i > 0 {
			b = append(b, ',')
		}
		b = append(b, v...)
	}
	return string(b)
}

// joinQuoted 为标识符加反引号并以逗号连接。
func joinQuoted(s []string) string {
	if len(s) == 0 {
		return ""
	}
	var b []byte
	for i, v := range s {
		if i > 0 {
			b = append(b, ',')
		}
		q := quoteIdent(v)
		b = append(b, q...)
	}
	return string(b)
}

// quoteIdent 为标识符加反引号并转义内部反引号。
func quoteIdent(id string) string {
	id = strings.TrimSpace(id)
	if id == "" {
		return ""
	}
	return "`" + strings.ReplaceAll(id, "`", "``") + "`"
}

// qualified 返回反引号包裹的 database.table。
func qualified(db string, tbl string) string {
	return quoteIdent(db) + "." + quoteIdent(tbl)
}

// exportTableToKafka 执行单表的批量导出到 Kafka。
// 支持稳定的 ORDER BY、消息键以及游标范围分页。
func exportTableToKafka(db *sql.DB, database string, table string, brokers []string, topic string, bs int, orderBy string, keyColumn string, cursorColumn string, cursorStart string, cursorEnd string) error {
	cols, err := clickhouse.GetColumns(db, database, table)
	if err != nil {
		return err
	}
	var names []string
	for _, c := range cols {
		names = append(names, c.Name)
	}
	if bs <= 0 {
		bs = batchSize
	}
	if err := kadmin.WaitTopicReady(brokers, topic, 10*time.Second); err != nil {
		return err
	}
	w := kprod.NewWriter(brokers, topic, bs)
	defer w.Close()
	ctx := context.Background()
	offset := 0
	total := 0
	var lastCursor any
	var cursorIdx = -1
	if strings.TrimSpace(cursorColumn) != "" {
		for i, n := range names {
			if n == cursorColumn {
				cursorIdx = i
				break
			}
		}
	}
	for {
		base := fmt.Sprintf("SELECT %s FROM %s", joinQuoted(names), qualified(database, table))
		var where string
		if cursorIdx >= 0 {
			if lastCursor != nil {
				where = fmt.Sprintf(" WHERE %s > %s", quoteIdent(cursorColumn), sqlLiteral(lastCursor))
			} else if strings.TrimSpace(cursorStart) != "" {
				where = fmt.Sprintf(" WHERE %s >= %s", quoteIdent(cursorColumn), sqlLiteral(cursorStart))
			}
			if strings.TrimSpace(cursorEnd) != "" {
				if where == "" {
					where = fmt.Sprintf(" WHERE %s <= %s", quoteIdent(cursorColumn), sqlLiteral(cursorEnd))
				} else {
					where += fmt.Sprintf(" AND %s <= %s", quoteIdent(cursorColumn), sqlLiteral(cursorEnd))
				}
			}
		}
		base += where
		effOrder := orderBy
		if cursorIdx >= 0 {
			if strings.TrimSpace(effOrder) == "" {
				effOrder = quoteIdent(cursorColumn)
			} else if !strings.Contains(effOrder, cursorColumn) {
				effOrder = quoteIdent(cursorColumn) + ", " + effOrder
			}
		}
		if o := normalizeOrderBy(effOrder, names); o != "" {
			base += fmt.Sprintf(" ORDER BY %s", o)
		}
		query := fmt.Sprintf("%s LIMIT %d, %d", base, offset, bs)
		printJSON(map[string]any{"event": "export_query", "database": database, "table": table, "query": query})
		rows, err := db.Query(query)
		if err != nil {
			printErrJSON(map[string]any{"query": query, "error": err.Error()})
			return err
		}
		n := 0
		var msgs []kprod.Message
		for rows.Next() {
			vals := make([]any, len(names))
			ptrs := make([]any, len(names))
			for i := range vals {
				ptrs[i] = &vals[i]
			}
			if err := rows.Scan(ptrs...); err != nil {
				rows.Close()
				return err
			}
			if cursorIdx >= 0 {
				lastCursor = vals[cursorIdx]
			}
			m := map[string]any{}
			for i, name := range names {
				v := vals[i]
				switch t := v.(type) {
				case []byte:
					m[name] = string(t)
				case time.Time:
					m[name] = t.Format("2006-01-02 15:04:05")
				default:
					m[name] = v
				}
			}
			var key []byte
			if strings.TrimSpace(keyColumn) != "" {
				for i, n := range names {
					if n == keyColumn {
						kv := vals[i]
						switch t := kv.(type) {
						case []byte:
							key = t
						default:
							key = []byte(fmt.Sprint(kv))
						}
						break
					}
				}
			}
			msg, err := kprod.MessageFromMap(m, key)
			if err != nil {
				rows.Close()
				return err
			}
			msgs = append(msgs, msg)
			n++
		}
		rows.Close()
		if n == 0 {
			break
		}
		if err := kprod.WriteBatchWithRetry(ctx, brokers, topic, w, msgs, 3); err != nil {
			return err
		}
		offset += n
		total += n
		printJSON(map[string]any{"event": "batch_exported", "database": database, "table": table, "offset": offset, "size": n})
		time.Sleep(10 * time.Millisecond)
	}
	printJSON(map[string]any{"event": "export_completed", "database": database, "table": table, "total": total})
	return nil
}

func sqlLiteral(v any) string {
	switch t := v.(type) {
	case []byte:
		return "'" + strings.ReplaceAll(string(t), "'", "''") + "'"
	case string:
		return "'" + strings.ReplaceAll(t, "'", "''") + "'"
	case time.Time:
		return "'" + t.Format("2006-01-02 15:04:05") + "'"
	default:
		return fmt.Sprint(v)
	}
}

func normalizeOrderBy(expr string, cols []string) string {
	expr = strings.TrimSpace(expr)
	if expr == "" {
		return ""
	}
	// build set of known columns
	known := map[string]struct{}{}
	for _, c := range cols {
		known[c] = struct{}{}
	}
	parts := strings.Split(expr, ",")
	var out []string
	for _, p := range parts {
		t := strings.TrimSpace(p)
		if t == "" {
			continue
		}
		// accept patterns: col, `col`, col ASC, col DESC
		var base, suffix string
		tokens := strings.Fields(t)
		if len(tokens) == 0 {
			continue
		}
		base = tokens[0]
		if len(tokens) > 1 {
			u := strings.ToUpper(tokens[1])
			if u == "ASC" || u == "DESC" {
				suffix = " " + u
			}
		}
		// strip backticks if present
		if strings.HasPrefix(base, "`") && strings.HasSuffix(base, "`") && len(base) >= 2 {
			base = base[1 : len(base)-1]
		}
		if _, ok := known[base]; !ok {
			// skip unknown/complex tokens to avoid语法错误
			continue
		}
		out = append(out, quoteIdent(base)+suffix)
	}
	if len(out) == 0 {
		return ""
	}
	return strings.Join(out, ", ")
}
