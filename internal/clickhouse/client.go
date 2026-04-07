// clickhouse 包提供通过 database/sql 与 ClickHouse 交互的辅助方法。
package clickhouse

import (
	"crypto/tls"
	"database/sql"
	"fmt"
	"math"
	"strings"

	ch "github.com/ClickHouse/clickhouse-go/v2"
)

// Connect 返回一个可用的 ClickHouse *sql.DB 连接（v2 驱动）。
func Connect(host string, port int, user string, password string, database string, secure bool) (*sql.DB, error) {
	opts := &ch.Options{
		Addr: []string{fmt.Sprintf("%s:%d", host, port)},
		Auth: ch.Auth{Database: database, Username: user, Password: password},
	}
	if secure {
		opts.TLS = &tls.Config{InsecureSkipVerify: true}
	}
	db := ch.OpenDB(opts)
	if err := db.Ping(); err != nil {
		return nil, err
	}
	return db, nil
}

// TableRows 保存表名与基于 system.parts 的行数估算。
type TableRows struct {
	Table string
	Rows  uint64
}

// CountTableRows 基于 system.parts 估算单表的行数。
func CountTableRows(db *sql.DB, database string, table string) (uint64, error) {
	var n uint64
	err := db.QueryRow("SELECT sum(rows) FROM system.parts WHERE database = ? AND table = ? AND active = 1", database, table).Scan(&n)
	if err != nil {
		return 0, err
	}
	return n, nil
}

// CountAllTablesRows 返回库内所有表的估算行数。
func CountAllTablesRows(db *sql.DB, database string) ([]TableRows, error) {
	rows, err := db.Query("SELECT table, sum(rows) AS rows FROM system.parts WHERE database = ? AND active = 1 GROUP BY table ORDER BY table", database)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []TableRows
	for rows.Next() {
		var t TableRows
		if err := rows.Scan(&t.Table, &t.Rows); err != nil {
			return nil, err
		}
		out = append(out, t)
	}
	return out, nil
}

// Column 描述 ClickHouse 列的名称、类型与位置。
type Column struct {
	Name     string
	Type     string
	Position uint64
}

type TypeDiff struct {
	Column     string `json:"column"`
	SourceType string `json:"source_type"`
	TargetType string `json:"target_type"`
	Issue      string `json:"issue"`
}

// GetColumns 查询 system.columns 获取源表的列结构。
func GetColumns(db *sql.DB, database string, table string) ([]Column, error) {
	rs, err := db.Query("SELECT name, type, position FROM system.columns WHERE database = ? AND table = ? ORDER BY position", database, table)
	if err != nil {
		return nil, err
	}
	defer rs.Close()
	var cols []Column
	for rs.Next() {
		var c Column
		if err := rs.Scan(&c.Name, &c.Type, &c.Position); err != nil {
			return nil, err
		}
		cols = append(cols, c)
	}
	return cols, nil
}

func AnalyzeTypeDiff(sourceCols []Column, targetCols []Column) []TypeDiff {
	srcMap := map[string]Column{}
	tgtMap := map[string]Column{}
	for _, c := range sourceCols {
		srcMap[c.Name] = c
	}
	for _, c := range targetCols {
		tgtMap[c.Name] = c
	}
	var diffs []TypeDiff
	for _, t := range targetCols {
		s, ok := srcMap[t.Name]
		if !ok {
			diffs = append(diffs, TypeDiff{Column: t.Name, SourceType: "", TargetType: t.Type, Issue: "missing_in_source"})
			continue
		}
		if normalizeCHType(s.Type) != normalizeCHType(t.Type) {
			diffs = append(diffs, TypeDiff{Column: t.Name, SourceType: s.Type, TargetType: t.Type, Issue: "type_mismatch"})
		}
	}
	for _, s := range sourceCols {
		if _, ok := tgtMap[s.Name]; !ok {
			diffs = append(diffs, TypeDiff{Column: s.Name, SourceType: s.Type, TargetType: "", Issue: "missing_in_target"})
		}
	}
	return diffs
}

func GetTableEngine(db *sql.DB, database string, table string) (string, error) {
	var eng string
	err := db.QueryRow("SELECT engine FROM system.tables WHERE database = ? AND name = ?", database, table).Scan(&eng)
	if err != nil {
		return "", err
	}
	return eng, nil
}

// GetTableKeys 读取源表的排序键与分区键表达式。
func GetTableKeys(db *sql.DB, database string, table string) (string, string, error) {
	var primaryKey, sortingKey, partitionKey string
	err := db.QueryRow("SELECT primary_key, sorting_key, partition_key FROM system.tables WHERE database = ? AND name = ?", database, table).Scan(&primaryKey, &sortingKey, &partitionKey)
	if err != nil {
		return "", "", err
	}
	key := strings.TrimSpace(primaryKey)
	if key == "" {
		key = strings.TrimSpace(sortingKey)
	}
	return key, partitionKey, nil
}

// quoteIdent 转义反引号并为标识符加反引号。
func quoteIdent(id string) string {
	if id == "" {
		return ""
	}
	return "`" + strings.ReplaceAll(id, "`", "``") + "`"
}

// qualified 返回反引号包裹的 database.table。
func qualified(db string, tbl string) string {
	return quoteIdent(db) + "." + quoteIdent(tbl)
}

func normalizeOrderByExpr(expr string) string {
	s := strings.TrimSpace(expr)
	if s == "" {
		return "tuple()"
	}
	ls := strings.ToLower(s)
	if strings.HasPrefix(ls, "tuple(") {
		return s
	}
	if strings.HasPrefix(s, "(") && strings.HasSuffix(s, ")") {
		return s
	}
	if strings.Contains(s, ",") {
		return "(" + s + ")"
	}
	return s
}

func isNullableType(t string) bool {
	s := strings.ToLower(strings.TrimSpace(t))
	return strings.HasPrefix(s, "nullable(")
}

func unwrapNullable(t string) string {
	s := strings.TrimSpace(t)
	if isNullableType(s) {
		i := strings.Index(s, "(")
		j := strings.LastIndex(s, ")")
		if i >= 0 && j > i {
			return strings.TrimSpace(s[i+1 : j])
		}
	}
	return s
}

func defaultExprForType(t string) string {
	s := strings.ToLower(strings.TrimSpace(t))
	switch {
	case strings.HasPrefix(s, "string"):
		return "''"
	case strings.HasPrefix(s, "fixedstring"):
		return "''"
	case strings.Contains(s, "int"):
		return "0"
	case strings.Contains(s, "uint"):
		return "0"
	case strings.Contains(s, "float"):
		return "0"
	case strings.HasPrefix(s, "decimal"):
		return "0"
	case strings.HasPrefix(s, "date32"):
		return "toDate(0)"
	case strings.HasPrefix(s, "date"):
		return "toDate(0)"
	case strings.HasPrefix(s, "datetime64"):
		return "toDateTime(0)"
	case strings.HasPrefix(s, "datetime"):
		return "toDateTime(0)"
	case strings.HasPrefix(s, "uuid"):
		return "toUUID('00000000-0000-0000-0000-000000000000')"
	default:
		return ""
	}
}

func mapBaseIntTo64(t string) string {
	s := strings.ToLower(strings.TrimSpace(t))
	switch s {
	case "int128", "int256":
		return "Int64"
	case "uint128", "uint256":
		return "UInt64"
	default:
		return t
	}
}

func mapTypeTo64(t string) string {
	if isNullableType(t) {
		base := unwrapNullable(t)
		mapped := mapBaseIntTo64(base)
		if mapped != base {
			return "Nullable(" + mapped + ")"
		}
		return t
	}
	mapped := mapBaseIntTo64(t)
	return mapped
}

func mapBaseIntToString(t string) string {
	s := strings.ToLower(strings.TrimSpace(t))
	switch s {
	case "int128", "int256", "uint128", "uint256":
		return "String"
	default:
		return t
	}
}

func mapTypeToString(t string) string {
	if isNullableType(t) {
		base := unwrapNullable(t)
		mapped := mapBaseIntToString(base)
		if mapped != base {
			return "Nullable(" + mapped + ")"
		}
		return t
	}
	mapped := mapBaseIntToString(t)
	return mapped
}

// CreateKafkaTable 创建与源表结构一致的 Kafka 引擎表。
func CreateKafkaTable(db *sql.DB, database string, table string, brokers []string, topic string, group string, format string, numConsumers int, maxBlockSize int, autoOffsetReset string) error {
	cols, err := GetColumns(db, database, table)
	if err != nil {
		return err
	}
	if len(cols) == 0 {
		return fmt.Errorf("source table has no columns: %s.%s", database, table)
	}
	var ddlCols string
	for i, c := range cols {
		if i > 0 {
			ddlCols += ","
		}
		ddlCols += fmt.Sprintf("%s %s", quoteIdent(c.Name), mapTypeToString(c.Type))
	}
	name := qualified(database, "kafka_"+table+"_sink")
	ddl := ""
	if strings.EqualFold(strings.TrimSpace(autoOffsetReset), "skip") {
		ddl = fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (%s) ENGINE = Kafka SETTINGS kafka_broker_list = '%s', kafka_topic_list = '%s', kafka_group_name = '%s', kafka_format = '%s', kafka_num_consumers = %d, kafka_max_block_size = %d, kafka_skip_broken_messages = %d", name, ddlCols, stringsJoin(brokers), topic, group, format, numConsumers, maxBlockSize, 1000)
	} else {
		if strings.TrimSpace(autoOffsetReset) == "" {
			autoOffsetReset = "latest"
		}
		ddl = fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (%s) ENGINE = Kafka SETTINGS kafka_broker_list = '%s', kafka_topic_list = '%s', kafka_group_name = '%s', kafka_format = '%s', kafka_num_consumers = %d, kafka_max_block_size = %d, kafka_auto_offset_reset = '%s', kafka_skip_broken_messages = %d", name, ddlCols, stringsJoin(brokers), topic, group, format, numConsumers, maxBlockSize, autoOffsetReset, 1000)
	}
	_, err = db.Exec(ddl)
	if err != nil {
		hasUnknownOffset := isUnknownKafkaAutoOffsetResetError(err) && !strings.EqualFold(strings.TrimSpace(autoOffsetReset), "skip")
		hasUnknownSkipBroken := isUnknownKafkaSkipBrokenMessagesError(err)
		if hasUnknownOffset && hasUnknownSkipBroken {
			ddl2 := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (%s) ENGINE = Kafka SETTINGS kafka_broker_list = '%s', kafka_topic_list = '%s', kafka_group_name = '%s', kafka_format = '%s', kafka_num_consumers = %d, kafka_max_block_size = %d", name, ddlCols, stringsJoin(brokers), topic, group, format, numConsumers, maxBlockSize)
			if _, e2 := db.Exec(ddl2); e2 == nil {
				return nil
			}
			return fmt.Errorf("ddl_failed: %s ; error: %v", ddl2, err)
		}
		if hasUnknownOffset {
			ddl2 := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (%s) ENGINE = Kafka SETTINGS kafka_broker_list = '%s', kafka_topic_list = '%s', kafka_group_name = '%s', kafka_format = '%s', kafka_num_consumers = %d, kafka_max_block_size = %d, kafka_skip_broken_messages = %d", name, ddlCols, stringsJoin(brokers), topic, group, format, numConsumers, maxBlockSize, 1000)
			var e2 error
			_, e2 = db.Exec(ddl2)
			if e2 == nil {
				return nil
			}
			if isUnknownKafkaSkipBrokenMessagesError(e2) {
				ddl3 := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (%s) ENGINE = Kafka SETTINGS kafka_broker_list = '%s', kafka_topic_list = '%s', kafka_group_name = '%s', kafka_format = '%s', kafka_num_consumers = %d, kafka_max_block_size = %d", name, ddlCols, stringsJoin(brokers), topic, group, format, numConsumers, maxBlockSize)
				var e3 error
				_, e3 = db.Exec(ddl3)
				if e3 == nil {
					return nil
				}
				return fmt.Errorf("ddl_failed: %s ; error: %v", ddl3, err)
			}
			return fmt.Errorf("ddl_failed: %s ; error: %v", ddl2, err)
		}
		if hasUnknownSkipBroken {
			ddl2 := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (%s) ENGINE = Kafka SETTINGS kafka_broker_list = '%s', kafka_topic_list = '%s', kafka_group_name = '%s', kafka_format = '%s', kafka_num_consumers = %d, kafka_max_block_size = %d, kafka_auto_offset_reset = '%s'", name, ddlCols, stringsJoin(brokers), topic, group, format, numConsumers, maxBlockSize, autoOffsetReset)
			var e2 error
			_, e2 = db.Exec(ddl2)
			if e2 == nil {
				return nil
			}
			if isUnknownKafkaAutoOffsetResetError(e2) && !strings.EqualFold(strings.TrimSpace(autoOffsetReset), "skip") {
				ddl3 := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (%s) ENGINE = Kafka SETTINGS kafka_broker_list = '%s', kafka_topic_list = '%s', kafka_group_name = '%s', kafka_format = '%s', kafka_num_consumers = %d, kafka_max_block_size = %d", name, ddlCols, stringsJoin(brokers), topic, group, format, numConsumers, maxBlockSize)
				var e3 error
				_, e3 = db.Exec(ddl3)
				if e3 == nil {
					return nil
				}
				return fmt.Errorf("ddl_failed: %s ; error: %v", ddl3, err)
			}
			return fmt.Errorf("ddl_failed: %s ; error: %v", ddl2, err)
		}
		return fmt.Errorf("ddl_failed: %s ; error: %v", ddl, err)
	}
	return nil
}

// CreateKafkaTableFromSource 在 kafkaDatabase 中按 sourceDatabase.table 的结构创建 Kafka 引擎表。
func CreateKafkaTableFromSource(db *sql.DB, sourceDatabase string, table string, kafkaDatabase string, brokers []string, topic string, group string, format string, numConsumers int, maxBlockSize int, autoOffsetReset string, extraColumns map[string]string) error {
	cols, err := GetColumns(db, sourceDatabase, table)
	if err != nil {
		return err
	}
	if len(cols) == 0 {
		return fmt.Errorf("source table has no columns: %s.%s", sourceDatabase, table)
	}
	var ddlCols string
	present := map[string]struct{}{}
	for i, c := range cols {
		present[c.Name] = struct{}{}
		if i > 0 {
			ddlCols += ","
		}
		ddlCols += fmt.Sprintf("%s %s", quoteIdent(c.Name), mapTypeToString(c.Type))
	}
	// append extra columns if not present in source schema
	if len(extraColumns) > 0 {
		for name, typ := range extraColumns {
			name = strings.TrimSpace(name)
			typ = strings.TrimSpace(typ)
			if name == "" || typ == "" {
				continue
			}
			if _, ok := present[name]; ok {
				continue
			}
			if ddlCols != "" {
				ddlCols += ","
			}
			ddlCols += fmt.Sprintf("%s %s", quoteIdent(name), typ)
		}
	}
	name := qualified(kafkaDatabase, "kafka_"+table+"_sink")
	ddl := ""
	if strings.EqualFold(strings.TrimSpace(autoOffsetReset), "skip") {
		ddl = fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (%s) ENGINE = Kafka SETTINGS kafka_broker_list = '%s', kafka_topic_list = '%s', kafka_group_name = '%s', kafka_format = '%s', kafka_num_consumers = %d, kafka_max_block_size = %d, kafka_skip_broken_messages = %d", name, ddlCols, stringsJoin(brokers), topic, group, format, numConsumers, maxBlockSize, 1000)
	} else {
		if strings.TrimSpace(autoOffsetReset) == "" {
			autoOffsetReset = "latest"
		}
		ddl = fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (%s) ENGINE = Kafka SETTINGS kafka_broker_list = '%s', kafka_topic_list = '%s', kafka_group_name = '%s', kafka_format = '%s', kafka_num_consumers = %d, kafka_max_block_size = %d, kafka_auto_offset_reset = '%s', kafka_skip_broken_messages = %d", name, ddlCols, stringsJoin(brokers), topic, group, format, numConsumers, maxBlockSize, autoOffsetReset, 1000)
	}
	_, err = db.Exec(ddl)
	if err != nil {
		hasUnknownOffset := isUnknownKafkaAutoOffsetResetError(err) && !strings.EqualFold(strings.TrimSpace(autoOffsetReset), "skip")
		hasUnknownSkipBroken := isUnknownKafkaSkipBrokenMessagesError(err)
		if hasUnknownOffset && hasUnknownSkipBroken {
			ddl2 := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (%s) ENGINE = Kafka SETTINGS kafka_broker_list = '%s', kafka_topic_list = '%s', kafka_group_name = '%s', kafka_format = '%s', kafka_num_consumers = %d, kafka_max_block_size = %d", name, ddlCols, stringsJoin(brokers), topic, group, format, numConsumers, maxBlockSize)
			if _, e2 := db.Exec(ddl2); e2 == nil {
				return nil
			}
			return fmt.Errorf("ddl_failed: %s ; error: %v", ddl2, err)
		}
		if hasUnknownOffset {
			ddl2 := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (%s) ENGINE = Kafka SETTINGS kafka_broker_list = '%s', kafka_topic_list = '%s', kafka_group_name = '%s', kafka_format = '%s', kafka_num_consumers = %d, kafka_max_block_size = %d, kafka_skip_broken_messages = %d", name, ddlCols, stringsJoin(brokers), topic, group, format, numConsumers, maxBlockSize, 1000)
			var e2 error
			_, e2 = db.Exec(ddl2)
			if e2 == nil {
				return nil
			}
			if isUnknownKafkaSkipBrokenMessagesError(e2) {
				ddl3 := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (%s) ENGINE = Kafka SETTINGS kafka_broker_list = '%s', kafka_topic_list = '%s', kafka_group_name = '%s', kafka_format = '%s', kafka_num_consumers = %d, kafka_max_block_size = %d", name, ddlCols, stringsJoin(brokers), topic, group, format, numConsumers, maxBlockSize)
				var e3 error
				_, e3 = db.Exec(ddl3)
				if e3 == nil {
					return nil
				}
				return fmt.Errorf("ddl_failed: %s ; error: %v", ddl3, err)
			}
			return fmt.Errorf("ddl_failed: %s ; error: %v", ddl2, err)
		}
		if hasUnknownSkipBroken {
			ddl2 := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (%s) ENGINE = Kafka SETTINGS kafka_broker_list = '%s', kafka_topic_list = '%s', kafka_group_name = '%s', kafka_format = '%s', kafka_num_consumers = %d, kafka_max_block_size = %d, kafka_auto_offset_reset = '%s'", name, ddlCols, stringsJoin(brokers), topic, group, format, numConsumers, maxBlockSize, autoOffsetReset)
			var e2 error
			_, e2 = db.Exec(ddl2)
			if e2 == nil {
				return nil
			}
			if isUnknownKafkaAutoOffsetResetError(e2) && !strings.EqualFold(strings.TrimSpace(autoOffsetReset), "skip") {
				ddl3 := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (%s) ENGINE = Kafka SETTINGS kafka_broker_list = '%s', kafka_topic_list = '%s', kafka_group_name = '%s', kafka_format = '%s', kafka_num_consumers = %d, kafka_max_block_size = %d", name, ddlCols, stringsJoin(brokers), topic, group, format, numConsumers, maxBlockSize)
				var e3 error
				_, e3 = db.Exec(ddl3)
				if e3 == nil {
					return nil
				}
				return fmt.Errorf("ddl_failed: %s ; error: %v", ddl3, err)
			}
			return fmt.Errorf("ddl_failed: %s ; error: %v", ddl2, err)
		}
		return fmt.Errorf("ddl_failed: %s ; error: %v", ddl, err)
	}
	return nil
}

func isUnknownKafkaAutoOffsetResetError(err error) bool {
	s := strings.ToLower(err.Error())
	if !strings.Contains(s, "unknown setting") {
		return false
	}
	return strings.Contains(s, "kafka_auto_offset_reset")
}

func isUnknownKafkaSkipBrokenMessagesError(err error) bool {
	s := strings.ToLower(err.Error())
	if !strings.Contains(s, "unknown setting") {
		return false
	}
	return strings.Contains(s, "kafka_skip_broken_messages")
}

func isUnknownAllowNullableKeySettingError(err error) bool {
	if err == nil {
		return false
	}
	s := strings.ToLower(err.Error())
	if !strings.Contains(s, "unknown setting") {
		return false
	}
	return strings.Contains(s, "allow_nullable_key")
}

// CreateMaterializedView 通过物化视图将 Kafka 表写入目标 MergeTree 表。
func CreateMaterializedView(db *sql.DB, kafkaDatabase string, sourceTable string, targetDatabase string, targetTable string) error {
	if targetDatabase == "" {
		targetDatabase = kafkaDatabase
	}
	mv := qualified(targetDatabase, "mv_from_kafka_"+sourceTable)
	sink := qualified(kafkaDatabase, "kafka_"+sourceTable+"_sink")
	sinkCols, _ := GetColumns(db, kafkaDatabase, "kafka_"+sourceTable+"_sink")
	var b strings.Builder
	for i, c := range sinkCols {
		if i > 0 {
			b.WriteString(",")
		}
		base := strings.ToLower(unwrapNullable(c.Type))
		isNull := isNullableType(c.Type)
		name := quoteIdent(c.Name)
		switch base {
		case "int128", "int256":
			if isNull {
				b.WriteString(fmt.Sprintf("toInt64OrNull(%s) AS %s", name, name))
			} else {
				b.WriteString(fmt.Sprintf("toInt64(ifNull(%s, 0)) AS %s", name, name))
			}
		case "uint128", "uint256":
			if isNull {
				b.WriteString(fmt.Sprintf("toUInt64OrNull(%s) AS %s", name, name))
			} else {
				b.WriteString(fmt.Sprintf("toUInt64(ifNull(%s, 0)) AS %s", name, name))
			}
		default:
			if isNull {
				b.WriteString(name)
			} else {
				def := defaultExprForType(base)
				if strings.TrimSpace(def) == "" {
					b.WriteString(name)
				} else {
					b.WriteString(fmt.Sprintf("ifNull(%s, %s) AS %s", name, def, name))
				}
			}
		}
	}
	selectList := b.String()
	ddl := fmt.Sprintf("CREATE MATERIALIZED VIEW IF NOT EXISTS %s TO %s AS SELECT %s FROM %s SETTINGS stream_like_engine_allow_direct_select=1, input_format_skip_unknown_fields=1, input_format_defaults_for_omitted_fields=1, input_format_null_as_default=1, input_format_json_try_infer_numbers_from_strings=1, input_format_json_read_objects_as_strings=1, date_time_input_format='best_effort', max_partitions_per_insert_block=1000", mv, qualified(targetDatabase, targetTable), selectList, sink)
	_, err := db.Exec(ddl)
	return err
}

// CreateMaterializedViewOwn 创建自带 MergeTree 存储的物化视图（可直接查询）。
func CreateMaterializedViewOwn(db *sql.DB, kafkaDatabase string, sourceDatabase string, sourceTable string, targetDatabase string, engine string, orderBy string, partitionBy string, versionColumn string, signColumn string, ttlDays int, ttlColumn string, maxPartitionsPerInsertBlock int) error {
	if targetDatabase == "" {
		targetDatabase = kafkaDatabase
	}
	if err := CreateDatabaseIfNotExists(db, targetDatabase); err != nil {
		return err
	}
	mv := qualified(targetDatabase, "mv_from_kafka_"+sourceTable)
	eng := strings.ToLower(strings.TrimSpace(engine))
	if eng == "" {
		eng = "merge"
	}
	ob := strings.TrimSpace(orderBy)
	if ob == "" {
		if strings.TrimSpace(sourceDatabase) != "" {
			if sk, _, err := GetTableKeys(db, sourceDatabase, sourceTable); err == nil && strings.TrimSpace(sk) != "" {
				ob = sk
			} else {
				ob = "tuple()"
			}
		} else {
			ob = "tuple()"
		}
	}
	ob = normalizeOrderByExpr(ob)
	var parts string
	if strings.TrimSpace(partitionBy) != "" {
		parts = fmt.Sprintf(" PARTITION BY %s", partitionBy)
	} else {
		// Prefer time-based monthly partition to avoid high-cardinality partitions
		var timeCol string
		if strings.TrimSpace(ttlColumn) != "" {
			timeCol = strings.TrimSpace(ttlColumn)
		} else {
			sinkCols, _ := GetColumns(db, kafkaDatabase, "kafka_"+sourceTable+"_sink")
			// first pass: common time column names with date/datetime types
			var cands = []string{"create_date", "updated_at", "update_ts", "time", "event_time", "insert_ts", "created_at"}
			for _, cand := range cands {
				for _, c := range sinkCols {
					if strings.EqualFold(c.Name, cand) {
						tn := strings.ToLower(c.Type)
						if strings.Contains(tn, "date") || strings.Contains(tn, "datetime") {
							timeCol = c.Name
							break
						}
					}
				}
				if timeCol != "" {
					break
				}
			}
			// second pass: any column with date/datetime type
			if timeCol == "" {
				for _, c := range sinkCols {
					tn := strings.ToLower(c.Type)
					if strings.Contains(tn, "date") || strings.Contains(tn, "datetime") {
						timeCol = c.Name
						break
					}
				}
			}
		}
		if strings.TrimSpace(timeCol) != "" {
			parts = fmt.Sprintf(" PARTITION BY toYYYYMM(%s)", quoteIdent(timeCol))
		} else {
			parts = " PARTITION BY tuple()"
		}
	}
	var storage string
	switch eng {
	case "replacing":
		vc := strings.TrimSpace(versionColumn)
		if vc != "" {
			storage = fmt.Sprintf("ENGINE = ReplacingMergeTree(%s)%s ORDER BY %s", quoteIdent(vc), parts, ob)
		} else {
			storage = fmt.Sprintf("ENGINE = ReplacingMergeTree()%s ORDER BY %s", parts, ob)
		}
	case "collapsing":
		sc := strings.TrimSpace(signColumn)
		if sc == "" {
			sc = "sign"
		}
		storage = fmt.Sprintf("ENGINE = CollapsingMergeTree(%s)%s ORDER BY %s", quoteIdent(sc), parts, ob)
	case "versioned_collapsing":
		sc := strings.TrimSpace(signColumn)
		if sc == "" {
			sc = "sign"
		}
		vc := strings.TrimSpace(versionColumn)
		if vc == "" {
			vc = "version"
		}
		storage = fmt.Sprintf("ENGINE = VersionedCollapsingMergeTree(%s, %s)%s ORDER BY %s", quoteIdent(sc), quoteIdent(vc), parts, ob)
	default:
		storage = fmt.Sprintf("ENGINE = MergeTree%s ORDER BY %s", parts, ob)
	}
	if ttlDays > 0 {
		col := strings.TrimSpace(ttlColumn)
		if col == "" {
			sinkCols, _ := GetColumns(db, kafkaDatabase, "kafka_"+sourceTable+"_sink")
			cand := ""
			for _, c := range sinkCols {
				tn := strings.ToLower(c.Type)
				if strings.Contains(tn, "date") {
					n := strings.ToLower(c.Name)
					if n == "update_ts" || n == "updated_at" || n == "ts" || n == "event_time" || n == "created_at" || n == "insert_ts" {
						cand = c.Name
						break
					}
				}
			}
			if cand == "" {
				for _, c := range sinkCols {
					tn := strings.ToLower(c.Type)
					if strings.Contains(tn, "date") {
						cand = c.Name
						break
					}
				}
			}
			col = cand
		}
		if strings.TrimSpace(col) != "" {
			sinkCols, _ := GetColumns(db, kafkaDatabase, "kafka_"+sourceTable+"_sink")
			tname := ""
			for _, c := range sinkCols {
				if c.Name == col {
					tname = c.Type
					break
				}
			}
			tn := strings.ToLower(tname)
			if strings.Contains(tn, "nullable") {
				storage = storage + fmt.Sprintf(" TTL toDateTime(%s) + INTERVAL %d DAY WHERE %s IS NOT NULL", quoteIdent(col), ttlDays, quoteIdent(col))
			} else {
				storage = storage + fmt.Sprintf(" TTL toDateTime(%s) + INTERVAL %d DAY", quoteIdent(col), ttlDays)
			}
		}
	}
	sinkCols, _ := GetColumns(db, kafkaDatabase, "kafka_"+sourceTable+"_sink")
	var b strings.Builder
	for i, c := range sinkCols {
		if i > 0 {
			b.WriteString(",")
		}
		base := strings.ToLower(unwrapNullable(c.Type))
		isNull := isNullableType(c.Type)
		name := quoteIdent(c.Name)
		if eng == "replacing" && strings.TrimSpace(versionColumn) != "" && c.Name == strings.TrimSpace(versionColumn) {
			b.WriteString(fmt.Sprintf("toUInt64(ifNull(%s, 0)) AS %s", name, name))
			continue
		}
		switch base {
		case "int128", "int256":
			if isNull {
				b.WriteString(fmt.Sprintf("toInt64OrNull(%s) AS %s", name, name))
			} else {
				b.WriteString(fmt.Sprintf("toInt64(ifNull(%s, 0)) AS %s", name, name))
			}
		case "uint128", "uint256":
			if isNull {
				b.WriteString(fmt.Sprintf("toUInt64OrNull(%s) AS %s", name, name))
			} else {
				b.WriteString(fmt.Sprintf("toUInt64(ifNull(%s, 0)) AS %s", name, name))
			}
		default:
			if isNull {
				b.WriteString(name)
			} else {
				def := defaultExprForType(base)
				if strings.TrimSpace(def) == "" {
					b.WriteString(name)
				} else {
					b.WriteString(fmt.Sprintf("ifNull(%s, %s) AS %s", name, def, name))
				}
			}
		}
	}
	selectList := b.String()
	if maxPartitionsPerInsertBlock <= 0 {
		maxPartitionsPerInsertBlock = 1000
	}
	ddl := fmt.Sprintf("CREATE MATERIALIZED VIEW IF NOT EXISTS %s %s SETTINGS allow_nullable_key=1 AS SELECT %s FROM %s SETTINGS stream_like_engine_allow_direct_select=1, input_format_skip_unknown_fields=1, input_format_defaults_for_omitted_fields=1, input_format_null_as_default=1, input_format_json_try_infer_numbers_from_strings=1, input_format_json_read_objects_as_strings=1, date_time_input_format='best_effort', max_partitions_per_insert_block=%d", mv, storage, selectList, qualified(kafkaDatabase, "kafka_"+sourceTable+"_sink"), maxPartitionsPerInsertBlock)
	if _, err := db.Exec(ddl); err != nil {
		if isUnknownAllowNullableKeySettingError(err) {
			ddl2 := fmt.Sprintf("CREATE MATERIALIZED VIEW IF NOT EXISTS %s %s AS SELECT %s FROM %s SETTINGS stream_like_engine_allow_direct_select=1, input_format_skip_unknown_fields=1, input_format_defaults_for_omitted_fields=1, input_format_null_as_default=1, input_format_json_try_infer_numbers_from_strings=1, input_format_json_read_objects_as_strings=1, date_time_input_format='best_effort', max_partitions_per_insert_block=%d", mv, storage, selectList, qualified(kafkaDatabase, "kafka_"+sourceTable+"_sink"), maxPartitionsPerInsertBlock)
			if _, e2 := db.Exec(ddl2); e2 == nil {
				return nil
			}
			return fmt.Errorf("ddl_failed: %s ; error: %v", ddl2, err)
		}
		return err
	}
	return nil
}

// CreateMaterializedViewToKafka 通过物化视图将源表的行推送到 Kafka 引擎表。
func CreateMaterializedViewToKafka(db *sql.DB, sourceDatabase string, sourceTable string, kafkaDatabase string) error {
	if kafkaDatabase == "" {
		kafkaDatabase = sourceDatabase
	}
	mv := qualified(kafkaDatabase, "mv_to_kafka_"+sourceTable)
	targetKafka := qualified(kafkaDatabase, "kafka_"+sourceTable+"_sink")
	src := qualified(sourceDatabase, sourceTable)
	// Build column list (typed to sink schema) and SELECT list (cast to 64-bit when needed)
	sinkCols, _ := GetColumns(db, kafkaDatabase, "kafka_"+sourceTable+"_sink")
	srcCols, _ := GetColumns(db, sourceDatabase, sourceTable)
	srcTypes := map[string]string{}
	for _, sc := range srcCols {
		srcTypes[sc.Name] = sc.Type
	}
	var selectDDL strings.Builder
	for i, c := range sinkCols {
		if i > 0 {
			selectDDL.WriteString(",")
		}
		// SELECT expression typed to sink schema, with explicit cast when needed
		base := strings.ToLower(unwrapNullable(c.Type))
		isNull := isNullableType(c.Type)
		name := quoteIdent(c.Name)
		switch base {
		case "int128", "int256":
			if isNull {
				selectDDL.WriteString(fmt.Sprintf("toInt64OrNull(%s) AS %s", name, name))
			} else {
				selectDDL.WriteString(fmt.Sprintf("toInt64(%s) AS %s", name, name))
			}
		case "uint128", "uint256":
			if isNull {
				selectDDL.WriteString(fmt.Sprintf("toUInt64OrNull(%s) AS %s", name, name))
			} else {
				selectDDL.WriteString(fmt.Sprintf("toUInt64(%s) AS %s", name, name))
			}
		case "string":
			selectDDL.WriteString(fmt.Sprintf("toString(%s) AS %s", name, name))
		default:
			selectDDL.WriteString(name)
		}
	}
	selectList := selectDDL.String()
	ddl := fmt.Sprintf("CREATE MATERIALIZED VIEW IF NOT EXISTS %s TO %s AS SELECT %s FROM %s", mv, targetKafka, selectList, src)
	_, err := db.Exec(ddl)
	if err != nil {
		return fmt.Errorf("ddl_failed: %s ; error: %v", ddl, err)
	}
	return nil
}

func BuildMvSelectWithCasts(inputCols []Column, targetCols []Column) (string, error) {
	inMap := map[string]Column{}
	for _, c := range inputCols {
		inMap[c.Name] = c
	}
	var exprs []string
	for _, t := range targetCols {
		if _, ok := inMap[t.Name]; !ok {
			return "", fmt.Errorf("target_column_missing_in_input: %s", t.Name)
		}
		src := quoteIdent(t.Name)
		expr := BuildStrictCastExpr(src, t.Type)
		exprs = append(exprs, fmt.Sprintf("%s AS %s", expr, quoteIdent(t.Name)))
	}
	return strings.Join(exprs, ","), nil
}

func BuildSelectToString(cols []Column) string {
	var exprs []string
	for _, c := range cols {
		name := quoteIdent(c.Name)
		exprs = append(exprs, fmt.Sprintf("CAST(%s AS String) AS %s", name, name))
	}
	return strings.Join(exprs, ",")
}

func BuildStrictCastExpr(sourceExpr string, targetType string) string {
	t := strings.TrimSpace(targetType)
	if t == "" {
		return sourceExpr
	}
	if inner, ok := unwrapType(t, "Nullable"); ok {
		return fmt.Sprintf("CAST(%s, '%s')", BuildStrictCastExpr(sourceExpr, inner), t)
	}
	if inner, ok := unwrapType(t, "LowCardinality"); ok {
		return fmt.Sprintf("CAST(%s, '%s')", BuildStrictCastExpr(sourceExpr, inner), t)
	}
	if isDateLikeType(t) {
		return fmt.Sprintf("CAST(parseDateTimeBestEffort(%s), '%s')", sourceExpr, t)
	}
	if strings.EqualFold(baseTypeName(t), "String") {
		return sourceExpr
	}
	return fmt.Sprintf("CAST(%s, '%s')", sourceExpr, t)
}

func unwrapType(t string, fn string) (string, bool) {
	s := strings.TrimSpace(t)
	prefix := fn + "("
	if !strings.HasPrefix(s, prefix) || !strings.HasSuffix(s, ")") {
		return "", false
	}
	return strings.TrimSpace(s[len(prefix) : len(s)-1]), true
}

func baseTypeName(t string) string {
	s := strings.TrimSpace(t)
	for {
		if inner, ok := unwrapType(s, "Nullable"); ok {
			s = inner
			continue
		}
		if inner, ok := unwrapType(s, "LowCardinality"); ok {
			s = inner
			continue
		}
		break
	}
	if i := strings.IndexByte(s, '('); i >= 0 {
		s = s[:i]
	}
	return strings.TrimSpace(s)
}

func isDateLikeType(t string) bool {
	base := strings.ToLower(baseTypeName(t))
	return base == "date" || base == "date32" || base == "datetime" || base == "datetime64"
}

func normalizeCHType(t string) string {
	return strings.ReplaceAll(strings.ToLower(strings.TrimSpace(t)), " ", "")
}

// CreateDatabaseIfNotExists 若数据库不存在则创建。
func CreateDatabaseIfNotExists(db *sql.DB, database string) error {
	_, err := db.Exec(fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", quoteIdent(database)))
	return err
}

// CreateTargetTableLikeSource 创建与源表列一致的 MergeTree 目标表。
func CreateTargetTableLikeSource(db *sql.DB, sourceDatabase string, sourceTable string, targetDatabase string, targetTable string, orderBy string, partitionBy string) error {
	if targetDatabase == "" {
		targetDatabase = sourceDatabase
	}
	if targetTable == "" {
		targetTable = sourceTable + "_replica"
	}
	if err := CreateDatabaseIfNotExists(db, targetDatabase); err != nil {
		return err
	}
	cols, err := GetColumns(db, sourceDatabase, sourceTable)
	if err != nil {
		return err
	}
	var ddlCols string
	for i, c := range cols {
		if i > 0 {
			ddlCols += ","
		}
		ddlCols += fmt.Sprintf("%s %s", quoteIdent(c.Name), c.Type)
	}
	if strings.TrimSpace(orderBy) == "" {
		orderBy = "tuple()"
	}
	orderBy = normalizeOrderByExpr(orderBy)
	var parts string
	if strings.TrimSpace(partitionBy) != "" {
		parts = fmt.Sprintf(" PARTITION BY %s", partitionBy)
	}
	name := qualified(targetDatabase, targetTable)
	ddl := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (%s) ENGINE = MergeTree%s ORDER BY %s SETTINGS allow_nullable_key=1", name, ddlCols, parts, orderBy)
	if _, err = db.Exec(ddl); err != nil {
		if isUnknownAllowNullableKeySettingError(err) {
			ddl2 := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (%s) ENGINE = MergeTree%s ORDER BY %s", name, ddlCols, parts, orderBy)
			if _, e2 := db.Exec(ddl2); e2 == nil {
				return nil
			}
			return fmt.Errorf("ddl_failed: %s ; error: %v", ddl2, err)
		}
		return err
	}
	return nil
}

// CreateReadableView 创建指向目标表的只读视图。
func CreateReadableView(db *sql.DB, sourceTable string, targetDatabase string, targetTable string) error {
	if targetDatabase == "" {
		return fmt.Errorf("targetDatabase empty")
	}
	if targetTable == "" {
		return fmt.Errorf("targetTable empty")
	}
	view := qualified(targetDatabase, "v_"+sourceTable)
	ddl := fmt.Sprintf("CREATE VIEW IF NOT EXISTS %s AS SELECT * FROM %s", view, qualified(targetDatabase, targetTable))
	_, err := db.Exec(ddl)
	return err
}

// DropTableIfExists 若存在则删除表。
func DropTableIfExists(db *sql.DB, database string, table string) error {
	_, err := db.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s", qualified(database, table)))
	return err
}

// DropMaterializedViewIfExists 若存在则删除视图。
func DropMaterializedViewIfExists(db *sql.DB, database string, view string) error {
	_, err := db.Exec(fmt.Sprintf("DROP VIEW IF EXISTS %s", qualified(database, view)))
	return err
}

// PartitionsForRows 按 rowsPerPartition 将行数映射为分区数。
func PartitionsForRows(rows uint64, rowsPerPartition int) int {
	if rowsPerPartition <= 0 {
		return 1
	}
	if rows == 0 {
		return 1
	}
	p := int(math.Ceil(float64(rows) / float64(rowsPerPartition)))
	if p < 1 {
		p = 1
	}
	return p
}

// stringsJoin 用逗号连接字符串，不包含空格。
func stringsJoin(s []string) string {
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
