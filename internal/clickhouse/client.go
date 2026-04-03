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
			diffs = append(diffs, TypeDiff{
				Column:     t.Name,
				SourceType: "",
				TargetType: t.Type,
				Issue:      "missing_in_source",
			})
			continue
		}
		if normalizeCHType(s.Type) != normalizeCHType(t.Type) {
			diffs = append(diffs, TypeDiff{
				Column:     t.Name,
				SourceType: s.Type,
				TargetType: t.Type,
				Issue:      "type_mismatch",
			})
		}
	}
	for _, s := range sourceCols {
		if _, ok := tgtMap[s.Name]; !ok {
			diffs = append(diffs, TypeDiff{
				Column:     s.Name,
				SourceType: s.Type,
				TargetType: "",
				Issue:      "missing_in_target",
			})
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

// CreateKafkaTable 创建与源表结构一致的 Kafka 引擎表。
func CreateKafkaTable(db *sql.DB, database string, table string, brokers []string, topic string, group string, format string, numConsumers int, maxBlockSize int, autoOffsetReset string) error {
	cols, err := GetColumns(db, database, table)
	if err != nil {
		return err
	}
	ddlCols := buildStringColumnsDDL(cols)
	name := qualified(database, "kafka_"+table+"_sink")
	return createKafkaTableByDDL(db, name, ddlCols, brokers, topic, group, format, numConsumers, maxBlockSize, autoOffsetReset)
}

// CreateKafkaTableFromSource 在 kafkaDatabase 中按 sourceDatabase.table 的结构创建 Kafka 引擎表。
func CreateKafkaTableFromSource(db *sql.DB, sourceDatabase string, table string, kafkaDatabase string, brokers []string, topic string, group string, format string, numConsumers int, maxBlockSize int, autoOffsetReset string) error {
	cols, err := GetColumns(db, sourceDatabase, table)
	if err != nil {
		return err
	}
	ddlCols := buildStringColumnsDDL(cols)
	name := qualified(kafkaDatabase, "kafka_"+table+"_sink")
	return createKafkaTableByDDL(db, name, ddlCols, brokers, topic, group, format, numConsumers, maxBlockSize, autoOffsetReset)
}

func isUnknownKafkaAutoOffsetResetError(err error) bool {
	s := strings.ToLower(err.Error())
	if !strings.Contains(s, "unknown setting") {
		return false
	}
	return strings.Contains(s, "kafka_auto_offset_reset")
}

// CreateMaterializedView 通过物化视图将 Kafka 表写入目标 MergeTree 表。
func CreateMaterializedView(db *sql.DB, kafkaDatabase string, sourceTable string, targetDatabase string, targetTable string) error {
	if targetDatabase == "" {
		targetDatabase = kafkaDatabase
	}
	targetCols, err := GetColumns(db, targetDatabase, targetTable)
	if err != nil {
		return err
	}
	kafkaCols, err := GetColumns(db, kafkaDatabase, "kafka_"+sourceTable+"_sink")
	if err != nil {
		return err
	}
	selectExpr, err := BuildMvSelectWithCasts(kafkaCols, targetCols)
	if err != nil {
		return err
	}
	mv := qualified(kafkaDatabase, "mv_from_kafka_"+sourceTable)
	ddl := fmt.Sprintf("CREATE MATERIALIZED VIEW IF NOT EXISTS %s TO %s AS SELECT %s FROM %s SETTINGS stream_like_engine_allow_direct_select=1, input_format_skip_unknown_fields=1, date_time_input_format='best_effort'", mv, qualified(targetDatabase, targetTable), selectExpr, qualified(kafkaDatabase, "kafka_"+sourceTable+"_sink"))
	_, err = db.Exec(ddl)
	return err
}

// CreateMaterializedViewOwn 创建自带 MergeTree 存储的物化视图（可直接查询）。
func CreateMaterializedViewOwn(db *sql.DB, kafkaDatabase string, sourceDatabase string, sourceTable string, targetDatabase string) error {
	if targetDatabase == "" {
		targetDatabase = kafkaDatabase
	}
	if err := CreateDatabaseIfNotExists(db, targetDatabase); err != nil {
		return err
	}
	sourceCols, err := GetColumns(db, sourceDatabase, sourceTable)
	if err != nil {
		return err
	}
	kafkaCols, err := GetColumns(db, kafkaDatabase, "kafka_"+sourceTable+"_sink")
	if err != nil {
		return err
	}
	selectExpr, err := BuildMvSelectWithCasts(kafkaCols, sourceCols)
	if err != nil {
		return err
	}
	mv := qualified(targetDatabase, "mv_from_kafka_"+sourceTable)
	ddl := fmt.Sprintf("CREATE MATERIALIZED VIEW IF NOT EXISTS %s ENGINE = MergeTree ORDER BY tuple() AS SELECT %s FROM %s SETTINGS stream_like_engine_allow_direct_select=1, input_format_skip_unknown_fields=1, date_time_input_format='best_effort'", mv, selectExpr, qualified(kafkaDatabase, "kafka_"+sourceTable+"_sink"))
	_, err = db.Exec(ddl)
	return err
}

// CreateMaterializedViewToKafka 通过物化视图将源表的行推送到 Kafka 引擎表。
func CreateMaterializedViewToKafka(db *sql.DB, sourceDatabase string, sourceTable string, kafkaDatabase string) error {
	if kafkaDatabase == "" {
		kafkaDatabase = sourceDatabase
	}
	sourceCols, err := GetColumns(db, sourceDatabase, sourceTable)
	if err != nil {
		return err
	}
	selectExpr := BuildSelectToString(sourceCols)
	mv := qualified(kafkaDatabase, "mv_to_kafka_"+sourceTable)
	targetKafka := qualified(kafkaDatabase, "kafka_"+sourceTable+"_sink")
	src := qualified(sourceDatabase, sourceTable)
	ddl := fmt.Sprintf("CREATE MATERIALIZED VIEW IF NOT EXISTS %s TO %s AS SELECT %s FROM %s", mv, targetKafka, selectExpr, src)
	_, err = db.Exec(ddl)
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

func createKafkaTableByDDL(db *sql.DB, name string, ddlCols string, brokers []string, topic string, group string, format string, numConsumers int, maxBlockSize int, autoOffsetReset string) error {
	ddl := ""
	if strings.EqualFold(strings.TrimSpace(autoOffsetReset), "skip") {
		ddl = fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (%s) ENGINE = Kafka SETTINGS kafka_broker_list = '%s', kafka_topic_list = '%s', kafka_group_name = '%s', kafka_format = '%s', kafka_num_consumers = %d, kafka_max_block_size = %d", name, ddlCols, stringsJoin(brokers), topic, group, format, numConsumers, maxBlockSize)
	} else {
		if strings.TrimSpace(autoOffsetReset) == "" {
			autoOffsetReset = "latest"
		}
		ddl = fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (%s) ENGINE = Kafka SETTINGS kafka_broker_list = '%s', kafka_topic_list = '%s', kafka_group_name = '%s', kafka_format = '%s', kafka_num_consumers = %d, kafka_max_block_size = %d, kafka_auto_offset_reset = '%s'", name, ddlCols, stringsJoin(brokers), topic, group, format, numConsumers, maxBlockSize, autoOffsetReset)
	}
	_, err := db.Exec(ddl)
	if err != nil {
		if isUnknownKafkaAutoOffsetResetError(err) && !strings.EqualFold(strings.TrimSpace(autoOffsetReset), "skip") {
			ddl2 := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (%s) ENGINE = Kafka SETTINGS kafka_broker_list = '%s', kafka_topic_list = '%s', kafka_group_name = '%s', kafka_format = '%s', kafka_num_consumers = %d, kafka_max_block_size = %d", name, ddlCols, stringsJoin(brokers), topic, group, format, numConsumers, maxBlockSize)
			if _, e2 := db.Exec(ddl2); e2 == nil {
				return nil
			}
			return fmt.Errorf("ddl_failed: %s ; error: %v", ddl2, err)
		}
		return fmt.Errorf("ddl_failed: %s ; error: %v", ddl, err)
	}
	return nil
}

func buildStringColumnsDDL(cols []Column) string {
	var ddlCols string
	for i, c := range cols {
		if i > 0 {
			ddlCols += ","
		}
		ddlCols += fmt.Sprintf("%s String", quoteIdent(c.Name))
	}
	return ddlCols
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
	var parts string
	if strings.TrimSpace(partitionBy) != "" {
		parts = fmt.Sprintf(" PARTITION BY %s", partitionBy)
	}
	name := qualified(targetDatabase, targetTable)
	ddl := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (%s) ENGINE = MergeTree%s ORDER BY %s", name, ddlCols, parts, orderBy)
	_, err = db.Exec(ddl)
	return err
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
