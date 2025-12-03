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
	var ddlCols string
	for i, c := range cols {
		if i > 0 {
			ddlCols += ","
		}
		ddlCols += fmt.Sprintf("%s %s", quoteIdent(c.Name), c.Type)
	}
	name := qualified(database, "kafka_"+table+"_sink")
	ddl := ""
	if strings.EqualFold(strings.TrimSpace(autoOffsetReset), "skip") {
		ddl = fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (%s) ENGINE = Kafka SETTINGS kafka_broker_list = '%s', kafka_topic_list = '%s', kafka_group_name = '%s', kafka_format = '%s', kafka_num_consumers = %d, kafka_max_block_size = %d", name, ddlCols, stringsJoin(brokers), topic, group, format, numConsumers, maxBlockSize)
	} else {
		if strings.TrimSpace(autoOffsetReset) == "" {
			autoOffsetReset = "latest"
		}
		ddl = fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (%s) ENGINE = Kafka SETTINGS kafka_broker_list = '%s', kafka_topic_list = '%s', kafka_group_name = '%s', kafka_format = '%s', kafka_num_consumers = %d, kafka_max_block_size = %d, kafka_auto_offset_reset = '%s'", name, ddlCols, stringsJoin(brokers), topic, group, format, numConsumers, maxBlockSize, autoOffsetReset)
	}
	_, err = db.Exec(ddl)
	if err != nil {
		s := err.Error()
		if strings.Contains(s, "Unknown setting 'kafka_auto_offset_reset'") && !strings.EqualFold(strings.TrimSpace(autoOffsetReset), "skip") {
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

// CreateKafkaTableFromSource 在 kafkaDatabase 中按 sourceDatabase.table 的结构创建 Kafka 引擎表。
func CreateKafkaTableFromSource(db *sql.DB, sourceDatabase string, table string, kafkaDatabase string, brokers []string, topic string, group string, format string, numConsumers int, maxBlockSize int, autoOffsetReset string) error {
	cols, err := GetColumns(db, sourceDatabase, table)
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
	name := qualified(kafkaDatabase, "kafka_"+table+"_sink")
	ddl := ""
	if strings.EqualFold(strings.TrimSpace(autoOffsetReset), "skip") {
		ddl = fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (%s) ENGINE = Kafka SETTINGS kafka_broker_list = '%s', kafka_topic_list = '%s', kafka_group_name = '%s', kafka_format = '%s', kafka_num_consumers = %d, kafka_max_block_size = %d", name, ddlCols, stringsJoin(brokers), topic, group, format, numConsumers, maxBlockSize)
	} else {
		if strings.TrimSpace(autoOffsetReset) == "" {
			autoOffsetReset = "latest"
		}
		ddl = fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (%s) ENGINE = Kafka SETTINGS kafka_broker_list = '%s', kafka_topic_list = '%s', kafka_group_name = '%s', kafka_format = '%s', kafka_num_consumers = %d, kafka_max_block_size = %d, kafka_auto_offset_reset = '%s'", name, ddlCols, stringsJoin(brokers), topic, group, format, numConsumers, maxBlockSize, autoOffsetReset)
	}
	_, err = db.Exec(ddl)
	if err != nil {
		s := err.Error()
		if strings.Contains(s, "Unknown setting 'kafka_auto_offset_reset'") && !strings.EqualFold(strings.TrimSpace(autoOffsetReset), "skip") {
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

// CreateMaterializedView 通过物化视图将 Kafka 表写入目标 MergeTree 表。
func CreateMaterializedView(db *sql.DB, kafkaDatabase string, sourceTable string, targetDatabase string, targetTable string) error {
	if targetDatabase == "" {
		targetDatabase = kafkaDatabase
	}
	mv := qualified(targetDatabase, "mv_from_kafka_"+sourceTable)
	// Materialized view consuming Kafka and writing into target table
	ddl := fmt.Sprintf("CREATE MATERIALIZED VIEW IF NOT EXISTS %s TO %s AS SELECT * FROM %s SETTINGS stream_like_engine_allow_direct_select=1, input_format_skip_unknown_fields=1, input_format_allow_errors_num=1000000, input_format_allow_errors_ratio=0.1, date_time_input_format='best_effort'", mv, qualified(targetDatabase, targetTable), qualified(kafkaDatabase, "kafka_"+sourceTable+"_sink"))
	_, err := db.Exec(ddl)
	return err
}

// CreateMaterializedViewOwn 创建自带 MergeTree 存储的物化视图（可直接查询）。
func CreateMaterializedViewOwn(db *sql.DB, kafkaDatabase string, sourceTable string, targetDatabase string) error {
	if targetDatabase == "" {
		targetDatabase = kafkaDatabase
	}
	if err := CreateDatabaseIfNotExists(db, targetDatabase); err != nil {
		return err
	}
	mv := qualified(targetDatabase, "mv_from_kafka_"+sourceTable)
	ddl := fmt.Sprintf("CREATE MATERIALIZED VIEW IF NOT EXISTS %s ENGINE = MergeTree ORDER BY tuple() AS SELECT * FROM %s SETTINGS stream_like_engine_allow_direct_select=1, input_format_skip_unknown_fields=1, input_format_allow_errors_num=1000000, input_format_allow_errors_ratio=0.1, date_time_input_format='best_effort'", mv, qualified(kafkaDatabase, "kafka_"+sourceTable+"_sink"))
	_, err := db.Exec(ddl)
	return err
}

// CreateMaterializedViewToKafka 通过物化视图将源表的行推送到 Kafka 引擎表。
func CreateMaterializedViewToKafka(db *sql.DB, sourceDatabase string, sourceTable string, kafkaDatabase string) error {
	if kafkaDatabase == "" {
		kafkaDatabase = sourceDatabase
	}
	mv := qualified(kafkaDatabase, "mv_to_kafka_"+sourceTable)
	targetKafka := qualified(kafkaDatabase, "kafka_"+sourceTable+"_sink")
	src := qualified(sourceDatabase, sourceTable)
	ddl := fmt.Sprintf("CREATE MATERIALIZED VIEW IF NOT EXISTS %s TO %s AS SELECT * FROM %s", mv, targetKafka, src)
	_, err := db.Exec(ddl)
	if err != nil {
		return fmt.Errorf("ddl_failed: %s ; error: %v", ddl, err)
	}
	return nil
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
