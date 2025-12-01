// config 包定义配置结构以及加载方法。
package config

import (
	"os"
	"strings"

	"github.com/spf13/viper"
	"gopkg.in/yaml.v3"
)

// ClickHouse 保存 ClickHouse 连接配置。
type ClickHouse struct {
	Host     string `mapstructure:"host"`
	Port     int    `mapstructure:"port"`
	User     string `mapstructure:"user"`
	Password string `mapstructure:"password"`
	Database string `mapstructure:"database"`
	Secure   bool   `mapstructure:"secure"`
}

// Kafka 保存 Kafka broker 地址。
type Kafka struct {
	Brokers []string `mapstructure:"brokers"`
}

// Sync 保存全局同步默认值与相关路径。
type Sync struct {
	RowsPerPartition int    `mapstructure:"rows_per_partition"`
	BatchSize        int    `mapstructure:"batch_size"`
	GroupName        string `mapstructure:"group_name"`
	TargetDatabase   string `mapstructure:"target_database"`
	TablesFile       string `mapstructure:"tables_file"`
}

// Table 描述单表级的覆盖配置（brokers、批量、游标等）。
type Table struct {
	Name             string   `mapstructure:"name" yaml:"name" json:"name"`
	Brokers          []string `mapstructure:"brokers" yaml:"brokers" json:"brokers"`
	RowsPerPartition int      `mapstructure:"rows_per_partition" yaml:"rows_per_partition" json:"rows_per_partition"`
	BatchSize        int      `mapstructure:"batch_size" yaml:"batch_size" json:"batch_size"`
	GroupName        string   `mapstructure:"group_name" yaml:"group_name" json:"group_name"`
	TargetTable      string   `mapstructure:"target_table" yaml:"target_table" json:"target_table"`
	TargetDatabase   string   `mapstructure:"target_database" yaml:"target_database" json:"target_database"`
	CurrentDatabase  string   `mapstructure:"current_database" yaml:"current_database" json:"current_database"`
	ExportOrderBy    string   `mapstructure:"export_order_by" yaml:"export_order_by" json:"export_order_by"`
	ExportKeyColumn  string   `mapstructure:"export_key_column" yaml:"export_key_column" json:"export_key_column"`
	CursorColumn     string   `mapstructure:"cursor_column" yaml:"cursor_column" json:"cursor_column"`
	CursorStart      string   `mapstructure:"cursor_start" yaml:"cursor_start" json:"cursor_start"`
	CursorEnd        string   `mapstructure:"cursor_end" yaml:"cursor_end" json:"cursor_end"`
}

// Logging 控制日志级别/格式以及可选的文件输出。
type Logging struct {
	Level  string `mapstructure:"level"`
	Format string `mapstructure:"format"`
	File   string `mapstructure:"file"`
}

// Config 是通过 viper 从 yaml/json/toml 加载的根配置。
type Config struct {
	ClickHouse ClickHouse `mapstructure:"clickhouse"`
	Kafka      Kafka      `mapstructure:"kafka"`
	Sync       Sync       `mapstructure:"sync"`
	Logging    Logging    `mapstructure:"logging"`
}

// LoadTablesFile 加载 tables.yaml；既支持数组形式也支持 {tables: []} 包裹形式。
func LoadTablesFile(path string) ([]Table, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var wrapper struct {
		Tables []Table `yaml:"tables" json:"tables"`
	}
	if err := yaml.Unmarshal(b, &wrapper); err == nil && len(wrapper.Tables) > 0 {
		return wrapper.Tables, nil
	}
	var arr []Table
	if err := yaml.Unmarshal(b, &arr); err == nil {
		return arr, nil
	}
	return nil, nil
}

// Load 读取配置文件并反序列化到 Config。
func Load(path string) (*Config, error) {
	vp := viper.New()
	vp.SetConfigFile(path)
	if err := vp.ReadInConfig(); err != nil {
		return nil, err
	}
	var c Config
	if err := vp.Unmarshal(&c); err != nil {
		return nil, err
	}
	return &c, nil
}

// JoinBrokers 去除空格后用逗号连接 broker 列表。
func JoinBrokers(brokers []string) string {
	var out []string
	for _, b := range brokers {
		t := strings.TrimSpace(b)
		if t != "" {
			out = append(out, t)
		}
	}
	return strings.Join(out, ",")
}
