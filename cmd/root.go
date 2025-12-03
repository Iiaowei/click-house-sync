// cmd 包定义 ch-sync 的 CLI 命令与全局参数。
package cmd

import (
	"bytes"
	"click-house-sync/internal/config"
	"click-house-sync/internal/logging"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"unicode"

	"github.com/spf13/cobra"
	"go.uber.org/zap"
)

var (
	chHost                string
	chPort                int
	chUser                string
	chPassword            string
	chDatabase            string
	chSecure              bool
	kafkaBrokers          string
	kafkaTopic            string
	rowsPerPartition      int
	replicationFactor     int
	batchSize             int
	groupName             string
	targetTable           string
	targetDatabase        string
	exportOrderBy         string
	exportKeyColumn       string
	cursorColumn          string
	cursorStart           string
	cursorEnd             string
	mvOwnTable            bool
	watch                 bool
	pollInterval          int
	cursorStartFromTarget bool
	logLevel              string
	logFormat             string
	logFile               string
	logger                *zap.SugaredLogger
	configPath            string
	tablesFile            string
	jsonPretty            bool
	jsonColorMode         string
	kafkaMaxBlockSize     int
	queryableMV           bool
	respectTableDB        bool
	kafkaAutoOffsetReset  string
	queueSize             int
	writers               int
)

// rootCmd 是 ch-sync 的根命令。
var rootCmd = &cobra.Command{
	Use:   "ch-sync",
	Short: "ClickHouse Kafka 数据同步工具",
	PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
		if configPath == "" {
			auto := autoFindConfig()
			if auto != "" {
				configPath = auto
			}
		}
		if configPath != "" {
			conf, err := config.Load(configPath)
			if err != nil {
				return err
			}
			applyConfig(cmd, conf)
		}
		if logger == nil {
			lg, cleanup, err := logging.New(logLevel, logFormat, logFile)
			if err != nil {
				return err
			}
			logger = lg
			_ = cleanup
		}
		cmd.SilenceUsage = true
		cmd.SilenceErrors = true
		return nil
	},
}

// Execute 执行根命令并输出结构化错误信息。
func Execute() {
	if err := rootCmd.Execute(); err != nil {
		printErrJSON(map[string]any{"error": err.Error()})
		os.Exit(1)
	}
}

func init() {
	rootCmd.PersistentFlags().StringVar(&chHost, "ch-host", "127.0.0.1", "ClickHouse 主机名或地址")
	rootCmd.PersistentFlags().IntVar(&chPort, "ch-port", 9000, "ClickHouse 端口号")
	rootCmd.PersistentFlags().StringVar(&chUser, "ch-user", "default", "ClickHouse 用户名")
	rootCmd.PersistentFlags().StringVar(&chPassword, "ch-password", "", "ClickHouse 密码")
	rootCmd.PersistentFlags().StringVar(&chDatabase, "ch-database", "default", "默认连接的数据库名")
	rootCmd.PersistentFlags().BoolVar(&chSecure, "ch-secure", false, "启用 TLS 连接 ClickHouse")
	rootCmd.PersistentFlags().StringVar(&kafkaBrokers, "kafka-brokers", "127.0.0.1:9092", "Kafka broker 列表，逗号分隔")
	rootCmd.PersistentFlags().StringVar(&kafkaTopic, "kafka-topic", "", "Kafka topic 名称（为空按 <db>_<table> 生成）")
	rootCmd.PersistentFlags().IntVar(&rowsPerPartition, "rows-per-partition", 1000000, "每分区行数（用于估算分区数）")
	rootCmd.PersistentFlags().IntVar(&replicationFactor, "replication-factor", 1, "Kafka 副本因子")
	rootCmd.PersistentFlags().IntVar(&batchSize, "batch-size", 10000, "导出与写入的批量大小")
	rootCmd.PersistentFlags().StringVar(&groupName, "group-name", "ch-sync", "Kafka 消费者组名（默认前缀）")
	rootCmd.PersistentFlags().StringVar(&targetTable, "target-table", "", "目标表名（默认源表名后缀 _replica）")
	rootCmd.PersistentFlags().StringVar(&targetDatabase, "target-database", "", "目标数据库名（默认与源一致）")
	rootCmd.PersistentFlags().StringVar(&exportOrderBy, "export-order-by", "", "导出查询的 ORDER BY 表达式，用于稳定批次读取顺序")
	rootCmd.PersistentFlags().StringVar(&exportKeyColumn, "export-key-column", "", "Kafka 消息键列名，用于哈希分区与分区内顺序")
	rootCmd.PersistentFlags().StringVar(&cursorColumn, "cursor-column", "", "游标列名（数值/时间/字符串），用于范围分页")
	rootCmd.PersistentFlags().StringVar(&cursorStart, "cursor-start", "", "游标起始值（包含），时间建议 'YYYY-MM-DD HH:MM:SS' 格式")
	rootCmd.PersistentFlags().StringVar(&cursorEnd, "cursor-end", "", "游标结束值（包含，可选）")
	rootCmd.PersistentFlags().BoolVar(&mvOwnTable, "mv-own-table", true, "物化视图自带存储（ENGINE=MergeTree），不写入目标表")
	rootCmd.PersistentFlags().BoolVar(&watch, "watch", false, "持续导出（轮询新增数据）")
	rootCmd.PersistentFlags().IntVar(&pollInterval, "poll-interval", 5, "watch 模式下轮询间隔秒数")
	rootCmd.PersistentFlags().BoolVar(&cursorStartFromTarget, "cursor-start-from-target", false, "自动从目标表/视图的最大游标继续导出")
	rootCmd.PersistentFlags().StringVar(&logLevel, "log-level", "info", "日志级别 debug|info|warn|error")
	rootCmd.PersistentFlags().StringVar(&logFormat, "log-format", "console", "日志格式 console|json")
	rootCmd.PersistentFlags().StringVar(&logFile, "log-file", "", "日志文件路径 留空输出到控制台")
	rootCmd.PersistentFlags().StringVar(&configPath, "config", "", "配置文件路径 支持 yaml|json|toml")
	rootCmd.PersistentFlags().StringVar(&tablesFile, "tables-file", "", "外部表清单文件路径 用于批量同步")
	rootCmd.PersistentFlags().BoolVar(&jsonPretty, "json-pretty", true, "格式化JSON输出")
	rootCmd.PersistentFlags().StringVar(&jsonColorMode, "json-color", "auto", "JSON高亮模式 auto|always|never")
	rootCmd.PersistentFlags().IntVar(&kafkaMaxBlockSize, "kafka-max-block-size", 1048576, "Kafka 引擎表的 kafka_max_block_size 设置")
	rootCmd.PersistentFlags().BoolVar(&queryableMV, "queryable-mv", false, "创建可查询的物化视图（ENGINE=MergeTree），替代推送型 MV")
	rootCmd.PersistentFlags().BoolVar(&respectTableDB, "respect-table-db", false, "优先使用 tables.yaml 中每表的 current_database 作为源库（忽略 --ch-database）")
	rootCmd.PersistentFlags().StringVar(&kafkaAutoOffsetReset, "kafka-auto-offset-reset", "latest", "Kafka 引擎表的 kafka_auto_offset_reset 设置（earliest|latest|skip）")
	rootCmd.PersistentFlags().IntVar(&queueSize, "queue-size", 10, "导出读写队列容量（默认 10，队列满则读端等待）")
	rootCmd.PersistentFlags().IntVar(&writers, "writers", 1, "并行写端 goroutine 数（默认 1）")
}

func brokersList() []string {
	s := strings.TrimSpace(kafkaBrokers)
	if s == "" {
		return []string{}
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

func applyConfig(cmd *cobra.Command, conf *config.Config) {
	if conf == nil {
		return
	}
	if !cmd.Flags().Changed("log-level") && conf.Logging.Level != "" {
		logLevel = conf.Logging.Level
	}
	if !cmd.Flags().Changed("log-format") && conf.Logging.Format != "" {
		logFormat = conf.Logging.Format
	}
	if !cmd.Flags().Changed("log-file") && conf.Logging.File != "" {
		logFile = conf.Logging.File
	}

	if !cmd.Flags().Changed("ch-host") && conf.ClickHouse.Host != "" {
		chHost = conf.ClickHouse.Host
	}
	if !cmd.Flags().Changed("ch-port") && conf.ClickHouse.Port > 0 {
		chPort = conf.ClickHouse.Port
	}
	if !cmd.Flags().Changed("ch-user") && conf.ClickHouse.User != "" {
		chUser = conf.ClickHouse.User
	}
	if !cmd.Flags().Changed("ch-password") && conf.ClickHouse.Password != "" {
		chPassword = conf.ClickHouse.Password
	}
	if !cmd.Flags().Changed("ch-database") && conf.ClickHouse.Database != "" {
		chDatabase = conf.ClickHouse.Database
	}
	if !cmd.Flags().Changed("ch-secure") {
		chSecure = conf.ClickHouse.Secure
	}

	brokersJoined := config.JoinBrokers(conf.Kafka.Brokers)
	if !cmd.Flags().Changed("kafka-brokers") && brokersJoined != "" {
		kafkaBrokers = brokersJoined
	}

	if !cmd.Flags().Changed("rows-per-partition") && conf.Sync.RowsPerPartition > 0 {
		rowsPerPartition = conf.Sync.RowsPerPartition
	}
	if !cmd.Flags().Changed("batch-size") && conf.Sync.BatchSize > 0 {
		batchSize = conf.Sync.BatchSize
	}
	if !cmd.Flags().Changed("queue-size") && conf.Sync.QueueSize > 0 {
		queueSize = conf.Sync.QueueSize
	}
	if !cmd.Flags().Changed("writers") && conf.Sync.Writers > 0 {
		writers = conf.Sync.Writers
	}
	if !cmd.Flags().Changed("group-name") && conf.Sync.GroupName != "" {
		groupName = conf.Sync.GroupName
	}
	if !cmd.Flags().Changed("target-database") && conf.Sync.TargetDatabase != "" {
		targetDatabase = conf.Sync.TargetDatabase
	}
	if !cmd.Flags().Changed("tables-file") && conf.Sync.TablesFile != "" {
		tablesFile = conf.Sync.TablesFile
	}
}

func autoFindConfig() string {
	cwd, _ := os.Getwd()
	cand := []string{
		filepath.Join(cwd, "config.yaml"),
		filepath.Join(cwd, "config.yml"),
		filepath.Join(cwd, "config.json"),
	}
	exe, _ := os.Executable()
	exedir := filepath.Dir(exe)
	cand = append(cand,
		filepath.Join(exedir, "config.yaml"),
		filepath.Join(exedir, "config.yml"),
		filepath.Join(exedir, "config.json"),
	)
	home, _ := os.UserHomeDir()
	cand = append(cand,
		filepath.Join(home, ".ch-sync", "config.yaml"),
		filepath.Join(home, ".ch-sync", "config.yml"),
		filepath.Join(home, ".ch-sync", "config.json"),
	)
	for _, p := range cand {
		if _, err := os.Stat(p); err == nil {
			return p
		}
	}
	return ""
}

func printJSON(v any) {
	var b []byte
	if jsonPretty {
		b, _ = json.MarshalIndent(v, "", "  ")
	} else {
		b, _ = json.Marshal(v)
	}
	if shouldColorStdout() {
		s := colorizeJSONString(string(b))
		os.Stdout.WriteString(s)
		os.Stdout.WriteString("\n")
		return
	}
	os.Stdout.Write(b)
	os.Stdout.Write([]byte("\n"))
}

func printErrJSON(v any) {
	var b []byte
	if jsonPretty {
		b, _ = json.MarshalIndent(v, "", "  ")
	} else {
		b, _ = json.Marshal(v)
	}
	if shouldColorStderr() {
		s := colorizeJSONString(string(b))
		os.Stderr.WriteString(s)
		os.Stderr.WriteString("\n")
		return
	}
	os.Stderr.Write(b)
	os.Stderr.Write([]byte("\n"))
}

func printRedJSON(v any) {
	var b []byte
	if jsonPretty {
		b, _ = json.MarshalIndent(v, "", "  ")
	} else {
		b, _ = json.Marshal(v)
	}
	if shouldColorStdout() {
		s := colorizeJSONString(string(b))
		os.Stdout.WriteString("\x1b[31m")
		os.Stdout.WriteString(s)
		os.Stdout.WriteString("\x1b[0m\n")
		return
	}
	os.Stdout.Write(b)
	os.Stdout.Write([]byte("\n"))
}

func shouldColorStdout() bool {
	switch jsonColorMode {
	case "always":
		return true
	case "never":
		return false
	default:
		fi, err := os.Stdout.Stat()
		if err != nil {
			return false
		}
		return (fi.Mode() & os.ModeCharDevice) != 0
	}
}

func shouldColorStderr() bool {
	switch jsonColorMode {
	case "always":
		return true
	case "never":
		return false
	default:
		fi, err := os.Stderr.Stat()
		if err != nil {
			return false
		}
		return (fi.Mode() & os.ModeCharDevice) != 0
	}
}

func colorizeJSONString(s string) string {
	var out bytes.Buffer
	inString := false
	escaped := false
	var sb bytes.Buffer
	i := 0
	for i < len(s) {
		ch := s[i]
		if inString {
			sb.WriteByte(ch)
			if escaped {
				escaped = false
				i++
				continue
			}
			if ch == '\\' {
				escaped = true
				i++
				continue
			}
			if ch == '"' {
				inString = false
				j := i + 1
				for j < len(s) && (s[j] == ' ' || s[j] == '\t') {
					j++
				}
				isKey := j < len(s) && s[j] == ':'
				str := sb.String()
				sb.Reset()
				if isKey {
					out.WriteString("\x1b[33m")
					out.WriteString(str)
					out.WriteString("\x1b[0m")
				} else {
					out.WriteString("\x1b[32m")
					out.WriteString(str)
					out.WriteString("\x1b[0m")
				}
			}
			i++
			continue
		} else {
			if ch == '"' {
				inString = true
				sb.WriteByte(ch)
				i++
				continue
			}
			if (ch >= '0' && ch <= '9') || ch == '-' {
				j := i
				for j < len(s) {
					c := s[j]
					if (c >= '0' && c <= '9') || c == '-' || c == '+' || c == '.' || c == 'e' || c == 'E' {
						j++
					} else {
						break
					}
				}
				out.WriteString("\x1b[36m")
				out.WriteString(s[i:j])
				out.WriteString("\x1b[0m")
				i = j
				continue
			}
			if unicode.IsLetter(rune(ch)) {
				j := i
				for j < len(s) && unicode.IsLetter(rune(s[j])) {
					j++
				}
				ident := s[i:j]
				switch ident {
				case "true", "false":
					out.WriteString("\x1b[35m")
					out.WriteString(ident)
					out.WriteString("\x1b[0m")
				case "null":
					out.WriteString("\x1b[90m")
					out.WriteString(ident)
					out.WriteString("\x1b[0m")
				default:
					out.WriteString(ident)
				}
				i = j
				continue
			}
			out.WriteByte(ch)
			i++
		}
	}
	return out.String()
}

func printJSONWithRedKeys(v any, keys []string) {
	var b []byte
	if jsonPretty {
		b, _ = json.MarshalIndent(v, "", "  ")
	} else {
		b, _ = json.Marshal(v)
	}
	if shouldColorStdout() {
		red := map[string]struct{}{}
		for _, k := range keys {
			k = strings.TrimSpace(k)
			if k != "" {
				red[k] = struct{}{}
			}
		}
		s := colorizeJSONStringWithRedKeys(string(b), red)
		os.Stdout.WriteString(s)
		os.Stdout.WriteString("\n")
		return
	}
	os.Stdout.Write(b)
	os.Stdout.Write([]byte("\n"))
}

func colorizeJSONStringWithRedKeys(s string, red map[string]struct{}) string {
	var out bytes.Buffer
	inString := false
	escaped := false
	var sb bytes.Buffer
	i := 0
	pendingKey := ""
	for i < len(s) {
		ch := s[i]
		if inString {
			sb.WriteByte(ch)
			if escaped {
				escaped = false
				i++
				continue
			}
			if ch == '\\' {
				escaped = true
				i++
				continue
			}
			if ch == '"' {
				inString = false
				j := i + 1
				for j < len(s) && (s[j] == ' ' || s[j] == '\t') {
					j++
				}
				isKey := j < len(s) && s[j] == ':'
				str := sb.String()
				sb.Reset()
				if isKey {
					out.WriteString("\x1b[33m")
					out.WriteString(str)
					out.WriteString("\x1b[0m")
					k := strings.Trim(str, "\"")
					pendingKey = k
				} else {
					if pendingKey != "" {
						if _, ok := red[pendingKey]; ok {
							out.WriteString("\x1b[31m")
							out.WriteString(str)
							out.WriteString("\x1b[0m")
						} else {
							out.WriteString("\x1b[32m")
							out.WriteString(str)
							out.WriteString("\x1b[0m")
						}
						pendingKey = ""
					} else {
						out.WriteString("\x1b[32m")
						out.WriteString(str)
						out.WriteString("\x1b[0m")
					}
				}
			}
			i++
			continue
		} else {
			if ch == '"' {
				inString = true
				sb.WriteByte(ch)
				i++
				continue
			}
			if (ch >= '0' && ch <= '9') || ch == '-' {
				j := i
				for j < len(s) {
					c := s[j]
					if (c >= '0' && c <= '9') || c == '-' || c == '+' || c == '.' || c == 'e' || c == 'E' {
						j++
					} else {
						break
					}
				}
				if pendingKey != "" {
					if _, ok := red[pendingKey]; ok {
						out.WriteString("\x1b[31m")
						out.WriteString(s[i:j])
						out.WriteString("\x1b[0m")
					} else {
						out.WriteString("\x1b[36m")
						out.WriteString(s[i:j])
						out.WriteString("\x1b[0m")
					}
					pendingKey = ""
				} else {
					out.WriteString("\x1b[36m")
					out.WriteString(s[i:j])
					out.WriteString("\x1b[0m")
				}
				i = j
				continue
			}
			if unicode.IsLetter(rune(ch)) {
				j := i
				for j < len(s) && unicode.IsLetter(rune(s[j])) {
					j++
				}
				ident := s[i:j]
				switch ident {
				case "true", "false":
					out.WriteString("\x1b[35m")
					out.WriteString(ident)
					out.WriteString("\x1b[0m")
				case "null":
					out.WriteString("\x1b[90m")
					out.WriteString(ident)
					out.WriteString("\x1b[0m")
				default:
					out.WriteString(ident)
				}
				i = j
				continue
			}
			out.WriteByte(ch)
			i++
		}
	}
	return out.String()
}

func lookupTableConfig(name string) (*config.Table, error) {
	var candidates []string
	if tablesFile != "" {
		candidates = append(candidates, tablesFile)
	}
	if cwd, err := os.Getwd(); err == nil {
		candidates = append(candidates, filepath.Join(cwd, "tables.yaml"))
	}
	if exe, err := os.Executable(); err == nil {
		exedir := filepath.Dir(exe)
		candidates = append(candidates, filepath.Join(exedir, "tables.yaml"))
	}
	if home, err := os.UserHomeDir(); err == nil {
		candidates = append(candidates, filepath.Join(home, ".ch-sync", "tables.yaml"))
	}
	for _, f := range candidates {
		if _, err := os.Stat(f); err == nil {
			list, err := config.LoadTablesFile(f)
			if err != nil {
				return nil, err
			}
			for _, t := range list {
				if t.Name == name {
					tt := t
					return &tt, nil
				}
			}
		}
	}
	return nil, nil
}
