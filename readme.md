# click-house-sync

ClickHouse ↔ Kafka 同步与迁移工具，提供自动创建 Kafka 引擎表、物化视图，以及按批次将 ClickHouse 表数据导出到 Kafka 的能力。命令行工具名为 `ch-sync`。

## 特性

- 一键为单表创建 Kafka Topic、Kafka 引擎表与物化视图
- 按批次导出数据到 Kafka，支持稳定排序与消息键保证分区内有序
- 支持游标范围分页（时间/数值/字符串），可持续 `watch` 导出增量
- 批量处理：从 `tables.yaml` 扫描/生成/同步多张表
- 生成库内建表 DDL，并可批量执行 SQL 文件
- 通过 `config.yaml` 管理全局参数，优先使用命令行覆盖

## 构建与打包

- 构建二进制

```bash
make build
# 输出：bin/ch-sync 和复制的 config.yaml
```

- 跨平台打包

```bash
make package-mac   # 生成 dist/ch-sync-darwin-arm64-<VERSION>.tar.gz
make package-linux # 生成 dist/ch-sync-linux-amd64-<VERSION>.tar.gz
make release       # 同时生成两个平台的包并列出文件
```

打包产物包含：按架构命名的目录（例如 `ch-sync-darwin-arm64/`、`ch-sync-linux-amd64/`），目录内含：`ch-sync` 二进制、`config.yaml`、存在时的 `tables.yaml`，以及本 `readme.md`。

> 版本号 `VERSION` 来自 `git describe`，若不可用则回退为 `0.1.0`。

## 配置文件示例（config.yaml）

```yaml
clickhouse:
  host: 127.0.0.1
  port: 9000
  user: default
  password: ""
  database: default
  secure: false

kafka:
  brokers: ["127.0.0.1:9092"]

sync:
  rows_per_partition: 1000000
  batch_size: 10000
  group_name: ch-sync
  target_database: ""
  tables_file: tables.yaml

logging:
  level: info
  format: console # console|json
  file: ""        # 留空输出到控制台
```

- 支持通过 `--config` 指定路径；未指定时将自动搜索当前目录、可执行文件目录以及 `$HOME/.ch-sync/` 下的 `config.yaml|yml|json`。

## 表清单示例（tables.yaml）

支持两种格式：

1) 包裹在 `tables:` 数组下

```yaml
tables:
  - name: users
    current_database: default
    target_database: default
    brokers: ["127.0.0.1:9092"]
    rows_per_partition: 1000000
    batch_size: 10000
    group_name: ch-sync
    export_order_by: id
    export_key_column: id
    cursor_column: created_at
    cursor_start: "2024-01-01 00:00:00"
    cursor_end: ""
```

2) 直接数组形式

```yaml
- name: orders
  current_database: default
  target_database: default
  brokers: ["127.0.0.1:9092"]
  export_order_by: id
  export_key_column: user_id
```

## 常用命令

- 一键全流程（建 Topic/表/MV 并导出）

```bash
./ch-sync auto --table users \
  --export-order-by id \
  --export-key-column id \
  --cursor-column created_at \
  --cursor-start "2024-01-01 00:00:00"
```

- 仅准备资源（不导出数据）

```bash
./ch-sync prepare --table users
```

- 分批导出到 Kafka

```bash
./ch-sync export --table users \
  --export-order-by id \
  --export-key-column id \
  --cursor-column created_at \
  --cursor-start "2024-01-01 00:00:00" \
  --watch --poll-interval 5
```

- 批量同步多表

```bash
./ch-sync sync --tables users,orders --prepare-only
# 或导出：
./ch-sync sync --tables users,orders --full-export
```

- 生成 `tables.yaml`

```bash
./ch-sync gen-tables --output tables.yaml --include-views=false
```

- 生成库内建表 DDL

```bash
./ch-sync gen-ddl --output create_tables.sql
```

- 执行 SQL 文件

```bash
./ch-sync exec-ddl --file create_tables.sql --continue-on-error
```

- 统计行数（含物化视图差异）

```bash
./ch-sync count --table users --with-mv --diff
```

- 创建目标表（与源表结构一致）

```bash
./ch-sync create-target --table users \
  --order-by "id" \
  --partition-by "toYYYYMM(created_at)"
```

## Kafka 子命令

- 列举主题名称

```bash
./ch-sync kafka topics --kafka-brokers 127.0.0.1:9092
```

- 统计主题数量

```bash
./ch-sync kafka topics-count --kafka-brokers 127.0.0.1:9092
```

- 查看单个主题信息

```bash
./ch-sync kafka topic-info --topic my_topic --kafka-brokers 127.0.0.1:9092
```

- 查看所有主题信息

```bash
./ch-sync kafka topics-info --kafka-brokers 127.0.0.1:9092
```

## 运行参数要点

- `--kafka-brokers` 支持逗号分隔多个 broker
- 未指定 `--kafka-topic` 时，默认使用 `<database>_<table>`
- `--queryable-mv` 创建可直接查询的 MergeTree 物化视图（不写入目标表）
- `--respect-table-db` 优先使用 `tables.yaml` 中的表级 `current_database`
- JSON 输出可通过 `--json-pretty` 与 `--json-color` 控制美化与高亮

## 依赖环境

- Go 1.25+（参考 go.mod）
- ClickHouse、Kafka 可用并可访问

---

如需进一步示例或帮助，可执行：

```bash
./ch-sync --help
```
