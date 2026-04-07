# Changelog

## 2025-12-11

- 新增：跨平台打包支持 Windows/macOS/Linux；新增 `build-windows`/`package-windows` 目标，`release` 同步生成三平台产物。
- 产物格式：Windows 输出 `zip`（含 `ch-sync.exe`、`config.yaml`、可选 `tables.yaml`、`README.md`）；macOS/Linux 输出 `tar.gz`。
- 默认优化：查询型 MV 的分区策略改进——优先按时间月分区（自动探测时间列）；当未找到时间列时默认 `PARTITION BY tuple()`，不再回退源表分区键，避免高基数分区导致插入失败。
- 默认优化：查询型 MV 的 SELECT 追加 `max_partitions_per_insert_block=10000`，降低“单块分区过多”报错概率。
- 新增：提供参数 `--max-partitions-per-insert-block`（配置文件键 `sync.max_partitions_per_insert_block`），用于配置查询型物化视图的单块分区上限（默认 1000）。
- 更新：`docker-compose` 流水线示例补充同步性能与稳定性参数：`--writers 4`、`--queue-size 200`、`--kafka-auto-offset-reset skip`、`--kafka-max-block-size 700`、`--max-partitions-per-insert-block 10000`，并在 prepare 阶段增加 `--recreate-topic`。

## 2025-12-10

- 新增：可查询物化视图支持引擎与结构参数：`mv-engine`（merge|replacing|collapsing|versioned_collapsing）、`mv-order-by`、`mv-partition-by`、`version-column`、`sign-column`、`version-time-column`、`mv-ttl-days`、`mv-ttl-column`；可通过 CLI 或 `tables.yaml`/配置文件设置。
- 新增：Kafka sink 建表支持追加必要列（如 `version`/`sign`），并在 `export` 过程中自动推导删除标记与版本号（从 `version_time_column` 或常见时间列推导）。
- 新增：`gen-tables` 支持 `--version-time-column`，并在生成的 `tables.yaml` 中新增 `version_time_column` 字段。
- 新增：写入 Kafka 时启用 `kafka_skip_broken_messages=1000` 容错，兼容不支持该设置的 ClickHouse 版本自动降级。
- 新增：源表→Kafka 的物化视图 `mv_to_kafka_*` 对 `Int128/Int256/UInt128/UInt256` 等类型做安全映射与显式转换，避免类型不兼容。
- 新增：目标表与可查询 MV 的 DDL 增强，增加 `allow_nullable_key=1` 与输入格式容错设置（`input_format_*`、`date_time_input_format='best_effort'`、`max_partitions_per_insert_block=1000`），并针对不支持的版本自动降级；支持 TTL（按指定时间列与天数）。
- 修复：可查询 MV 的 TTL 表达式移除 `now()`，改为在列可空时使用 `WHERE <col> IS NOT NULL`，避免 ClickHouse 报错“TTL expression cannot contain non-deterministic functions”。
- 修复：Kafka 写入 JSON 时对 `NaN`/`±Inf` 浮点值进行清洗（转为 `0`），避免 `json: unsupported value: +Inf` 导致写入失败。
- 默认优化：查询型物化视图未指定分区时，优先使用按月时间分区（`toYYYYMM(<时间列>)`），自动探测 `create_date`/`updated_at`/`update_ts`/`time`/`event_time`/`insert_ts`/`created_at`，无时间列再回落到源表分区键，避免高基数分区导致写入块超限。
- 行为变更：`kafka_max_block_size` 默认值由 `1048576` 调整为 `700`。
- 行为变更：`ORDER BY` 表达式规范化，自动包裹逗号分隔的列并过滤未知列，生成有效的排序表达式。
- 行为变更：在 `prepare`/`sync`/`auto` 过程中，当未显式指定游标列时，会优先使用 `version_time_column` 作为游标与默认排序列。
- 移除：`respect-table-db` 选项；默认行为为当未通过 `--ch-database` 指定时使用 `tables.yaml` 的 `current_database`。
- 移除：`gen-tables` 的 `--topic-prefix` 与 `--topic-suffix` 参数。
- 清理：移除未使用的第三方依赖，简化构建体积。

## 2025-12-04

- 新增：游标导出持久化。批次成功写入后，将末尾游标持久化到 `tables.yaml` 对应表的 `cursor_start`，支持 `export`/`sync`/`auto`（需提供 `--tables-file`）。
- 新增：程序退出或异常（SIGINT/SIGTERM）时，自动持久化当前游标，确保下次可续传。
- 行为约束：仅在启用游标（设置 `cursor_column`）且存在 `tables.yaml` 时生效；当未找到该表配置项时不新建，避免意外写入。
- 事件输出：新增 `cursor_updated` 与 `update_cursor_failed` 事件，便于审计与排查。
- 格式化：时间类型游标以 `YYYY-MM-DD HH:MM:SS` 字符串写入。

## 2025-12-03

- 新增：`gen-tables` 支持 `--tables` 指定表名，按逗号分隔生成 `tables.yaml`
- 新增：`gen-ddl` 支持 `--tables` 指定表名，仅生成所选表的建表 DDL
- 输出增强：同步批次事件 `batch_exported` 增加四个字段 `tables_total`、`table_index`、`table_rows_total`、`kafka_messages`
- 稳定性修复：行数统计改为仅统计 `system.parts` 中 `active = 1` 的分片，避免合并过程导致的重复计数
- 帮助信息更新：`gen-tables`/`gen-ddl` 的 `--help` 中展示新参数说明
