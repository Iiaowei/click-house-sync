# Changelog

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
