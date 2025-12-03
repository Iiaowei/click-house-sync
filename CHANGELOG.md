# Changelog

## 2025-12-03

- 新增：`gen-tables` 支持 `--tables` 指定表名，按逗号分隔生成 `tables.yaml`
- 新增：`gen-ddl` 支持 `--tables` 指定表名，仅生成所选表的建表 DDL
- 输出增强：同步批次事件 `batch_exported` 增加四个字段 `tables_total`、`table_index`、`table_rows_total`、`kafka_messages`
- 稳定性修复：行数统计改为仅统计 `system.parts` 中 `active = 1` 的分片，避免合并过程导致的重复计数
- 帮助信息更新：`gen-tables`/`gen-ddl` 的 `--help` 中展示新参数说明

