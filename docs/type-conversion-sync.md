# ClickHouse 集群间同步类型转换实施说明

## 目标

在源/目标集群字段类型不一致时，提供稳定同步方案：

1. 中间层 Kafka 表统一使用 `String`；
2. 物化视图写入目标表时做严格类型转换；
3. 提供可执行 up/down DDL；
4. 提供数据质量巡检 SQL。

## 设计

### 1) 类型差异识别

通过 `system.columns` 分别读取源表与目标表字段定义，按列名输出：

- `type_mismatch`
- `missing_in_source`
- `missing_in_target`

`prepare/auto/sync` 会在输出 JSON 中包含 `type_diffs`。

### 2) 中间层 String 化

`kafka_<table>_sink` 统一定义为：

```sql
`col_a` String, `col_b` String, ...
```

目的：隔离上游类型变化，避免 Kafka 表解析阶段直接失败。

### 3) 严格转换落目标表

`mv_from_kafka_<table>` 使用逐列表达式：

- 日期时间：`CAST(parseDateTimeBestEffort(col), 'TargetType')`
- 其他类型：`CAST(col, 'TargetType')`

策略为严格失败，不使用 `OrNull` 容错函数。

## DDL 交付

建议使用：

```bash
./ch-sync gen-ddl --with-sync-cast --include-rollback --include-quality-sql --output sync_cast_rebuild.sql
```

输出内容包含：

- up DDL：重建 String Kafka 表 + 目标表 + 转换 MV
- down DDL：恢复“源类型 Kafka 表 + SELECT * MV”
- 巡检 SQL：行数、空值率、范围统计

## 数据质量监控

建议至少执行以下检查：

1. 源/目标行数对比；
2. 关键字段空值率；
3. 数值/时间字段 min/max 范围；
4. 主键重复率（如有主键字段）。

可通过：

```bash
./ch-sync count --table <table> --quality-sql
```

获取巡检 SQL 清单。

## 性能影响评估

转换链路新增 CAST 计算，主要影响：

- MV 消费 CPU 增加；
- 高基数字段转换会提高单批延迟；
- 时间解析字段（`parseDateTimeBestEffort`）成本高于原生数值转换。

建议：

1. 先小流量表灰度；
2. 调整 `--kafka-max-block-size`、`--batch-size`；
3. 在大表场景提高 `--writers`，同时控制 `--queue-size`；
4. 先执行质量巡检，减少脏数据导致的反复失败。

## 最佳实践

1. 目标表先确定最终业务类型，再启用同步；
2. 所有变更先生成并评审 SQL，再执行；
3. 保留 up/down SQL 文件并做演练；
4. 对关键表设置固定巡检阈值（空值率、行数偏差）。
