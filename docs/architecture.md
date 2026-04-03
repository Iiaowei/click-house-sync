# 架构说明

## 目标

`ch-sync` 用于在 ClickHouse 集群间通过 Kafka 做数据同步，兼顾：

- 历史回补
- 实时增量
- 类型差异处理
- 可审计与可回滚

## 核心对象

- 业务表：`demo.<table>`
- Kafka 引擎表：`demo_stream.kafka_<table>_sink`
- 源侧物化视图：`demo_stream.mv_to_kafka_<table>`
- 目标侧物化视图：`demo_stream.mv_from_kafka_<table>`
- 目标落库表：`demo.<table>`

## 数据流

1. 源业务表写入 `demo.<table>`
2. `mv_to_kafka_<table>` 将新增行写入 Kafka topic
3. 目标侧 `kafka_<table>_sink` 读取 Kafka topic
4. `mv_from_kafka_<table>` 将数据写入目标业务表 `demo.<table>`

## 同步阶段

### 阶段 A：准备对象

- 创建 Topic
- 创建 Kafka 引擎表
- 创建 `mv_from_kafka_*` / `mv_to_kafka_*`

### 阶段 B：历史回补

- 通过 `sync --full-export` 将历史数据写入 Kafka
- 目标侧持续消费并落库

### 阶段 C：持续增量

- 源侧 `mv_to_kafka_*` 负责将新增数据实时推送到 Kafka

## 类型转换策略

- Kafka 引擎中间层字段统一 `String`
- `mv_from_kafka_*` 按目标类型严格 CAST
- 日期时间字段通过 `parseDateTimeBestEffort` 转换

详见：`docs/type-conversion-sync.md`

## 结构差异比对

- 单表：`schema-diff`
- 批量：`schema-diff-batch`

批量输出包含：

- `total_count`
- `correct_count`
- `error_count`
- `error_tables`（含字段与类型差异）
