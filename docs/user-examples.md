# 用户实例

## 场景 1：单表准备

```bash
./ch-sync prepare --table users
```

用途：

- 创建 Topic
- 创建 Kafka 引擎表
- 创建物化视图

## 场景 2：批量准备与全量回补

```bash
./ch-sync sync --tables-file tables.yaml --prepare-only
./ch-sync sync --tables-file tables.yaml --full-export
```

用途：

- 第一条只建链路
- 第二条做历史数据回补

## 场景 3：结构差异检查

### 单表比对

```bash
./ch-sync schema-diff --table t_sync_01 \
  --source-database demo \
  --target-db demo \
  --target-host 127.0.0.1 \
  --target-port 9001
```

### 批量比对

```bash
./ch-sync schema-diff-batch \
  --tables-file tables.yaml \
  --target-host 127.0.0.1 \
  --target-port 9001 \
  --target-db demo
```

返回结构包含总量、正确量、错误量与错误明细。

## 场景 4：Docker 一键测试

```bash
docker compose up -d --build
docker compose logs -f ch-sync-pipeline
```

流水线产物目录：

- `docker/pipeline-output/demo_tables.sql`
- `docker/pipeline-output/tables.yaml`
- `docker/pipeline-output/schema_diff_summary.json`
- `docker/pipeline-output/pipeline.done`

停止并重置：

```bash
docker compose down -v --remove-orphans
```

## 场景 5：查看关键对象

```bash
docker compose exec ck-source clickhouse-client -q "SHOW TABLES FROM demo"
docker compose exec ck-source clickhouse-client -q "SHOW TABLES FROM demo_stream"
docker compose exec ck-target clickhouse-client -q "SHOW TABLES FROM demo"
docker compose exec ck-target clickhouse-client -q "SHOW TABLES FROM demo_stream"
```
