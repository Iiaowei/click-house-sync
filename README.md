# click-house-sync

ClickHouse ↔ Kafka 同步与迁移工具，命令行工具名为 `ch-sync`。

## 文档导航

- 架构说明：`docs/architecture.md`
- 用户实例：`docs/user-examples.md`
- 类型转换专项：`docs/type-conversion-sync.md`
- 变更记录：`CHANGELOG.md`

## 核心能力

- 一键创建 Topic、Kafka 引擎表、物化视图
- 支持批量同步（`tables.yaml`）
- 支持游标分页与持续增量导出
- 支持结构差异比对（单表 / 批量）
- 支持类型转换重建 DDL（含回滚与质量 SQL）

## 快速开始

### 1) 构建

```bash
make build
```

### 2) 查看帮助

```bash
./ch-sync --help
```

### 3) 常见命令

```bash
# 仅准备资源
./ch-sync prepare --table users

# 批量同步
./ch-sync sync --tables users,orders --prepare-only

# 结构比对（单表）
./ch-sync schema-diff --table users --target-host 127.0.0.1 --target-port 9001

# 结构比对（批量）
./ch-sync schema-diff-batch --tables-file tables.yaml --target-host 127.0.0.1 --target-port 9001 --target-db demo
```

## Docker 测试环境

仓库内置 `docker-compose.yml`，包含：

- `ck-source`（源 ClickHouse）
- `ck-target`（目标 ClickHouse）
- `kafka`（Confluent Kafka）
- `ch-init`（初始化数据）
- `ch-sync-pipeline`（自动化同步流程）

启动：

```bash
docker compose up -d --build
```

停止并清理：

```bash
docker compose down -v --remove-orphans
```

## 配置与约定

- 默认配置文件：`config.yaml`
- 批量表配置：`tables.yaml`
- Docker 容器内配置：`docker/config.container.yaml`

## 许可证

MIT
