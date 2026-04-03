#!/usr/bin/env bash
set -euo pipefail

for i in $(seq 1 60); do
  if clickhouse-client --host ck-source --query "SELECT 1" >/dev/null 2>&1; then
    break
  fi
  sleep 1
done

for i in $(seq 1 60); do
  if clickhouse-client --host ck-target --query "SELECT 1" >/dev/null 2>&1; then
    break
  fi
  sleep 1
done

clickhouse-client --host ck-source --query "CREATE DATABASE IF NOT EXISTS demo"
for i in $(seq -w 1 10); do
  table="t_sync_${i}"
  clickhouse-client --host ck-source --query "CREATE TABLE IF NOT EXISTS demo.${table} (id UInt64, name String, amount Float64, created_at DateTime) ENGINE = MergeTree ORDER BY id"
  clickhouse-client --host ck-source --query "INSERT INTO demo.${table} SELECT number + 1 AS id, concat('name_', toString(number + 1)) AS name, toFloat64(number) + 0.5 AS amount, now() - toIntervalSecond(number) AS created_at FROM numbers(100) WHERE (SELECT count() FROM demo.${table}) = 0"
done

clickhouse-client --host ck-target --query "CREATE DATABASE IF NOT EXISTS demo"
clickhouse-client --host ck-target --query "CREATE TABLE IF NOT EXISTS demo.users (id UInt64, name String, amount Decimal(18,2), created_at DateTime64(3)) ENGINE = MergeTree ORDER BY id"
