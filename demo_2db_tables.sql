CREATE DATABASE IF NOT EXISTS demo;
CREATE DATABASE IF NOT EXISTS mv;

CREATE TABLE IF NOT EXISTS demo.demo_table_01 (
  id UInt64,
  value String,
  ts DateTime
) ENGINE = MergeTree
PARTITION BY toDate(ts)
ORDER BY id;
INSERT INTO demo.demo_table_01 SELECT number + 1, concat('row_', toString(number + 1)), now() FROM numbers(100);

CREATE TABLE IF NOT EXISTS demo.demo_table_02 (
  id UInt64,
  value String,
  ts DateTime
) ENGINE = MergeTree
PARTITION BY toDate(ts)
ORDER BY id;
INSERT INTO demo.demo_table_02 SELECT number + 1, concat('row_', toString(number + 1)), now() FROM numbers(100);

CREATE TABLE IF NOT EXISTS demo.demo_table_03 (
  id UInt64,
  value String,
  ts DateTime
) ENGINE = MergeTree
PARTITION BY toDate(ts)
ORDER BY id;
INSERT INTO demo.demo_table_03 SELECT number + 1, concat('row_', toString(number + 1)), now() FROM numbers(100);

CREATE TABLE IF NOT EXISTS demo.demo_table_04 (
  id UInt64,
  value String,
  ts DateTime
) ENGINE = MergeTree
PARTITION BY toDate(ts)
ORDER BY id;
INSERT INTO demo.demo_table_04 SELECT number + 1, concat('row_', toString(number + 1)), now() FROM numbers(100);

CREATE TABLE IF NOT EXISTS demo.demo_table_05 (
  id UInt64,
  value String,
  ts DateTime
) ENGINE = MergeTree
PARTITION BY toDate(ts)
ORDER BY id;
INSERT INTO demo.demo_table_05 SELECT number + 1, concat('row_', toString(number + 1)), now() FROM numbers(100);

CREATE TABLE IF NOT EXISTS demo.demo_table_06 (
  id UInt64,
  value String,
  ts DateTime
) ENGINE = MergeTree
PARTITION BY toDate(ts)
ORDER BY id;
INSERT INTO demo.demo_table_06 SELECT number + 1, concat('row_', toString(number + 1)), now() FROM numbers(100);

CREATE TABLE IF NOT EXISTS demo.demo_table_07 (
  id UInt64,
  value String,
  ts DateTime
) ENGINE = MergeTree
PARTITION BY toDate(ts)
ORDER BY id;
INSERT INTO demo.demo_table_07 SELECT number + 1, concat('row_', toString(number + 1)), now() FROM numbers(100);

CREATE TABLE IF NOT EXISTS demo.demo_table_08 (
  id UInt64,
  value String,
  ts DateTime
) ENGINE = MergeTree
PARTITION BY toDate(ts)
ORDER BY id;
INSERT INTO demo.demo_table_08 SELECT number + 1, concat('row_', toString(number + 1)), now() FROM numbers(100);

CREATE TABLE IF NOT EXISTS demo.demo_table_09 (
  id UInt64,
  value String,
  ts DateTime
) ENGINE = MergeTree
PARTITION BY toDate(ts)
ORDER BY id;
INSERT INTO demo.demo_table_09 SELECT number + 1, concat('row_', toString(number + 1)), now() FROM numbers(100);

CREATE TABLE IF NOT EXISTS demo.demo_table_10 (
  id UInt64,
  value String,
  ts DateTime
) ENGINE = MergeTree
PARTITION BY toDate(ts)
ORDER BY id;
INSERT INTO demo.demo_table_10 SELECT number + 1, concat('row_', toString(number + 1)), now() FROM numbers(100);

CREATE TABLE IF NOT EXISTS demo.demo_table_01 (
  id UInt64,
  value String,
  ts DateTime
) ENGINE = MergeTree
PARTITION BY toDate(ts)
ORDER BY id;
INSERT INTO demo.demo_table_01 SELECT number + 1, concat('row_', toString(number + 1)), now() FROM numbers(100);

CREATE TABLE IF NOT EXISTS demo.demo_table_02 (
  id UInt64,
  value String,
  ts DateTime
) ENGINE = MergeTree
PARTITION BY toDate(ts)
ORDER BY id;
INSERT INTO demo.demo_table_02 SELECT number + 1, concat('row_', toString(number + 1)), now() FROM numbers(100);

CREATE TABLE IF NOT EXISTS demo.demo_table_03 (
  id UInt64,
  value String,
  ts DateTime
) ENGINE = MergeTree
PARTITION BY toDate(ts)
ORDER BY id;
INSERT INTO demo.demo_table_03 SELECT number + 1, concat('row_', toString(number + 1)), now() FROM numbers(100);

CREATE TABLE IF NOT EXISTS demo.demo_table_04 (
  id UInt64,
  value String,
  ts DateTime
) ENGINE = MergeTree
PARTITION BY toDate(ts)
ORDER BY id;
INSERT INTO demo.demo_table_04 SELECT number + 1, concat('row_', toString(number + 1)), now() FROM numbers(100);

CREATE TABLE IF NOT EXISTS demo.demo_table_05 (
  id UInt64,
  value String,
  ts DateTime
) ENGINE = MergeTree
PARTITION BY toDate(ts)
ORDER BY id;
INSERT INTO demo.demo_table_05 SELECT number + 1, concat('row_', toString(number + 1)), now() FROM numbers(100);

CREATE TABLE IF NOT EXISTS demo.demo_table_06 (
  id UInt64,
  value String,
  ts DateTime
) ENGINE = MergeTree
PARTITION BY toDate(ts)
ORDER BY id;
INSERT INTO demo.demo_table_06 SELECT number + 1, concat('row_', toString(number + 1)), now() FROM numbers(100);

CREATE TABLE IF NOT EXISTS demo.demo_table_07 (
  id UInt64,
  value String,
  ts DateTime
) ENGINE = MergeTree
PARTITION BY toDate(ts)
ORDER BY id;
INSERT INTO demo.demo_table_07 SELECT number + 1, concat('row_', toString(number + 1)), now() FROM numbers(100);

CREATE TABLE IF NOT EXISTS demo.demo_table_08 (
  id UInt64,
  value String,
  ts DateTime
) ENGINE = MergeTree
PARTITION BY toDate(ts)
ORDER BY id;
INSERT INTO demo.demo_table_08 SELECT number + 1, concat('row_', toString(number + 1)), now() FROM numbers(100);

CREATE TABLE IF NOT EXISTS demo.demo_table_09 (
  id UInt64,
  value String,
  ts DateTime
) ENGINE = MergeTree
PARTITION BY toDate(ts)
ORDER BY id;
INSERT INTO demo.demo_table_09 SELECT number + 1, concat('row_', toString(number + 1)), now() FROM numbers(100);

CREATE TABLE IF NOT EXISTS demo.demo_table_10 (
  id UInt64,
  value String,
  ts DateTime
) ENGINE = MergeTree
PARTITION BY toDate(ts)
ORDER BY id;
INSERT INTO demo.demo_table_10 SELECT number + 1, concat('row_', toString(number + 1)), now() FROM numbers(100);
