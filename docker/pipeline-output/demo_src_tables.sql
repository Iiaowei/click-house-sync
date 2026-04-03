CREATE DATABASE IF NOT EXISTS demo_src;

CREATE TABLE demo_src.t_sync_01
(
    `id` UInt64,
    `name` String,
    `amount` Float64,
    `created_at` DateTime
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 8192;

CREATE TABLE demo_src.t_sync_02
(
    `id` UInt64,
    `name` String,
    `amount` Float64,
    `created_at` DateTime
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 8192;

CREATE TABLE demo_src.t_sync_03
(
    `id` UInt64,
    `name` String,
    `amount` Float64,
    `created_at` DateTime
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 8192;

CREATE TABLE demo_src.t_sync_04
(
    `id` UInt64,
    `name` String,
    `amount` Float64,
    `created_at` DateTime
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 8192;

CREATE TABLE demo_src.t_sync_05
(
    `id` UInt64,
    `name` String,
    `amount` Float64,
    `created_at` DateTime
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 8192;

CREATE TABLE demo_src.t_sync_06
(
    `id` UInt64,
    `name` String,
    `amount` Float64,
    `created_at` DateTime
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 8192;

CREATE TABLE demo_src.t_sync_07
(
    `id` UInt64,
    `name` String,
    `amount` Float64,
    `created_at` DateTime
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 8192;

CREATE TABLE demo_src.t_sync_08
(
    `id` UInt64,
    `name` String,
    `amount` Float64,
    `created_at` DateTime
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 8192;

CREATE TABLE demo_src.t_sync_09
(
    `id` UInt64,
    `name` String,
    `amount` Float64,
    `created_at` DateTime
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 8192;

CREATE TABLE demo_src.t_sync_10
(
    `id` UInt64,
    `name` String,
    `amount` Float64,
    `created_at` DateTime
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 8192;

