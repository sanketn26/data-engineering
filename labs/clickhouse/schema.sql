CREATE TABLE IF NOT EXISTS events_by_tenant
(
    timestamp DateTime,
    customer_id String,
    user_id String,
    service LowCardinality(String),
    endpoint LowCardinality(String),
    region LowCardinality(String),
    latency_ms UInt32,
    status_code UInt16,
    bytes UInt32
)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(timestamp)
ORDER BY (customer_id, timestamp);

CREATE TABLE IF NOT EXISTS events_by_time AS events_by_tenant
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(timestamp)
ORDER BY (timestamp, customer_id);

CREATE TABLE IF NOT EXISTS tiny_inserts
(
    timestamp DateTime,
    customer_id String,
    latency_ms UInt32
)
ENGINE = MergeTree
ORDER BY (customer_id, timestamp)
SETTINGS parts_to_delay_insert = 20, parts_to_throw_insert = 40;
