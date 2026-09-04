# Downsampling

## The Problem

Your IoT platform collects 333,333 data points per second. After one year, you have:

`333,333 × 60 × 60 × 24 × 365 = ~10 trillion data points`

At 8 bytes per data point, that is ~80 TB. And that is just the raw values — plus timestamps, device IDs, sensor names.

You cannot keep 10 trillion raw data points in a fast query engine for a year. The cost is prohibitive.

More importantly: when a user looks at a "last 1 year" chart, they are viewing it in a browser window perhaps 1000 pixels wide. No browser renders 10 trillion data points. You are going to aggregate anyway.

**Downsampling is the process of aggregating high-resolution data into lower-resolution summaries, discarding the raw values.**

---

## Resolution Tiers

A common tiered retention strategy:

| Resolution | Retention | Use Case |
|-----------|----------|---------|
| Raw (1 second) | 7 days | Debugging, recent anomaly investigation |
| 1 minute aggregates | 30 days | Short-term trend analysis |
| 5 minute aggregates | 90 days | Medium-term charts |
| 1 hour aggregates | 1 year | Long-term trend |
| 1 day aggregates | 5+ years | Historical reporting |

The query layer selects the appropriate resolution based on the time range requested:

- "Last 1 hour" → use raw or 1-minute data
- "Last 30 days" → use 5-minute or hourly data
- "Last 1 year" → use hourly or daily data

---

## What to Aggregate

When downsampling, you must decide what statistics to compute:

| Aggregation | Use |
|------------|-----|
| `min` | Was there a brief spike? |
| `max` | Was there a brief spike? |
| `avg` | General trend |
| `sum` | Total volume (counters) |
| `p50, p95, p99` | Latency percentiles |
| `count` | Number of samples |
| `last` | Current state (gauges) |

**Do not only keep `avg`.** A 1-minute average of CPU usage hides a 5-second spike to 100%. Keeping `max` in the 1-minute aggregate preserves evidence of the spike.

---

## Continuous Aggregation

In TimescaleDB, continuous aggregates automatically maintain downsampled views:

```sql
-- Create a continuous aggregate: 1-hour average CPU per host
CREATE MATERIALIZED VIEW cpu_hourly
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 hour', timestamp) AS hour,
    host,
    avg(cpu_usage) as avg_cpu,
    max(cpu_usage) as max_cpu,
    min(cpu_usage) as min_cpu
FROM metrics
WHERE metric = 'cpu_usage'
GROUP BY hour, host;

-- Automatically refresh as new data arrives
SELECT add_continuous_aggregate_policy('cpu_hourly',
    start_offset => INTERVAL '3 hours',
    end_offset => INTERVAL '1 hour',
    schedule_interval => INTERVAL '1 hour');
```

---

## Rollups in ClickHouse

ClickHouse has a `GraphiteMergeTree` engine for pre-configured retention rules, or you can implement rollups with materialized views:

```sql
-- 1-hour rollups of raw metric data
CREATE MATERIALIZED VIEW metrics_1h
ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(hour)
ORDER BY (metric, host, hour)
AS SELECT
    toStartOfHour(timestamp) as hour,
    metric,
    host,
    avgState(value) as avg_value,
    maxState(value) as max_value,
    minState(value) as min_value,
    countState() as sample_count
FROM metrics
GROUP BY hour, metric, host;
```

---

## TTL: Automatic Expiration

Both ClickHouse and TimescaleDB support automatic data expiration:

```sql
-- ClickHouse: expire raw data after 7 days
ALTER TABLE metrics MODIFY TTL timestamp + INTERVAL 7 DAY;

-- TimescaleDB: drop chunks older than 30 days
SELECT add_retention_policy('metrics', INTERVAL '30 days');
```

---

## How to Apply This at Work

When designing a time-series system:

1. Define the resolution tiers you need
2. Compute the storage requirement at each tier
3. Implement continuous aggregation or scheduled rollup jobs
4. Set TTL on raw data
5. Build the query layer to select resolution based on time range
6. Keep at minimum: avg + max + count in every aggregate (not just avg)
