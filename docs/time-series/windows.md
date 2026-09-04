# Time Windows

## Window Types for Time Series

### Tumbling Windows

Fixed-size, non-overlapping windows. The classic "per-minute" or "per-hour" aggregation.

```
09:00–09:01 | 09:01–09:02 | 09:02–09:03 | ...
```

```sql
-- TimescaleDB
SELECT time_bucket('1 minute', timestamp) AS minute,
       avg(latency_ms)
FROM requests
GROUP BY minute ORDER BY minute;

-- ClickHouse
SELECT toStartOfMinute(timestamp) AS minute,
       avg(latency_ms)
FROM requests
GROUP BY minute ORDER BY minute;
```

### Sliding Windows

Windows that overlap. A 5-minute window sliding by 1 minute.

```
09:00–09:05
09:01–09:06
09:02–09:07
...
```

Useful for rolling averages and anomaly detection. Expensive in database terms — each data point appears in multiple windows.

```sql
-- TimescaleDB: rolling 5-minute average per minute
SELECT timestamp,
       avg(latency_ms) OVER (
           ORDER BY timestamp
           RANGE BETWEEN INTERVAL '5 minutes' PRECEDING AND CURRENT ROW
       ) AS rolling_5min_avg
FROM requests;
```

### Session Windows

Variable-length windows bounded by inactivity. A user session is a sequence of events with no gaps longer than 30 minutes.

```sql
-- Identify session starts (events where previous event was >30 min ago)
SELECT *,
       CASE WHEN timestamp - LAG(timestamp) OVER (PARTITION BY user_id ORDER BY timestamp)
               > INTERVAL '30 minutes'
               OR LAG(timestamp) OVER (PARTITION BY user_id ORDER BY timestamp) IS NULL
            THEN 1 ELSE 0 END AS is_session_start
FROM events;
```

---

## Functions for Time-Series Analysis

| Function | Use |
|----------|-----|
| `rate()` | Per-second rate of a counter |
| `delta()` | Absolute change over a time range |
| `deriv()` | Slope (per-second change rate) |
| `moving_average()` | Smoothed trend |
| `percentile_cont()` | Latency percentiles |
| `stddev()` | Variability / anomaly detection |
| `lag()` / `lead()` | Compare to previous/next value |

---

## Common Time-Series Query Patterns

**"Latest value per device"**

```sql
-- TimescaleDB: use DISTINCT ON for efficiency
SELECT DISTINCT ON (device_id)
    device_id, timestamp, temperature
FROM sensor_readings
ORDER BY device_id, timestamp DESC;

-- ClickHouse: argMax
SELECT device_id, argMax(temperature, timestamp) AS latest_temp
FROM sensor_readings
GROUP BY device_id;
```

**"Average over the last hour, updated every minute"**

Sliding window continuous aggregate — expensive to compute from scratch, efficient with pre-computed rollups.

**"Detect anomaly: value > 2 standard deviations from rolling mean"**

```sql
WITH stats AS (
    SELECT device_id, timestamp, value,
           avg(value) OVER w AS rolling_mean,
           stddev(value) OVER w AS rolling_std
    FROM sensor_readings
    WINDOW w AS (PARTITION BY device_id
                 ORDER BY timestamp
                 RANGE BETWEEN INTERVAL '1 hour' PRECEDING AND CURRENT ROW)
)
SELECT * FROM stats
WHERE abs(value - rolling_mean) > 2 * rolling_std;
```
