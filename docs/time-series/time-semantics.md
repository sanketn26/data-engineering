# Time Semantics

## Core Concepts

### Metric

A named quantity measured over time: `cpu_usage`, `http_request_duration`, `order_count`.

### Label / Tag

Metadata that identifies a specific instance of a metric: `{host="server-1", region="us-east"}`.

### Sample / Observation

A single (timestamp, value) pair for a specific metric+label combination.

### Series

A stream of samples for one (metric, label set) combination. Each unique label combination is a distinct series.

---

## Timestamps

Always use millisecond or nanosecond precision for time-series data. Second precision loses too much information at high sample rates.

Use UTC. Never store timestamps in local time — timezone conversions are a constant source of bugs in time-series systems.

---

## Sampling

**Regular sampling**: a measurement taken at fixed intervals (every second, every 30 seconds). Most IoT and infrastructure monitoring.

**Irregular sampling**: events that happen at unpredictable times (a log entry when a request arrives, a transaction when a user buys something). More common in business event streams.

Regular sampling is simpler to aggregate and visualise. Irregular sampling requires interpolation or resampling for some analyses.

---

## Missing Values

Regular sampling produces gaps when:
- A device is offline
- A scrape fails
- Network packets are dropped

**How different systems handle gaps**:
- Prometheus: `null` for gaps, PromQL functions handle them
- InfluxDB: genuinely missing values (no interpolation by default)
- TimescaleDB: use `time_bucket_gapfill()` to fill gaps with null or interpolated values

```sql
-- TimescaleDB: fill gaps with linear interpolation
SELECT
    time_bucket_gapfill('5 minutes', timestamp) AS bucket,
    device_id,
    interpolate(avg(temperature)) AS temperature
FROM sensor_readings
WHERE timestamp BETWEEN '2024-01-15' AND '2024-01-16'
GROUP BY bucket, device_id;
```

---

## Rates and Derivatives

A **counter** only increases (e.g., total HTTP requests since server start). The raw value is not useful for dashboards; you want the **rate of change**.

```
# PromQL: requests per second over a 5-minute window
rate(http_requests_total[5m])

# Python equivalent
def rate(values, timestamps):
    delta_value = values[-1] - values[0]
    delta_time = (timestamps[-1] - timestamps[0]).total_seconds()
    return delta_value / delta_time
```

A **gauge** goes up and down (CPU usage, memory, temperature). Rates are meaningful only for counters.

---

## Moving Averages and EWMA

Moving averages smooth noisy data:

```python
import pandas as pd

# Simple moving average (rolling window)
cpu_smooth = cpu_series.rolling(window=60).mean()

# Exponential weighted moving average (more weight to recent values)
cpu_ewma = cpu_series.ewm(span=60).mean()
```

EWMA reacts faster to changes than SMA. Use it when you want smoothing but want to detect trends quickly.
