# Time Series

## What Makes Time Series Data Different?

Consider this sequence:

```
10:00:00  CPU  40%
10:00:01  CPU  43%
10:00:02  CPU  95%
10:00:03  CPU  72%
10:00:04  CPU  68%
```

What makes this fundamentally different from a user table or an orders table?

1. **Append-only**: values don't change after creation (you append new observations, not update old ones)
2. **High write rate**: millions of samples per second in large systems
3. **Time is the primary access dimension**: almost every query has a time range filter
4. **Aggregation dominates**: raw values are rarely the final answer; you need rates, averages, percentiles
5. **Retention tiers**: recent data needs fast access, old data can be compressed or discarded

These properties make generic databases inefficient and motivate specialised time-series databases.

---

## What This Module Covers

| Topic | What You Will Learn |
|-------|-------------------|
| [Time Semantics](time-semantics.md) | Timestamps, sampling, irregular data |
| [Windows](windows.md) | Tumbling, sliding, session windows for time series |
| [Cardinality](cardinality.md) | Why label cardinality kills Prometheus |
| [Downsampling](downsampling.md) | Why you cannot keep all raw data |
| [TSDB Comparison](tsdbs.md) | Prometheus vs VictoriaMetrics vs TimescaleDB vs InfluxDB |

---

## Running Use Case

The **IoT Platform**:
- 10 million devices
- Each sends `{device_id, sensor, value, timestamp}` every 30 seconds
- Total: 10M ÷ 30 = ~333,333 data points per second
- Queries: "latest value", "1-hour chart", "1-year trend", anomaly detection

And the **Security / Observability Platform**:
- Prometheus scraping metrics from thousands of services
- Each service exposes hundreds of metrics with multiple label dimensions
