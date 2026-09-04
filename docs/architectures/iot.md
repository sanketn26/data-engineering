# IoT Platform Architecture

## Requirements

- **Scale**: 10 million devices, one sample every 30 seconds = 333,333 samples/sec
- **Queries**: latest value per device, 1-hour chart, 1-year trend, anomaly detection
- **Retention**: 7 days raw, 30 days at 1-minute, 1 year at hourly

---

## Architecture

```
Devices → MQTT Broker → Kafka → Flink → ClickHouse (hot: 7 days)
                                       → TimescaleDB (warm: 30 days, 1-min)
                                       → Iceberg/S3 (cold: 1 year, hourly)
```

---

## ClickHouse Table Design

```sql
CREATE TABLE device_metrics (
    timestamp DateTime64(3),
    device_id String,
    sensor LowCardinality(String),
    value Float32,
    unit LowCardinality(String),
    quality UInt8  -- 0=bad, 1=good
) ENGINE = MergeTree()
PARTITION BY (toYYYYMMDD(timestamp))
ORDER BY (device_id, sensor, timestamp)
TTL timestamp + INTERVAL 7 DAY;
```

Query for latest value:
```sql
SELECT device_id, sensor, argMax(value, timestamp) AS latest
FROM device_metrics
WHERE device_id IN ('d001', 'd002', 'd003')
GROUP BY device_id, sensor;
```

---

## Downsampling Pipeline

Flink computes 1-minute aggregates and writes to TimescaleDB:

```python
# Flink: tumbling 1-minute windows per (device_id, sensor)
stream \
    .key_by(lambda e: (e['device_id'], e['sensor'])) \
    .window(TumblingEventTimeWindows.of(Time.minutes(1))) \
    .aggregate(MetricAggregator())  # min, max, avg, count
    .add_sink(TimescaleDBSink())
```

---

## Anomaly Detection

Flink: real-time z-score on per-device rolling statistics:

```python
class AnomalyDetector(KeyedProcessFunction):
    def open(self, ctx):
        self.mean_state = self.get_runtime_context().get_state(
            ValueStateDescriptor("mean", Types.FLOAT()))
        self.variance_state = self.get_runtime_context().get_state(
            ValueStateDescriptor("variance", Types.FLOAT()))

    def process_element(self, reading, ctx):
        mean = self.mean_state.value() or reading['value']
        variance = self.variance_state.value() or 1.0
        std = variance ** 0.5

        z_score = abs(reading['value'] - mean) / std
        if z_score > 3:
            yield AnomalyEvent(reading['device_id'], z_score)

        # Update running statistics (Welford's algorithm)
        n = ...
        new_mean = mean + (reading['value'] - mean) / n
        # ... update variance
        self.mean_state.update(new_mean)
```
