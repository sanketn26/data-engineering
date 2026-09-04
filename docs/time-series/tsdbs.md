# TSDB Comparison

## The Systems

| System | Type | Strength | Weakness |
|--------|------|---------|---------|
| Prometheus | Pull-based TSDB | Infrastructure monitoring, alerting | High-cardinality, long-term retention |
| VictoriaMetrics | TSDB | Prometheus-compatible, more efficient | Newer ecosystem |
| TimescaleDB | Time-series SQL | Full SQL, familiar tooling | PostgreSQL limitations at extreme scale |
| InfluxDB | Purpose-built TSDB | Flexible schema, IoT | Licensing changes (v3), resource usage |
| ClickHouse | Columnar OLAP | Extreme query speed, flexible SQL | Not purpose-built for TS, no PromQL |

---

## Prometheus

**Architecture**: pull-based scraping. Prometheus scrapes `/metrics` endpoints from your services on a configurable schedule.

**Storage model**: per-series chunks compressed on disk. Time-indexed inverted index for label lookup.

**Retention**: typically 15 days on local disk. Long-term storage via remote write to VictoriaMetrics, Thanos, or Cortex.

**Best for**: Kubernetes and microservice infrastructure monitoring. The ecosystem (Grafana, AlertManager, exporters) is unmatched.

**Do not use for**: cardinality > ~10 million active series, long-term retention (use remote storage instead), business events with high-cardinality dimensions.

---

## VictoriaMetrics

Drop-in replacement for Prometheus with:
- 10–100× better storage efficiency
- Handles higher cardinality
- Better performance on high-write workloads
- Compatible with PromQL and Prometheus remote write

For new deployments where Prometheus would be the choice, VictoriaMetrics is often a better choice unless the existing Prometheus ecosystem (exporters, tooling) is a hard requirement.

---

## TimescaleDB

PostgreSQL extension that adds time-series capabilities:
- **Hypertables**: partitioned automatically by time
- **Continuous aggregates**: materialized views that update automatically
- **Compression**: columnar compression on older chunks
- **Retention policies**: automatic chunk expiration

**Best for**: teams that know SQL and want time-series without learning new query languages. Latency/throughput is worse than purpose-built TSDBs but tooling (existing PostgreSQL connectors, BI tools) is unmatched.

```sql
-- Create a hypertable (time-partitioned table)
CREATE TABLE metrics (
    timestamp TIMESTAMPTZ NOT NULL,
    host TEXT,
    metric TEXT,
    value DOUBLE PRECISION
);
SELECT create_hypertable('metrics', 'timestamp');

-- Automatic compression on chunks older than 7 days
ALTER TABLE metrics SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'host, metric'
);
SELECT add_compression_policy('metrics', INTERVAL '7 days');
```

---

## InfluxDB

Purpose-built TSDB with a schemaless data model. Very flexible for IoT where device schemas evolve unpredictably.

Significant license changes in v3 (BSL license, not open source). Evaluate carefully for new deployments.

---

## ClickHouse as a TSDB

ClickHouse is not a TSDB, but many teams use it for time-series data:

**Advantages**:
- Extreme query performance
- Full SQL without learning PromQL
- Can handle high-cardinality data (user_id is just a column, not a label)
- Compression is excellent

**Disadvantages**:
- No native time-series features (no continuous aggregates, no retention policies out of the box)
- No alerting integration
- Not designed for high-write rate from individual inserts (need batching)

**Use ClickHouse for time-series when**: your queries are complex, cardinality is high, you need ad-hoc SQL, and you are not scraping Kubernetes metrics.

---

## Decision Guide

```
Do you need Prometheus/Grafana ecosystem compatibility?
  Yes → Prometheus or VictoriaMetrics
  No → continue

Is your team comfortable with SQL?
  Yes → TimescaleDB
  No → continue

Do you need high-cardinality (per-user, per-device) analytics?
  Yes → ClickHouse
  No → continue

IoT with flexible schema?
  Yes → InfluxDB (if comfortable with licensing) or ClickHouse

Default/infrastructure monitoring → Prometheus + VictoriaMetrics for long-term
```
