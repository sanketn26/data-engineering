# TSDB vs OLAP

Time series databases (TSDBs) and OLAP engines are both used for analytical workloads. The distinction matters.

---

## The Problem They Each Solve

**TSDBs** are optimised for storing and querying measurements over time with high cardinality at the label/tag level. The query pattern is: "give me metric X for service Y over the last 6 hours, aggregated by 1-minute intervals."

**OLAP engines** (ClickHouse, Pinot) are optimised for analytical queries over large datasets — GROUP BY, filtering, aggregations across many columns. The query pattern is: "count active users per region per day for the last 90 days, filtered by plan type."

The data models differ. The access patterns differ. The optimisations differ.

---

## Data Model

**TSDB** (Prometheus/VictoriaMetrics):
```
metric_name{label1="v1", label2="v2"} value timestamp
```
Labels are the dimensions. High cardinality in labels is a significant concern.

**OLAP** (ClickHouse):
```sql
CREATE TABLE events (
    timestamp DateTime,
    customer_id UInt64,
    user_id UInt64,
    region String,
    latency_ms Float64,
    status_code UInt16
) ENGINE = MergeTree()
ORDER BY (customer_id, timestamp);
```
Rows with many columns. Columnar storage optimised for arbitrary GROUP BY.

---

## Cardinality

**TSDBs**: designed for moderate label cardinality. Prometheus recommends <100K unique label value combinations per metric. High cardinality (user IDs, request IDs as labels) kills performance — index size explodes.

**OLAP**: can handle high cardinality columns because the index is sparse (ClickHouse) or inverted (Pinot). A column with 100 million distinct user IDs is acceptable.

This is the defining distinction. If your time series data has high-cardinality dimensions (per-user metrics, per-request tracing), a TSDB is wrong. Use ClickHouse.

---

## Query Language

**TSDB**:
- Prometheus: PromQL (`rate(http_requests_total[5m])`)
- VictoriaMetrics: MetricsQL (superset of PromQL)
- InfluxDB: Flux or InfluxQL
- TimescaleDB: SQL (PostgreSQL extension)

**OLAP**:
- ClickHouse: SQL with extensions
- Pinot: SQL
- Druid: SQL

For engineers comfortable with SQL, TimescaleDB or ClickHouse is easier to use than Prometheus for analytical queries.

---

## Downsampling and Retention

**TSDBs**: built-in recording rules (Prometheus), continuous aggregates (TimescaleDB), rollup policies (VictoriaMetrics). Designed from the start for tiered retention.

**OLAP**: achievable via materialized views (ClickHouse) or Airflow jobs writing to separate tables, but not built-in in the same way. ClickHouse TTL can expire data per tier.

TSDBs have more native support for the "raw → 1min → 1hour → 1day" retention pyramid.

---

## Ingestion Rate

**TSDBs**: optimised for time series writes. VictoriaMetrics handles millions of samples/second. Prometheus pull model limits write rate but scales with federation.

**OLAP**: also handles high ingestion rates. ClickHouse handles millions of rows/second. But the row model (wide events with many columns) differs from the TSDB sample model.

---

## Alerting Integration

**TSDBs**: native alerting. Prometheus Alertmanager, VictoriaMetrics vmalert, Grafana alert rules on top of any TSDB. Tightly coupled.

**OLAP**: alerting requires an external layer querying the OLAP engine on a schedule. Possible but not native.

For infrastructure monitoring + alerting, TSDBs are the standard choice.

---

## When to Use a TSDB

- Infrastructure monitoring (CPU, memory, disk, network)
- Application metrics (request rate, error rate, latency percentiles)
- Alerting based on metric thresholds
- Grafana dashboards showing metrics over rolling windows
- Moderate cardinality labels (<100K unique combinations per metric)

## When to Use ClickHouse (OLAP) for Time Series

- High-cardinality event data (per-user, per-request, per-session)
- Business analytics alongside time-based queries
- You need full SQL with JOINs against other tables
- Events have many dimensions (not just metric + labels)
- You're already running ClickHouse for other workloads

## When to Use TimescaleDB

- You want TSDB features but need full SQL (PostgreSQL)
- Moderate scale (< 100K samples/sec)
- You need JOINs between time series and relational data in one system
- Existing PostgreSQL expertise

---

## Summary Decision

```
Infrastructure monitoring + alerting?         → TSDB (Prometheus / VictoriaMetrics)
High-cardinality event analytics?             → ClickHouse
SQL-based analytics with time series?         → TimescaleDB or ClickHouse
Sub-second fresh data with fixed dashboards?  → Pinot
Full flexibility + historical depth?          → ClickHouse
```
