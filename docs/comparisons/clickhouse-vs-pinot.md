# ClickHouse vs Pinot

Both ClickHouse and Pinot are columnar OLAP engines designed for fast analytical queries. They make different architectural trade-offs for different workloads.

---

## The Core Difference

**ClickHouse** is a general-purpose column-oriented database. You load data (batch or streaming via Kafka Engine), define an ORDER BY that becomes the primary sort key, and run SQL. It handles both streaming ingestion and historical analysis.

**Pinot** is purpose-built for real-time analytics with ultra-low latency on data ingested within the last few seconds. It uses a separate segment model (realtime + offline) and a star-tree index for fast aggregations.

---

## Architecture

**ClickHouse**:
```
Kafka → ClickHouse Kafka Engine (materialized view) → MergeTree table
     or
Spark/Flink → ClickHouse INSERT
```
Single system handling ingestion + storage + query.

**Pinot**:
```
Kafka → Pinot Realtime Server (CONSUMING segments)
     → Pinot Offline Server (ONLINE segments, pushed from Spark/Hadoop)
Broker routes queries to appropriate servers
```
Separate realtime and offline serving paths merged at query time.

---

## Query Latency

| Scenario | ClickHouse | Pinot |
|----------|-----------|-------|
| Dashboard queries on hot data | <100ms | <10ms |
| Fresh data (last 30 seconds) | ~500ms–2s | <100ms |
| Large aggregations on cold data | Seconds | Seconds |
| High concurrency (1000 QPS) | Good with caching | Excellent |
| Ad-hoc queries | Excellent | Limited (less expressive SQL) |

Pinot wins on freshness. ClickHouse wins on flexibility.

---

## Freshness

**Pinot**: ingests from Kafka directly into CONSUMING segments. Query results include data ingested seconds ago.

**ClickHouse**: Kafka Engine + materialized view approach adds latency. Typically 5–30 seconds behind real time depending on flush interval.

If you need data visible in dashboards within seconds of being produced, Pinot.

---

## SQL Expressiveness

**ClickHouse**: full SQL with window functions, JOINs, subqueries, CTEs, array functions, LAMBDA functions, user-defined functions. Extremely expressive.

**Pinot**: SQL support is improving but JOINs are limited (primarily designed for single-table queries). Complex transformations better done upstream.

---

## Indexing

**ClickHouse**: sparse primary index based on ORDER BY key. Skipping indexes (minmax, bloom filter). Per-column compression codecs.

**Pinot**: inverted index, sorted index, range index, bloom filter, JSON index, text index, FST index. **Star-tree index** for pre-aggregated rollup queries — uniquely powerful for fixed-dimension dashboards at high concurrency.

---

## Operations

**ClickHouse**: single binary, ZooKeeper or ClickHouse Keeper for distributed deployments. Simpler to run and maintain. Excellent managed offerings (ClickHouse Cloud, Altinity).

**Pinot**: four separate services (Controller, Broker, Server, Minion). More moving parts. Helix for cluster state. Heavier to operate.

---

## Cost Profile

For equivalent workloads, ClickHouse tends to be cheaper to run due to lower operational overhead and more efficient storage compression. Pinot's additional infrastructure (Minion jobs, separate realtime/offline servers) adds cost.

---

## When to Use ClickHouse

- Customer-facing dashboards with <100ms latency requirement
- Ad-hoc analysis with complex SQL
- Multi-table JOINs in analytical queries
- Unified system for both ingestion and query serving
- Simpler operations are a priority
- General OLAP workload

## When to Use Pinot

- Data must be visible in dashboards within seconds of production
- Very high query concurrency (thousands of QPS)
- Fixed-dimension aggregation dashboards (star-tree index shines)
- Anomaly detection on live data
- LinkedIn/Uber/Walmart-scale analytics where Pinot was originally built

---

## Typical Production Setup

Many platforms use ClickHouse for most workloads and only add Pinot when Kafka-native ingestion with sub-second freshness becomes a hard requirement:

```
Kafka → ClickHouse (5-30s latency, great SQL)   → Most dashboards
      → Pinot (< 5s latency, limited SQL)        → Real-time monitoring dashboards
```
