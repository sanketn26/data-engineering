# Technology Selection Framework

A structured approach to choosing data technologies based on workload properties — not trends.

---

## The Wrong Question

"What is the best tool for data engineering?"

This question has no answer. Every technology is the best choice for some workloads and the wrong choice for others.

## The Right Question

"What are the properties of my workload, and which tool was designed for those properties?"

---

## Step 1: Characterise the Workload

Answer these questions before looking at any tool:

### Volume
- How much data per day? (GB, TB, PB)
- How fast does it arrive? (rows/sec, MB/sec)
- How long do you need to retain it?

### Velocity
- What is the acceptable latency from event to query result?
  - Milliseconds → you need a streaming engine
  - Seconds → micro-batch or streaming
  - Minutes → micro-batch acceptable
  - Hours → batch is fine

### Access Pattern
- Read-heavy or write-heavy?
- Point lookups (single row by key) or analytical scans (aggregate millions of rows)?
- Predictable queries or ad-hoc exploration?
- Join-heavy or mostly single-table?

### Cardinality
- How many distinct values in your key dimensions?
- Per-user metrics? Per-request tracing? (High cardinality — avoid TSDBs)
- Service-level metrics? (Moderate cardinality — TSDBs work well)

### Mutability
- Append-only (events, logs) or update-heavy (CDC from OLTP)?
- Do you need to delete records (GDPR)?
- Do you need ACID transactions?

### Query Complexity
- Simple aggregations or complex multi-table JOINs?
- Graph traversal?
- Time-windowed aggregations?

---

## Step 2: Apply the Framework

### Ingestion Layer

| Requirement | Tool |
|-------------|------|
| High-throughput event streaming | Kafka |
| IoT sensor data | Kafka or MQTT → Kafka |
| CDC from databases | Debezium → Kafka |
| File-based batch | S3/GCS + Airflow |

### Processing Layer

| Requirement | Tool |
|-------------|------|
| Batch transformation, lakehouse writes | Spark |
| Sub-second streaming, complex state | Flink |
| Micro-batch streaming (minutes latency) | Spark Structured Streaming |
| Distributed ML, hyperparameter tuning | Ray |
| Workflow orchestration | Airflow |

### Storage Layer

| Requirement | Tool |
|-------------|------|
| Historical data lake, ACID, time travel | Iceberg |
| Frequent upserts, CDC pattern | Hudi |
| Delta Lake ecosystem, Unity Catalog | Delta |
| Key-value, high write throughput | Cassandra / DynamoDB |
| Document data | MongoDB |

### Query / Serving Layer

| Requirement | Tool |
|-------------|------|
| Dashboard queries <100ms | ClickHouse |
| Sub-second freshness (data from last 5s) | Pinot |
| Federated ad-hoc queries across systems | Trino |
| Infrastructure monitoring + alerting | Prometheus / VictoriaMetrics |
| High-cardinality time series analytics | ClickHouse |
| Graph traversal and pattern matching | Neo4j |
| SQL on time series, moderate scale | TimescaleDB |

---

## Step 3: Design for Failure

Every tool has failure modes. Before finalising your choice, ask:

- What happens when [tool] falls behind on ingestion?
- What happens when a node fails mid-processing?
- What happens when a bad schema change arrives?
- What happens when a query runs for 10x longer than expected?
- How do I replay or reprocess data if there is a bug?

If you cannot answer these questions for your chosen tool, you do not understand it well enough to run it in production.

---

## Common Architecture Patterns

### Real-Time Analytics (SaaS dashboards)
```
Events → Kafka → Flink (enrichment) → ClickHouse (dashboards)
                                     → Iceberg (history)
       → Spark (batch ETL) ──────────→ Iceberg
       → Trino (ad-hoc queries on Iceberg)
```

### Observability Platform
```
Metrics → Prometheus / VictoriaMetrics (alerting, Grafana)
Logs    → Kafka → ClickHouse (search, analysis)
Traces  → Kafka → ClickHouse (distributed trace analysis)
```

### E-Commerce / CDC
```
PostgreSQL → Debezium → Kafka → Flink (CDC processing) → Hudi (lakehouse)
                              → Kafka Streams (order state)
                              → ClickHouse (reporting)
```

### IoT / Time Series
```
Devices → Kafka → Flink (windows, anomaly detection) → TimescaleDB / ClickHouse
                → Spark (batch downsampling)         → S3 (long-term storage)
```

### Fraud Detection
```
Events → Kafka → Flink (real-time scoring, <200ms) → Alert
                → Spark (batch graph computation) → Neo4j (fraud ring detection)
                → ClickHouse (analyst investigation)
```
