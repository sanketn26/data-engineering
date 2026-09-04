# ClickHouse vs Trino

ClickHouse and Trino are both analytical query engines. They are not competing choices — they solve different problems. The mistake is using one where the other belongs.

---

## The Core Difference

**ClickHouse** is a columnar storage + query engine. It owns and manages its data. Its performance comes from how it stores data: sorted, compressed, with a sparse primary index.

**Trino** is a query engine only. It has no storage. It queries data wherever it lives: S3 (Iceberg, Hudi, Delta), Kafka, PostgreSQL, ClickHouse, MySQL — and joins across them.

ClickHouse: fast because of *how it stores*.
Trino: flexible because it *doesn't store anything*.

---

## Architecture

**ClickHouse**:
```
Data → ClickHouse storage (MergeTree parts on local disk/S3)
     → ClickHouse query engine reads its own storage
```

**Trino**:
```
Query → Trino Coordinator → splits work across Workers
                          → Workers call Connectors
                          → Connectors read from Iceberg/Kafka/Postgres/ClickHouse
```

---

## Query Performance

| Scenario | ClickHouse | Trino |
|----------|-----------|-------|
| Single-table scan on ClickHouse data | **Extremely fast** | Slower (network hop) |
| Cross-system JOIN (Iceberg + Postgres) | Not possible | Yes |
| Complex SQL on Iceberg (lakehouse) | Via S3 connector (slower) | **Native, optimised** |
| Ad-hoc queries on cold data | Fast | Slower without caching |
| Dashboard queries (<100ms) | **Excellent** | Adequate with caching |

---

## What ClickHouse Cannot Do

- Query data stored in Iceberg on S3 as efficiently as its own storage
- Join ClickHouse data with a Postgres table in a single query
- Query data in Kafka directly without ingesting it first
- Act as a data catalogue or virtual query layer across systems

---

## What Trino Cannot Do

- Match ClickHouse's latency on ClickHouse-native storage
- Persist data (it always reads from source)
- Serve as a low-latency dashboard engine without a caching layer in front

---

## Use Cases

**Use ClickHouse for**:
- Customer-facing dashboards requiring <100ms latency
- Real-time aggregations over recently ingested event data
- Pre-aggregated metrics tables with fast GROUP BY
- High-concurrency query serving

**Use Trino for**:
- Ad-hoc analytics across the data lake (Iceberg on S3)
- Federated queries joining Iceberg data with operational databases
- Exploratory analysis by data scientists who need SQL access to all data
- ETL: reading from Iceberg and writing transformed results back

---

## In a Typical Production Stack

```
Kafka → Flink → ClickHouse (real-time dashboards, <100ms)

                Iceberg on S3 (data lake, full history)
                    ↑
              Spark writes
                    ↓
              Trino (ad-hoc queries, cross-table joins, data science)
```

ClickHouse and Trino are not competitors here. They are complementary:
- ClickHouse serves the product dashboards
- Trino serves the data scientists and analysts running ad-hoc SQL

---

## When You See a Trade-Off

Sometimes you can query ClickHouse data through Trino (via the ClickHouse connector). This gives you flexibility at the cost of latency. The ClickHouse connector in Trino is useful for ad-hoc joins of ClickHouse data with Iceberg data, but it is not suitable for low-latency dashboard serving.

Similarly, ClickHouse has an S3-compatible storage backend and can query some Iceberg tables, but it is not a general-purpose federated query engine.

**Use the right tool for the workload, not the tool that can technically do both.**
