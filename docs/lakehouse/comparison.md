# Choosing a Table Format

## The Wrong Question

> Which table format is best?

This question has no answer. The right question is:

> **Given my workload, what properties do I need from a table format?**

---

## The Decision Questions

Work through these questions:

### 1. What is the write pattern?

| Write Pattern | Implication |
|--------------|-------------|
| Append-only (Kafka ingestion, batch ETL) | All three formats work well |
| High-frequency upserts (CDC from OLTP) | Hudi (MoR) or Iceberg + Flink |
| Occasional updates, mostly reads | Delta Lake or Iceberg |
| Row-level deletes (GDPR compliance) | All three support it; Hudi is optimised for it |

### 2. Which engines need to read this table?

| Engine Access | Recommendation |
|--------------|----------------|
| Spark only | Delta Lake (native integration) |
| Spark + Trino + Flink | Iceberg |
| Spark + Trino | Both work; Iceberg has stronger Trino support |
| Databricks | Delta Lake |

### 3. Do you need streaming incremental reads?

Hudi's incremental read feature is the strongest. Iceberg supports incremental reads via snapshots. Delta Lake supports it via Change Data Feed.

### 4. How important is operational simplicity?

- **Simplest**: Delta Lake (minimal configuration, especially on Databricks)
- **Most features**: Iceberg (partition evolution, hidden partitioning, engine-agnostic)
- **Most complex for high-update workloads**: Hudi (CoW/MoR decisions, compaction tuning)

---

## Workload Scenarios

### SaaS Analytics Platform (append-heavy, multi-engine)

Kafka → Spark (append) → **Iceberg** → Trino (ad-hoc) + ClickHouse (dashboards)

Why Iceberg: Trino reads it natively, Spark writes natively, no updates required.

### E-Commerce CDC Pipeline

PostgreSQL → Debezium CDC → Kafka → **Hudi** (MoR) → Spark for analytics

Why Hudi: order records are frequently updated (status changes), record keys are natural (order_id), incremental reads needed downstream.

### Databricks-Centric Data Platform

Spark batch ETL → **Delta Lake** → Spark SQL for analytics

Why Delta: native Databricks integration, excellent Spark support, OPTIMIZE/VACUUM tooling.

---

## The Honest Summary

If you are starting fresh in 2024 and need multi-engine access: **Iceberg** is the safest choice.

If your stack is Databricks: **Delta Lake** wins for operational simplicity.

If your primary workload is CDC with high-frequency upserts: consider **Hudi**, but verify your team has the operational expertise.

All three are production-grade. The differences matter at the margins.
