# Data Engineering Academy

**You don't need to memorise more tools. You need to understand why they exist.**

---

Most data engineering learning resources teach you *what* Kafka, Spark, Flink, and ClickHouse do. This academy teaches you *why* they were built, *how* they actually work, and *when* they will fail you.

The goal is not a list of facts. The goal is engineering intuition.

---

## What You Will Walk Away With

After completing this academy, given an unfamiliar data problem, you should be able to:

- Identify the fundamental constraints (volume, velocity, latency, access pattern)
- Reason toward an appropriate architecture without memorising a predefined stack
- Explain *why* each component exists, not just *what* it does
- Predict where a system will fail before it does
- Debug a production incident you have never seen before
- Justify every technology choice through workload, not fashion

---

## Who This Is For

- Experienced Data Engineers who want to go deeper
- Senior Software Engineers moving into data infrastructure
- Platform Engineers supporting data workloads
- ML Engineers working with data pipelines
- SREs responsible for data platform reliability
- Engineers preparing for Staff-level data responsibilities

**You should already know:** Python, SQL, Linux basics, Docker, basic databases, basic cloud concepts, Git.

**You will learn:** distributed data concepts from first principles.

---

## The Teaching Loop

Every major lesson follows this sequence:

```
USE CASE → WHY → INTUITION → WHAT → INTERNALS → HOW → GOTCHAS → APPLY
```

You will not encounter a concept without first understanding *why it was necessary to invent it*.

---

## Running Use Cases

Throughout the academy, five fictional production systems evolve to illustrate real-world trade-offs:

| System | Technologies |
|--------|-------------|
| **SaaS Analytics Platform** — millions of users generating product events | Kafka → Spark → Iceberg → Trino → ClickHouse |
| **Security / Observability Platform** — billions of logs, metrics, traces | Kafka → Flink → ClickHouse → Pinot |
| **E-Commerce Platform** — orders, payments, clickstream, recommendations | CDC → Kafka → Lakehouse → Graph DB |
| **IoT Platform** — millions of devices sending sensor readings | Kafka → TSDB → downsampling → object storage |
| **Fraud Detection** — user–device–IP–transaction relationships | Kafka → Flink → Graph DB → ClickHouse |

These systems reappear throughout every module. The same event schema flows through Spark, Kafka, ClickHouse, Iceberg, and Neo4j so you can see how the *same workload looks different from different engines*.

---

## How to Navigate

→ [Start Here](start-here.md) — understand the big picture before diving in
→ [How to Study](how-to-study.md) — how to get the most out of this material
→ [Phase 0: Foundations](foundations/index.md) — build the mental models everything else depends on

Or jump directly to a technology:

[Spark](spark/index.md) · [Kafka](kafka/index.md) · [Flink](flink/index.md) · [Airflow](airflow/index.md) · [Iceberg](lakehouse/iceberg.md) · [Trino](query-engines/trino.md) · [ClickHouse](olap/clickhouse.md) · [Pinot](olap/pinot.md) · [Ray](distributed-python/ray.md) · [Neo4j](graph/neo4j.md)
