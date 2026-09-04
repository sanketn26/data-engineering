# Comparisons

Direct technology comparisons — when to use each, what the trade-offs are, and how they fit together.

---

## Available Comparisons

### Processing Engines

- [Spark vs Flink](spark-vs-flink.md) — batch-first vs stream-first; when each applies
- [Spark vs Ray](spark-vs-ray.md) — data transformation vs distributed Python

### Query and OLAP Engines

- [ClickHouse vs Pinot](clickhouse-vs-pinot.md) — OLAP engine trade-offs
- [ClickHouse vs Trino](clickhouse-vs-trino.md) — storage-coupled vs federated query

### Storage Paradigms

- [TSDB vs OLAP](tsdb-vs-olap.md) — time series databases vs analytical engines
- [Graph vs Relational](../graph/graph-vs-relational.md) — when graph modelling outperforms SQL

---

## How to Use These Pages

Each comparison follows the same structure:

1. What problem each tool solves
2. Where they differ technically
3. Concrete workload-based decision rules
4. Where they complement each other

The goal is not to pick a winner. In production, most platforms use multiple tools from each category. The goal is to understand *which workload belongs where*.

---

## A Note on "vs" Framing

Most "ClickHouse vs Trino" questions in practice become "ClickHouse for dashboards, Trino for ad-hoc" after five minutes of analysis. The tools often serve different layers of the same system.

When you find yourself asking "which one should I use", the better question is usually: "which workload properties does this query have, and which tool was designed for those properties?"
