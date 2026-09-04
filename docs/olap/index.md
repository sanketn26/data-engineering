# OLAP

## The Question

You have 500 billion rows of event data. An analyst types a SQL query. They expect results in seconds, not hours.

How?

---

## What This Module Covers

| Topic | What You Will Learn |
|-------|-------------------|
| [Why Columnar Storage](columnar-storage.md) | The physics of analytical queries |
| [ClickHouse](clickhouse.md) | MergeTree, ORDER BY, production operations |
| [Apache Pinot](pinot.md) | Real-time OLAP for user-facing analytics |

---

## The Core Insight

Analytical queries scan many rows but touch few columns. Row-oriented storage wastes most of its I/O reading columns the query doesn't need. Columnar storage reads only what is required.

This simple insight — combined with aggressive compression and vectorised execution — is what makes modern OLAP systems 100–1000× faster than row-oriented databases for analytical workloads.
