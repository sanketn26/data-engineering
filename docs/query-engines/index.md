# Query Engines

## The Problem

Data exists across many systems. Analysts need to query it without copying everything into one place.

---

## What This Module Covers

| Topic | What You Will Learn |
|-------|-------------------|
| [Trino](trino.md) | Federated queries, connectors, execution model |

---

## Federated Queries

A **query engine** like Trino separates the question "where does data live?" from "who processes the SQL?". The engine connects to data sources via connectors and executes queries across them in a distributed manner.

This is fundamentally different from databases like ClickHouse that own their data. Trino never stores data — it borrows it.
