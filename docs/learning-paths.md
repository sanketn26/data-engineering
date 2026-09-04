---
title: Learning paths
description: Role-based routes through the academy — foundations, Staff data platform, streaming on-call, lakehouse, and interview sprint.
---

# Learning paths

Do not try to read every page in order. Pick a route with a finish line. A session is 60–90 minutes: required reading, one prediction or lab, and one exit check. Optional depth never blocks progress.

!!! note "How to use a path"
    For each listed page: predict, read, run the required lab or simulation, then explain the idea in one paragraph without looking. If the exit check fails, use the [recovery loop](how-to-study.md#when-an-exit-check-does-not-pass) and revisit it after the adjacent lesson.

## Core path — six weeks

This is the shortest complete route. Check off evidence, not page views.

| Week | Required sessions | Deliverable |
|---:|---|---|
| 1 | Scale, partitioning, data modelling | Workload table + grains/keys |
| 2 | Kafka log/partitions, CDC | Produce/consume lab + CDC handoff |
| 3 | Spark mental model/shuffle, transformations | Spark UI screenshot + incremental model |
| 4 | Flink time/state, Airflow idempotency | Watermark model + replay plan |
| 5 | Lakehouse, Trino/warehouse, ClickHouse | Storage/query decision record |
| 6 | Quality, security, cost, architecture | [Capstone](capstone.md) scored ≥15/18 |

Progress evidence:

- [ ] Quantified requirements
- [ ] Declared grains, keys, and time semantics
- [ ] Replay and reconciliation plan
- [ ] Lab evidence captured
- [ ] Failure runbook exercised
- [ ] Unit-cost worksheet
- [ ] Capstone reviewed at 15/18 or better

## Foundations first

**Who:** senior engineers who have used Spark or Kafka as a library and have never had to explain a shuffle or an ISR.

| Order | Page | Exit criterion |
|------:|------|----------------|
| 1 | [Start here](start-here.md) | You can state the academy objective in your own words |
| 2 | [Data at scale](foundations/scale.md) | You can say what breaks at 100 GB vs 10 TB vs 1 PB/day |
| 3 | [Partitioning](foundations/partitions.md) | You can design a key and name the skew failure |
| 4 | [Data modelling](foundations/data-modelling.md) | Every fact has a grain and history policy |
| 5 | [Distributed execution](foundations/distributed-execution.md) | Job → stage → task is automatic |
| 6 | [The log](kafka/log.md) | You can explain why a log is not a queue and not a database |
| 7 | [CDC](foundations/cdc.md) | Snapshot and stream meet without lost updates |
| 8 | [The shuffle](spark/shuffle.md) | You can draw shuffle write/read and a straggler |
| 9 | [Transformation engineering](foundations/transformation-engineering.md) | Incremental equals full rebuild |
| 10 | [Batch vs stream](foundations/batch-vs-stream.md) | You can pick one for a given latency and correctness need |

Then continue into Kafka partitions and Spark gotchas.

## Staff data platform

**Who:** people who will be asked to own the stack, not a job.

Follow Foundations first, then:

| Order | Page | Why it is on the path |
|------:|------|------------------------|
| 1 | [Kafka replication](kafka/replication.md) | Durability is a replica and ack story |
| 2 | [Exactly-once](kafka/exactly-once.md) | EOS is a protocol, not a checkbox |
| 3 | [Iceberg](lakehouse/iceberg.md) | A table on object storage is metadata |
| 4 | [Trino](query-engines/trino.md) | Federation and why the coordinator dies |
| 5 | [ClickHouse](olap/clickhouse.md) | Dashboard latency is physical design |
| 6 | [Airflow idempotency](airflow/idempotency.md) | Orchestration is not processing |
| 7 | [Quality](quality/index.md) | Green jobs, wrong numbers |
| 8 | [Cost engineering](reference/cost-engineering.md) | Unit cost, not just uptime, is what you own |
| 9 | [Platform delivery](platform-delivery.md) | CI/CD and IaC for a data platform, not a webapp |
| 10 | [SaaS analytics architecture](architectures/analytics-platform.md) | Put it on one diagram |
| 11 | [Selection framework](reference/selection-framework.md) | Workload chooses the tool |

## On-call streaming

**Who:** you get paged for lag, stalled jobs, or “the dashboard is empty.”

| Order | Page | Incident it prepares |
|------:|------|----------------------|
| 1 | [Kafka partitions](kafka/partitions.md) | Hot partition, consumer lag on one p |
| 2 | [Kafka gotchas](kafka/gotchas.md) | Rebalance storms, poison pills |
| 3 | [Spark gotchas](spark/gotchas.md) | Executor OOM, skew join |
| 4 | [Time semantics](flink/time.md) | “Why did the window never close?” |
| 5 | [Watermarks](flink/windows.md) | Idle source, late events |
| 6 | [Checkpoints](flink/checkpoints.md) | Recovery that replays too far |
| 7 | [Incidents](incidents/index.md) | Full drills with telemetry |

Run the Kafka, Spark, and Flink labs. Do not skip the “now break it” steps.

## Lakehouse and analytics

**Who:** warehouse / lake migrations, “should we use Iceberg or Delta,” dashboard SLAs.

!!! note "Only ClickHouse has a hands-on lab here"
    Iceberg, Hudi, Delta, Trino, and Pinot have no Compose lab in [labs/](labs/index.md) yet — for those, this path is reading plus [simulations](simulations/index.md) only. Run the [ClickHouse lab](labs/index.md) for hands-on time in this path.

1. [Why table formats exist](lakehouse/why-table-formats.md)
2. [Iceberg](lakehouse/iceberg.md), [Hudi](lakehouse/hudi.md), [Delta](lakehouse/delta.md)
3. [Choosing a table format](lakehouse/comparison.md)
4. [Columnar storage](olap/columnar-storage.md)
5. [ClickHouse](olap/clickhouse.md) vs [Pinot](olap/pinot.md) vs [Trino](query-engines/trino.md)
6. [ClickHouse vs Pinot](comparisons/clickhouse-vs-pinot.md), [ClickHouse vs Trino](comparisons/clickhouse-vs-trino.md)
7. [Observability architecture](architectures/observability.md) or [SaaS analytics](architectures/analytics-platform.md)

## Interview / design sprint (two weeks)

Not an interview-prep clone. Use this if you need to talk about data platforms in a Staff interview quickly.

**Week 1:** [Scale](foundations/scale.md), [Partitions](foundations/partitions.md), [Shuffle](spark/shuffle.md), [Log](kafka/log.md), [Replication](kafka/replication.md), [Event time](flink/time.md).

**Week 2:** [Iceberg](lakehouse/iceberg.md), [ClickHouse](olap/clickhouse.md), [Selection framework](reference/selection-framework.md), then **one** architecture page covered and re-derived: [Observability](architectures/observability.md) or [Fraud](architectures/fraud.md).

Protocol: cover the solution, write requirements, V1, bottleneck, V2, failure modes. Never start from the finished diagram.

## Specialised stores

Jump here only after you can explain why a warehouse is the wrong tool for the access pattern.

- Time series: [cardinality](time-series/cardinality.md), [downsampling](time-series/downsampling.md), [TSDB vs OLAP](comparisons/tsdb-vs-olap.md)
- NoSQL: [NoSQL thinking](databases/nosql.md), [Cassandra](databases/cassandra.md), [DynamoDB](databases/dynamodb.md)
- Graph: [graph thinking](graph/graph-thinking.md), [modelling](graph/graph-modelling.md), [vs relational](graph/graph-vs-relational.md)
- Distributed Python: [Ray](distributed-python/ray.md), [Spark vs Ray](comparisons/spark-vs-ray.md)
