---
title: Practice map
description: A guided bridge from each core concept to its simulation, runnable lab, and production incident.
---

# Practice map

Practice should arrive when an idea is ready to become visible, not after you
have finished the entire course. Use the shortest loop that answers your current
question:

> **Read the picture → predict → simulate → run the real system → diagnose**

You do not need every column on every visit. A simulation is enough for a first
pass. Add the runnable lab when you want to inspect real metrics, and the
incident when you want to practise deciding what those metrics mean.

## The core practice loops

| Idea | Read | Make it visible | Run it | Diagnose |
|---|---|---|---|---|
| Why one key becomes one bottleneck | [Partitioning](foundations/partitions.md) | [Kafka partitions](simulations/kafka-partitions.html) | [Kafka lab](labs/index.md#kafka-labskafka) | [Kafka lag](incidents/index.md#incident-1-kafka-lag-on-one-partition) |
| Why distributed grouping moves bytes | [Spark shuffle](spark/shuffle.md) | [Shuffle visualiser](simulations/spark-shuffle.html) | [Spark lab](labs/index.md#spark-labsspark) | [Skewed join OOM](incidents/index.md#incident-2-spark-executor-oom-on-a-skewed-join) |
| Why event time can stop | [Flink time](flink/time.md) | [Watermark simulator](simulations/watermark-simulator.html) | [Flink lab](labs/index.md#flink-labsflink) | [Stalled watermark](incidents/index.md#incident-3-flink-watermark-stalled-no-output) |
| Why physical order changes query cost | [ClickHouse](olap/clickhouse.md) | [ORDER BY explorer](simulations/clickhouse-order-by.html) | [ClickHouse lab](labs/index.md#clickhouse-labsclickhouse) | [Slow query](incidents/index.md#incident-4-clickhouse-query-10-slower) |
| Why label choices consume memory | [Cardinality](time-series/cardinality.md) | [Cardinality calculator](simulations/cardinality-calculator.html) | [Time-series lab](labs/index.md#time-series-labstime-series) | Use the lab's head-series metric |
| Why a partition key is a capacity decision | [Cassandra](databases/cassandra.md) | [Consistent hashing](simulations/consistent-hashing-visualizer.html) | [Cassandra lab](labs/index.md#cassandra-labscassandra) | Use the lab's partition histogram |

## Focused simulation loops

These simulations do not need a full service running locally. Each isolates one
mechanism so you can change one variable and see the consequence.

| Mechanism | Read first | Simulation | Stop when you can explain |
|---|---|---|---|
| Queue growth and recovery | [Backpressure](foundations/backpressure.md) | [Backpressure calculator](simulations/backpressure-calculator.html) | Why matching arrival rate stops growth but does not drain the backlog |
| Replica acknowledgements | [Kafka replication](kafka/replication.md) | [ISR failure simulator](simulations/kafka-isr-simulator.html) | Why `acks=all` depends on the current ISR and `min.insync.replicas` |
| Snapshot metadata and pruning | [Iceberg](lakehouse/iceberg.md) | [Manifest explorer](simulations/iceberg-manifest-explorer.html) | How a reader finds the current table without listing object storage |
| Row-group pruning | [Parquet internals](foundations/parquet-internals.md) | [Row-group explorer](simulations/parquet-row-group-explorer.html) | Why sorted data produces useful min/max ranges |
| Probabilistic skipping | [Columnar storage](olap/columnar-storage.md) | [Bloom-filter playground](simulations/bloom-filter-playground.html) | Why false positives waste reads but never remove correct rows |
| DynamoDB hot keys | [DynamoDB](databases/dynamodb.md) | [Hot-key simulator](simulations/dynamodb-hot-key-simulator.html) | Why table capacity cannot rescue one overloaded partition key |
| Slowly changing dimensions | [Data modelling](foundations/data-modelling.md) | [SCD2 timeline](simulations/scd2-timeline-explorer.html) | Which version is valid for an event at a particular time |

## A 30-minute practice session

1. Read through **Build the mental picture** on the linked concept page.
2. Write one prediction before opening the simulation.
3. Change one input only and explain the changed output.
4. Return to **Check your understanding** on the concept page.
5. Stop, or schedule the runnable lab as a separate session.

## A 60-minute lab session

1. Reproduce the simulation's safe case in the real service.
2. Run the lab's **Break** step and watch the named metric.
3. Run the automated check; a `PASS` proves the intended condition occurred.
4. Open the paired incident and rank hypotheses before reading its resolution.
5. Write one sentence connecting the design choice to the observed signal.

Start small: [Kafka partitions](simulations/kafka-partitions.html) is the easiest
first simulation, and the [Kafka lab](labs/index.md#kafka-labskafka) is the most
direct first system lab.
