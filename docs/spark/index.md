# Apache Spark

!!! info "Version and source policy"
    Examples target the pinned lab baseline. Check [Versions & Primary Sources](../reference/version-matrix.md) before applying configuration to another Spark release.

09:12 AM. The nightly SaaS rollup — p95 latency per `service` per hour — has been running for three hours against a normal time of eleven minutes. The Spark UI shows 39 of 40 executors idle and one core pegged at 100%. Nothing crashed. Nothing logged an error.

A. Add executors — the cluster is under-provisioned.
B. One shuffle partition owns `cust_0042` (38% of the day's volume), and no executor count fixes a single hot key.
C. The join to the 30 MB tenant dimension is shuffling 5 TB instead of broadcasting.
D. The driver, not the executors, is the bottleneck.

Pick one before reading on — this module exists to make that call automatic instead of a guess. Here is the shape of the data behind it:

```text
{timestamp, customer_id, user_id, service, endpoint, region, latency_ms, status_code, bytes}
```

5 TB of Parquet. You need p95 latency per `service` per hour, plus a join to a 30 MB tenant dimension. A 32-core box reading at 500 MB/s spends ~3 hours **on the scan**, before the `groupBy`, before `cust_0042` (38% of volume) sits down on a single core.

Spark is a coordination layer for that problem: **partition the scan, pipeline narrow work, shuffle only when keys must meet, retry tasks when a node dies.** It is not a database, not a stream processor of last resort, and not the right tool for a 2 GB CSV.

This module is the first *implementation* of the mechanics from [Phase 0](../foundations/index.md) and [Phase 1](../foundations/parquet-internals.md). If shuffle still feels like a metaphor, stay in foundations.

---

## Map of this module

| Lesson | What it is really about |
|--------|-------------------------|
| [The Spark Mental Model](mental-model.md) | Driver vs executors, lazy DAGs, narrow vs wide, what an action costs |
| [The Shuffle](shuffle.md) | Write, fetch, read, serialisers, spill, partition math, salting, AQE, broadcast RAM |
| [Catalyst & Tungsten](optimizer.md) | Why `explain()` is part of the job; AQE; codegen |
| [Production Gotchas](gotchas.md) | Driver/executor OOM, small files, UDFs, cache, speculation, dynamic allocation |
| [Labs](labs.md) | Run it, watch `:4040`, then **break** it (skew, `collect`, 10k partitions) |

Keep the SaaS event in your head. Observability, CDC, IoT, and fraud show up when the access pattern changes (high cardinality, changelogs, tiny rows, graph-shaped joins).

---

## The central intuition

Divide into partitions. Run a **task** per partition. When the answer needs two partitions to meet — `groupBy`, `join`, `distinct`, global `sort` — **shuffle**. The shuffle is the bill. Minimise it, then make the remainder even.

```mermaid
flowchart LR
    R["Read splits"] --> N["Narrow: filter, project"]
    N --> W["Wide: exchange"]
    W --> A["Agg / join"]
    A --> O["Write"]
```

Spark SQL (DataFrames) is the path Catalyst can see. RDDs are a trap unless you are building an engine. Structured Streaming is **micro-batch Spark**, not Flink. If you need event-time state at 200 ms, you are in the [Flink module](../flink/index.md).

---

## When *not* to use Spark

!!! danger "Spark is a distributed runtime with a startup tax"
    A 40-executor job that processes 80 MB is a scheduling demonstration, not a pipeline.

| Situation | Prefer | Why Spark is the wrong hammer |
|-----------|--------|-------------------------------|
| ≤ tens of GB, SQL, one box | DuckDB, Polars, Postgres, ClickHouse | No shuffle, no driver, interactive |
| Ad-hoc over the lake by analysts | [Trino](../query-engines/trino.md) / Athena | App lifecycle and JARs vs a SQL session |
| Sub-second dashboards | [ClickHouse](../olap/clickhouse.md) / Pinot | Spark jobs are minutes, not 50 ms |
| 200 ms fraud / CEP | [Flink](../flink/index.md) | Spark SS trigger + shuffle ≫ 200 ms |
| Point gets / CDC apply of one row | KV / the OLTP source | Full scans in costume |
| Python ML with irregular tasks | [Ray](../distributed-python/ray.md) | See [Spark vs Ray](../comparisons/spark-vs-ray.md) |
| 10 GB/day startup | Airflow + SQL | Cluster ops will exceed the work |

Use Spark when **working set and shuffle are large**, the job is **tabular**, the SLA is **minutes to hours**, and you want **retries at task granularity**. Nightly SaaS rollups, CDC-to-Iceberg MERGE at scale, feature backfills: yes.

!!! tip "Two Spark products people conflate"
    **Spark SQL / batch** is this module. **Structured Streaming** is micro-batch on the same engine — useful for 1–5 minute marts, a poor 200 ms fraud path. See [Batch vs Stream](../foundations/batch-vs-stream.md) before you `readStream` the SaaS topic “because Kafka.”

Staff-level question the rest of the pages train: *which bytes move, which key is hot, which JVM dies first?* If you cannot answer on a whiteboard, config will not save the job.

---

## What Spark actually is (engineering level)

Three layers you will debug separately:

1. **Language API** — DataFrame / SQL (use this), RDD (avoid), pandas API on Spark (measure twice).
2. **Catalyst** — logical plan → optimised logical → physical. Predicate pushdown, column pruning, join selection. [Optimizer](optimizer.md).
3. **Tungsten + scheduler** — whole-stage codegen, off-heap rows, stages/tasks, shuffle service.

PySpark 3.x: the JVM still does the SQL. Python is for UDFs and the driver. That split is the source of [UDF gotchas](gotchas.md) and `memoryOverhead` kills.

Config you should be able to justify before production (all appear in later pages):

```python
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark.conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes", str(128 * 1024 * 1024))
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", str(64 * 1024 * 1024))
spark.conf.set("spark.sql.shuffle.partitions", "400")  # starting guess; AQE coalesces
```

If you cannot explain each line’s blast radius (especially broadcast × executor count), do not paste them.

Cluster shape:

```text
Driver  →  cluster manager (YARN / K8s / standalone)  →  executors (cores × memory)
Spark UI on the driver; History Server after the app dies
```

If the driver is a laptop on VPN, you do not have a production job. You have a hobby.

---

## Running workload (reuse this schema)

```json
{
  "timestamp": "2024-01-15T10:30:00Z",
  "customer_id": "cust_0042",
  "user_id": "user_98712",
  "service": "api-gateway",
  "endpoint": "/v2/events",
  "region": "eu-west-1",
  "latency_ms": 45,
  "status_code": 200,
  "bytes": 1024
}
```

~10 million events/day at the small end; TBs when the enterprise tenants land. **Skew is the plot.** If your lab data is uniform `customer_id`, you are not studying this platform.

IoT cousin: same Spark, different poison (millions of tiny files, time prune). CDC cousin: `MERGE` into Iceberg, not `append` of duplicates. Fraud cousin: Spark for **offline** features; online path is not Spark.

---

## Defaults that will betray you

Spark 3.x is usable in production only after you treat these as **suspect**, not as wisdom:

| Default | Typical value | Why it is wrong on this workload |
|---------|---------------|----------------------------------|
| `spark.sql.shuffle.partitions` | 200 | 200 × 10 MB = waste; 200 × 50 GB = OOM |
| `spark.sql.autoBroadcastJoinThreshold` | 10 MB | Fine until the dim is 80 MB and you SMJ a TB; lethal if you “fix” it to 2 GB |
| AQE | on in many distros, off in others | **Check the Environment tab.** Do not assume. |
| `spark.serializer` | Java (RDDs) | SQL uses Tungsten anyway; Kryo still matters for closures/RDDs |
| Python UDF comfort | “it works” | JVM ↔ Python is a hidden shuffle |
| Local `SparkSession` on a laptop as driver | convenient | VPN blip kills the app; `collect` is one cell away |

The [labs](labs.md) exist to make those defaults visible at 1e6 rows so you believe them at 2 TB.

---

## How to read a Spark job in production

Before you add executors:

1. `explain("formatted")` — count `Exchange`, confirm `PushedFilters`, join type.
2. Spark UI **SQL** tab — actual vs expected (AQE rewrites what you explained).
3. Slow **stage** — shuffle bytes, spill, task max/median.
4. Config that is **actually** set (`Environment` tab): AQE, broadcast threshold, shuffle partitions, DA, speculation.

If max/median task time > 5, go to [Shuffle](shuffle.md) and [Partitioning](../foundations/partitions.md), not to the AWS console.

A one-screen incident card:

```text
action: write parquet date=2024-01-15
stages: 2 (scan+partial agg | final agg)
R: 200 configured / 48 after AQE
shuffle read: 1.8 TB  spill disk: 420 GB  (BAD)
task max/median: 47 min / 3 min          (SKEW)
join: SortMergeJoin customers 400 MB     (should project + BHJ or skew-join)
files out: 200 × date × region           (SMALL FILES tomorrow)
```

If the team cannot fill that card, they are not ready to tune executor count.

---

## How this module expects you to work

- Run the [labs](labs.md) on a local `SparkSession`. Docker is optional.
- Change **one** variable (partitions, skew, broadcast) and predict the UI.
- Prefer native functions over Python UDFs.
- British spelling in these pages matches the rest of the academy (optimise, serialise). The APIs do not: `serialize` in JVM configs stays American.

When you can explain why `groupBy("customer_id")` on this schema produces one 40-minute task, you are done with Spark-the-tool and ready to use Spark-the-platform.

**Suggested order:** [mental model](mental-model.md) → [shuffle](shuffle.md) (do not skip) → [optimizer](optimizer.md) → [gotchas](gotchas.md) → [labs](labs.md) last, with the UI open.

Next: [The Spark Mental Model](mental-model.md).
