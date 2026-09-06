---
description: The on-call list for Spark at scale — driver OOM from collect, exploding broadcast joins, small-file storms, and Python UDF memory overhead.
---

# Production Gotchas

03:00 AM: the driver process for last night's SaaS rollup got OOM-killed four minutes in. The same code ran fine on last week's smaller batch. Nothing in the diff touched memory settings.

A. `events.groupBy("customer_id").count().toPandas()` — cardinality is 50 million.
B. The broadcast dimension grew from 8 MB to 900 MB and nobody noticed.
C. `.cache()` on the full 2 TB frame is stealing execution memory.
D. A Python UDF's `memoryOverhead` was never budgeted.

All four are real, and all four show up in the diff below — that is the point. The SaaS p95 job that worked on 20 GB will fail in stereotyped ways at 2 TB: the driver will eat a `collect`, one executor will eat `cust_0042`, the lake will eat a million 2 MB files, and a Python UDF will eat the NIC between JVM and pandas. These are not “Spark quirks.” They are the [mental model](mental-model.md) plus [shuffle](shuffle.md) plus [execution](../foundations/distributed-execution.md) showing up as pages.

This page is the on-call list. For each item: what the SaaS (or CDC / IoT) job looked like, why it is lethal at scale, how to see it, how to fix it.

---

## Start with the situation { #use-case }

A typical production submit that looks grown-up in Git:

```python
events = spark.read.json("s3://raw/events/")          # inference
joined = events.join(customers, "customer_id")         # customers grew
flagged = joined.withColumn("kind", py_udf("endpoint"))
flagged.cache()
n = flagged.count()
pdf = flagged.groupBy("customer_id").count().toPandas()
flagged.write.partitionBy("date", "region", "service").parquet("s3://out/")
```

Every line is a known incident class. We will walk them in the order they usually page you: **memory (driver, executor)**, **files**, **CPU/Python**, **cache**, **scheduler (speculation, dynamic allocation)**.

---

## Why the obvious approach breaks at scale { #why-this-is-hard-at-scale }

On a sample, `collect` is 200 rows, `partitionBy` is 3 files, the UDF is 0.4 s, cache “makes the second count fast.” At 2 TB:

- Driver heap is **one machine**.
- Executor heap is **one partition at a time**, but Python RSS is extra.
- S3 LIST/GET is **per object**.
- Cache is **not** extra RAM; it is RAM stolen from shuffle.
- Speculative copies **double** a skewed task.
- Dynamic allocation **deletes shuffle files** if you skip the shuffle service.

The failure is often **retries until SLA miss**, not a clean stack trace.

---

## Build the mental picture { #intuition }

| Symptom | First suspect |
|---------|----------------|
| App dies before tasks run | Driver (collect, broadcast, listing) |
| One task red, others green | Skew / fat partition / UDF on whale |
| All tasks slow, CPU low | UDF, S3, shuffle fetch |
| Tomorrow’s job 20× slower | Small files you wrote today |
| Random `FetchFailed` | Dynamic allocation / lost executors |
| “We cached it” still slow | Cache evicted or spilled |

---

## Internals (the failure mechanisms)

### Driver vs executor memory

- **Driver:** plans, file indexes, **broadcast bytes**, **all `collect`/`toPandas`/`take` that you thought were small**.
- **Executor JVM:** `spark.executor.memory` × `spark.memory.fraction` for execution+storage.
- **Executor overhead:** off-heap, **Python workers**, native libs. K8s/YARN cgroup kills when JVM + overhead + python > limit.

PySpark OOMs that “don’t show in Spark UI heap” are almost always **overhead / Python**.

### Small files

Write path: `R` shuffle partitions × **Hive partition directories**.

```text
365 days × 12 regions × 80 services × 200 tasks  →  millions of objects
```

Read path: driver lists them; S3 rate-limits; 8 MB of data in 4 000 files is a **metadata** job.

### UDF boundary

Native expr: Tungsten loop.  
**Python UDF:** per row (or per batch if pandas) JVM ↔ Python.  
**pandas UDF / `mapInPandas`:** Arrow batches — better, still not Parquet-native.

### Cache

`MEMORY_AND_DISK` will **spill** the cache. Spilled cache can be **slower** than re-reading Parquet from S3 (sequential, columnar, encoded).

### Speculation

Duplicates tasks slower than `spark.speculation.multiplier` × median. Good for **bad hardware**. Bad for **bad keys** and for **non-idempotent** sinks.

### Dynamic allocation

Executors come and go with load. Map output lives on the executor **unless** an **external shuffle service** (or shuffle tracking + service) keeps it. Without that, reducers fetch ghosts.

---

## How (fixes that are real PySpark 3.x)

### 1. Driver OOM — `collect` / `toPandas` / accidental wide agg

```python
# DANGEROUS — 2 TB to the driver
events.collect()
events.toPandas()

# DANGEROUS if customer_id cardinality is 50e6
events.groupBy("customer_id").count().toPandas()
```

**Fix:** `write` the result. `take(100)`, `show()`, `limit`. For pandas, **aggregate until it is a mart**.

```python
mart = events.groupBy("service", "region").count()
mart.write.mode("overwrite").parquet("s3://marts/service_region/")
# local analysis: spark.read.parquet(...)  # or download the mart, not the lake
```

!!! danger "Notebook `display(df)` on Databricks/EMR Studio"
    Often a hidden `collect` of a sample that becomes a full collect when someone “turns off the limit.” Treat UI displays as actions.

### 2. Executor OOM — fat partitions, explosions, broadcasts

Causes: \(R\) too small; whale key; `join` fan-out (`orders` 1:N `payments` 1:N `items`); broadcast dim that grew; `collect_list` of a tenant.

```python
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark.conf.set("spark.sql.shuffle.partitions", "800")  # or AQE advisory 128 MB
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", str(64 * 1024 * 1024))
# never:
# spark.conf.set("spark.sql.autoBroadcastJoinThreshold", str(2 * 1024**3))
```

PySpark:

```python
spark.conf.set("spark.executor.memory", "16g")
spark.conf.set("spark.executor.memoryOverhead", "4g")  # Python UDFs / Arrow
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
```

Check join cardinality **before** the write:

```python
from pyspark.sql import functions as F
events.groupBy("customer_id").count().agg(F.max("count"), F.avg("count")).show()
left.groupBy("order_id").count().filter("count > 1").count()
```

### 3. Small files

```python
# BOMB
events.write.partitionBy("date", "region", "service").parquet("s3://output/")

# Control files: partition lake on date only; compact
(
    events
    .repartition(64, "date")
    .write
    .mode("append")
    .partitionBy("date")
    .option("maxRecordsPerFile", 2_000_000)
    .parquet("s3://output/")
)
```

Better: Iceberg/Delta `OPTIMIZE` / compaction. Target **128–512 MB** files. See [Lakehouse](../lakehouse/index.md).

### 4. Python UDF vs pandas UDF vs native

```python
from pyspark.sql.functions import udf, pandas_udf, when
from pyspark.sql.types import StringType
import pandas as pd

# Native — first choice
events.withColumn(
    "kind",
    when(F.col("endpoint").startswith("/api/internal"), "internal").otherwise("external"),
)

# Scalar Python UDF — last choice (row-wise JVM↔Python)
@udf(StringType())
def classify(ep):
    return "internal" if ep and ep.startswith("/api/internal") else "external"

# pandas UDF — batch Arrow; still Python
@pandas_udf(StringType())
def classify_pd(ep: pd.Series) -> pd.Series:
    return ep.str.startswith("/api/internal").map({True: "internal", False: "external"})
```

Iterator / `mapInPandas` for heavier ML. **Never** on the inner loop of a 2 TB join if `when`/`regexp_extract` will do.

### 5. Cache abuse

```python
# Wrong: cache the lake
events.cache()

# Right: cache a *small* reused dim or an expensive intermediate that fits
dim = spark.read.parquet("s3://dims/services/").cache()
dim.count()
# ... several joins against dim
dim.unpersist()
```

Storage tab: **fraction cached** < 1 means you are lying to yourself. If execution memory is spilling while cache holds 80% of the heap, `unpersist`.

### 6. Speculative execution

```python
spark.conf.set("spark.speculation", "true")
spark.conf.set("spark.speculation.multiplier", "3")
spark.conf.set("spark.speculation.quantile", "0.9")
```

Leave **on** for noisy clouds **if** sinks are idempotent (Iceberg overwrite-by-partition, MERGE on key). **Off** for skewed jobs until keys are fixed; **off** for `foreach` that POSTs to an API.

### 7. Dynamic allocation

```python
spark.conf.set("spark.dynamicAllocation.enabled", "true")
spark.conf.set("spark.dynamicAllocation.minExecutors", "2")
spark.conf.set("spark.dynamicAllocation.maxExecutors", "80")
spark.conf.set("spark.dynamicAllocation.shuffleTracking.enabled", "true")  # 3.x
spark.conf.set("spark.shuffle.service.enabled", "true")  # still required in most YARN/K8s setups
spark.conf.set("spark.dynamicAllocation.executorIdleTimeout", "60s")
```

If you cannot run a shuffle service, **do not** enable DA on shuffle-heavy SaaS rollups. Idle timeout will murder map-output executors mid-stage.

### 8. Extra shuffle, extra jobs

```python
# extra shuffle
events.repartition(1000).groupBy("service").count()

# extra job
df.count()
df.write.parquet(...)  # computed twice unless cached
```

### 9. Types and JSON

```python
schema = StructType([
    StructField("timestamp", TimestampType()),
    StructField("customer_id", StringType()),
    StructField("latency_ms", IntegerType()),
    # ...
])
events = spark.read.schema(schema).json("s3://raw/events/")
# better: convert once to Parquet/Avro at the edge
```

Casting `latency_ms` from string on 10 billion rows is a job you will pay daily until you fix the writer.

---

## Production gotchas (short list you can paste)

!!! production-gotcha "Broadcast threshold cargo-cult"
    Copy-pasting `autoBroadcastJoinThreshold=2g` from a blog. Driver collects 2 GB; 100 executors store 2 GB each.

!!! production-gotcha "`foreachPartition` that opens 200k HTTP connections"
    One connection **per partition** is the contract. 8 000 partitions = 8 000 sockets. Batch inside the partition.

!!! production-gotcha "Non-idempotent write + speculation + retry"
    Duplicates in the lake. Use table formats or `overwrite` a date partition.

!!! production-gotcha "IoT: one file per device per minute"
    Small-files gotcha as a lifestyle. Buffer, rollup, then Spark.

---

## How it fails { #failure-modes }

| Class | Log / UI | Outcome |
|-------|----------|---------|
| Driver OOM | `java.lang.OutOfMemoryError` on driver, UI dies | App gone |
| Executor JVM OOM | Task failed, `ExecutorLostFailure` | Stage retry |
| Cgroup kill | `Exit code 137`, “exceeding memory limits” | Python/overhead |
| FetchFailed loop | `FetchFailedException` | DA / node loss |
| Shuffle spill disk full | `No space left on device` | Node-local disks too small |
| S3 `SlowDown` | Task retries | Small files / too much concurrency |
| Cartesian | One task records → billions | Missing join key / type mismatch |
| Duplicate mart rows | Counts drift | Speculation + append |

---

## How to investigate { #debugging }

| Gotcha | Where to look |
|--------|----------------|
| Driver OOM | Driver log, heap dump; SQL broadcast size; `toPandas` in code search |
| Executor OOM | Stage task peak execution memory, GC time; `dmesg` 137 |
| Small files | `aws s3 ls` counts, Iceberg `files` table, Spark job **0** listing 40 min |
| UDF | Plan `BatchEvalPython`; executor CPU in Python, not JVM; Arrow OOM |
| Cache | Storage tab, fraction cached, disk spill of cache |
| Speculation | Task “speculative” flag, duplicate output |
| DA | Executors tab count over time vs FetchFailed |

```python
spark.sparkContext.setLogLevel("WARN")
print(spark.conf.get("spark.sql.adaptive.enabled"))
print(spark.conf.get("spark.dynamicAllocation.enabled"))
print(spark.conf.get("spark.shuffle.service.enabled"))
print(spark.conf.get("spark.speculation"))
```

History Server after the app is dead — production Spark UI is not a souvenir.

---

## Scale

| Factor | Which gotcha arrives first |
|--------|----------------------------|
| **10×** volume | Shuffle partitions / whale OOM |
| **100×** | Small files + driver listing; UDF no longer “fine” |
| **1000×** | DA/shuffle service mandatory; table format compaction as a **product**; per-tenant jobs so one collect cannot exist |

A UDF that adds 5 µs/row is 5 s at 1e6 rows and **1.4 h** at 1e9 rows *serialised through Python*. Native stays in the noise.

---

## Trade-offs

| Hardening | You give up |
|-----------|-------------|
| No Python UDFs | Some ML preproc moves to a Ray/pandas job |
| No DA | Idle capacity $ during the map-heavy first stage |
| Speculation off | Bad-node tail latency |
| Coalesce before write | Less write parallelism |
| Strict schemas | Ingest fails on new fields (you want this, with a DLQ) |

---

## Alternatives

- **SQL engines** if the job is a scan+agg without Spark-only features — fewer ways to `collect`.
- **Flink** if the “gotcha” is that you ran a 24/7 Spark SS query with unbounded state.
- **Compaction as a separate service** (Iceberg) rather than every Spark job trying to `coalesce` perfectly.

---

## How to apply at work

Pre-flight for every production job:

1. `explain` — unexpected `Exchange` or nested-loop join?
2. Partition count vs bytes?
3. Any `collect` / `toPandas` / `show` on large frames in the DAG path?
4. UDFs on the hot path — native rewrite?
5. Output file count = ? Compaction?
6. AQE, broadcast threshold, DA + shuffle service, speculation **explicit** in submit config.
7. Idempotent sink?

Code-search the repo for `.collect(`, `.toPandas(`, `@udf`, `repartition(`, `cache(`. That list is your audit.

---

## Check your understanding { #exercise }

The SaaS job below runs 12 minutes on 5 GB and 6 hours (then dies) on 2 TB. `cust_0042` is 38% of 2 TB. 80 executors, DA on, no shuffle service, `speculation=true`, default shuffle partitions 200, default broadcast 10 MB. `customers` is 400 MB.

```python
@udf("string")
def kind(ep):
    return "internal" if ep.startswith("/api") else "external"

raw = spark.read.json("s3://raw/events/")  # 90 days mixed
raw = raw.withColumn("kind", kind("endpoint")).cache()
j = raw.join(spark.read.parquet("s3://dims/customers"), "customer_id")
j.write.partitionBy("date", "region").parquet("s3://lake/events/")
print(j.count())
```

1. List **at least six** independent gotchas.
2. Which kills you first at 2 TB, and what does the UI show?
3. Rewrite the job (bullet points + key conf) for 2 TB.
4. After the rewrite, what still requires a **product** decision about `cust_0042`?
5. Why is `print(j.count())` after `write` a trap even if memory is fine?

??? question "Worked answer"
    1. JSON inference full 90-day scan; Python UDF; cache of the lake; SMJ of 400 MB (not broadcast at 10 MB) **or** someone raises threshold and broadcasts 400 MB × 80; `partitionBy date,region` small files; DA without shuffle service; speculation + append duplicates; `count` extra job; no date filter; default 200 reducers vs whale.
    2. **Likely:** full 90-day JSON scan + UDF (SLA), then **whale reducer OOM/spill** on the join shuffle, then FetchFailed from DA. UI: huge scan bytes, `BatchEvalPython`, one task shuffle-read enormous, executors fluctuating.
    3. Convert JSON→Parquet once; filter `date=`; native `when` for kind; **do not cache** raw; broadcast **projected** customers or AQE skew join; `partitionBy("date")` only; `maxRecordsPerFile` / Iceberg; AQE on; \(R\) from bytes or advisory 128 MB; DA **off** or shuffle service on; speculation off until idempotent table write; schema explicit.
    4. **Isolate or salt** the tenant. Config will not make 38% of 2 TB fit in one reducer.
    5. `count()` is another **action** → recomputes the DAG (cache may have spilled/evicted). You pay the job twice. Use write metrics / Iceberg snapshot summary.
