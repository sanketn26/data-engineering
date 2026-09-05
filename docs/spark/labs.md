# Spark Labs

These labs exist so the SaaS event

```text
{timestamp, customer_id, user_id, service, endpoint, region, latency_ms, status_code, bytes}
```

stops being a slide and becomes a **Spark UI histogram**. Each lab is a mechanism: shuffle, skew, join strategy, partition count — then a **Now break it** section that forces the failure you will see at 2 TB, at laptop scale.

You do **not** need Docker. A local `SparkSession` and `http://localhost:4040` are enough. Docker is optional if you prefer an isolated JVM.

---

## Use case

You are the on-call for the analytics platform. Before you change production `spark.sql.shuffle.partitions`, you must be able to:

1. Point at **Shuffle Write / Read** for a `groupBy("service")`.
2. Recognise a **whale** `customer_id` in the task duration chart.
3. Measure **broadcast vs sort-merge** on a small dimension.
4. See **too many partitions** as scheduler overhead, not “more parallelism.”
5. Reproduce **driver pain** from `collect()` without taking the company down.

Observability / IoT / CDC / fraud: same labs, different keys (`pod`, `device_id`, `order_id`). Swap the column; the UI does not care.

---

## Why this is hard at scale

Laptop labs lie in two ways: everything fits in RAM, and skew of 80% on 1e6 rows is still **seconds**. The skill is mapping **what you see** (one yellow task, spill 0, 20 tiny tasks) onto **what 2 TB would do** (one task 4 h, disk full, 20 000 files).

If you only watch wall-clock of `time.time()`, you will miss that. Watch **`:4040`**.

---

## Intuition

```text
Predict the stage graph → run → compare UI → change one knob → repeat
```

One knob per run. If you enable AQE, salt, *and* broadcast together, you will not know which line moved the histogram.

---

## Internals (what the UI is showing)

| UI place | Meaning |
|----------|---------|
| Jobs | One per **action** (`count`, `write`, `collect`) |
| Stages | Cut at **shuffle** |
| Shuffle Write | Map-side bytes |
| Shuffle Read | Reduce-side bytes |
| Spill (Disk) | Sorter/reducer did not fit |
| Task time max vs median | Skew or bad split |
| SQL / DAG | `Exchange`, `BroadcastHashJoin`, `SortMergeJoin`, `BatchEvalPython` |
| Storage | Cache size / fraction |
| Executors | Local mode: one executor, still **multiple tasks** |

Local mode: `spark.master=local[4]` → 4 slots. Waves still exist. Stragglers still exist.

---

## How — setup

**Prerequisites:** Python 3.9+, ~8 GB RAM, Java 11 or 17 (PySpark’s JVM).

```bash
pip install 'pyspark==3.5.0' pandas pyarrow
# Spark UI
# http://localhost:4040  (increments to 4041 if 4040 is taken)
```

Optional Docker:

```bash
docker run -it --rm -p 4040:4040 \
  -v "$PWD":/opt/labs -w /opt/labs \
  bitnami/spark:3.5 \
  python /opt/labs/lab_shuffle.py
```

Shared session factory (paste at the top of each script or import):

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def session(app, partitions=20, aqe=False):
    b = (
        SparkSession.builder.appName(app)
        .master("local[4]")
        .config("spark.ui.enabled", "true")
        .config("spark.sql.shuffle.partitions", str(partitions))
        .config("spark.sql.adaptive.enabled", str(aqe).lower())
        .config("spark.sql.adaptive.coalescePartitions.enabled", str(aqe).lower())
        .config("spark.sql.adaptive.skewJoin.enabled", str(aqe).lower())
        .config("spark.driver.memory", "2g")
        .config("spark.executor.memory", "2g")
    )
    return b.getOrCreate()
```

Keep the UI open between jobs **in one process** (`input("pause")`) or you will lose the live UI when `spark.stop()` runs. History server is optional; pausing is simpler.

---

## Lab 1 — Observing the shuffle

**Goal:** `groupBy` creates an `Exchange`. Uniform keys → even tasks.

```python
# lab_shuffle.py
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import random

spark = (
    SparkSession.builder.appName("Lab1-Shuffle")
    .master("local[4]")
    .config("spark.ui.enabled", "true")
    .config("spark.sql.shuffle.partitions", "20")
    .config("spark.sql.adaptive.enabled", "false")
    .getOrCreate()
)

random.seed(1)
data = [
    (f"customer_{i % 100}", f"service_{i % 10}", random.randint(10, 500), (i % 5) * 100)
    for i in range(1_000_000)
]
events = spark.createDataFrame(
    data, ["customer_id", "service", "latency_ms", "bytes"]
)
events.cache()
print("cached rows", events.count())  # action 1: no shuffle (narrow + cache)

result = events.groupBy("service").agg(
    F.count("*").alias("count"),
    F.avg("latency_ms").alias("avg_latency"),
    F.sum("bytes").alias("bytes"),
)
result.write.mode("overwrite").parquet("/tmp/lab1_output")  # action 2: shuffle

print("Open http://localhost:4040 — Stages: Shuffle Write vs Read")
input("press Enter to stop")
spark.stop()
```

**Observe:**

- Job for `count` (cache materialise): **no** shuffle (or tiny).
- Job for `write`: two stages. Stage with `HashAggregate` + `Exchange`.
- Shuffle Write ≈ Shuffle Read.
- 20 tasks in the reduce stage; durations similar (uniform `service` has 10 keys — **several partitions empty**). Empty tasks are a teaching moment: \(R=20\) for 10 keys is already waste.

---

## Lab 2 — Data skew

**Goal:** 80% of rows in `customer_1`. One reduce task owns the whale.

```python
# lab_skew.py
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import random, time

spark = (
    SparkSession.builder.appName("Lab2-Skew")
    .master("local[4]")
    .config("spark.sql.shuffle.partitions", "16")
    .config("spark.sql.adaptive.enabled", "false")
    .getOrCreate()
)

random.seed(2)
data = []
for i in range(1_000_000):
    customer = "customer_1" if random.random() < 0.8 else f"customer_{random.randint(2, 1000)}"
    data.append((customer, random.randint(10, 500), 100))

events = spark.createDataFrame(data, ["customer_id", "latency_ms", "bytes"])
events.cache().count()

t0 = time.time()
(
    events.groupBy("customer_id")
    .agg(F.count("*"), F.sum("latency_ms"))
    .write.mode("overwrite")
    .parquet("/tmp/lab2_skewed")
)
print("skewed seconds", time.time() - t0)
print("UI: one task duration >> median. Note Shuffle Read of that task.")
input("press Enter")
spark.stop()
```

**Observe:** Tasks tab sorted by duration. One task’s **Shuffle Read Records** ≈ 800 000. That is `cust_0042` in costume.

**Salted rerun** (same data, new app or unpersist):

```python
from pyspark.sql.functions import col, concat, lit, floor, rand, split, count, sum as Fsum

SALTS = 8
salted = events.withColumn(
    "k", concat(col("customer_id"), lit("#"), floor(rand(3) * SALTS).cast("int").cast("string"))
)
partial = salted.groupBy("k").agg(count("*").alias("n"), Fsum("latency_ms").alias("s"))
final = (
    partial.withColumn("customer_id", split("k", "#").getItem(0))
    .groupBy("customer_id")
    .agg(Fsum("n"), Fsum("s"))
)
final.write.mode("overwrite").parquet("/tmp/lab2_salted")
```

**Observe:** first shuffle more even; second shuffle tiny. AQE skew join will **not** save this agg — it is not a join. Enable AQE and re-run the unsalted agg: coalescing empty partitions ≠ splitting the whale.

---

## Lab 3 — Join strategies

**Goal:** SMJ vs BHJ.

```python
# lab_joins.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast
import time, random

spark = SparkSession.builder.appName("Lab3-Joins").master("local[4]").getOrCreate()
random.seed(3)

orders = spark.createDataFrame(
    [(i, f"customer_{i % 10000}", float(random.random() * 100)) for i in range(2_000_000)],
    ["order_id", "customer_id", "amount"],
)
customers = spark.createDataFrame(
    [(f"customer_{i}", f"Segment {i % 5}") for i in range(10000)],
    ["customer_id", "segment"],
)
orders.cache().count()
customers.cache().count()

spark.conf.set("spark.sql.adaptive.enabled", "false")
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")  # force SMJ
t0 = time.time()
print("SMJ", orders.join(customers, "customer_id").count(), "in", time.time() - t0)

spark.conf.set("spark.sql.autoBroadcastJoinThreshold", str(50 * 1024 * 1024))
t0 = time.time()
print("BHJ", orders.join(broadcast(customers), "customer_id").count(), "in", time.time() - t0)

print("SQL tab: SortMergeJoin vs BroadcastHashJoin; count Exchanges")
input("press Enter")
spark.stop()
```

**Observe:** SMJ has **Exchanges** on both sides (or sort + exchange). BHJ: broadcast of `customers`, no shuffle of `orders`. Time gap is smaller locally than in a 50-executor cluster — **predict** it would grow with NIC.

---

## Lab 4 — Partition count

```python
# lab_partitions.py
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import time

spark = SparkSession.builder.appName("Lab4-Partitions").master("local[4]").getOrCreate()
spark.conf.set("spark.sql.adaptive.enabled", "false")

events = spark.createDataFrame(
    [(f"customer_{i}", i % 50, i * 0.1) for i in range(2_000_000)],
    ["customer_id", "service_id", "latency"],
)
events.cache().count()

for n in [4, 16, 64, 400, 2000]:
    spark.conf.set("spark.sql.shuffle.partitions", str(n))
    t0 = time.time()
    (
        events.groupBy("customer_id")
        .agg(F.avg("latency"), F.count("*"))
        .write.mode("overwrite")
        .parquet(f"/tmp/lab4_{n}")
    )
    print(f"R={n}: {time.time() - t0:.2f}s")

spark.stop()
```

**Observe:** \(R=4\) on `local[4]` → fat tasks. \(R=2000\) → thousands of tiny tasks, write **2000 files** under `/tmp/lab4_2000`. `ls /tmp/lab4_2000 | wc -l`. That is the small-files gotcha in miniature.

Enable AQE with `advisoryPartitionSizeInBytes=64m` and re-run \(R=2000\): reduce-stage task count should **drop**. SQL tab shows `CustomShuffleReader` / `AQEShuffleRead`.

---

## Now break it

Do these **on purpose**. Stop the session between experiments. Predict first.

### Break A — `collect()` the events

```python
spark = session("break-collect")
events = spark.range(0, 5_000_000).select(
    F.concat(F.lit("cust_"), (F.col("id") % 1000).cast("string")).alias("customer_id"),
    F.rand().alias("x"),
)
# Predict: driver heap climbs. 5e6 rows is usually survivable with 2g.
# Uncomment the next line only after estimating bytes: row * 5e6
# rows = events.collect()
print(events.limit(20).collect())  # the acceptable cousin
input("inspect UI Jobs: collect vs limit")
spark.stop()
```

To go harsher without killing your laptop: `spark.driver.memory=256m` and `spark.range(50_000_000).collect()`. Expect **driver OOM**. That is [gotcha #1](gotchas.md).

### Break B — too many partitions

```python
spark = session("break-parts", partitions=4000, aqe=False)
df = spark.range(0, 100_000)
df.groupBy(F.col("id") % 10).count().write.mode("overwrite").parquet("/tmp/break_parts")
print("4000 reduce tasks on 100k rows. UI: scheduler delay, empty tasks.")
input("pause")
spark.stop()
```

### Break C — Python UDF vs native

```python
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType

spark = session("break-udf", partitions=8)
df = spark.range(0, 2_000_000)

@udf(IntegerType())
def plus_one(x):
    return int(x) + 1

t0 = __import__("time").time()
df.select(plus_one("id").alias("y")).agg(F.sum("y")).collect()
print("udf", __import__("time").time() - t0)

t0 = __import__("time").time()
df.select((F.col("id") + 1).alias("y")).agg(F.sum("y")).collect()
print("native", __import__("time").time() - t0)
print("SQL tab: BatchEvalPython vs WholeStageCodegen")
input("pause")
spark.stop()
```

### Break D — cache the world

```python
spark = session("break-cache")
df = spark.range(0, 3_000_000).select(F.rand().alias("x"), F.rand().alias("y"))
df.cache()
df.count()
print("Storage tab: size in memory. Now run a shuffle and watch execution vs storage.")
df.groupBy((F.col("x") * 10).cast("int")).count().show()
input("pause")
spark.stop()
```

### Break E — explode join

```python
spark = session("break-explode", partitions=8)
left = spark.createDataFrame([(1, "a"), (1, "b"), (1, "c")], ["k", "l"])
right = spark.createDataFrame([(1, "x"), (1, "y"), (1, "z")], ["k", "r"])
left.join(right, "k").show()  # 9 rows from 3×3
print("Scale this pattern to order_id with many payments × many items.")
input("pause")
spark.stop()
```

---

## Production gotchas (in the lab)

!!! production-gotcha "`createDataFrame` from a giant Python list"
    Labs 1–2 build 1e6 tuples **on the driver** then parallelise. That is a driver-side collect in reverse. Fine for 1e6; not how you load 2 TB. In production, **read files**.

!!! production-gotcha "AQE on by default in some 3.5 builds"
    If Lab 2 looks “too even,” check Environment: `spark.sql.adaptive.enabled`. Force `false` when you want to see the 2013 default.

!!! production-gotcha "UI port already bound"
    Second session → `:4041`. Do not debug the previous job’s UI.

---

## Failure modes you should have seen

| Experiment | Failure |
|------------|---------|
| Break A | Driver OOM / huge collect job |
| Break B | Tiny files, task overhead |
| Break C | Python eval in plan, slower agg |
| Lab 2 | Straggler task |
| Lab 4 \(R=4\) | Fat tasks (OOM if you inflate row count) |
| DA (not local) | FetchFailed — skip locally; read [Gotchas](gotchas.md) |

---

## Debugging (checklist per lab)

1. Which **action** created the job?
2. How many **stages**? Did you predict the `Exchange`?
3. Task **max / median** duration?
4. Shuffle bytes vs input bytes (map-side agg should shrink)?
5. SQL join type / `BatchEvalPython`?
6. File count on disk after write?

If you cannot answer those without scrolling randomly, re-run Lab 1 only until you can.

---

## Scale

Map laptop → cluster:

| Laptop | 10× | 100× | 1000× |
|--------|-----|------|-------|
| 1e6 rows, 80% whale | 1e7, still RAM | spill on one task | that task is a dedicated job |
| \(R=20\) | still OK | 128 MB math | AQE + salt |
| BHJ 10k dim | BHJ 1e6 dim maybe | 400 MB × N executors | do not broadcast |
| 2000 files in `/tmp` | S3 SlowDown | driver listing OOM | Iceberg compaction |

---

## Trade-offs

Running labs locally **hides** NIC, S3, fetch failures, and multi-executor broadcast RAM. Compensate by **reading the metrics**, not the wall clock. A 1.2× SMJ vs BHJ locally can be 8× on a 50-node cluster.

---

## Alternatives

- Spark History Server + a recorded event log if you cannot keep the UI up.
- [Shuffle simulation](../simulations/spark-shuffle.html) for partition math without JVM.
- A single-node Spark on a 16 GB VM if `local[4]` is too small to see spill — increase rows until Spill (Disk) > 0 instead of installing Hadoop.

---

## How to apply at work

When you next change shuffle partitions in prod:

1. Replay **Lab 4** with **production `explain` bytes** plugged into the \(R \approx B/s\) formula ([Shuffle](shuffle.md)).
2. Take a screenshot of **task duration** from staging with production-like skew (sample **all** of `cust_0042`, not a random 1%).
3. Refuse a config PR that does not include a UI screenshot.

---

## Exercise

You must demo to a new teammate **in 20 minutes** that `cust_0042` (here `customer_1`) is why last night’s job missed SLA.

1. Which lab do you run first, and which **two UI numbers** do you show?
2. They suggest `spark.speculation=true` on the unsalted agg. What happens in the UI? Do you accept the change?
3. They suggest `repartition(200)` before `groupBy`. Design a 2-run experiment (this laptop) to accept or reject.
4. Extend Lab 3: make `customers` **too big to broadcast** locally (e.g. 3e6 rows) with `autoBroadcastJoinThreshold=10m`. What join did you get? What metric on the **driver** would scare you if you raised the threshold to 2 GB in prod?
5. After they write 400 files of a 10-row mart, which lab number do you point at?

??? question "Worked answer"
    1. **Lab 2.** Show (a) task duration max vs median, (b) Shuffle Read records/bytes of the max task ≈ 80% of total.
    2. Speculative copies of the **same whale task**. Two tasks hammering the same 80% key. UI: speculative task flag, no improvement (or worse). **Reject** until salted/isolated; sinks must be idempotent anyway.
    3. Run groupBy **without** extra `repartition`; record shuffle stages (should be **one** exchange). Add `.repartition(200)` before groupBy; SQL tab should show **two** Exchanges. Time should not improve. Reject.
    4. SMJ (`SortMergeJoin`) with Exchanges. Raising threshold to 2 GB would **collect 2 GB to the driver** then multiply across executors — driver heap + executor RAM. On this laptop you might BHJ and GC; in prod with 80 executors it is an outage.
    5. **Lab 4** / Break B: partition count drives **file count**. Coalesce the mart to 1 file.

---

## Exercise (stretch)

Generate **IoT-shaped** data: 10 000 `device_id`s, 100 rows each, write `partitionBy("device_id")` from 50 Spark partitions. Count files. Then rewrite with `partitionBy("date")` only (add a constant date). Explain the file-count formula to a teammate using [Partitioning](../foundations/partitions.md).

??? question "Worked answer (stretch)"
    File count ≈ **(# of non-empty output directories) × (tasks that wrote to each)**. `partitionBy(device_id)` with 10k ids and 50 tasks can approach tens of thousands of tiny files (not always 10k×50 if not every task sees every device, but it is bad). Date-only: ~50 files (or 50 × 1 date dir). Compaction / `maxRecordsPerFile` is the production control.
