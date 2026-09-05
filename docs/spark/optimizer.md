# Catalyst & Tungsten

You write a DataFrame query over a week of SaaS events. Spark does not “run the API.” It **rewrites** the query into something that might scan 40 GB of three columns instead of 8 TB of JSON-shaped Parquet, might filter before it joins, and might generate JVM bytecode that never boxes a `Row`.

If you do not read `explain()`, you are hoping Catalyst agrees with you. Production is not a hope.

---

## Use case

```python
events.join(customers, "customer_id") \
      .filter(F.col("region") == "eu-west-1") \
      .filter(F.col("date") == "2024-01-15") \
      .groupBy("service") \
      .agg(F.avg("latency_ms"), F.count("*"))
```

`events` is date-partitioned Parquet, 8 TB for the week, 1.1 TB for the 15th. `customers` is 40 MB. `region` is a **data** column, not a partition.

What you want Catalyst to do:

1. Push `date =` into **partition pruning** (only one day’s files).
2. Push `region =` into **Parquet row-group filters** if stats allow.
3. Read **only** `customer_id, service, latency_ms, region, date`.
4. **Broadcast** `customers` instead of shuffling 1.1 TB.
5. Partial aggregate `service` before any remaining exchange.

If any of those is missing in `explain("formatted")`, the job is wrong even if it “works” on a sample.

Observability: pruning on `hour=` is the difference between a 2 TB trace scan and a 12 GB one. CDC: `MERGE` plans that explode into Cartesian nested-loop joins. IoT: skipping the `value` column until the last projection.

---

## Why this is hard at scale

The optimiser is a **pattern matcher**, not an oracle:

- It cannot push a filter through a Python UDF. The UDF is a black box.
- It cannot prune `timestamp` if you partitioned on `date` and filtered on `timestamp` (unless Iceberg hidden partitioning).
- Statistics may be **stale** (Hive) or **absent** (a pile of Parquet). AQE exists because compile-time stats lie.
- Whole-stage codegen **fails open** to interpreted execution on some expressions; you will not notice except in CPU.
- AQE changes the plan **mid-job**. The plan you pasted in the PR may not be the plan that ran.

At 8 TB, a missed pushdown is an incident. At 80 MB, it is a lab.

---

## Intuition

Two layers:

- **Catalyst** — *what* operators, in *which* order, with *which* join algorithm.
- **Tungsten** — *how* a stage runs: compact rows, off-heap, generated loops.

```text
SQL / DataFrame
    → Unresolved logical plan
    → Analysis (names, types)
    → Logical optimisation (rules)
    → Physical planning (join strategy, exchanges)
    → Codegen (Tungsten)
    → Tasks
```

Your job is to write queries **the optimiser can see through**: native functions, explicit schemas, partition columns in `WHERE`, small dimensions that stay small.

---

## Internals

### Catalyst pipeline

**Predicate pushdown.** Filter before join/scan.

```python
# You write join then filter
events.join(customers, "customer_id").filter(events.region == "eu-west-1")

# Catalyst rewrites to filter events first (and maybe customers)
```

**Column pruning.** Parquet `ReadSchema` lists only needed fields. `SELECT *` then `groupBy` is how you disable this.

**Partition pruning.** `PartitionFilters: [date=2024-01-15]` in `FileScan`. If you do not see it, you are paying for the week.

**Constant folding / simplify.** `where(true)`, dead casts.

**Join selection** (physical):

| Strategy | When | Shuffle |
|----------|------|---------|
| Broadcast hash join (BHJ) | One side < threshold (10 MB default) | Small side to all executors |
| Sort-merge join (SMJ) | Both large, equi-join | Both sides |
| Shuffle hash join | Sometimes, smaller build side | Both sides |
| Broadcast nested loop | Non-equi / no condition | **Danger** — Cartesian |

**Partial vs final HashAggregate.** `count`/`sum` combine map-side. `countDistinct` and `collect_list` do **not** shrink the same way.

### Tungsten

- **UnsafeRow**: bytes, not JVM objects. Fewer allocations, worse to look at in a debugger.
- **Whole-stage codegen**: fuse a pipeline (`FileScan → Filter → Project → PartialAgg`) into one loop. Stage boundary (`Exchange`) **breaks** fusion — another reason shuffles cost CPU beyond the NIC.
- **Off-heap / unified memory**: execution vs storage (`spark.memory.fraction`, `storageFraction`). Cache fights shuffle buffers.

If codegen bails (`WholeStageCodegen` not wrapping your operators), you are back to Volcano-style virtual calls. `explain` shows it.

### Adaptive Query Execution (Spark 3.x)

Compile-time \(R=200\) is a guess. AQE **reoptimises** after each shuffle using map-output stats.

```python
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes", str(128 * 1024 * 1024))
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "5")
spark.conf.set("spark.sql.adaptive.localShuffleReader.enabled", "true")
# optional: let AQE broadcast if the *measured* side is small
spark.conf.set("spark.sql.adaptive.autoBroadcastJoinThreshold", str(64 * 1024 * 1024))
```

What AQE actually does:

1. **Coalesce** tiny shuffle partitions (fixes “200 tasks on 80 MB”).
2. **SMJ → BHJ** if runtime size says so.
3. **Skew join split** on fat join partitions.
4. **Local shuffle reader** when reducers are on the same executor as the map output (saves NIC).

It will **not**: invent a partition column you forgot; push through UDFs; make `SELECT *` cheap; split a skewed **aggregation** key (that is salting — [Shuffle](shuffle.md)).

---

## How

### Always explain expensive jobs

```python
from pyspark.sql import functions as F

result = (
    events
    .filter((F.col("date") == "2024-01-15") & (F.col("status_code") >= 500))
    .select("customer_id", "service", "latency_ms", "date", "status_code")
    .groupBy("service")
    .agg(F.count("*").alias("n"), F.avg("latency_ms").alias("avg_ms"))
)
result.explain(mode="formatted")
# mode="extended" for logical + physical
# mode="cost" when stats exist
```

Checklist in the formatted plan:

| You want to see | You do not want to see |
|-----------------|------------------------|
| `PushedFilters` / `PartitionFilters` | `FileScan` of the entire prefix |
| `ReadSchema` with 4 fields | Every column in the table |
| `BroadcastHashJoin` for 40 MB dim | `BroadcastNestedLoopJoin` |
| `HashAggregate` (partial) then `Exchange` then `HashAggregate` | `Exchange` before any filter |
| `WholeStageCodegen` wrapping the scan pipeline | Python `BatchEvalPython` on the hot path |

### Native expressions, not UDFs

```python
# Catalyst can push / codegen
df.filter(F.col("endpoint").startswith("/api/internal"))

# Catalyst cannot see through this
@F.udf("boolean")
def is_internal(ep):
    return ep.startswith("/api/internal") if ep else False
df.filter(is_internal(F.col("endpoint")))
```

The UDF version scans more, serialises to Python, and disables some fusion. See [Gotchas](gotchas.md).

### Join hints when you know sizes

```python
df.hint("broadcast", "customers").join(customers, "customer_id")
# or
events.join(customers.hint("broadcast"), "customer_id")
```

Hints are how you stop AQE oscillating in a tight SLA. They are also how you OOM if you are wrong. Prefer measuring.

### Iceberg / Delta

Table formats give Catalyst **manifest-level** pruning (min/max, partition specs, sometimes clustering). A well-sorted Iceberg table can skip files for `customer_id =` even without identity partitioning — if you maintained the sort. That is optimiser **input**, not magic.

---

## Production gotchas

!!! production-gotcha "`where(col("date") == F.current_date())` in a file name sense"
    If `date` is a string partition `2024-01-15` and you compare to a `DateType`, analysis may cast **the column** and **disable pruning**. Cast the literal: `F.lit("2024-01-15")` matching stored type.

!!! production-gotcha "Dynamic partition pruning only on the probe side"
    Spark can skip fact partitions using the **build** side of a join (DPP). If you disable broadcast and stats are empty, you scan all dates of a 90-day fact for one customer’s country. Check the plan for `dynamicpruningexpression`.

!!! production-gotcha "AQE changed file counts overnight"
    Coalesce reduced \(R\) from 2000 to 24. Downstream assumed 2000 files. Or the opposite: a bigger day stopped coalescing. Pin `advisoryPartitionSize` and compact in the table format.

!!! production-gotcha "`percentiles` and `countDistinct` look like `count`"
    They do not partial-aggregate the same way. Shuffle bytes stay huge. HyperLogLog (`approx_count_distinct`) is the optimiser-friendly cousin.

---

## Failure modes

| Failure | Optimiser story |
|---------|-----------------|
| Cartesian explosion | Nested-loop join; missing equi-join key, or type mismatch (`string` vs `bigint` customer_id) so Spark **cannot** BHJ/SMJ as you thought |
| Full lake scan | Filter on non-partition column; or `to_date(timestamp)` on the column instead of `date=` |
| Executor OOM after AQE BHJ | Runtime broadcast of a side that was 8 MB yesterday, 900 MB today |
| Silent nulls | Analysis coerced types; JSON inference |
| Codegen huge method | Very wide rows; Spark falls back; CPU 5× |

Type mismatch joins are infamous: `customer_id` int vs string → **cast** → sometimes a **BroadcastNestedLoopJoin**. `explain` catches it in review; production catches it in the bill.

---

## Debugging

```python
spark.conf.set("spark.sql.adaptive.enabled", "true")
# After the job, SQL tab shows the *final* AQE plan. explain() before run is compile-time.
```

| Place | What |
|-------|------|
| `explain("formatted")` | Intended physical plan |
| Spark UI SQL | Adaptive plan, time per node, scan bytes |
| `spark.sparkContext.setLogLevel("INFO")` | Rule application (noisy) |
| Metrics `input size` vs table size | Pushdown proof |
| `BatchEvalPython` in plan | UDF tax |

Compare **bytes scanned** to **bytes on disk for that date**. If they match the whole table, pruning failed.

---

## Scale

| Factor | Optimiser consequence |
|--------|------------------------|
| **10×** rows, same schema | Same plan, 10× scan; pushdown still the biggest win |
| **100×** files | Planning time and driver file listing dominate; Iceberg manifests matter more than a Catalyst rule |
| **1000×** | You need **incremental** plans (scan one snapshot / one hour). A perfect full-scan plan is still a full scan |

Cost-based optimiser (CBO) helps when stats exist. On a raw lake they often do not — **AQE is the CBO for people without stats**.

### Dynamic partition pruning (DPP)

A common SaaS pattern: join a **tiny** tenant table (`WHERE customer_id IN (whales we care about)`) to a **date-partitioned** fact.

```python
whales = spark.read.parquet("s3://dims/watchlist/")  # few hundred ids
fact = spark.read.parquet("s3://analytics/events/")  # 90 days
fact.join(F.broadcast(whales), "customer_id") \
    .groupBy("date", "customer_id").count()
```

If the fact is partitioned by `date` **and** you also filter dates from the dim, Spark can inject a **dynamic pruning subquery** so unused day directories are never listed. In `explain`, look for `dynamicpruningexpression`. If you SMJ two huge sides with no broadcast and no stats, DPP never fires and you scan 90 days to answer “yesterday for three tenants.”

### Statistics you can actually collect

```python
spark.sql("ANALYZE TABLE events COMPUTE STATISTICS FOR ALL COLUMNS")
# Iceberg: snapshot summaries + manifests already carry min/max per file
```

Hive-style `ANALYZE` goes stale the next ingest. Prefer **file-level** min/max (Parquet footers, Iceberg manifests) plus AQE. Do not build a religion around table-level row counts from last March.

---

## Trade-offs

| Choice | Gain | Cost |
|--------|------|------|
| AQE on (default-on in many 3.x distros) | Runtime join/partition fixes | Less predictable file counts and stage graphs |
| Broadcast | No fat shuffle | RAM × executors |
| Native functions | Pushdown + codegen | Less Python flexibility |
| Wide Parquet schema (400 cols) | Generic lake | Even with pruning, foot-guns of `SELECT *` |
| Hints | Stability | Stale hints after data grows |

---

## Alternatives

- **Trino / ClickHouse** optimiser + storage layout may beat Spark for *interactive* SQL with the same Parquet. Spark wins at **heavy** transforms, UDF-ish ETL (still prefer native), and **writes** (Iceberg jobs).
- **Materialised marts** so Catalyst never sees the 8 TB join again.
- **Flink** for incremental plans; its optimiser is different (operator chaining, not Catalyst).

---

## How to apply at work

PR template:

1. Paste `explain("formatted")` on production-sized partitions (or a sampled **path** that still has the partition spec).
2. Assert `PartitionFilters` / `PushedFilters`.
3. Assert join type.
4. List UDFs and why they are not native.
5. AQE flags in the SparkConf of the **submit**, not in a notebook cell that never shipped.

If scan bytes in the last prod run were 8 TB for a “yesterday” job, Catalyst is not your problem — **you never gave it a date filter**.

---

## Exercise

```python
q = (
    spark.read.parquet("s3://analytics/events/")   # partitioned by date
    .filter(F.to_date("timestamp") == F.lit("2024-01-15").cast("date"))
    .join(customers, F.col("customer_id") == F.col("cust_id"))
    .withColumn("bucket", udf_bucket("endpoint"))
    .groupBy("bucket", "region")
    .count()
)
```

`customers` is 25 MB with column `cust_id` **integer**; events `customer_id` is **string**. `udf_bucket` is a Python UDF. AQE on.

1. Will partition pruning fire? Why?
2. What join strategy do you fear, and why?
3. Where does the UDF sit relative to pushdown?
4. Rewrite the query so Catalyst can do the right thing. Name each change.
5. After the rewrite, which AQE feature still matters at 100× files?

??? question "Worked answer"
    1. **Probably not.** Filter is on `to_date(timestamp)`, not on partition column `date`. Function on the column defeats identity partition pruning. Use `filter(F.col("date") == "2024-01-15")` (matching type).
    2. **BroadcastNestedLoopJoin or SMJ with a cast** because `string = int` is not a clean equi-join on the same type. Could explode or shuffle-cast everything. Cast **one** side explicitly after making types equal; then BHJ of 25 MB.
    3. UDF after the join in code, but it **blocks** predicate/column work on `endpoint` and forces `BatchEvalPython`. If `udf_bucket` could be `when`/`regexp`, do that **before** join to shrink rows, and keep it native so codegen holds.
    4. Filter `date=`; `select` needed cols; `customers.withColumn("customer_id", F.col("cust_id").cast("string"))` (or cast events if that is the source of truth); `broadcast(customers)`; replace UDF with native `when`; `groupBy`.
    5. **Coalesce + DPP / runtime BHJ.** File listing at 100× needs a table format; AQE will still coalesce the agg shuffle and can DPP if the join can broadcast a set of dates/keys.
