# Catalyst & Tungsten

You write a DataFrame query. Spark does not execute it literally. First it applies two layers of optimization.

---

## Catalyst: The Query Optimizer

Catalyst is Spark's rule-based query optimizer. It transforms your logical plan into an optimized physical plan before any computation occurs.

### The Optimization Pipeline

```
SQL / DataFrame API
        ↓
  Unresolved Logical Plan
        ↓
  Analysis (resolve column names, types)
        ↓
  Resolved Logical Plan
        ↓
  Logical Optimization (Catalyst rules)
        ↓
  Optimized Logical Plan
        ↓
  Physical Planning (choose join strategy etc.)
        ↓
  Code Generation (Tungsten)
        ↓
  Execution
```

### Key Optimizations

**Predicate Pushdown**: move filters as early as possible in the plan

```python
# You write:
events.join(customers, "customer_id") \
      .filter(events.region == "APAC")

# Catalyst rewrites to:
events.filter(events.region == "APAC") \
      .join(customers, "customer_id")
```

Filtering before the join means fewer rows participate in the (expensive) join.

**Column Pruning**: read only the columns you need

```python
# You write:
events.select("customer_id", "bytes").groupBy("customer_id").sum("bytes")

# Catalyst ensures Parquet reader only reads customer_id and bytes columns
# Other columns are never read from disk
```

**Partition Pruning**: skip entire partitions based on filters

```python
events.filter(events.date == "2024-01-15")
# If events are partitioned by date, only the 2024-01-15 directory is read
```

---

## Tungsten: The Execution Engine

Tungsten optimises Spark's physical execution layer.

### Whole-Stage Code Generation

Traditional query engines iterate row-by-row through a virtual function dispatch mechanism. Tungsten compiles query fragments into JVM bytecode that processes data in tight loops without virtual dispatch.

```
Traditional: for each row → call filter function → call aggregate function
Tungsten:    for each row → (inlined filter + aggregate in one tight loop)
```

### Off-Heap Memory

Tungsten manages memory directly rather than relying on the JVM garbage collector. Objects are stored in compact binary format in off-heap memory. This:
- Reduces GC pressure (large heap → long GC pauses)
- Improves cache locality (data is densely packed)
- Allows better memory accounting

---

## Adaptive Query Execution (Spark 3+)

AQE is a runtime optimization system that adjusts the physical plan based on actual statistics observed during execution.

Enable it:

```python
spark.conf.set("spark.sql.adaptive.enabled", "true")
```

**What AQE does:**

1. **Coalesce shuffle partitions**: if shuffle produces many small partitions, AQE merges them
   - Prevents the "200 tiny tasks" problem automatically

2. **Convert sort-merge join to broadcast join**: if AQE discovers at runtime that one side is small, it switches to broadcast

3. **Skew join optimization**: AQE detects and splits skewed partitions automatically

```python
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionFactor", 5)
```

---

## Using EXPLAIN

Always examine your physical plan before running expensive queries:

```python
result = events.groupBy("service").agg(
    F.count("*"),
    F.avg("latency_ms")
)
result.explain(mode="formatted")
```

Look for:
- `FileScan` with `PushedFilters` — good (predicate pushdown working)
- `BroadcastHashJoin` vs `SortMergeJoin` — check if broadcast is appropriate
- `Exchange` — these are shuffles
- `Sort` before an exchange — additional sorting cost

---

## How to Apply This at Work

1. Run `explain()` on slow queries to understand the physical plan
2. Check that filters appear before joins in the plan
3. Verify that Parquet column pruning is working (FileScan shows only needed columns)
4. Enable AQE for all production jobs on Spark 3+
5. Look for `BroadcastNestedLoopJoin` — this is almost always a problem (cross join)
