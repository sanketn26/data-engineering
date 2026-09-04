# Production Gotchas

The following mistakes appear in production Spark jobs repeatedly. Knowing them in advance saves incident responses.

---

## 1. Calling collect() on Large DataFrames

```python
# DANGEROUS
all_events = events.collect()  # Moves all data to the Driver
```

`collect()` transfers every row from every executor to the driver's JVM. If the DataFrame has 100 GB, the driver needs 100+ GB of heap. Result: `OutOfMemoryError` in the driver.

**Fix**: use `write()` to write results, or `take(n)` if you need a sample, or `show(20)` for inspection.

---

## 2. Driver OOM from Accidental Aggregation

```python
# This collects to driver
counts = events.groupBy("customer_id").count().toPandas()
```

If there are 50 million customers, this pulls 50 million rows into the driver as a pandas DataFrame.

**Fix**: write to storage, then read a sample. Or use `limit()` if you only need a subset.

---

## 3. Too Few Shuffle Partitions

```python
# Default 200 partitions for a 5 TB dataset
# Each partition = 25 GB → executor OOM or massive spill
result = events.groupBy("service", "region").agg(...)
```

**Fix**: set `spark.sql.shuffle.partitions` appropriately, or enable AQE.

---

## 4. Too Many Small Files on Output

```python
events.write.partitionBy("date", "region").parquet("s3://output/")
```

If `date` has 365 values and `region` has 50 values, and you have 200 shuffle partitions, you could write `365 × 50 × 200 = 3.65 million` tiny files.

Reading 3.65 million files in a future Spark job means 3.65 million S3 API calls just to list files.

**Fix**: coalesce before writing, or repartition to a reasonable number:

```python
events.repartition(100).write.partitionBy("date").parquet("s3://output/")
```

---

## 5. Skewed Joins

```python
events.join(customers, "customer_id")
```

If one customer has 80% of all events, the task handling that customer's partition takes 20× longer than others.

**Fix**: broadcast join if customer table is small, or use AQE skew join optimization.

---

## 6. Exploding Joins

```python
# Both sides have multiple rows per key
orders.join(payments, "order_id")
```

If an order can have multiple payments (e.g., partial payments), and a payment can reference multiple orders, the join output is a cross product: orders×payments per key.

**Fix**: understand the join cardinality before writing. Use `count()` on join key groups to check for multiples.

---

## 7. Caching the Wrong Things

Caching a 500 GB DataFrame on a cluster with 400 GB of total executor memory is pointless. Spark will spill cached data to disk, which is slower than just re-reading it.

Cache only:
- DataFrames reused multiple times in the same job
- DataFrames that fit comfortably in executor memory
- DataFrames that are expensive to recompute (complex joins, many aggregations)

---

## 8. Python UDF Performance

```python
@udf(returnType=StringType())
def classify_endpoint(endpoint):
    return "internal" if endpoint.startswith("/api/internal") else "external"

events.withColumn("type", classify_endpoint(events.endpoint))
```

Python UDFs break out of JVM-land into Python. Each row serializes from JVM → Python → JVM. For millions of rows this is very slow.

**Fix**: use built-in Spark functions where possible:

```python
from pyspark.sql.functions import when

events.withColumn(
    "type",
    when(events.endpoint.startswith("/api/internal"), "internal").otherwise("external")
)
```

If you must use Python logic, use **Pandas UDFs** (vectorized UDFs) which operate on batches via Apache Arrow:

```python
from pyspark.sql.functions import pandas_udf

@pandas_udf(StringType())
def classify_endpoint(endpoints: pd.Series) -> pd.Series:
    return endpoints.apply(lambda e: "internal" if e.startswith("/api/internal") else "external")
```

---

## 9. Repartition Without Reason

```python
# Often seen "just to be safe"
events.repartition(1000).groupBy("service").count()
```

Adding a `repartition()` before `groupBy()` adds a shuffle before the shuffle. Spark will shuffle to repartition, then shuffle again for the groupBy.

**Fix**: trust Spark's partitioning or use `coalesce()` to reduce partitions without a full shuffle.

---

## 10. Ignoring Data Types

```python
# Implicit string-to-integer conversion in every row
events.withColumn("latency_int", events.latency_ms.cast("int"))
# Done on 10 billion rows = 10 billion casts
```

Schema mismatch requires per-row conversion. Define schemas explicitly when reading data and avoid type surprises:

```python
schema = StructType([
    StructField("latency_ms", IntegerType()),  # specify correct type at read time
    ...
])
events = spark.read.schema(schema).json("s3://raw-events/")
```

---

## How to Apply This at Work

Before every production Spark job:

1. Check `explain()` — are there unexpected shuffles or cross joins?
2. Check partition count — is it appropriate for the data volume?
3. Check for `collect()` or `toPandas()` on large DataFrames
4. Check for Python UDFs on hot paths
5. Check output file count — will downstream jobs suffer from small files?
6. Enable AQE in Spark 3+
