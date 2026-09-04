# Delta Lake

## The Core Mechanism: The Transaction Log

Delta Lake stores all table changes in a **transaction log** — a directory `_delta_log/` containing JSON files describing every operation.

```
s3://orders/
    _delta_log/
        00000000000000000000.json   (initial table creation)
        00000000000000000001.json   (first write)
        00000000000000000002.json   (second write)
        00000000000000000010.checkpoint.parquet  (checkpoint at version 10)
    part-0001.parquet
    part-0002.parquet
    ...
```

To determine the current state of the table, Delta reads the transaction log in order and replays the operations.

---

## A Transaction Log Entry

```json
{
  "add": {
    "path": "part-0003.parquet",
    "size": 134217728,
    "dataChange": true,
    "stats": "{\"numRecords\":1000000,...}"
  }
}
```

An `add` entry means "this file is now part of the table."

```json
{
  "remove": {
    "path": "part-0001.parquet",
    "dataChange": true
  }
}
```

A `remove` entry means "this file is no longer part of the table" (e.g., after a DELETE or OPTIMIZE).

---

## Checkpoints

Reading millions of JSON log entries would be slow. Delta Lake creates a **checkpoint** every 10 commits by default: a Parquet file summarising all active `add` and `remove` entries.

To reconstruct the table state:
1. Find the latest checkpoint
2. Read all JSON log entries after that checkpoint
3. Apply them to the checkpoint state

---

## Time Travel

Delta Lake supports time travel through the transaction log:

```python
# By version
df = spark.read.format("delta").option("versionAsOf", 5).load("s3://orders/")

# By timestamp
df = spark.read.format("delta").option("timestampAsOf", "2024-01-15").load("s3://orders/")
```

---

## ACID Operations

```python
from delta.tables import DeltaTable

dt = DeltaTable.forPath(spark, "s3://orders/")

# UPSERT (MERGE)
dt.alias("target").merge(
    source=updates.alias("source"),
    condition="target.order_id = source.order_id"
).whenMatchedUpdateAll() \
 .whenNotMatchedInsertAll() \
 .execute()

# DELETE
dt.delete("status = 'CANCELLED' AND order_date < '2023-01-01'")
```

---

## VACUUM

Deleted files remain on disk (they are just removed from the transaction log). `VACUUM` removes files that are no longer part of any valid table version:

```python
dt.vacuum(retentionHours=168)  # delete files older than 7 days
```

!!! warning "Production Gotcha"
    Do not run VACUUM with `retentionHours=0`. This deletes all non-current files, making time travel impossible and potentially breaking concurrent readers. Default minimum is 7 days.

---

## OPTIMIZE

Merge small files into larger ones:

```python
dt.optimize().executeCompaction()

# Or with Z-ordering (co-locate related data)
dt.optimize().executeZOrderBy("customer_id", "order_date")
```

Z-ordering rearranges data within files so that records with similar key values are stored together. This improves query performance for filters on the Z-ordered columns.

---

## When to Use Delta Lake

- Spark-centric workloads (Delta is natively integrated with Spark/Databricks)
- Need simple ACID operations without complex configuration
- Databricks environment (Delta Lake is the default)

---

## Iceberg vs Delta: The Practical Difference

For most workloads, both work well. The key differences:

| Dimension | Iceberg | Delta Lake |
|-----------|---------|-----------|
| Engine support | Spark, Trino, Flink, Hive, Presto | Primarily Spark (improving) |
| Partition evolution | Yes, first-class | Limited |
| Hidden partitioning | Yes | No |
| Spark integration | Good | Excellent (native) |
| Databricks | Works | First-class |

If you use Databricks: Delta Lake.
If you need multi-engine access (Trino + Spark + Flink): Iceberg.
