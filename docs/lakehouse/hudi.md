# Apache Hudi

## The Problem Hudi Solves

Iceberg and Delta Lake are excellent for append-heavy workloads and batch ETL. But what about:

- A database CDC stream producing INSERT, UPDATE, DELETE events
- A user deletes their account (GDPR) — delete all their records from the lake
- A late-arriving correction — update a record written 3 days ago

These are **mutation-heavy** workloads. They require efficient upserts and deletes, not just appends.

Hudi was designed at Uber for exactly this: ingesting database CDC streams into a data lake efficiently.

---

## Copy-on-Write vs Merge-on-Read

Hudi supports two storage types with different read/write trade-offs.

### Copy-on-Write (CoW)

On every upsert, rewrite the affected Parquet files immediately.

```
File-001: [record A, record B, record C]
Upsert record B (new value) →
New File-001: [record A, record B_new, record C]
```

- **Writes**: more expensive (full file rewrite)
- **Reads**: fast (no merge needed, files are always clean)
- **Best for**: read-heavy workloads, infrequent updates

### Merge-on-Read (MoR)

Write updates to small delta files (log files). Merge on read.

```
File-001 (base): [record A, record B, record C]
Log file: [UPDATE record B → new value]

On read: merge base + log → [record A, record B_new, record C]
```

- **Writes**: fast (append to log file)
- **Reads**: slower (must merge base + log)
- **Best for**: write-heavy workloads, streaming ingestion

Periodically run compaction to merge log files into base files, improving read performance.

---

## The Timeline

Hudi maintains a **timeline** of all actions on the table:

```
2024-01-15T10:00:00.000Z commit (writes records)
2024-01-15T10:05:00.000Z commit
2024-01-15T10:10:00.000Z compaction
2024-01-15T10:15:00.000Z commit
2024-01-15T10:20:00.000Z clean (removes old files)
```

The timeline allows:
- Incremental queries: "give me all changes since timestamp T"
- Point-in-time queries (like Iceberg's time travel)
- Compaction scheduling

---

## Incremental Processing

A key Hudi feature: incremental reads. Instead of reading the full table, read only changes since a specific commit.

```python
# Read only records changed since last commit
hudi_options = {
    'hoodie.datasource.query.type': 'incremental',
    'hoodie.datasource.read.begin.instanttime': '20240115100000',
}
changes = spark.read.format("hudi").options(**hudi_options).load(table_path)
```

This enables efficient downstream pipelines that only process what changed, not the full table.

---

## Record Keys and Precombine

Hudi needs to identify records for upsert:

- **Record key**: the primary key of each record (`hoodie.datasource.write.recordkey.field`)
- **Precombine field**: used to resolve conflicts when multiple versions of the same record exist in one batch (`hoodie.datasource.write.precombinekey.field` — usually a timestamp)

```python
hudi_options = {
    'hoodie.table.name': 'orders',
    'hoodie.datasource.write.recordkey.field': 'order_id',
    'hoodie.datasource.write.precombinekey.field': 'updated_at',
    'hoodie.datasource.write.operation': 'upsert',
    'hoodie.datasource.write.table.type': 'MERGE_ON_READ',
}

orders_df.write.format("hudi") \
    .options(**hudi_options) \
    .mode("append") \
    .save(table_path)
```

---

## When to Use Hudi

- CDC ingestion from databases (inserts, updates, deletes)
- GDPR compliance requiring row-level deletes
- Streaming writes with frequent updates to existing records
- Need efficient incremental processing downstream

---

## When NOT to Use Hudi

- Append-only workloads (Iceberg or Delta are simpler)
- No need for row-level updates
- Team unfamiliar with CoW/MoR trade-offs

Hudi's operational complexity is higher than Iceberg or Delta for simple append workloads. Choose the simplest tool that solves the problem.
