# Why Table Formats Exist

## The Problem with Raw Parquet

Imagine your data lake looks like this:

```
s3://events/
    part-001.parquet   (written by Spark job at 9 AM)
    part-002.parquet   (written by Spark job at 9 AM)
    part-003.parquet   (written by Spark job at 10 AM)
    part-004.parquet   (written by Spark job at 10 AM)
```

This seems fine. But now consider concurrent operations:

- **Readers** querying the table while writers append new files
- **Writers** attempting to update a record (update which file? rewrite it? what if another reader is mid-scan?)
- **Schema changes**: a field is renamed — which files have the old schema, which have the new?
- **Failed jobs**: a job wrote part-005 and part-006 then crashed — those files are partial results
- **Deletion requests** (GDPR): remove all records for user X — scan all files? rewrite all of them?

**Where is the table?** There is no single definition of "what the table looks like right now." There is only a pile of files.

---

## What Table Formats Add

A table format provides:

1. **A transactional metadata layer** — an authoritative definition of which files constitute the table at a given point in time
2. **ACID semantics** — readers see a consistent snapshot; partial writes are invisible
3. **Schema evolution** — the format tracks schema changes over time
4. **Partition evolution** — the partitioning scheme can change without rewriting all data
5. **Time travel** — query the table as it existed at any past point
6. **Compaction/maintenance** — merge small files, remove deleted data

---

## The Three Formats

All three major table formats solve the same problem with different trade-offs:

| Format | Primary Strength | Primary Use Case |
|--------|-----------------|-----------------|
| [Apache Iceberg](iceberg.md) | Engine-agnostic, partition evolution, strong consistency | Multi-engine lakehouse |
| [Apache Hudi](hudi.md) | CDC, upserts, incremental processing | CDC-heavy workloads, frequent updates |
| [Delta Lake](delta.md) | Spark integration, simplicity | Spark-centric workloads |

See the [detailed comparison](comparison.md) for workload-based guidance.
