# Lakehouse

## The Evolution of Data Storage

**Data Warehouse** (traditional): structured, governed, expensive, fast for analytics.

**Data Lake** (raw storage): cheap, flexible, any format — but no reliability guarantees, no transactions, schema chaos.

**Lakehouse**: bring warehouse-quality reliability and governance to data lake infrastructure. Cheap object storage. Open formats. ACID transactions. Multiple engines.

---

## What This Module Covers

| Topic | What You Will Learn |
|-------|-------------------|
| [Why Table Formats Exist](why-table-formats.md) | The problems raw Parquet cannot solve |
| [Apache Iceberg](iceberg.md) | Metadata hierarchy, time travel, partition evolution |
| [Apache Hudi](hudi.md) | CDC, upserts, CoW vs MoR |
| [Delta Lake](delta.md) | Transaction log, checkpoints, Spark integration |
| [Choosing a Format](comparison.md) | Workload-based decision guide |

---

## The Core Value Proposition

With a table format, your data lake gains:

- **Snapshot isolation**: readers see a consistent view while writers are writing
- **Schema enforcement**: you cannot accidentally write the wrong schema
- **Time travel**: `SELECT * FROM events AS OF '2024-01-15'`
- **Efficient updates**: update or delete specific records without rewriting entire partitions
- **Partition pruning**: the format knows exactly which files contain which data

These properties were previously available only in data warehouses. Lakehouses bring them to open-format, multi-engine architectures.
