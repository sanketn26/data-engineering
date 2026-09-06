---
title: Parquet Internals
description: The one file-format diagram behind Spark, Iceberg, Delta, Hudi, and Trino's lake reads — row groups, column chunks, pages, and predicate pushdown.
---

# Parquet Internals

**Time:** 30 minutes reading + 20 minutes exercise<br>
**Prerequisites:** [Why Columnar Storage](../olap/columnar-storage.md), [Object Storage Internals](object-storage.md)<br>
**Outcomes:** draw a Parquet file's internal structure from memory; explain predicate pushdown as a footer lookup; distinguish partition pruning from row-group statistics; diagnose small-file pathology from first principles.

A code review comment says "this Spark job scans 8 TB for a query that only needs 40 GB." Before you can answer *why*, you need to know what's actually inside the files Spark is opening — not "Parquet is columnar," but the specific nesting that makes column projection, row-group pruning, and predicate pushdown possible at all.

[Why Columnar Storage](../olap/columnar-storage.md) makes the general case for columnar engines — ClickHouse granules, Pinot segments, Parquet row groups are all the same idea. This page is the one format underneath Spark, Iceberg, Delta, Hudi, and Trino's lake reads specifically, at the level of bytes on disk.

## File anatomy

```text
File
├── Row Group            (e.g. 128 MB; horizontal slice of the file)
│   ├── Column Chunk: customer_id
│   │      ├── Page      (e.g. 1 MB; smallest unit read/decoded)
│   │      └── Page
│   ├── Column Chunk: timestamp
│   └── Column Chunk: latency_ms
│
└── Row Group
    ├── Column Chunk: customer_id
    ├── Column Chunk: timestamp
    └── Column Chunk: latency_ms

Footer: schema + per-row-group, per-column min/max/null-count stats
```

- **Row group**: a horizontal slice of the file (all columns, a range of rows). This is the unit `EXPLAIN` refers to when it prunes "row groups" — a reader can skip an entire row group using only the footer's min/max stats, without opening a single page inside it.
- **Column chunk**: one column's data, for one row group. This is what makes column projection (`SELECT customer_id` reads only that chunk, not the whole row group) possible.
- **Page**: the smallest unit actually read and decoded — typically ~1 MB. Each page carries its own encoding (dictionary, RLE, delta) chosen per column, which is why Parquet uses dictionary, run-length, and general-purpose compression **per page**, not per file.
- **Footer**: written last, read first. A reader opens a Parquet file by seeking to the footer (a range read — see [object storage internals](object-storage.md)) to get the schema and every row group's min/max/null-count stats *before* deciding which row groups and columns to touch at all.

**Predicate pushdown** is exactly this footer lookup: `WHERE customer_id = 'cust_0042'` checks each row group's min/max for `customer_id` and skips row groups where the predicate cannot match, without decoding a single page in the skipped groups.

!!! tip "Partitioning and Parquet statistics are different mechanisms"
    [Partition pruning](partitions.md#partition-pruning) skips whole **files** using directory-level metadata (`date=2024-01-15/`) before any file is opened. Row-group statistics skip **row groups within a file that was already selected to open**. A table with no partitioning at all can still prune heavily via row-group stats if the data happens to be sorted on the filtered column — and a well-partitioned table with unsorted data inside each partition gets no row-group pruning benefit at all. Confusing the two is why "I partitioned by `date`, why is `WHERE region = 'eu-west-1'` still slow" is a recurring design-review question — that filter was never a partition column, and if the rows inside each day aren't sorted by `region`, it isn't pruning at the row-group level either.

## Small-file pathology, specifically for Parquet

Parquet's footer, schema, and per-row-group stats are fixed overhead **per file**, independent of how many rows the file holds. A file with 100 rows pays close to the same footer/metadata cost as a file with 10 million rows, so:

- **Planning cost** scales with file *count*, not data volume — a query touching 100,000 tiny files opens 100,000 footers before reading a byte of data (compounding the `LIST` cost from [object storage internals](object-storage.md)).
- **Compression ratio degrades** — dictionary and RLE encoding work on the values *within* a column chunk; a column chunk with a handful of rows has little redundancy to exploit.
- **Row groups smaller than the working page size** waste the vectorised-execution benefit described in [why columnar storage](../olap/columnar-storage.md#simd-and-vectorised-execution) — SIMD batches of 8,192-ish rows have nothing to batch.

The fix is the same one Iceberg/Delta/Hudi compaction jobs exist to automate: periodically rewrite many small files into fewer files sized to a target row-group size (commonly 128 MB–1 GB), not writing at that size to begin with — see [partitioning](partitions.md#how-many-partitions) for the write-side sizing rules that prevent the problem in the first place.

## How it fails { #failure-modes }

- A query's `EXPLAIN` shows full-file scans despite a `WHERE` clause — the filtered column isn't in the footer stats' useful range because the data isn't sorted on it (see the tip above).
- Millions of tiny files from an unbatched streaming writer — planning time dominates before any row is read.
- A wide `SELECT *` over billions of rows reconstructing 80 column chunks per row group when only 2 columns were needed — someone dropped column projection somewhere in the pipeline (a `.select(*)` before a `.filter`, or a BI tool that always requests every column).
- Dictionary encoding silently disabled because a column exceeds the dictionary size threshold (e.g. near-unique strings) — check the page's actual encoding, don't assume.

## Practice the idea

Use the [Parquet row-group explorer](../simulations/parquet-row-group-explorer.html).
Run the same customer filter on sorted and unsorted data. Before toggling, write
down how many row groups you expect the footer statistics to eliminate.

## Check your understanding { #exercise }

A Spark job runs `spark.read.parquet("s3://lake/events/").filter("customer_id = 'cust_0042'").select("customer_id", "latency_ms")`. The table is partitioned by `date` only (not by `customer_id`), and files are written by a nightly job with **no explicit sort** before write, at ~256 MB each.

1. Does `date` partitioning help this query at all? Why or why not — what's missing from the query?
2. Does row-group pruning help find `cust_0042` faster, given files are unsorted?
3. What single change to the write path would make this specific query's `explain()` show meaningfully less I/O, without partitioning by `customer_id`?

??? success "Exit check"
    (1) No — the query never filters on `date`, so partition pruning has nothing to act on; every date's files get listed and opened. (2) Row-group min/max stats on an **unsorted** `customer_id` column are close to useless — with random write order, almost every row group's min/max range spans most customer IDs, so almost no row groups get skipped. (3) Sort the data by `customer_id` (or `customer_id, date`) before writing each file — with sorted data, each row group's `customer_id` min/max becomes a tight range, and most row groups for other customers get skipped via the footer stats alone, without needing a partition column at all.
