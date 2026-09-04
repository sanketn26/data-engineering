---
title: Cloud Data Warehouses
description: Decide when managed warehouse separation, elasticity, and operations beat an open lake stack — and know what's actually running under BigQuery, Snowflake, and Redshift.
---

# Cloud Data Warehouses

**Time:** 70 minutes reading + 30 minutes decision exercise<br>
**Prerequisites:** [columnar storage](../olap/columnar-storage.md), [Trino](trino.md), [data modelling](../foundations/data-modelling.md)<br>
**Outcomes:** explain what BigQuery, Snowflake, and Redshift actually do with a query and a byte on disk; predict which one queues, which one scans everything, and which one skews; compare managed warehouses with lakehouse/query-engine stacks; choose from workload evidence, not the vendor slide.

A finance analyst runs the same `GROUP BY region` query every Monday. On BigQuery it costs money per byte scanned whether the warehouse is busy or idle. On Snowflake it queues behind Friday's batch load if the warehouse is undersized. On Redshift it is fast for years, then one day a table grows past the machine's memory and every query holding a hash table for that table starts spilling to disk together.

None of that is a vendor defect. It is the architecture showing through the SQL. BigQuery, Snowflake, and Redshift are not interchangeable implementations of Trino wearing different logos — they package storage, execution, governance, workload management, and operations behind three different physical models and three different commercial contracts.

---

## Use case

Same workload as the rest of this module: SaaS analytics, hundreds of millions of events per day, an analyst who wants `p95 latency by plan tier` this afternoon — except now the company decided it does not want to operate Iceberg compaction, a catalog, and a Trino cluster. It wants to `COPY INTO` or stream the events into a managed warehouse and let the vendor own storage layout, scaling, and the 2 a.m. pages.

```sql
SELECT plan, APPROX_PERCENTILE(latency_ms, 0.95) AS p95
FROM analytics.events
WHERE event_date BETWEEN '2024-06-01' AND '2024-06-07'
GROUP BY plan;
```

The SQL is nearly identical across BigQuery, Snowflake, and Redshift. What happens underneath it — what gets scanned, what gets billed, what queues, what skews — is not.

---

## Why this is hard

Three vendors sell "just write SQL," and each one hides a different failure mode behind that promise.

- **BigQuery** bills (on-demand) by **bytes scanned**, not by time. A `SELECT *` on a wide table with no partition filter is a cost incident, not a performance incident — the query may finish in seconds and still cost more than a day of Snowflake compute.
- **Snowflake** separates storage from compute into **virtual warehouses**, but a warehouse is a fixed-size cluster you provision. Undersize it and queries queue behind each other; oversize it and you pay for idle credits.
- **Redshift** is the most "you own it" of the three: distribution keys, sort keys, and vacuum/analyze are your responsibility even though AWS runs the boxes. Get the distribution key wrong and one node does the work of the whole cluster while the others idle.

The trap is treating "managed" as "the physics went away." It didn't. It moved into billing dimensions and configuration knobs that look optional and are not.

---

## Intuition

Think of each warehouse as **the same three ingredients — columnar storage, a cost-based optimizer, distributed execution — assembled around a different scarce resource**:

| Warehouse | The resource you actually manage |
|---|---|
| BigQuery | **Bytes scanned** (on-demand) or **slot-seconds** (capacity/editions) |
| Snowflake | **Warehouse size and count** (concurrency and queueing) |
| Redshift | **Node/slice count and physical layout** (distkey, sortkey) |

Every gotcha in this page is a symptom of forgetting which resource you are actually spending.

---

## Internals

### BigQuery: Dremel over Colossus

Storage (**Colossus**, Google's distributed file system) and compute (**Dremel**, the query execution engine) are fully separate services, elastic independently, with no persistent cluster you provision.

- Tables are stored in **Capacitor**, a columnar format, sharded across Colossus. You never see files or manage compaction directly.
- A query runs as a tree of **mixers** and **leaf nodes**: leaf nodes read columnar shards in parallel, mixers merge partial results up the tree — the same shuffle-tree shape as Trino's stages, but Google-operated and typically backed by an in-memory shuffle tier rather than local disk.
- Compute is measured in **slots** (a bundle of CPU, RAM, and IO). **On-demand** pricing bills bytes scanned and Google allocates slots dynamically; **capacity pricing (editions/reservations)** buys a pool of slots you share across queries and bills slot-time regardless of bytes.
- Pruning comes from **partitioning** (by ingestion time or a date/timestamp column — at most one partition column) and **clustering** (up to four columns that sort storage blocks so BigQuery can skip blocks by min/max, similar to a ClickHouse granule skip).

### Snowflake: micro-partitions and virtual warehouses

- On load, Snowflake automatically slices data into **micro-partitions**: immutable, compressed, columnar blocks of roughly 50–500 MB uncompressed. Every micro-partition carries per-column min/max metadata.
- A separate **cloud services layer** holds that metadata and does pruning *before* any compute cluster touches storage — this is why `SHOW` / metadata-only queries can return instantly with zero warehouse running.
- Compute is a **virtual warehouse**: an independent MPP cluster, T-shirt sized (XS doubles to S doubles to M …), that reads shared, decoupled cloud storage. Multiple warehouses can read the same tables with no contention; **multi-cluster warehouses** auto-scale out (not up) to absorb concurrent queries instead of queueing them.
- **Clustering keys** are optional and reorder micro-partitions over time via an automatic background service (**automatic clustering**) that burns credits — unlike BigQuery's clustering, which is free storage layout, Snowflake reclustering is metered compute.
- A 24-hour **result cache** and per-warehouse local SSD cache mean the fastest query is often the one you already ran.

### Redshift: MPP, slices, distribution, and sort keys

- Classic Redshift is **shared-nothing MPP**: a leader node plans and distributes work; each compute node splits into **slices** (one slice per vCPU core), and each slice owns a physical subset of every table's rows.
- **Distribution style** decides which slice a row lives on: `KEY` (hash a column — co-locates matching join keys, the highest-leverage and highest-risk choice), `ALL` (replicate small dimension tables to every node, avoiding a shuffle), `EVEN` (round-robin, safe default, no join locality).
- **Sort key** (compound or interleaved) decides on-disk row order per slice, enabling zone-map pruning — the Redshift analogue of ClickHouse's `ORDER BY` or Iceberg's file-level min/max stats.
- **RA3 nodes** decouple storage from compute (managed storage backed by S3-like infrastructure with a local SSD cache), closing much of the gap with BigQuery/Snowflake's separation — but distribution keys and sort keys are still yours to choose and still physically place data.
- **Redshift Spectrum** lets Redshift compute query external tables directly in S3 — architecturally the same federation idea as Trino, scoped to Redshift's optimizer.
- **Concurrency scaling** spins up transient additional clusters for read queries when the main cluster queues; it does not fix a bad distribution key.

### One table, three physical fates

| Concern | BigQuery | Snowflake | Redshift |
|---|---|---|---|
| Storage/compute separation | Full (Colossus / Dremel) | Full (cloud storage / virtual warehouses) | Full on RA3; coupled on legacy dense-storage/compute nodes |
| Unit of pruning | Partition + cluster block | Micro-partition min/max | Sort-key zone map |
| Unit of parallelism | Slot | Warehouse cluster | Slice |
| What skew looks like | A slot stuck on one huge shard | A warehouse queueing behind one heavy query | One slice at 100% CPU, the rest idle |
| Who chooses physical layout | Mostly the optimizer (partition/cluster hints) | Mostly automatic (clustering optional) | You, explicitly (distkey/sortkey) |

---

## How

### BigQuery: dry-run before you run

```sql
-- Always dry-run an unfamiliar query first — it's free and shows bytes billed
-- bq query --dry_run --use_legacy_sql=false 'SELECT ...'

SELECT plan, APPROX_QUANTILES(latency_ms, 100)[OFFSET(95)] AS p95
FROM `project.analytics.events`
WHERE event_date BETWEEN '2024-06-01' AND '2024-06-07'  -- partition filter: required
GROUP BY plan;
```

```sql
-- What did this actually cost, and did it use a partition filter?
SELECT job_id, total_bytes_billed, query
FROM `region-us`.INFORMATION_SCHEMA.JOBS_BY_PROJECT
WHERE creation_time > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 DAY)
ORDER BY total_bytes_billed DESC
LIMIT 20;
```

### Snowflake: size the warehouse to the query, not the other way around

```sql
USE WAREHOUSE analytics_wh;

SELECT plan, APPROX_PERCENTILE(latency_ms, 0.95) AS p95
FROM analytics.events
WHERE event_date BETWEEN '2024-06-01' AND '2024-06-07'
GROUP BY plan;

-- Is this table worth a clustering key, or is metadata pruning already enough?
SELECT SYSTEM$CLUSTERING_INFORMATION('analytics.events', '(event_date)');

-- Is a warehouse queueing instead of scaling?
SELECT * FROM TABLE(INFORMATION_SCHEMA.WAREHOUSE_LOAD_HISTORY(
  DATE_RANGE_START => DATEADD('hour', -1, CURRENT_TIMESTAMP())));
```

### Redshift: read the plan for a broadcast or a skew, not just a row count

```sql
EXPLAIN
SELECT plan, PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY latency_ms) AS p95
FROM analytics.events
WHERE event_date BETWEEN '2024-06-01' AND '2024-06-07'
GROUP BY plan;

-- Skew and unsorted percentage, table by table
SELECT "table", skew_rows, unsorted, size
FROM svv_table_info
ORDER BY skew_rows DESC;

-- Did any step spill or broadcast unexpectedly?
SELECT query, step, rows, workmem, is_diskbased
FROM svl_query_summary
WHERE query = pg_last_query_id();
```

---

## Gotchas

!!! production-gotcha "BigQuery: SELECT * on a partitioned table"
    Partitioning only saves you if the query also prunes columns. `SELECT *` on a 40-column, 5-year table with a one-day `WHERE` still scans every column for that day — bytes billed scale with columns × days, not just days.

!!! production-gotcha "Snowflake: warehouse too small for the load window"
    An `XS` warehouse doing a nightly load and hourly dashboards will queue the dashboards behind the load. The fix is usually a second, right-sized warehouse for the dashboard workload, not a bigger single warehouse — Snowflake bills warehouse-seconds whether or not every query needs that much compute.

!!! production-gotcha "Redshift: distribution key becomes the join key becomes the hotspot"
    A `KEY`-distributed fact table on `customer_id` looks perfect for joins to a dimension table also keyed on `customer_id` — until one enterprise tenant is 40% of rows and their slice does 40% of every query's work while the rest of the cluster waits.

!!! production-gotcha "All three: reservations/warehouses/nodes sized for last year's data"
    BigQuery slot reservations, Snowflake warehouse size, and Redshift node count are capacity decisions made once and rarely revisited. Data volume does not ask permission before it 10×s.

!!! production-gotcha "Vendor-specific SQL as an unpriced migration cost"
    `APPROX_QUANTILES` (BigQuery) vs `APPROX_PERCENTILE` (Snowflake) vs `PERCENTILE_CONT` (Redshift) is the easy case. Stored procedures, UDFs, and semi-structured (`STRUCT`/`VARIANT`/`SUPER`) column types are the ones that turn "switch warehouses" into a quarter-long migration.

---

## Failure modes

| Failure | What it looks like | Root |
|---|---|---|
| **Runaway on-demand bill (BigQuery)** | One dashboard refresh costs more than a day of normal spend | Unpartitioned or unfiltered scan on a huge table, no `maximum_bytes_billed` guard |
| **Dashboard queue (Snowflake)** | Queries pile up in `QUERY_HISTORY` with nonzero queue time | Single undersized warehouse serving both batch load and interactive traffic |
| **One slice, all the wait (Redshift)** | `svv_table_info` shows high `skew_rows`; one node's CPU pegged, others idle | Distribution key with a dominant value (a whale tenant, a default/null bucket) |
| **Silent full scan** | Query "worked" but billed/ran far more than expected | Function on the partition/sort column disabled pruning (`DATE(ts) = ...` instead of `ts BETWEEN ...`) |
| **Concurrency limit hit** | New queries rejected or queued cluster-wide | Too many warehouses/reservations/WLM queues provisioned for peak, not headroom |
| **Cost attribution gap** | Finance cannot tell which team spent what | No labels/tags (BigQuery labels, Snowflake resource monitors + query tags, Redshift WLM queues) enforced at query time |

---

## Debugging

The three vendors expose the same three questions through different system views — learn the question, not just the syntax.

1. **What did this query actually touch?** BigQuery: `INFORMATION_SCHEMA.JOBS_BY_PROJECT.total_bytes_billed`. Snowflake: `QUERY_HISTORY.bytes_scanned` / `EXPLAIN USING TEXT`. Redshift: `EXPLAIN` plan + `svl_query_summary.rows`.
2. **Was it parallel or did it bottleneck on one unit?** BigQuery: per-stage timing in the query execution details UI. Snowflake: `QUERY_HISTORY` execution profile, look for one operator dominating wall time. Redshift: `svv_table_info.skew_rows` and per-slice timing in `svl_query_summary`.
3. **Was it queued behind something else?** BigQuery: reservation slot contention in `INFORMATION_SCHEMA.JOBS_TIMELINE`. Snowflake: `WAREHOUSE_LOAD_HISTORY` queued-vs-running load. Redshift: WLM queue wait time in `stl_wlm_query`.

If you cannot answer all three for your slowest query this week, you are guessing at a capacity decision, not making one.

---

## Scale 10× / 100× / 1000×

Assume today's baseline: ~10 TB in the warehouse, a dozen analysts, a nightly load plus intraday dashboards.

| Scale | What breaks | What you change |
|---|---|---|
| **10×** (~100 TB) | BigQuery: bytes-scanned bill grows with data even if query patterns don't change. Snowflake: one warehouse now serves noticeably more concurrent load, queueing starts. Redshift: distribution skew that was invisible at 10 TB now stalls real queries. | Partition/cluster tightening (BigQuery), a second warehouse split by workload (Snowflake), redistribution or a switch to `ALL`/even distribution for the skewed table (Redshift) |
| **100×** | BigQuery: on-demand cost becomes unpredictable enough that finance asks for a cap — time to evaluate capacity pricing. Snowflake: multi-cluster warehouses become necessary, not optional, and reclustering credits become a real line item. Redshift: single-cluster MPP hits a ceiling; RA3 managed storage or a move toward Spectrum for cold data. | Reservations/editions (BigQuery), auto-scaling multi-cluster warehouses (Snowflake), RA3 + Spectrum for cold tiers (Redshift) |
| **1000×** | The warehouse is no longer the only consumer of this data — ML, streaming, and other engines want the same bytes without three copies. | Externalize cold history to open table formats (Iceberg/Parquet) queryable by the warehouse's external-table feature *and* by Trino/Spark, keeping the managed warehouse for hot, governed BI |

None of the three warehouses' elasticity substitutes for physical design. Elastic compute makes a bad layout expensive faster, not correct.

---

## Trade-offs

| You get | You give up |
|---|---|
| No cluster, catalog, or compaction to operate | Storage layout choices become billing/queueing choices you must still learn |
| Governance, ACLs, lineage, and support bundled | Vendor-specific SQL, procedures, and semi-structured types as exit friction |
| Elastic compute matched to a commercial plan | A ceiling where the "just add compute" story gets expensive fast (BigQuery bytes, Snowflake credits, Redshift nodes) |
| One system for storage, compute, and workload isolation | The isolation is only as good as the queues/warehouses/reservations you actually configured, not the ones you assumed existed |

A managed warehouse is a **good default** for a team that wants SQL analytics without operating a lake stack. It is a **bad default** once multiple engines (ML training, streaming, ad-hoc Spark) need the same bytes — that is when the "just copy it out" tax starts compounding.

---

## Alternatives

| Option | When it wins |
|---|---|
| **Trino + Iceberg** | Multiple engines need the same open files; portability and no per-vendor lock-in matter more than turnkey ops — see [Trino](trino.md) |
| **Spark SQL** | Heavy transformation, iterative ML, or huge writes dominate over interactive BI |
| **ClickHouse / Pinot** | Sub-second, high-QPS product-facing dashboards — a warehouse's query tax is the wrong shape here; see [ClickHouse vs Trino](../comparisons/clickhouse-vs-trino.md) |
| **Databricks SQL warehouse** | You already run Databricks for ML/ETL and want one billing surface across both |
| **DuckDB** | Single-analyst, laptop-scale Parquet; no concurrency or governance need |

---

## Apply

You are looking at a managed-warehouse decision when:

1. The team operating the platform is small and does not want to run compaction, a catalog, or a query-engine cluster.
2. The workload is governed BI/SQL analytics, not multi-engine ML or sub-second serving.
3. Vendor SQL, procedures, and semi-structured types are an acceptable one-way door for this workload.

Then: pick the warehouse whose scarce resource matches your actual pain — bytes-scanned unpredictability (avoid pure on-demand BigQuery at large unfiltered-scan risk), concurrency queueing (Snowflake multi-cluster), or physical layout ownership (Redshift, if you're prepared to own distkey/sortkey). Put a **cost guard** in from day one — `maximum_bytes_billed`, a resource monitor, or a WLM queue limit — before the first accidental full scan, not after.

Related: [Trino](trino.md), [columnar storage](../olap/columnar-storage.md), [cost engineering](../reference/cost-engineering.md), [SaaS analytics architecture](../architectures/analytics-platform.md).

---

## Exercise

A 12-person company has two data engineers, 20 TB in the warehouse, spiky weekday BI traffic (idle nights/weekends), and no ML engine requirement. Compare a managed warehouse with Trino + Iceberg. Then decide **which** managed warehouse fits best and name the first metric you'd watch to know you chose wrong.

??? success "Exit check"
    A managed warehouse is the defensible V1 — two engineers cannot also operate Iceberg compaction, a catalog, and a Trino cluster. Given **spiky, idle-at-night** traffic, prefer BigQuery on-demand or Snowflake with auto-suspend over a warehouse that bills for idle nodes around the clock (a classic Redshift dense-compute trap); watch **bytes billed per query** (BigQuery) or **credits consumed while idle vs active** (Snowflake) in the first month. Revisit the choice when a second engine (ML training, streaming) needs the same bytes, when data crosses roughly 100 TB, or when the bill's trend line stops matching the traffic's — not because an architecture diagram looks more modern.
