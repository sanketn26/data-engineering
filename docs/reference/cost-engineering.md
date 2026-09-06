---
title: Cost Engineering
description: Turn architecture choices into workload units, budgets, and scaling forecasts.
---

# Cost Engineering

**Time:** 45 minutes reading + 45 minutes worksheet<br>
**Prerequisites:** [scale](../foundations/scale.md), [data movement](../foundations/data-movement.md)<br>
**Outcomes:** calculate unit cost; identify dominant cost drivers; compare managed and self-operated designs.

Finance pings the platform channel: the Kafka bill tripled this quarter and nobody can explain why in one sentence. Traffic is up maybe 20%.

A. Blame the replication factor. B. Blame cross-AZ consumer fetches. C. Blame retention-window creep. D. Blame partition count. E. All of the above, in proportions nobody has measured.

"Kafka is expensive" gets you nowhere in that meeting — only a broken-down number does. Cost is a workload property, and the rest of this page turns "expensive" into "three replicated copies plus cross-AZ consumers cost X per retained event": a claim you can actually defend.

## Unit economics

Choose a consumer-facing unit:

- cost per million events ingested;
- cost per TB transformed;
- cost per dashboard query;
- cost per active tenant;
- cost per retained device-month.

Then model:

```text
monthly cost = storage + compute + network + requests + licenses + platform labor
unit cost = monthly cost / useful units delivered
```

Compression, replication, retries, intermediate writes, and retention apply multipliers. Include development and backfill capacity, not only the happy-path daily run.

## Capacity worksheet

| Driver | Current | 10× response |
|---|---:|---|
| Events/day | 500 M | partition/key review before brokers |
| Raw bytes/day | 400 GB | compression and retention tiers |
| Shuffle bytes/day | 8 TB | incremental compute/pre-aggregation |
| Dashboard scans | 20 TB/day | physical layout/materialization |
| Cross-AZ bytes | 3 TB/day | placement and topology |
| On-call hours/month | 60 | managed service or simplification |

Do sensitivity analysis on the two largest terms. A precise estimate of tiny S3 request cost does not compensate for ignoring engineer time or a repeated 20 TB scan.

## What costs money, per system

Before modelling unit economics, name the specific bytes/requests/CPU-seconds each system bills you for. "Kafka is expensive" is not a line item; these are.

**Spark**

| Driver | What drives it |
|---|---|
| Scan bytes | Unpruned reads — missing partition filter or column projection |
| Shuffle bytes | Joins/aggregations without broadcast; skew multiplies wall time, not $ directly, but idle executors still bill |
| CPU-seconds | Executor-hours × core count, regardless of whether cores are waiting on I/O |
| Executor memory | Reserved whether used or spilled; oversized executors for a skewed job waste this per-task |
| Object-store requests | Small-file `GET`/`PUT` counts — see [object storage internals](../foundations/object-storage.md) |

**Kafka**

| Driver | What drives it |
|---|---|
| Retention × replication | Bytes stored × replication factor × retention window — the multiplier most estimates forget |
| Network | Cross-AZ replica traffic and cross-AZ consumer fetches, often the largest line item in cloud Kafka |
| Partitions | More partitions than needed costs metadata, rebalance time, and often broker memory — not compute directly |
| Consumer compute | Idle consumer fleets sized for peak lag recovery, not steady state |

**ClickHouse**

| Driver | What drives it |
|---|---|
| Stored bytes | Post-compression, but replicated — a 3× replication factor is a 3× storage bill |
| Replicas | Read/write availability trade against this multiplier directly |
| Merges | Background CPU proportional to insert rate and part count — see [ClickHouse](../olap/clickhouse.md) `too many parts` |
| Query scans | Bytes read per query × QPS; a bad `ORDER BY` inflates this without inflating stored bytes |

**Flink**

| Driver | What drives it |
|---|---|
| State size | Checkpoint size scales with retained keyed state — the dominant cost for long windows or unbounded state |
| Checkpoint frequency | More frequent checkpoints trade faster recovery for more sustained I/O and network to the state backend/store |
| Network shuffle | `keyBy`/rebalance between operators, same cost shape as Spark shuffle but continuous rather than per-batch |
| Parallelism | Task slots × TaskManager count; over-provisioned parallelism idles slots the same way over-provisioned Spark executors do |
| Retained state (RocksDB) | Local disk on TaskManagers plus the state backend's own storage bill if state exceeds what compacts away |

**Iceberg / lakehouse**

| Driver | What drives it |
|---|---|
| Small files | Planning cost (opening many footers) and storage request count — see [Parquet Internals](../foundations/parquet-internals.md) |
| Metadata planning | Manifest list/manifest reads scale with snapshot count and file count until compacted |
| Compaction | Background rewrite jobs are real compute cost, not "free" cleanup |
| Object requests | `LIST`/`GET`/`PUT` volume — see [object storage internals](../foundations/object-storage.md) |

Once these are named per system, "$ / TB processed," "$ / million events," and "$ / query" become numbers you can actually defend in a design review — not architecture diagrams with a vibe attached.

## Cost failure modes

- Autoscaling has no maximum or consumer budget.
- A full-refresh model grows linearly forever.
- Small files multiply object-store requests and planning.
- Cross-region replication is enabled without a recovery requirement.
- Shared infrastructure hides tenant unit cost.
- “Open source” is counted as zero while two engineers operate it.

## Check your understanding { #exercise }

Compare ClickHouse serving with Trino repeatedly scanning Iceberg for a 200 QPS dashboard. State rows/day, compressed bytes, bytes read/query, cache assumption, replicas, and operator time. Find the break-even variable rather than inventing a universal winner.

??? success "Exit check"
    The answer expresses assumptions and unit cost, includes network/replication/labor, and names a measurable break-even point such as scans per day or retained hot data—not merely instance prices.
