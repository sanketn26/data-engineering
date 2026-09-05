---
title: Cost Engineering
description: Turn architecture choices into workload units, budgets, and scaling forecasts.
---

# Cost Engineering

**Time:** 45 minutes reading + 45 minutes worksheet<br>
**Prerequisites:** [scale](../foundations/scale.md), [data movement](../foundations/data-movement.md)<br>
**Outcomes:** calculate unit cost; identify dominant cost drivers; compare managed and self-operated designs.

Cost is a workload property. “Kafka is expensive” is not useful; “three replicated copies plus cross-AZ consumers cost X per retained event” is reviewable.

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

## Cost failure modes

- Autoscaling has no maximum or consumer budget.
- A full-refresh model grows linearly forever.
- Small files multiply object-store requests and planning.
- Cross-region replication is enabled without a recovery requirement.
- Shared infrastructure hides tenant unit cost.
- “Open source” is counted as zero while two engineers operate it.

## Exercise

Compare ClickHouse serving with Trino repeatedly scanning Iceberg for a 200 QPS dashboard. State rows/day, compressed bytes, bytes read/query, cache assumption, replicas, and operator time. Find the break-even variable rather than inventing a universal winner.

??? success "Exit check"
    The answer expresses assumptions and unit cost, includes network/replication/labor, and names a measurable break-even point such as scans per day or retained hot data—not merely instance prices.
