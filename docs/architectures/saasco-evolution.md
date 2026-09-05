---
title: "SaaSCo: The Evolving Company"
description: One SaaS analytics company, six growth stages, and the architecture that was actually forced by each one — not chosen from a slide.
---

# SaaSCo: The Evolving Company

Every architecture page in this academy uses the same event:

```json
{
  "timestamp": "2024-01-15T10:30:00Z",
  "customer_id": "cust_0042",
  "user_id": "user_98712",
  "service": "api-gateway",
  "endpoint": "/v2/events",
  "region": "eu-west-1",
  "latency_ms": 45,
  "status_code": 200,
  "bytes": 1024
}
```

Treated as separate examples, [Data at Scale](../foundations/scale.md), [Kafka](../kafka/index.md), [Iceberg](../lakehouse/iceberg.md), [ClickHouse](../olap/clickhouse.md), and [Metadata](../metadata/index.md) look like independent lessons about independent products. They are not. They are six chapters in one company's growth, and every new component in this story is a response to a **measured** bottleneck from the previous stage — never a name from a conference talk. Read this page after you've done at least Phase 0 and skimmed the phase you're currently on; it is a spine connecting lessons you've already read, not new technical content.

This is the same discipline [Architectures](index.md) asks of every design: name the bottleneck before you add the box. SaaSCo just does it six times in a row, in order, with the receipts.

---

## Stage 1 — 40 GB/day, one machine (Phase 0)

**Team:** two engineers. **Stack:** a pandas script on a laptop, reading Parquet dumped nightly from Postgres.

`SELECT avg(latency_ms) GROUP BY customer_id, hour` for yesterday finishes in twelve minutes. Product is happy. There is no architecture here worth drawing — see [When *not* to use distributed data systems](../foundations/index.md#when-not-to-use-distributed-data-systems). The only thing that matters is that someone is already writing down volume, not vibes.

**Forcing function to the next stage:** one enterprise customer signs. Volume goes from 40 GB/day to 400 GB/day — not a smooth ramp, a step function, because [SaaS volume follows tenants, not time](../foundations/scale.md#production-gotchas). The laptop OOMs at `read_parquet` the same week the deal closes.

## Stage 2 — 400 GB/day, Spark appears (Phase 0 → Phase 3)

This is [Data at Scale](../foundations/scale.md)'s own worked **10×** row: *"40 GB → 400 GB/day: Spark on 10 r5.xlarge, Parquet, partition by `date`. p95 job ~15 minutes."* Spark is not adopted because it's "the modern way" — it's adopted because a single box's disk and NIC cannot read 400 GB inside the overnight SLA. See [Distributed Execution](../foundations/distributed-execution.md) for why a job becomes stages and tasks the moment this happens, and [Spark](../spark/index.md) for the first real implementation of Phase 0's primitives.

Everything still lands as one nightly batch. There is still no streaming, no Kafka, no lakehouse — because nothing yet requires them.

**Forcing function to the next stage:** more producers. Multiple services now write events, arriving throughout the day, and the object store's PUT rate from many independent writers starts to misbehave — the same [request-rate hotspotting](../foundations/object-storage.md#consistency-and-request-rate-considerations) that shows up whenever many writers hit adjacent keys at once.

## Stage 3 — 4 TB/day, Kafka appears (Phase 2)

This is scale.md's **100×** row: *"100+ cores, Iceberg with compaction, shuffle ~TB, must partition prune aggressively. Kafka in front because object-store PUT rate from many producers becomes a problem."* Kafka's reason to exist here is exactly the one named in [Architectures](index.md#the-questions-before-any-box): independent consumers need the *same* log, and many producers need one durable, ordered write path instead of each hammering S3 directly. See [The Log Abstraction](../kafka/log.md) for why a log is not a queue and not a database.

At this stage SaaSCo also gets its first real multi-writer problem: the Spark nightly job and a new near-real-time job both want to write to the same lake location. Directory listings on S3 start showing half-written partitions after a crash — the exact [rename-is-not-atomic](../foundations/object-storage.md#why-rename-heavy-systems-struggle) failure mode.

**Forcing function to the next stage:** "where is the table?" stops having a good answer. Concurrent readers and writers can no longer safely share a raw-Parquet prefix.

## Stage 4 — Multiple writers collide, Iceberg appears (Phase 6)

[Why Table Formats Exist](../lakehouse/why-table-formats.md) opens with exactly this scene: raw Parquet on S3, concurrent readers, writers, a job that dies mid-write, and no way to name a consistent snapshot. SaaSCo adopts [Iceberg](../lakehouse/iceberg.md) not because a lakehouse is trendy, but because the object-storage semantics from Stage 3 — no atomic rename, no atomic multi-file commit — have no other fix. This is also where the [manifest-instead-of-LIST](../foundations/object-storage.md#why-iceberg-uses-manifests-instead-of-list) design earns its keep: SaaSCo's file count is now in the hundreds of thousands, and listing a prefix during query planning is no longer fast enough to ignore.

**Forcing function to the next stage:** the product team ships a customer-facing dashboard. "Nightly" and even "every 15 minutes via Iceberg snapshot" are both too slow — the requirement becomes sub-second, interactive filtering by tenant.

## Stage 5 — Customer dashboards need sub-second, ClickHouse appears (Phase 8)

Iceberg and Trino answer "give me an answer in seconds to minutes over history." They do not answer "render this chart while a customer is looking at it." [ClickHouse](../olap/clickhouse.md) enters the stack as a **serving** layer fed from the same Kafka topic Stage 3 built, not as a replacement for the lakehouse — this is the same hot-path/cold-path split named in [Architectures](index.md#the-five-systems) for the observability and SaaS-analytics rows. [Why Columnar Storage](../olap/columnar-storage.md) and [ClickHouse `ORDER BY`](../olap/clickhouse.md) are the two lessons that explain why this specific box, and not "a faster Postgres," was the answer.

**Forcing function to the next stage:** SaaSCo now has 400 engineers, dozens of pipelines writing to the same tables, and a revenue dashboard that showed ₹0 one morning because a Spark job's schema change silently broke a downstream ClickHouse materialization. Nobody can say who owns `events_agg`, whether it's fresh, or which of three "active users" definitions is correct.

## Stage 6 — 400 engineers, contracts and lineage become load-bearing (Phase 10)

This is the [metadata](../metadata/index.md) module's own opening argument: *"a platform with 10,000 tables and no operating model for metadata is a swamp with a search box."* At Stage 1 through 4, one team could hold the whole pipeline in their heads. At Stage 6, that stops being true, and the failure mode changes shape — it's no longer "the job is too slow," it's "nobody can tell you if the number is right." [Data Contracts](../foundations/data-contracts.md) (schema and semantic compatibility between producer and consumer teams) and [metadata/lineage](../metadata/index.md) (who owns what, and can you walk backward from a wrong dashboard to its source) become the bottleneck — the same way shuffle bytes were the bottleneck at Stage 2.

---

## The pattern across all six stages

| Stage | Volume | New component | What actually forced it |
|---|---|---|---|
| 1 | 40 GB/day | None | — |
| 2 | 400 GB/day | Spark | One machine's disk/NIC misses the SLA |
| 3 | 4 TB/day | Kafka | Many producers, one ordered durable log needed |
| 4 | 4 TB/day (multi-writer) | Iceberg | No atomic commit on raw object storage |
| 5 | 4+ TB/day | ClickHouse | Sub-second dashboard requirement, not satisfiable by the lakehouse |
| 6 | 400 engineers | Contracts + lineage | Ownership and correctness, not volume, is now the bottleneck |

Notice what never happens: SaaSCo never adopts a component because of what it might need next quarter. Every arrow in this table is a **measured**, present-tense bottleneck — exactly the V1-then-bottleneck-then-V2 discipline in [How to read an architecture page](index.md#how-to-read-an-architecture-page). If you're designing a system at work and can't fill in a row like this — a specific number, a specific failure, a specific next component — you're choosing the box before you've earned it. See the [architecture-deletion exercise](index.md#exercise-delete-the-unnecessary-architecture) for the mirror image of this story: a stack with components that were never earned at all.
