---
title: "SaaSCo: The Evolving Company"
description: One SaaS analytics company, eight growth stages, and the architecture that was actually forced by each one — not chosen from a slide.
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

Treated as separate examples, [Data at Scale](../foundations/scale.md), [Kafka](../kafka/index.md), [Flink](../flink/index.md), [Airflow](../airflow/index.md), [Iceberg](../lakehouse/iceberg.md), [ClickHouse](../olap/clickhouse.md), and [Metadata](../metadata/index.md) look like independent lessons about independent products. They are not. They are eight chapters in one company's growth, and every new component in this story is a response to a **measured** bottleneck from the previous stage — never a name from a conference talk. Read this page after you've done at least Phase 0 and skimmed the phase you're currently on; it is a spine connecting lessons you've already read, not new technical content.

This is the same discipline [Architectures](index.md) asks of every design: name the bottleneck before you add the box. SaaSCo just does it eight times in a row, in order, with the receipts.

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

**Forcing function to the next stage:** product ships a fraud-review queue that needs to flag a stolen-card pattern *while the card is still being used*, not tomorrow morning. "Nightly Spark reads the Kafka topic" has a floor of hours, not seconds — the next requirement is a latency class the batch pipeline was never built to hit.

## Stage 4 — Freshness under 10 seconds, Flink appears (Phase 4)

The nightly Spark job already reads from the same Kafka topic Stage 3 built — but "read the topic once a night" and "react to each event within seconds, correctly, even out of order" are different problems, not the same problem run more often. [Flink](../flink/index.md) enters because the requirement is now **event time**, **windowed aggregation**, and **exactly-once state** that survives a crash — a stream processor's job, not a scheduler running a batch job more frequently. See [Flink: time semantics](../flink/time.md) and [windows](../flink/windows.md) for why "just run Spark every minute" quietly breaks the moment events arrive late or out of order, which they always eventually do.

At this stage SaaSCo also gets its first taste of streaming's real cost: keyed state per customer, checkpoints that must complete before the next one starts, and a watermark strategy someone has to actually choose, not accept as a default.

**Forcing function to the next stage:** SaaSCo now has a nightly batch DAG, a CDC pipeline, a fraud-detection Flink job, and three more one-off cron scripts that a new hire wrote to "just get it working." Nobody can say, on a given morning, which of the twelve things that touch the warehouse ran, in what order, or whether the 6 AM report used yesterday's fraud model or this morning's.

## Stage 5 — 100+ workflows, Airflow appears (Phase 5)

This is the same wall [Airflow](../airflow/index.md) opens with: cron scripts that were fine at three jobs become undebuggable at thirty, because "did the thing that has to run before this thing actually finish" stops being answerable by eyeballing a crontab. Airflow's job here is not "prettier cron" — it's dependency-aware scheduling, retries, backfills, and (once the job count crosses into the hundreds) [data-aware scheduling via Assets](../airflow/index.md#sensors-pools-mapping-slas-assets) so a downstream DAG starts when upstream data actually lands, not when a clock guesses it should have.

**Forcing function to the next stage:** the nightly Spark job and the Flink job's checkpoint sink both want to write to the same lake location, and a second Airflow DAG occasionally reads it mid-write. Directory listings on S3 start showing half-written partitions after a crash — the exact [rename-is-not-atomic](../foundations/object-storage.md#why-rename-heavy-systems-struggle) failure mode. "Where is the table?" stops having a good answer.

## Stage 6 — Multiple writers collide, Iceberg appears (Phase 6)

[Why Table Formats Exist](../lakehouse/why-table-formats.md) opens with exactly this scene: raw Parquet on S3, concurrent readers, writers, a job that dies mid-write, and no way to name a consistent snapshot. SaaSCo adopts [Iceberg](../lakehouse/iceberg.md) not because a lakehouse is trendy, but because the object-storage semantics from Stage 3 — no atomic rename, no atomic multi-file commit — have no other fix once Airflow, Spark, and Flink are all writing to the same tables. This is also where the [manifest-instead-of-LIST](../foundations/object-storage.md#why-iceberg-uses-manifests-instead-of-list) design earns its keep: SaaSCo's file count is now in the hundreds of thousands, and listing a prefix during query planning is no longer fast enough to ignore.

**Forcing function to the next stage:** the product team ships a customer-facing dashboard. "Nightly" and even "every 15 minutes via Iceberg snapshot" are both too slow — the requirement becomes sub-second, interactive filtering by tenant.

## Stage 7 — Customer dashboards need sub-second, ClickHouse appears (Phase 8)

Iceberg and Trino answer "give me an answer in seconds to minutes over history." They do not answer "render this chart while a customer is looking at it." [ClickHouse](../olap/clickhouse.md) enters the stack as a **serving** layer fed from the same Kafka topic Stage 3 built, not as a replacement for the lakehouse — this is the same hot-path/cold-path split named in [Architectures](index.md#the-five-systems) for the observability and SaaS-analytics rows. [Why Columnar Storage](../olap/columnar-storage.md) and [ClickHouse `ORDER BY`](../olap/clickhouse.md) are the two lessons that explain why this specific box, and not "a faster Postgres," was the answer.

**Forcing function to the next stage:** SaaSCo now has 400 engineers, dozens of pipelines writing to the same tables, and a revenue dashboard that showed ₹0 one morning because a Spark job's schema change silently broke a downstream ClickHouse materialization. Nobody can say who owns `events_agg`, whether it's fresh, or which of three "active users" definitions is correct.

## Stage 8 — 400 engineers, contracts and lineage become load-bearing (Phase 10)

This is the [metadata](../metadata/index.md) module's own opening argument: *"a platform with 10,000 tables and no operating model for metadata is a swamp with a search box."* At Stage 1 through 6, one team could hold the whole pipeline in their heads. At Stage 8, that stops being true, and the failure mode changes shape — it's no longer "the job is too slow," it's "nobody can tell you if the number is right." [Data Contracts](../foundations/data-contracts.md) (schema and semantic compatibility between producer and consumer teams) and [metadata/lineage](../metadata/index.md) (who owns what, and can you walk backward from a wrong dashboard to its source) become the bottleneck — the same way shuffle bytes were the bottleneck at Stage 2.

---

## The pattern across all eight stages

| Stage | Volume / requirement | New component | What actually forced it |
|---|---|---|---|
| 1 | 40 GB/day | None | — |
| 2 | 400 GB/day | Spark | One machine's disk/NIC misses the SLA |
| 3 | 4 TB/day | Kafka | Many producers, one ordered durable log needed |
| 4 | Freshness < 10 s | Flink | Windowed, event-time, exactly-once state a scheduled batch job cannot provide |
| 5 | 100+ workflows | Airflow | Dependency-aware scheduling and backfills a crontab cannot express |
| 6 | 4+ TB/day (multi-writer) | Iceberg | No atomic commit on raw object storage, now with three engines writing |
| 7 | 4+ TB/day | ClickHouse | Sub-second dashboard requirement, not satisfiable by the lakehouse |
| 8 | 400 engineers | Contracts + lineage | Ownership and correctness, not volume, is now the bottleneck |

Notice what never happens: SaaSCo never adopts a component because of what it might need next quarter. Every arrow in this table is a **measured**, present-tense bottleneck — exactly the V1-then-bottleneck-then-V2 discipline in [How to read an architecture page](index.md#how-to-read-an-architecture-page). If you're designing a system at work and can't fill in a row like this — a specific number, a specific failure, a specific next component — you're choosing the box before you've earned it. See the [architecture-deletion exercise](index.md#exercise-delete-the-unnecessary-architecture) for the mirror image of this story: a stack with components that were never earned at all.

---

## The final payoff

```text
START (Stage 1)                          END (Stage 8)

Postgres                                 CDC (Debezium)
Cron                                     Kafka
Python                                   Spark
                                          Flink
                                          S3
                                          Iceberg
                                          Airflow
                                          Trino
                                          ClickHouse
                                          Metadata / Contracts
                                          Lineage
                                          Observability
```

Eight components, eight stages, eight rows in the table above with a number and a failure attached to each one. Before you conclude that this is simply what "real" data platforms look like, ask the question this whole page was building toward:

> Which of these additions were caused by an actual, measured constraint SaaSCo hit — and which would a team have added anyway, on schedule, whether or not the constraint ever arrived?

Every stage above names its forcing function because that is the discipline this academy is teaching: not "here is the modern data stack," but "here is what each box cost to *not* have, at the moment not having it started actually hurting." A team that reads this page and walks away wanting to install all eight components in a greenfield 30 GB/day project has learned the opposite lesson — see the [architecture-deletion exercise](index.md#exercise-delete-the-unnecessary-architecture) and run it on this exact stack before you believe you've understood SaaSCo's story.
