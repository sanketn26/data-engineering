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

**Team:** five engineers.

Start by refusing the lazy answer. **400 GB is not, by itself, beyond one modern machine.** Do the arithmetic before you distribute anything:

```text
400 GB to scan in a 15-minute window
400 GB / 900 s ≈ 444 MB/s sustained

A single box with NVMe storage is comfortably in that class.
```

So "400 GB exists" does not force Spark. If the requirement were only *read 400 GB of Parquet and average a column*, the honest answer is still one machine — exactly what [When *not* to use distributed data systems](../foundations/index.md#when-not-to-use-distributed-data-systems) argues, and the discipline this whole page is trying to teach.

What actually forces the change is the **workload on top of the bytes**:

```text
400 GB/day raw events

- decode deeply nested JSON before anything is queryable
- join against 150 GB of slowly-changing dimensions
- sessionize per user (sort + stateful pass over the whole day)
- several wide group-bys at different grains
- emit 20 tenant-level output tables
- finish inside a 15-minute SLA
- AND backfill seven days whenever a schema correction lands
```

Now re-do the arithmetic. The scan is the cheap part. The 150 GB join does not fit alongside the working set in one machine's memory, so it spills; sessionization needs a sort over the day; the backfill multiplies the whole thing by seven while the *current* night's job still has to run. A single machine can be made to do any one of these. It cannot do all of them concurrently inside 15 minutes, and every attempt to make it fit is a bespoke memory-management project with no second engineer who understands it.

That is the defensible reason for Spark, and it is worth stating precisely:

> The bottleneck is not "400 GB exists." It is the combination of compute, joins, SLA, replay, and growing parallel workloads that makes scaling a single machine increasingly awkward — and a partitioned, retryable, horizontally-scalable execution model increasingly cheap by comparison.

This is [Data at Scale](../foundations/scale.md)'s worked **10×** row: *"40 GB → 400 GB/day: Spark on 10 r5.xlarge, Parquet, partition by `date`. p95 job ~15 minutes."* See [Distributed Execution](../foundations/distributed-execution.md) for why a job becomes stages and tasks the moment this happens, and [Spark](../spark/index.md) for the first real implementation of Phase 0's primitives.

Everything still lands as one nightly batch. There is still no streaming, no Kafka, no lakehouse — because nothing yet requires them.

**Forcing function to the next stage:** the *shape* of consumption changes, not just the volume. Fifteen services now emit the same event, and five teams want to react to it independently — fraud, billing, analytics, monitoring, and a new ML feature pipeline — each on its own schedule, each failing on its own schedule, each needing to replay history after a bug.

## Stage 3 — 4 TB/day, Kafka appears (Phase 2)

**Team:** 15 services owned by 7 teams.

Again, refuse the weak reason first. **"S3 can't take the PUT rate" is not why Kafka appears.** Modern S3 scales automatically to high request rates — thousands of requests per second per prefix, and higher with enough ramp-up, though a sharp ramp can produce temporary `503 Slow Down` responses ([object storage](../foundations/object-storage.md#consistency-and-request-rate-considerations)). Request-rate hotspotting is a real production consideration you plan for; it is not a reason to introduce a distributed log.

What Kafka is actually answering is the shape of the problem after Stage 2:

```text
BEFORE — every writer owns its own path to storage

service A ────► S3
service B ────► S3
service C ────► S3

Each writer picks its own format, its own layout, its own retry policy.
Each reader must know about every writer. No one owns "the event".


NEW REQUIREMENTS — the same event must feed five consumers

                       ┌──► fraud        (seconds, must replay after model change)
                       │
                       ├──► billing      (exactly-once, must never double-count)
event ─────────────────┼──► analytics    (hourly, tolerant of lag, huge backfills)
                       │
                       ├──► monitoring   (low latency, drops are acceptable)
                       │
                       └──► ML features  (replays 30 days on every retrain)

Each consumer:  runs independently
                fails independently
                needs replay from an arbitrary point
                moves at a different speed
                must see events for one entity IN ORDER
```

No arrangement of "write files to S3 and have everyone list the bucket" satisfies that. You need durable retention with a per-consumer read position, ordering per entity, and replay from an offset while a temporarily dead consumer catches up without affecting the other four. That is a **durable, replayable, partitioned log** — the abstraction, not the product:

```text
many producers + independent consumers + replay + per-entity ordering
                              ↓
                   durable replayable log
                              ↓
                            Kafka
```

This is scale.md's **100×** row: *"100+ cores, Iceberg with compaction, shuffle ~TB, must partition prune aggressively."* Kafka's reason to exist here is exactly the one named in [Architectures](index.md#the-questions-before-any-box): independent consumers need the *same* log. See [The Log Abstraction](../kafka/log.md) for why a log is not a queue and not a database — and note the organizational half of this: seven teams can now deploy their consumers on their own release cadence, which is a coordination win as much as a technical one.

**Forcing function to the next stage:** product ships a fraud-review queue that needs to flag a stolen-card pattern *while the card is still being used*, not tomorrow morning. "Nightly Spark reads the Kafka topic" has a floor of hours, not seconds — the next requirement is a latency class the batch pipeline was never built to hit.

## Stage 4 — Freshness under 10 seconds, Flink appears (Phase 4)

**Team:** 9 teams, one of which is a brand-new fraud product team.

The nightly Spark job already reads from the same Kafka topic Stage 3 built — but "read the topic once a night" and "react to each event within seconds, correctly, even out of order" are different problems, not the same problem run more often. [Flink](../flink/index.md) enters because the requirement is now **event time**, **windowed aggregation**, and **exactly-once state** that survives a crash — a stream processor's job, not a scheduler running a batch job more frequently. See [Flink: time semantics](../flink/time.md) and [windows](../flink/windows.md) for why "just run Spark every minute" quietly breaks the moment events arrive late or out of order, which they always eventually do.

At this stage SaaSCo also gets its first taste of streaming's real cost: keyed state per customer, checkpoints that must complete before the next one starts, and a watermark strategy someone has to actually choose, not accept as a default.

**Forcing function to the next stage:** SaaSCo now has a nightly batch DAG, a CDC pipeline, a fraud-detection Flink job, and three more one-off cron scripts that a new hire wrote to "just get it working." Nobody can say, on a given morning, which of the twelve things that touch the warehouse ran, in what order, or whether the 6 AM report used yesterday's fraud model or this morning's.

## Stage 5 — 100+ workflows, Airflow appears (Phase 5)

**Team:** 12 teams, 100+ workflows, no single person who knows what runs before what.

This is the same wall [Airflow](../airflow/index.md) opens with: cron scripts that were fine at three jobs become undebuggable at thirty, because "did the thing that has to run before this thing actually finish" stops being answerable by eyeballing a crontab. Airflow's job here is not "prettier cron" — it's dependency-aware scheduling, retries, backfills, and (once the job count crosses into the hundreds) [data-aware scheduling via Assets](../airflow/index.md#sensors-pools-mapping-slas-assets) so a downstream DAG starts when upstream data actually lands, not when a clock guesses it should have.

**Forcing function to the next stage:** the nightly Spark job and the Flink job's checkpoint sink both want to write to the same lake location, and a second Airflow DAG occasionally reads it mid-write. Directory listings on S3 start showing half-written partitions after a crash — the exact [rename-is-not-atomic](../foundations/object-storage.md#why-rename-heavy-systems-struggle) failure mode. "Where is the table?" stops having a good answer.

## Stage 6 — Multiple writers collide, Iceberg appears (Phase 6)

**Team:** 12 teams, three engines (Spark, Flink, Trino) writing the same tables.

[Why Table Formats Exist](../lakehouse/why-table-formats.md) opens with exactly this scene: raw Parquet on S3, concurrent readers, writers, a job that dies mid-write, and no way to name a consistent snapshot. SaaSCo adopts [Iceberg](../lakehouse/iceberg.md) not because a lakehouse is trendy, but because the object-storage semantics from Stage 3 — no atomic rename, no atomic multi-file commit — have no other fix once Airflow, Spark, and Flink are all writing to the same tables. This is also where the [manifest-instead-of-LIST](../foundations/object-storage.md#why-iceberg-uses-manifests-instead-of-list) design earns its keep: SaaSCo's file count is now in the hundreds of thousands, and listing a prefix during query planning is no longer fast enough to ignore.

**Forcing function to the next stage:** the product team ships a customer-facing dashboard. "Nightly" and even "every 15 minutes via Iceberg snapshot" are both too slow — the requirement becomes sub-second, interactive filtering by tenant.

## Stage 7 — Customer dashboards need sub-second, ClickHouse appears (Phase 8)

**Team:** 12 teams, plus a product team that now ships customer-facing analytics.

Iceberg and Trino answer "give me an answer in seconds to minutes over history." They do not answer "render this chart while a customer is looking at it." [ClickHouse](../olap/clickhouse.md) enters the stack as a **serving** layer fed from the same Kafka topic Stage 3 built, not as a replacement for the lakehouse — this is the same hot-path/cold-path split named in [Architectures](index.md#the-five-systems) for the observability and SaaS-analytics rows. [Why Columnar Storage](../olap/columnar-storage.md) and [ClickHouse `ORDER BY`](../olap/clickhouse.md) are the two lessons that explain why this specific box, and not "a faster Postgres," was the answer.

**Forcing function to the next stage:** SaaSCo now has 400 engineers, dozens of pipelines writing to the same tables, and a revenue dashboard that showed ₹0 one morning because a Spark job's schema change silently broke a downstream ClickHouse materialization. Nobody can say who owns `events_agg`, whether it's fresh, or which of three "active users" definitions is correct.

## Stage 8 — 400 engineers, contracts and lineage become load-bearing (Phase 10)

**Team:** 400 engineers, 40+ teams, ~80 event producers, ~400 downstream consumers.

This is the [metadata](../metadata/index.md) module's own opening argument: *"a platform with 10,000 tables and no operating model for metadata is a swamp with a search box."* At Stage 1 through 6, one team could hold the whole pipeline in their heads. At Stage 8, that stops being true, and the failure mode changes shape — it's no longer "the job is too slow," it's "nobody can tell you if the number is right." [Data Contracts](../foundations/data-contracts.md) (schema and semantic compatibility between producer and consumer teams) and [metadata/lineage](../metadata/index.md) (who owns what, and can you walk backward from a wrong dashboard to its source) become the bottleneck — the same way shuffle bytes were the bottleneck at Stage 2.

---

## The pattern across all eight stages

| Stage | Volume / requirement | People | New component | What actually forced it |
|---|---|---|---|---|
| 1 | 40 GB/day | 2 engineers | None | — |
| 2 | 400 GB/day | 5 engineers | Spark | Not the bytes — joins + sessionization + 20 outputs + a 7-day backfill, all inside a 15-minute SLA |
| 3 | 4 TB/day | 15 services, 7 teams | Kafka | Five independent consumers need the same events, in order per entity, with replay — and each team must deploy on its own cadence |
| 4 | Freshness < 10 s | 9 teams | Flink | Windowed, event-time, exactly-once state a scheduled batch job cannot provide |
| 5 | 100+ workflows | 12 teams | Airflow | Cross-team dependencies exceed what any human holds in their head; backfills a crontab cannot express |
| 6 | 4+ TB/day (multi-writer) | 12 teams, 3 engines | Iceberg | No atomic commit on raw object storage, now with three engines and three teams writing the same tables |
| 7 | 4+ TB/day | 12 teams | ClickHouse | Sub-second dashboard requirement, not satisfiable by the lakehouse |
| 8 | 400 engineers, 80 producers, 400 consumers | 40+ teams | Contracts + lineage | Ownership and correctness, not volume, is now the bottleneck |

### Architecture scales because of people as well as bytes

Read the "People" column on its own and a second story appears, one most architecture diagrams leave out entirely:

- **Kafka** became valuable partly because seven teams needed to deploy and fail *independently*. A shared library writing to shared files couples release cycles; a log does not.
- **Airflow** became necessary when coordination exceeded what a person could keep in their head — not when the job count crossed a threshold, but when the *owner* count did. Thirty jobs owned by one team is a crontab. Thirty jobs owned by twelve teams is an outage waiting for a Tuesday.
- **Contracts and lineage** became load-bearing because ownership boundaries multiplied. At two engineers, the contract is a conversation. At 400, an unwritten contract is a silent breakage.
- **Iceberg** was forced by three *engines*, but it was made urgent by three *teams* — nobody could coordinate a mid-write read by asking the person next to them.

When you fill in this table for your own system, put a headcount and a team count next to every row. A component you cannot justify with either bytes or boundaries is a component you have not earned.

Notice what never happens: SaaSCo never adopts a component because of what it might need next quarter. Every arrow in this table is a **measured**, present-tense bottleneck — exactly the V1-then-bottleneck-then-V2 discipline in [How to read an architecture page](index.md#how-to-read-an-architecture-page). If you're designing a system at work and can't fill in a row like this — a specific number, a specific failure, a specific next component — you're choosing the box before you've earned it. See the [architecture-deletion exercise](index.md#exercise-delete-the-unnecessary-architecture) for the mirror image of this story: a stack with components that were never earned at all.

---

## Stage 9 — SaaSCo after cost-cutting (the exercise) { #stage-9 }

Every stage so far asked *why did this box appear?* The harder question, and the one almost nobody practises, is the mirror image:

> **When can each box disappear?**

Two years later, the market turns. SaaSCo's new state:

```text
traffic fell 80%              →  ~800 GB/day, not 4 TB
team reduced 25 → 7 engineers →  one on-call rotation, total
fraud product discontinued    →  the sub-10-second requirement is gone
dashboard freshness relaxed   →  10 minutes is now acceptable to the customer
billing still exists          →  must never double-count, must be auditable
```

The stack is unchanged: Kafka, Flink, Spark, S3, Iceberg, Airflow, Trino, ClickHouse, contracts, lineage. Nine components, seven engineers.

**Your task:** name what you remove, in what order, and what signal would bring each one back.

Work through these before opening the answer:

1. **Does Flink still exist?** Its forcing function was Stage 4's sub-10-second fraud requirement. That product is gone. What is left that needs event-time windowing and exactly-once keyed state — and can a 10-minute batch do it instead?
2. **Do we still need Kafka for *all* event paths?** Kafka's forcing function was five independent consumers with replay. How many independent consumers are left? Is one of the remaining paths a single producer writing to a single consumer?
3. **Could one serving system disappear?** Trino and ClickHouse both answer queries. At 10-minute freshness and a much smaller concurrency, does the hot-path/cold-path split still pay for two systems and two sets of on-call knowledge?
4. **Could a Spark micro-batch replace a stream path?** "Every 5 minutes" and "within 10 seconds" are different architectures. Which one does the surviving requirement actually need?
5. **Could Airflow + Spark + Iceberg be sufficient** as the entire platform?
6. **What must you keep even though volume fell?** Not everything scales down with traffic.

??? success "Exit check"
    **Flink goes first.** Its entire justification was a latency class no surviving requirement asks for. A 5-minute Spark (or even Trino) job over Iceberg satisfies a 10-minute freshness SLA with a fraction of the operational surface — no checkpoint tuning, no watermark strategy, no keyed-state backend to restore. Removing it also removes the hardest thing to be on-call for with seven engineers.

    **ClickHouse goes next, probably.** It exists to serve sub-second interactive dashboards. At 10-minute freshness and reduced concurrency, Trino over Iceberg (or a ClickHouse-shaped materialized Iceberg table queried by Trino) may be adequate. Keep it only if you can name a query latency SLO it still meets and Trino does not — measure, don't assume. This is a genuinely close call, and the *reasoning* matters more than the verdict.

    **Kafka is the interesting one — do not reflexively delete it.** Volume is not why it exists. Ask how many independent consumers remain. If billing, analytics and monitoring still consume the same events on independent schedules and still need replay, Kafka is still earning its keep, at 800 GB/day exactly as at 4 TB/day, and you would shrink the cluster rather than remove it. If two of those three are gone and what remains is one producer feeding one consumer, then Kafka has become a durable queue with an ops burden, and a direct write to Iceberg plus a scheduled job is the honest V1.

    **Keep Iceberg.** Its forcing function was multiple writers and atomic commits, which is not a volume problem — you still have Spark and Airflow writing the same tables, and the cost of *removing* correct commit semantics is silent corruption, not a slow query.

    **Keep Airflow.** Seven engineers running 100+ workflows need dependency-aware scheduling and backfills *more* than 25 engineers did, not less. Fewer people means less human coordination capacity, not more.

    **Keep contracts and lineage — but shrink them.** Ownership boundaries still exist across teams, and the audit requirement on billing has not changed. What can go is the *ceremony*: a 40-team governance process is not right-sized for 7 engineers. Keep the schema registry and the lineage graph; drop the review board.

    **The plausible end state:** Kafka (smaller, or removed if consumers collapsed to one) → Spark micro-batch on Airflow → Iceberg → Trino. Roughly four load-bearing components instead of nine.

    **The general rule:** a component's *justification*, not its presence, is what you re-test when requirements change. Volume-forced components (Spark's sizing, Kafka's partition count, ClickHouse's shards) scale down with traffic. Semantics-forced components (Iceberg's atomic commits, contracts' compatibility guarantees) do not — they were never about volume, so shrinking volume does not retire them. Confusing the two is how teams delete the thing that was protecting them and keep the thing that was costing them.

**Why this exercise is the point of the whole page.** An engineer who can only add components is a liability at exactly the moment a company most needs judgement. If you can defend the eight additions above but cannot defend a single deletion, you have learned the shape of the story and not the reasoning behind it. Run this same exercise on the stack you actually operate — see also the greenfield version, [delete the unnecessary architecture](index.md#exercise-delete-the-unnecessary-architecture).

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
