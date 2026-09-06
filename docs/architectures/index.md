---
description: Five requirements-first architecture case studies showing why the same Kafka topic can't feed a fraud score, a dashboard, and a lakehouse table alike.
---

# Architectures

Before you read a single one of these five pages: what is the first box you would draw for a system that ingests events and needs both a live dashboard and a year of history? Most engineers draw Kafka, then a stream processor, then a lakehouse, then an OLAP store — all before writing down a single number. Try instead to name the **one** query that must be fast, the **one** failure that is unacceptable, and the **one** component you refuse to add yet. If you cannot do that in a sentence, you are not ready to draw a box.

These pages are design drills, not catalogues of boxes. Each system starts from **requirements**, not from a favourite stack. The stack is the last thing you write down.

Reading these five as independent case studies is useful; reading them as one company's timeline is more useful. [SaaSCo: The Evolving Company](saasco-evolution.md) walks a single SaaS-analytics business through six growth stages, and every new component in that story — Spark, Kafka, Iceberg, ClickHouse, contracts and lineage — is forced by a measured bottleneck in the stage before it, never adopted ahead of need.

The five running use cases in this academy share event shapes, but they do not share latency, retention, or failure budgets. That is the point: the same Kafka topic can feed a 50 ms fraud score, a 7-day observability dashboard, and a 2-year lakehouse table, and those three jobs should not share an engine.

---

## How to read an architecture page

Every page follows the same reasoning loop:

1. **Requirements** — volume, latency, access pattern, retention, cost, failure.
2. **V1** — the fewest parts that satisfy the SLA. Name what you would *not* add yet.
3. **Bottleneck** — the first thing that breaks as load or query mix grows.
4. **V2** — the smallest change that removes that bottleneck.
5. **Capacity sketch** — events/s, GB/day, partitions, cores, disk. Order-of-magnitude, not a quote.
6. **Failure modes** — what the on-call sees, and which layer absorbs it.
7. **10×** — what you change when volume, cardinality, or query concurrency jumps an order of magnitude.

If you skip V1 and draw V2, you will ship Iceberg, Trino, a catalogue, and three stream processors before you have a working dashboard. That is how platforms die of coordination cost.

!!! tip "Predict before you read V2"
    After the requirements, pause. Sketch V1 on paper. Name one thing you refuse to add. Then compare.

---

## The questions before any box

| Question | Why it decides the stack |
|----------|--------------------------|
| How many events/s at p99 day? | Kafka partition count, Flink parallelism, ClickHouse insert batching |
| How stale may the answer be? | Stream vs micro-batch vs nightly Spark |
| Point lookup, dashboard agg, or ad-hoc join? | KV / OLAP / lake + Trino |
| How long is the hot window? | NVMe vs object storage; TTL vs compaction |
| What is the unit of tenancy? | `customer_id` in every key, or separate clusters |
| What is allowed to be wrong? | At-least-once duplicates vs exactly-once sinks |
| What is the cost of replay? | Kafka retention, Iceberg snapshots, checkpoint interval |
| Who owns the data when it is wrong? | Catalogue, on-call, not "the platform team" |

A sentence like "we need streaming" is not a requirement. "The fraud score must leave Flink in 200 ms p99, and a 30 s delay is a missed chargeback" is a requirement.

---

## V1 is a feature, not a compromise

V1 for almost every event system in this academy is:

```
producers → Kafka (one cluster, RF=3 in prod / 1 in the lab)
         → one processor (Flink *or* Spark, not both)
         → one serving store (ClickHouse *or* a TSDB *or* Postgres)
```

That is enough to learn whether the SLA is real. What V1 is *not*:

- A lakehouse "because we might need history"
- Trino "because analysts will want SQL someday"
- Neo4j "because fraud might be a graph"
- Pinot *and* ClickHouse
- A metadata catalogue with no owners

Add a component when a **measured bottleneck** or a **hard requirement** forces it — not when a conference talk listed it.

---

## Every architecture has two limits { #two-limits }

An architecture diagram with no stated limits is a picture, not a design. Every V1 and V2 on the five architecture pages carries two statements, and you should write them for your own systems too:

```text
This architecture works while ...     ← the envelope you designed for
This architecture breaks when ...     ← the specific conditions that end it
```

Getting both down does three things at once: it tells a reader whether their situation is inside the envelope, it converts "when do we migrate?" from opinion into a threshold you can put a monitor on, and it makes the *next* architecture feel derived rather than predetermined.

Worked example — the simplest architecture in this academy, a **single-machine analytical pipeline** (one box, Parquet on local NVMe or S3, a Python or DuckDB job on a schedule):

**Works while:**

```text
the working set fits practical memory/disk on one machine
the batch SLA is comfortable relative to achievable scan + compute time
concurrency is low — one job at a time, few interactive readers
recovery time (rerun the whole job) is within business tolerance
one person can hold the whole pipeline in their head
```

**Breaks when:**

```text
the SLA falls below the achievable scan/compute time  ← do the arithmetic, don't guess
several workloads contend for the same machine
backfills block the current production run
failure recovery (a full rerun) exceeds what the business will accept
a second team needs to write to the same data concurrently
```

Note that only the *first* breaking condition is about size. Three of the five are about time, contention, and people — which is why "our data got big" is such an unreliable trigger for re-architecting, and why the [SaaSCo](saasco-evolution.md) story keeps having to justify each step with something more specific than a volume number.

### Do the arithmetic before you distribute

The most common way to skip this reasoning is to jump from a volume to a product. Don't. Ask instead whether the current system can *theoretically* satisfy the requirement:

```text
Dataset               = 2 TB
Disk throughput       = 1 GB/s

Minimum possible scan ≈ 2000 s ≈ 33 minutes
Requirement           = finish in 10 minutes
```

Now parallelism is *mathematically* forced — no single machine with that disk can do it, and you can say so in one line of a design doc. That is a very different claim from:

```text
2 TB is big → use Spark
```

The second is taste. The first is an argument. And notice the arithmetic cuts both ways: if the requirement had been "finish overnight," the same numbers would have told you *not* to distribute. See [SaaSCo Stage 2](saasco-evolution.md) for the same calculation run in earnest, including the case where the raw scan is comfortably feasible and something else forces the change anyway.

---

## The five systems

| Architecture | Dominant constraint | V1 serving store | First thing you do *not* add |
|--------------|---------------------|------------------|------------------------------|
| [Observability](observability.md) | Ingest volume + cardinality + tiered retention | ClickHouse | Pinot, a second Kafka cluster |
| [E-commerce](ecommerce.md) | CDC correctness, mutable orders, GDPR deletes | Postgres + Kafka + Iceberg/Hudi | Real-time recs graph |
| [IoT](iot.md) | High-frequency writes, downsample pyramid | ClickHouse *or* Timescale | Per-device Kafka topics |
| [Fraud](fraud.md) | Scoring latency + multi-hop rings | Flink state + ClickHouse features | Neo4j on the 200 ms path |
| [SaaS analytics](analytics-platform.md) | Multi-tenant isolation + cost | Kafka + ClickHouse | Per-customer clusters |

Open a page only after you can state its dominant constraint in one sentence. If two pages seem to need the same boxes, read the **access pattern** section — that is where they diverge.

---

## Worked selection (same events, different architecture)

Take one event:

```json
{
  "timestamp": "2024-01-15T10:30:00Z",
  "customer_id": "cust_0042",
  "user_id": "user_98712",
  "service": "api-gateway",
  "latency_ms": 45,
  "status_code": 200
}
```

| Consumer | Latency budget | Store | Why not the others |
|----------|----------------|-------|--------------------|
| On-call dashboard, last 15 min | < 1 s | ClickHouse | Trino on Iceberg is seconds; Prometheus dies on `user_id` |
| Nightly revenue by plan | Hours | Iceberg + Spark/dbt | ClickHouse is overkill for a once-a-day scan |
| "Users who share this device" | Minutes, batch | Neo4j (derived) | SQL recursive CTE will not stay fast at 10 hops |
| "Is *this* payment fraud?" | < 200 ms | Flink keyed state + feature lookup | Graph hop on the request path is a timeout |
| Infra CPU for `api-gateway` | Alert in 1 min | Prometheus / VictoriaMetrics | ClickHouse can do it; alerting is worse |

The event is the same. The **question** is not. Architecture is the mapping from question → store → processor, not from event → "the data platform."

---

## Capacity arithmetic you should always do

Before drawing brokers, compute three numbers.

**Ingest**

\[
\text{GB/day} \approx \text{events/s} \times \text{bytes/event} \times 86400 / 10^9
\]

A 500-byte event at 100k/s is ~4.3 TB/day uncompressed. After Kafka compression (often 3–5× on JSON, more on Avro) and ClickHouse columnar compression (often 8–15×), the disk number is not the wire number. Write both.

**Partitions**

Kafka parallelism is `min(partitions, consumers in the group)`. A starting rule:

- Target 5–15 MB/s **per partition** of uncompressed produce, or a few thousand events/s if handlers are heavy.
- Round up so each consumer in the target consumer group has work.
- Do not start at 1000 partitions "for the future." Metadata, rebalances, and ClickHouse Kafka engine all pay for partition count.

**Serving**

Dashboard QPS × scan bytes per query × compression ratio tells you whether ClickHouse fits on one node. If every query scans a day of 20 TB compressed, you do not have a "ClickHouse problem" — you have an `ORDER BY` / partition problem. See [ClickHouse](../olap/clickhouse.md) and the [ORDER BY simulation](../simulations/clickhouse-order-by.html).

A one-node V1 that answers the real queries is more informative than a 20-node drawing that has never seen a predicate.

---

## Failure is a first-class requirement

Write the failure story in V1, not as an appendix:

| Failure | Typical symptom | Layer that should absorb it |
|---------|-----------------|-----------------------------|
| Hot Kafka key | Lag on **one** partition | Key design, not "add consumers" |
| Processor crash | Checkpoint / offset rewind, duplicate sink writes | Idempotent sink or transactional sink |
| Serving node full | Inserts stall, queries time out | TTL, disk, merge backlog |
| Poison event | Consumer restart loop | DLQ, not infinite retry |
| Schema change | Silent nulls downstream | Registry + quality gate |
| Replay | Re-processing a day of Kafka | Idempotent tables, partition overwrite |

The [incidents](../incidents/index.md) page lets you practise these rows.
Architecture pages name **where** the failure lands; incidents teach you to
**see** it in metrics.

---

## Evolution at 10×, in one paragraph

At 10× volume you almost never swap the whole stack. You change:

- **Kafka**: more partitions *and* more brokers; check that produce is still keyed correctly (10× a hot key is still one partition).
- **Processor**: more task slots; state size, not CPU, is usually the cliff (RocksDB, checkpoint duration).
- **Serving**: shards by the query predicate (service+day, customer+day), not "more replicas of a bad `ORDER BY`."
- **Retention**: pull history off NVMe onto Iceberg; put Trino in front of *cold* data only.
- **Cost**: the 10× bill is storage and shuffle, not the extra microservice.

If 10× forces a new *kind* of system (graph, TSDB, Pinot), that requirement should have been visible in V1 as a query you could not express — not as a surprise.

---

## Anti-patterns these pages exist to prevent

- **Streaming because we might need it.** Nightly Spark on Iceberg is cheaper and easier to correct. Add Flink when the SLA is seconds and the logic is stateful.
- **One store for everything.** ClickHouse is a poor source of truth for orders. Postgres is a poor 5M events/s log store. Neo4j is a poor metrics TSDB.
- **Lakehouse as ingest.** Object storage plus a table format is a *batch and history* layer. Do not make the on-call wait for a compaction job.
- **Catalogue as governance.** A catalogue without owners, freshness SLAs, and PII tags is search over a swamp. See [metadata](../metadata/index.md).
- **Notebooks as pipelines.** See [notebooks](../notebooks/index.md).

---

## How this maps to the rest of the academy

| If you are designing… | Read first | Then compare |
|-----------------------|------------|--------------|
| Hot vs cold paths | [Observability](observability.md) | [ClickHouse vs Trino](../comparisons/clickhouse-vs-trino.md) |
| CDC + mutable facts | [E-commerce](ecommerce.md) | [Lakehouse comparison](../lakehouse/comparison.md) |
| Sensors + downsample | [IoT](iot.md) | [TSDB vs OLAP](../comparisons/tsdb-vs-olap.md) |
| Score vs investigate | [Fraud](fraud.md) | [Graph vs relational](../graph/graph-vs-relational.md), [Spark vs Flink](../comparisons/spark-vs-flink.md) |
| Tenant isolation + cost | [SaaS analytics](analytics-platform.md) | [Selection framework](../reference/selection-framework.md) |

After you can defend V1 and V2 for two of these systems, use the [selection framework](../reference/selection-framework.md) on a workload from *your* job. The framework is the same questions; the answers will differ.

---

## Lab and incident pairing

Do not treat architecture as reading-only:

1. Run the [Kafka lab](../labs/index.md) and watch a hot key — that is the fraud and analytics partition story.
2. Run the [ClickHouse lab](../labs/index.md) and change `ORDER BY` — that is the observability dashboard story.
3. Work the [incidents](../incidents/index.md) for Kafka lag, Spark skew OOM, Flink stalled watermarks, ClickHouse parts, Iceberg snapshots, Trino coordinator OOM.

If you cannot point to a metric that would fire in each architecture's first bottleneck, the diagram is still a slide.

---

## One-page V1 template (steal this)

```
Consumers & SLOs:
Volume (events/s, bytes, GB/day):
Key (Kafka / CH ORDER BY / Flink):
Hot path (boxes):
Cold path (or "none yet"):
Not adding:
Replay (days of Kafka):
Drop policy under overload:
Tenant / PII:
10× first cliff:
```

Fill it for two academy systems and one work system. If cold path is empty, good — say why (retention fits on CH). If "not adding" is empty, you are hiding a zoo.

---

## Shared vs split Kafka clusters

| Default | One Kafka (or one per region) |
|---------|--------------------------------|
| Split when | PCI vs clickstream; residency; blast radius of a bad ACL |
| Do not split when | "logs vs metrics" on the same team and SLA |

Two clusters double ACLs, disk, and on-call. [Security](../security/index.md) ACLs are cheaper than a second cluster until they are not.

---

## Exercise: delete the unnecessary architecture

Senior engineering includes knowing **when not to distribute**. This exercise is deliberately not a greenfield design — the stack already exists, and your job is to say what should not.

A team ingests **30 GB/day** from a single Postgres OLTP database and needs a daily revenue dashboard for **six internal analysts**, refreshed once per morning. Their current architecture:

```text
Postgres
   │  Debezium CDC
   ▼
Kafka
   │
   ▼
Flink (stateful streaming job)
   │
   ▼
S3 (raw)
   │
   ▼
Iceberg (via Spark compaction job)
   │
   ▼
Trino
   │
   ▼
ClickHouse (materialized for "speed")
   │
   ▼
dbt models on top of ClickHouse
   │
   ▼
Airflow orchestrating the whole thing
```

1. Name every component that exists to solve a problem this workload does not have (hint: check the volume, the latency requirement, and the number of consumers against each component's reason to exist in this academy — [Kafka's](../kafka/index.md), [Flink's](../flink/index.md), [Iceberg's](../lakehouse/index.md)).
2. Redesign V1 with the fewest components that satisfy "daily revenue dashboard, six analysts, 30 GB/day, refreshed each morning."
3. Name the one signal that, if it changed, would justify reintroducing each component you removed.

??? success "Exit check"
    At 30 GB/day with a once-daily refresh and six analysts, there is no requirement for streaming (Kafka + Flink), no requirement for a lakehouse table format built for concurrent multi-engine writers (Iceberg), and no requirement for a real-time serving store (ClickHouse) — all three exist to solve problems of *volume*, *concurrency*, or *latency* this workload does not have. A defensible V1 is a nightly Postgres → S3/warehouse extract (or direct query if Postgres can absorb six analysts' worth of read load) → dbt models → a warehouse or even Postgres read replica the analysts query directly, orchestrated by a single Airflow DAG. Reintroduce Kafka if a second independent consumer needs the same change log; reintroduce a lakehouse format if a second engine needs concurrent access to the same files; reintroduce ClickHouse if the SLA moves from "each morning" to "sub-second, interactively." Until one of those is true and measured, each component is a pager rotation with no workload behind it — see [When *not* to use distributed data systems](../foundations/index.md#when-not-to-use-distributed-data-systems).

---

## How to review someone else's architecture diagram

1. Circle every box. Ask which **SLO** dies if you delete it. If none, delete it.
2. Find the Kafka **key** and CH **ORDER BY**. If they are "TBD," the diagram is a mood.
3. Find the whale (one tenant, one service, one SKU). Ask where lag shows up.
4. Find GDPR / replay. If "later," it is now.
5. Compare to the closest academy page. Difference must be a **requirement**, not taste.

---

## Mapping modules → architecture decisions

| Module | Decision it informs |
|--------|---------------------|
| [Partitions](../foundations/partitions.md) | Kafka key, Spark skew, CH ORDER BY |
| [Batch vs stream](../foundations/batch-vs-stream.md) | Flink vs Spark vs nightly |
| [Exactly-once](../kafka/exactly-once.md) | Sink design, not a checkbox |
| [Watermarks](../flink/windows.md) | IoT / sessions |
| [Iceberg](../lakehouse/iceberg.md) | Expire, compaction, GDPR |
| [Columnar](../olap/columnar-storage.md) | Why CH is the tile store |
| [Cardinality](../time-series/cardinality.md) | Prom vs CH |
| [Graph modelling](../graph/graph-modelling.md) | Fraud rings vs SQL |
