---
description: Choose Airflow, Flink, Spark, or a warehouse from measured workload properties and SLAs, not resume pressure — a procedure, not a vendor comparison.
---

# Technology Selection Framework

Design review, Tuesday 2 PM. The proposal: migrate the nightly NPS CSV job (20 MB, due Friday noon) from Airflow to Flink, "so we're stream-native before Q3." Nobody in the room can name what breaks if it stays batch.

A. Approve it — Flink is the more modern choice. B. Reject it and ask what latency requirement changed. C. Approve it, but only if two other jobs migrate too, to amortize the ops cost. D. Table it until someone writes down the SLA.

Only one of these asks the question this page exists to force. Choose tools from **workload properties**, not from blogs or resume pressure. This page is a procedure: questions, a mapping table, failure questions, anti-patterns, then worked examples from the running systems.

Pairwise fights live in [comparisons](../comparisons/index.md). End-to-end boxes live in [architectures](../architectures/index.md). Come here first when the list of tools is still long.

---

## The wrong question

"What is the best data stack?"

There is no answer. Every system in this academy is optimal for some SLAs and expensive theatre for others.

## The right question

"What are the properties of **this** workload, and which tool was built for those properties? What will I **refuse** to add until a measurement forces it?"

---

## Step 0 — write the consumer

Start by naming **who waits on the result** and **what they do if it is late or
wrong**. Without that consumer, there is no stable basis for choosing a tool.

| Consumer | Example SLO |
|----------|-------------|
| Checkout authorisation | 200 ms, fail-open/closed decided |
| On-call Grafana | 1 s on 15 minutes of one service |
| Finance GMV | 07:00 yesterday, recon to 0.5% |
| Customer product analytics | 300 ms, tenant isolated |
| Data scientist ad-hoc | minutes, not on the product path |

One pipeline may serve several consumers. That is **several selections**, not one "platform."

---

## Step 1 — characterise the workload

Do this **before** naming Kafka.

### Volume

- Events/s at p99 day, not the CEO's "we will be Google."
- Bytes/event (JSON vs Avro).
- GB/day uncompressed **and** a compression guess.
- Retention per tier (replay vs hot vs lake).

### Latency (event to **this consumer's** answer)

| Budget | Default class |
|--------|----------------|
| < 200 ms stateful | Flink / KV / in-process; not Spark, not Trino |
| 1–30 s | Flink or Spark micro-batch or CH Kafka engine |
| Minutes | Spark Structured Streaming or batch |
| Hours / next morning | Spark / dbt / Airflow |

### Access pattern

- Point get vs scan vs multi-hop graph.
- Predictable tiles vs ad-hoc SQL.
- Join-heavy vs single-table.
- Tenant filter always present?

### Cardinality

- Bounded labels (`service`, `status`) vs `user_id` / `device_id`.
- High card → not a TSDB. See [TSDB vs OLAP](../comparisons/tsdb-vs-olap.md).

### Mutability

- Append-only events vs CDC upserts vs GDPR deletes.
- If you mutate, Iceberg/Hudi/OLTP — not append-only Parquet dumps.

### Failure and replay

- Can you rebuild from Kafka (how many days)?
- Must sinks be idempotent?
- What is allowed to be dropped under overload (debug logs vs payments)?

### Cost and team

- Who is on-call at 03:00?
- If the team has never run Flink, a minutes-SLA Spark job is not cowardice.

Write the answers in a table. If a cell is "unknown," the next task is **measurement**, not a PO for Pinot.

---

## Step 2 — map to a default (then argue)

### Ingest

| Requirement | Default | Not unless measured |
|-------------|---------|---------------------|
| High-throughput events | Kafka | Kinesis/PubSub as cloud equivalents |
| Device protocol | MQTT **into** Kafka | Kafka on the microcontroller |
| CDC | Debezium → Kafka | Polling `updated_at` |
| Nightly files | Object storage + Airflow | Kafka "for consistency" |

### Process

| Requirement | Default | Not unless measured |
|-------------|---------|---------------------|
| Lake SQL / shuffle | Spark | Flink batch as identity |
| Sub-second keyed state | Flink | Spark Structured Streaming |
| Minutes enrich | Either; pick team | Both |
| Distributed Python ML | Ray | Spark MLlib for DL |
| Orchestrate | Airflow | Notebooks |

### Store

| Requirement | Default | Not unless measured |
|-------------|---------|---------------------|
| Cheap history, time travel, multi-engine | Iceberg | Three table formats |
| Heavy CDC upserts | Hudi (or Iceberg merge if rate OK) | Raw JSON on S3 |
| OLTP truth | Postgres | ClickHouse |
| Hot analytics SQL | ClickHouse | Trino on the tile |
| Infra metrics | Prometheus/VM | CH as Alertmanager |
| Graph rings | Neo4j **derived** | Graph on the 200 ms path |

### Query

| Requirement | Default |
|-------------|---------|
| 100 ms tiles | ClickHouse (Pinot if QPS/freshness **fail** CH) |
| Federated ad-hoc | Trino |
| Graph | Neo4j |
| SQL time series moderate | Timescale or CH |

---

## Step 3 — design for failure (or you did not select)

For the **chosen** tool, answer:

1. What metric shows it is falling behind?
2. What happens on node death mid-write?
3. What happens on a poison schema?
4. How do you replay a bad hour?
5. What is the 10× cliff (state size, parts, partitions, coordinator RAM)?

If the answer is unclear, read the matching [incident](../incidents/index.md)
before treating the choice as production-ready.

---

## Step 4 — V1 vs zoo

Write **fewest parts** that meet the SLO. List **what you will not add yet**. Add a component when a **named bottleneck** or **hard requirement** appears.

See [architectures index](../architectures/index.md). V1 is usually Kafka + one processor + one serving store.

---

## Anti-patterns (instant no)

| Phrase in a design doc | Translate | Do this instead |
|------------------------|-----------|-----------------|
| "Kafka because we might need streaming someday" | We want a log-shaped resume | Nightly Spark; add Kafka when a consumer has a latency SLO |
| "Flink because we have Kafka" | Confused bus with processor | Spark from Kafka is valid |
| "Lakehouse as the product database" | Compaction on the checkout path | Postgres truth, lake derived |
| "ClickHouse for everything" | No OLTP, no federation, no graph | Split by access |
| "Trino for Grafana" | Coordinator as a dashboard server | CH or pre-agg |
| "Prometheus with user_id labels" | Cardinality bomb | CH events |
| "Neo4j on authorise" | Timeout | Flink + KV |
| "Notebooks in Airflow as the pipeline" | Unreviewable state | Module + tests |
| "Both Pinot and CH in V1" | Two ingest paths | CH until QPS hurts |
| "Ray for ETL because Python" | Wrong abstraction | Spark SQL |
| "One cluster to simplify" | Mixed SLAs, mixed failure | Split hot/cold |
| "Schema-less JSON forever" | Silent nulls | Registry + contracts |
| "We'll shard when we scale" | No key design | Key for today's whale |

If your doc contains three of these, stop drawing.

---

## Worked examples (running systems)

### Observability on-call tile

- Volume high, latency 1 s, access filter `service`+time, card high on logs, append-only, retain 7 d hot.
- **Select:** Kafka → Flink or CH Kafka engine → **ClickHouse** `ORDER BY (service, timestamp)`. Prometheus **beside** for infra.
- **Refuse:** Pinot, Trino on the tile, ES as SoR.

### E-commerce GMV

- Low order QPS, **mutate**, recon to Postgres, hours OK.
- **Select:** Postgres → Debezium → Kafka → Spark/dbt → Iceberg. Trino ad-hoc.
- **Refuse:** Flink until inventory SLA is seconds; CH as order SoR.

### IoT 1-minute chart

- High write, latest-by-device, pyramid retain.
- **Select:** MQTT → Kafka → Flink windows → CH or Timescale; hourly Iceberg.
- **Refuse:** Prometheus for `device_id`; topic-per-device.

### Fraud score

- 200 ms, keyed velocity, graph **offline**.
- **Select:** Flink/scorer + KV/CH features. Neo4j batch. Iceberg labels.
- **Refuse:** Spark micro-batch on the auth path; Cypher in the request.

### SaaS tenant dashboard

- 100k/s, tenant filter, cost sensitive.
- **Select:** Kafka key `customer_id` + quotas → CH tenant-first key → Iceberg rollups.
- **Refuse:** per-tenant clusters in V1; notebooks as the product.

---

## Decision record (copy this)

```
Consumer + SLO:
Volume (events/s, GB/day, retain):
Access (get / agg / join / graph):
Cardinality:
Mutability / deletes:
V1 (boxes):
Not adding yet:
Failure metric:
Replay plan:
10× change:
Anti-patterns we rejected:
```

If this does not fit on one page, you are designing three systems. Split the doc.

---

## After you selected

1. [Comparison](../comparisons/index.md) for the pair you almost picked.
2. [Architecture](../architectures/index.md) closest to yours — steal V1, not V3.
3. [Labs](../labs/index.md) for the failure you will actually hit.
4. [Glossary](glossary.md) for the words in the decision record.

Selection is finished when you can explain the choice to an on-call who does not like the tool — using the **workload table**, not a feature matrix.

---

## Constraint layers (do not mix them)

| Layer | Example | Effect |
|-------|---------|--------|
| Workload | 200 ms, 10M series | **Determines** engine class |
| Org | "We only operate Spark" | May pick a **worse** engine; document the tax |
| Vendor | Existing Snowflake contract | Same |
| Skill | No JVM | PySpark vs Flink ops cost |
| Law | EU residency | Region topology, not CH vs Pinot |

Workload first. Org constraints second. If org forces Spark at 200 ms, the record should say **SLA at risk**, not "Spark is streaming."

---

## Default V1 by consumer (summary)

| Consumer | V1 |
|----------|----|
| Nightly finance | PG dump / Iceberg + dbt; **no** Flink |
| Product analytics tiles | Kafka + CH |
| On-call logs | Kafka + CH; Prom beside |
| IoT charts | Kafka + CH/Timescale + pyramid |
| Fraud score | Flink/scorer + KV |
| Ad-hoc join lake+PG | Trino |
| Train models | Spark features → Ray/box |

---

## When to reopen a decision

- SLO missed after `ORDER BY` / key design was checked (new class of engine).
- 10× volume (usually **scale the same** engine).
- New consumer with a different SLA (add a path, do not morph the old one).
- Team gone (ops constraint changed).

Do not reopen because a conference named a product.

---

## Example anti-pattern write-up (copy)

> Rejected Kafka+Flink for weekly NPS CSV (20 MB). SLA is Friday noon. Airflow + Python + Iceberg (or a sheet) meets it. Revisit if NPS becomes an in-app live widget with a 30 s SLO.

That paragraph in a design doc is the framework working.

---

## Checklist before PR of an architecture

- [ ] Consumers and SLOs numbered
- [ ] GB/day math
- [ ] Keys named
- [ ] V1 boxes ≤ 5
- [ ] Not-adding list
- [ ] Failure metric per box
- [ ] Replay
- [ ] PII/retention
- [ ] Comparison pair considered
- [ ] Incident class linked

If the PR is a landscape diagram with 18 logos, bounce it.
