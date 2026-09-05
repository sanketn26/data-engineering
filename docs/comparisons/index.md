---
description: How to pick between competing data engines by SLA, state, and failure mode instead of a feature-matrix checkbox contest.
---

# Comparisons

A design review opens with a slide titled "Flink vs Spark: Feature Comparison" — twelve rows, checkmarks in both columns, a score of 9/10 vs 8/10. Nobody on the call has said a latency number yet. Predict before you read on: what single question, asked first, would make that entire slide unnecessary?

The question is "what is the SLA, in milliseconds, and what does the workload do with state" — everything else on the slide is a checkbox contest. These pages exist to stop **feature-matrix shopping**. Two engines can both "do SQL" and still be the wrong place for a workload. You choose by **access pattern, latency, state, and failure**, then you confirm with a number from one of the running systems.

If you want a procedure rather than a pairwise fight, use the [selection framework](../reference/selection-framework.md). Come back here when two names are left.

---

## How to use a comparison page

1. Write the workload in one sentence (events/s, latency, mutate or append, query shape).
2. Read **core difference** — design origin, not a bullet list of APIs.
3. Skip to **choose X / choose Y / choose neither**.
4. Work the **running-system example**. If your workload does not look like that example, do not copy the choice.
5. Only then skim the internals table.

A comparison is not a winner. Most production platforms use **both** tools in the same diagram, on different SLAs. "Spark vs Flink" in [observability](../architectures/observability.md) is Flink on the hot path and Spark on the cold path. That is a successful comparison.

!!! tip "Neither is a valid outcome"
    If the right answer is Postgres, a TSDB, or "a nightly job," the comparison page should say so. Adding both tools is how you get a zoo.

---

## Available comparisons

### Processing engines

| Page | Decide this | Not this |
|------|-------------|----------|
| [Spark vs Flink](spark-vs-flink.md) | Batch-first vs stream-first; state and event time | "Which is faster" in the abstract |
| [Spark vs Ray](spark-vs-ray.md) | DataFrame ETL vs distributed Python/ML | Replacing Spark SQL with Ray tasks |

### Query and OLAP

| Page | Decide this | Not this |
|------|-------------|----------|
| [ClickHouse vs Pinot](clickhouse-vs-pinot.md) | General OLAP vs ultra-fresh high-QPS tiles | Which has more index types |
| [ClickHouse vs Trino](clickhouse-vs-trino.md) | Store+query vs federated query | Putting Trino on the 100 ms dashboard |

### Storage paradigms

| Page | Decide this | Not this |
|------|-------------|----------|
| [TSDB vs OLAP](tsdb-vs-olap.md) | Infra metrics vs high-card events | "Time series must live in a TSDB" |
| [Graph vs relational](../graph/graph-vs-relational.md) | Multi-hop vs joins | Putting Neo4j on checkout |

Lakehouse formats (Iceberg / Hudi / Delta) are compared in [lakehouse](../lakehouse/comparison.md), not here — that choice is about **upserts, ecosystem, and deletes**, not query latency.

---

## Workload properties that actually split tools

| Property | Pulls you toward | Pulls you away |
|----------|------------------|----------------|
| p99 < 200 ms stateful | Flink, KV, in-process model | Spark micro-batch, Trino, graph |
| Minutes, huge shuffle, lakehouse | Spark | Ray (shuffle), Flink (unless already there) |
| Dashboard 100 ms, SQL, one cluster | ClickHouse | Trino, Postgres |
| Freshness 1–2 s, thousands of QPS, fixed tiles | Pinot | Ad-hoc ClickHouse SQL as the product |
| Join Iceberg to Postgres once | Trino | Loading Postgres into CH first "just in case" |
| `user_id` as a dimension | ClickHouse | Prometheus |
| CPU / error rate alerting | Prometheus / VM | CH-only Alertmanager clones |
| 4-hop fraud ring | Neo4j (batch) | Recursive SQL on the auth path |
| Hyperparam search, PyTorch | Ray | Spark MLlib |

Write the property **before** the tool name. If you cannot, you are collecting logos.

---

## Worked example: one event, five wrong fights

The SaaS analytics event from [labs](../labs/index.md):

```json
{"timestamp":"2024-01-15T10:30:00Z","customer_id":"cust_0042","user_id":"user_98712","service":"api-gateway","latency_ms":45,"status_code":200}
```

| Fight people start | Workload | Outcome |
|--------------------|----------|---------|
| Spark vs Flink | Tenant dashboard < 30 s vs 5 min lake ETL | **Both**: Flink/CH hot, Spark/Iceberg cold — [analytics](../architectures/analytics-platform.md) |
| Spark vs Ray | Same events into a GBDT | Spark features, **Ray** train/serve — not Ray Data as the warehouse |
| CH vs Pinot | 50 Grafana panels, 20 QPS | **ClickHouse**. Pinot if you productise 5k QPS live tiles |
| CH vs Trino | "Last 15 min error rate" vs "join 2y lake to billing Postgres" | **CH** then **Trino**. Never one engine |
| TSDB vs OLAP | `api-gateway` CPU vs per-`user_id` latency | Prometheus **and** CH. `user_id` never a label |

If your design doc still says "we will pick one of Spark or Flink for all processing," you have not specified latency.

---

## How "vs" collapses in the five architectures

| Architecture | Pair that is not a fight | Split |
|--------------|--------------------------|-------|
| [Observability](../architectures/observability.md) | CH vs Trino | CH hot 7 d, Trino on Iceberg |
| [E-commerce](../architectures/ecommerce.md) | Spark vs Flink | Spark/dbt lake; Flink only if inventory SLA is seconds |
| [IoT](../architectures/iot.md) | TSDB vs OLAP | VM/Prom for **infra**; CH/Timescale for **device series** |
| [Fraud](../architectures/fraud.md) | Spark vs Flink vs Neo4j | Flink score; Spark labels; Neo4j **batch** rings |
| [SaaS analytics](../architectures/analytics-platform.md) | CH vs Pinot | CH until QPS/freshness measurements say otherwise |

---

## Anti-patterns these pages exist to kill

- **Matrix scoring** ("Flink has 12 features, Spark has 9"). Count production constraints, not checkboxes.
- **Future streaming.** If the SLA is nightly, Spark. Kafka can still ingest.
- **One OLAP for federation and dashboards.** That is two jobs ([CH vs Trino](clickhouse-vs-trino.md)).
- **Ray because Python.** Spark is Python. Ray is for **task/actor** workloads Spark's DAG is bad at.
- **Pinot because LinkedIn.** Your QPS is not LinkedIn's.
- **TSDB because the x-axis is time.** Time is not a data model; cardinality is.

---

## Read next

1. Pick the pair you are actually arguing about at work.
2. Fill the choose-X / choose-Y / choose-neither table with **your** numbers.
3. If neither fits, [selection framework](../reference/selection-framework.md) from scratch.
4. Confirm failure modes in [incidents](../incidents/index.md) — a tool you cannot debug is not chosen, it is rented.

The goal is not to pick a winner. It is to put each workload on the engine that was designed for it, and to say **no** to the rest.

---

## How a design review should use "vs" pages

Bring **numbers**, not logos:

1. Latency SLO (p99) and QPS.
2. Data size and growth.
3. Query examples (real SQL/PromQL/Cypher).
4. Team that will be on-call.
5. Failure you will hit first (link an [incident](../incidents/index.md)).

If the review is "ClickHouse vs Snowflake vs BigQuery vs Trino vs Pinot vs Druid," you have not done Step 1 of the [framework](../reference/selection-framework.md). Narrow to **two** names with the table above.

---

## Complement, don't duplicate

| Layer | Typical pair | Duplicate that hurts |
|-------|--------------|----------------------|
| Process | Flink hot + Spark cold | Two stream processors on the same SLA |
| OLAP | CH tiles + Trino lake | Trino **on** CH for Grafana |
| Time | Prom infra + CH events | Prom **and** VM **and** CH **and** Timescale for one metric |
| ML | Spark features + Ray train | Spark MLlib DL + Ray + SageMaker for one model |

Paying for two systems is rational when SLAs differ. It is irrational when both do 15-minute batch SQL.

---

## "Neither" is underused

Worked **neither** answers from this academy:

| Request | Neither X nor Y | Use |
|---------|-----------------|-----|
| Store checkout orders | CH vs Pinot | Postgres |
| Alert on disk full | CH vs Trino | Prometheus |
| Tune PyTorch | Spark vs Flink | Ray |
| 4-hop fraud ring on auth | Spark vs Flink | Flink score **plus** batch graph — not SQL-only |
| 10 GB internal dashboard | CH vs Pinot | Postgres |

Write "neither" in the review notes so it does not come back as a Jira.

---

## Reading order if you are new

1. [Spark vs Flink](spark-vs-flink.md) — most teams fight this first.
2. [ClickHouse vs Trino](clickhouse-vs-trino.md) — second fight.
3. [TSDB vs OLAP](tsdb-vs-olap.md) — saves the observability project.
4. [Spark vs Ray](spark-vs-ray.md) — when ML appears.
5. [ClickHouse vs Pinot](clickhouse-vs-pinot.md) — only when QPS/freshness is measured.

Then [graph vs relational](../graph/graph-vs-relational.md) if multi-hop is real.

---

## What a comparison page will not tell you

- Cloud bill after discounts.
- Whether your org already has a site licence.
- Political "platform team owns X."

Those constraints belong in the decision record. They do not change the **workload** mapping; they change who operates the box. If politics forces Trino on a 100 ms tile, still write that the **engine** is wrong — so the next incident is unsurprising.

---

## Feature matrices (how to read them elsewhere)

Vendor matrices count connectors and SQL functions. Use them to answer "can it speak Iceberg?" not "should we." After "can," come back to **workload** on this site.

A matrix that scores Flink 9/10 and Spark 8/10 is advertising. A table that says "200 ms keyed state → Flink; 20 TB join → Spark" is a decision.

---

## Pairing with labs

| Comparison | Lab that makes it visceral |
|------------|----------------------------|
| Spark vs Flink | Spark shuffle vs Flink watermark |
| CH vs Trino | CH ORDER BY lab (Trino is tabletop incident 6) |
| CH vs Pinot | CH lab; Pinot is QPS thought experiment |
| TSDB vs OLAP | Cardinality sim |
| Spark vs Ray | Spark lab; Ray is the ML leftover |

If you cannot feel the difference in a lab or sim, you will pick from logos.

---

## One-sentence splits (memorise)

- Spark **batches partitions**; Flink **updates keys**.
- Ray **runs Python call graphs**; Spark **transforms tables**.
- CH **stores** for speed; Trino **visits** for flexibility.
- Pinot **serves known tiles hot**; CH **answers SQL**.
- TSDB **indexes series**; OLAP **scans columns**.

If you can say those without the page, you are done with this index. Use the child pages for the choose-X tables.
