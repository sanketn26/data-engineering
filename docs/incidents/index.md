---
description: Practice diagnosing Kafka hot partitions, Spark skew, and Flink watermark incidents from metrics before reaching for a fix.
---

# Production Incidents

02:47 AM. `P1 consumer_lag_seconds{topic="product-events"} > 180`. Three teams' dashboards go stale in the next ten minutes and you have not opened a terminal yet.

A. Restart the consumer group. B. Add more processing slots. C. Check per-partition lag before touching anything else. D. Page the on-call lead and wait for guidance.

Only one of those survives contact with the metrics below — figuring out which one, before you act, is the whole exercise on this page. These are drills, not blog posts. Read **Alert** and **Symptoms**. Write a hypothesis. Only then open **Resolution**.

The skill is not memorising the root cause. It is forming a **falsifiable** story from metrics: which layer, which key, which hop. That is what on-call actually is.

Related: [Kafka gotchas](../kafka/gotchas.md), [Spark gotchas](../spark/gotchas.md), [how to study](../how-to-study.md), [labs](../labs/index.md).

---

## How to use these

1. Read the alert and the telemetry. Do not skim to the details tag.
2. Pause. Name: **system**, **failure class** (skew, time, planning, memory, disk), **what you would query next**.
3. Expand resolution. Compare. If you were wrong, write *why the symptoms also fit your story* — that is how you get better, not by flipping the card.

Every incident below is a shape you will meet in the five architectures. Kafka hot partition is [analytics](../architectures/analytics-platform.md) whales and [fraud](../architectures/fraud.md) keys. Spark skew is e-commerce joins. Flink watermarks are [IoT](../architectures/iot.md) idle devices. ClickHouse parts/`ORDER BY` is every dashboard. Iceberg snapshots are cold paths. Trino coordinator OOM is "just one join."

---

## Incident 1 — Kafka: lag on one partition

### Alert

`P1 consumer_lag_seconds{topic="product-events", group="flink-enrich"} > 180` for **partition 7 only**. SLO: dashboards < 60 s fresh.

### Symptoms

- `kafka.consumer.lag` partition 7: **4.2 million** messages and climbing; partitions 0–6 and 8–23: **< 200**.
- Flink subtask 7 CPU 95%, checkpoint duration 2×; other subtasks idle-ish.
- Broker disk and network: leader of p7 is **hot**; other brokers fine.
- Produce rate global: unchanged at 80k/s. No ISR shrink. No rebalance storm (`rebalance_rate` quiet).
- `kafka-consumer-groups.sh --describe`: one consumer assigned p7+others in the group; lag **not** explained by fewer consumers than partitions (24 partitions, 24 subtasks).
- Sample keys on p7: one `customer_id=cust_0042` dominates (~70% of p7). Overall traffic: that tenant is ~12% of the company — enough to fill **one** hash bucket.
- Other consumer groups on the same topic: **same partition** lagging.

!!! question "Form a hypothesis"
    Is this (a) slow sink, (b) GC on one JVM, (c) key skew, (d) broker disk, (e) consumer count? What would **disprove** key skew in 30 seconds?

<details>
<summary>Resolution — Kafka hot partition</summary>

### Root cause

Partition key is `customer_id`. Tenant `cust_0042` (or a bot, or `service=api-gateway` in observability) hashes to partition 7. Kafka **cannot** split a partition across consumers in a group. Extra Flink slots will not help p7. This is [hot partition](../kafka/gotchas.md) + [analytics noisy neighbour](../architectures/analytics-platform.md).

Not a global produce outage (other partitions healthy). Not a ClickHouse sink (that would lag **all** partitions unless the sink is keyed the same way — and even then, check keys first).

### Fix (emergency)

1. **Quota / sample** that tenant at the gateway if product allows — stops the bleeding.
2. Scale **processing of that key**: a **separate consumer group** with a dedicated job that only reads a **new** topic for the whale, or a Flink sidecar — still one partition until you re-key.
3. Short-term re-key is a **new topic** (or new key) because changing keys mid-topic does not move old data.

Sustainable keying:

- Compound: `hash(customer_id) % N` **only for known whales**, suffix `cust_0042#0..15` so 16 partitions share the load; downstream re-aggregates.
- Do **not** round-robin **all** keys — you break per-tenant ordering and Flink keyed state locality for everyone.

### Prevention

- Alert **lag max / lag p50 ratio** per group, not only sum lag.
- Dashboards: produce bytes **per partition**.
- Tenant quotas.
- Capacity: if one customer is 40% of traffic, they are an architecture, not a row. See [Kafka partition sim](../simulations/kafka-partitions.html).
- Lab: [Kafka lab](../labs/index.md) hot-key exercise.

</details>

---

## Incident 2 — Spark: executor OOM on a skewed join

### Alert

`Spark job fct_orders_enrich` failed. `ExecutorLostFailure: Exit code 137` (OOMKilled). Airflow retry also failed. SLA: 07:00 gold table.

### Symptoms

- Spark UI: stage `SortMergeJoin` — **199 tasks succeed in 40 s**, **one task** runs 25+ min then dies. Task is partition `118`.
- Shuffle read on that task: **180 GB**; others ~1–2 GB. Peak execution memory at cap. Spill to disk huge then OOM.
- Driver fine; not a `collect()`.
- AQE enabled; `spark.sql.adaptive.skewJoin.enabled` true — still died (skew **larger** than AQE split budget / join type).
- Input: `events` 2 TB, `customers` 8 GB. Join key `customer_id`.
- `events` histogram (from a sample job): `customer_id=cust_wholesale` **~18% of rows** (bot + marketplace).
- Config: `spark.sql.shuffle.partitions=200`. Executor 16 GB, 4 cores.
- GC logs: old gen full on that executor only.

!!! question "Form a hypothesis"
    Driver OOM vs executor skew vs exploding join vs too few partitions globally? What number in the UI splits those?

<details>
<summary>Resolution — Spark skewed join</summary>

### Root cause

Sort-merge join shuffled on `customer_id`. One key landed in one reducer. That reducer built a huge join relation and died. Classic [data skew](../spark/gotchas.md). AQE skew split **can** fix moderate skew; 180 GB vs 2 GB is a **salt or broadcast** problem.

Not exploding join (that would blow **output** row counts on many tasks if many keys duplicate). Not "need 4000 executors" (they would sit idle).

### Fix

1. If `customers` is unique on `customer_id` and **small**: `broadcast(customers)` — **eliminates** the big shuffle. 8 GB broadcast is OK with memory; 80 GB is not.
2. If the whale is few keys: **salt** the join key on the large side, explode salt on the small side, drop salt after.
3. Two-path join: pull whale keys to a separate job; join the rest normally.
4. Raise executor memory **only** as a bridge — it does not fix 10×.

Emergency to hit 07:00: filter out the whale key, publish with a quality flag, backfill the whale.

### Prevention

- Pre-job: `groupBy(key).count()` approx on join keys; alert on max/median.
- Broadcast threshold honest; stats up to date.
- See [shuffle sim](../simulations/spark-shuffle.html); [Spark lab](../labs/index.md) skew exercise.
- Product: bot `customer_id` should not share the key space with paying tenants.

</details>

---

## Incident 3 — Flink: watermark stalled, no output

### Alert

`flink.job.records_out` for `iot-1min-rollups` is **0** for 40 minutes. Input `records_in` still 300k/s. Downstream Grafana 1-minute IoT panels frozen (last point old). Checkpoints **succeed**.

### Symptoms

- Flink UI: watermark for most keys **not advancing**; one Kafka partition shows source idle time **0** but the **event-time** watermark is stuck at `T-6 hours`.
- A subset of devices send **clocks in the past** (bad firmware) or a **dead producer** still holds a partition with no records — depending on strategy, watermark is `min` over partitions.
- Tumbling **event-time** 1-minute windows never close.
- Processing-time operator counters still move (you added a debug `map`).
- Late-event side output empty (windows never close, so "late" is undefined).
- Kafka lag **low** — this is not incident 1.
- Checkpoint size stable (state not exploding).

!!! question "Form a hypothesis"
    Slow sink vs watermark vs key skew vs checkpoint? Which UI number is the watermark?

<details>
<summary>Resolution — Flink watermark stall</summary>

### Root cause

Event-time windows **close on watermarks**, not on wall clock. Watermark was `min` over Kafka partitions (or over keys without idleness). One partition silent (device gateway down) **or** one partition of very old timestamps **held the min down**. All windows waited. Checkpoints succeeding proves the job is "healthy" in a useless sense.

This is [IoT](../architectures/iot.md) and [Flink time](../flink/time.md).

### Fix

1. Enable **source idleness** (`withIdleness(Duration.ofMinutes(1))`) so silent partitions do not stall the world.
2. Filter/clamp **impossible timestamps** (e.g. > 1 h skew) to a side output; do not let year-2019 firmware block 2026 windows.
3. For "latest value" product, consider **processing-time** or a hybrid flush.
4. Temporarily restart with a watermark strategy that ignores idle partitions; expect a burst of window output.

### Prevention

- Alert: `current_watermark` vs `now()` (allowed skew SLO).
- Alert: `records_out == 0 && records_in > 0` for windowed jobs.
- Lab: [Flink lab](../labs/index.md) idle/event-time exercise.
- Do not use event-time windows on a source you cannot watermark.

</details>

---

## Incident 4 — ClickHouse: query 10× slower

### Alert

Grafana `P2 dashboard_query_seconds{panel="error_rate"}` p95 **12 s** (was 0.8 s). On-call cannot page by service. Inserts still succeeding.

### Symptoms (two related shapes; both appear in prod)

**Shape A — wrong `ORDER BY` after a "cleanup" migration**

- `EXPLAIN` / `system.query_log`: `marks` read ≈ **all marks**; `selected_parts` large.
- Table `events` now `ORDER BY (timestamp, service)` (someone "optimised for time").
- Query: `WHERE service = 'api-gateway' AND timestamp > now() - 15 min`.
- `primary_key` cannot skip by `service`; must scan the 15-minute range **across all services**. At 5M events/s that range is huge.
- Compression and CPU up; disk IO up.

**Shape B — too many parts**

- `system.parts` for `events`: **tens of thousands** of active parts.
- `Insert` via Kafka engine **row-ish** or tiny batches every 50 ms.
- `system.metrics` `BackgroundMerges` saturated; `DelayedInserts` > 0 occasionally.
- Queries open too many files (`Too many parts` warnings in log).
- `ORDER BY` still `(service, timestamp)` — EXPLAIN would skip marks **if** parts were few.

!!! question "Form a hypothesis"
    Would you look at `query_log` marks first or `system.parts`? How do the two shapes differ in Grafana (all queries vs some)?

<details>
<summary>Resolution — ClickHouse ORDER BY or parts</summary>

### Root cause

**A:** Sparse index follows `ORDER BY`. Prefix of the key must match filters. Service-first is the [observability](../architectures/observability.md) and [SaaS](../architectures/analytics-platform.md) dashboard. Time-first is the global SRE scan. Migration inverted the product. See [ORDER BY sim](../simulations/clickhouse-order-by.html).

**B:** Each tiny insert creates a part. Merges cannot keep up. Queries concatenate thousands of index pieces. Same symptom (slow SELECT), different table.

### Fix

**A:** New table with `ORDER BY (service, timestamp)` (or `(customer_id, timestamp)`), insert-select or dual-write, swap. Cannot "just ALTER" the physical key cheaply on huge data.

**B:** Batch inserts (1–10 s). Pause Kafka engine, `OPTIMIZE TABLE ... FINAL` only as emergency (it is expensive). Raise insert block size. Fix the producer.

### Prevention

- `query_log` dashboard: marks read vs total.
- Alert `parts` count per table.
- Lab: [ClickHouse lab](../labs/index.md).
- Change `ORDER BY` is a **design review**, not a tidy-up.

</details>

---

## Incident 5 — Iceberg: snapshot accumulation, slow planning

### Alert

Spark/Trino jobs on `lake.events` **planning** 15–40 minutes (was 2). Queries that **read 1 day** still plan like they must inspect the world. S3 `LIST`/`GET` on metadata spiking. Compute idle during plan.

### Symptoms

- `snapshots` table: **180k snapshots**, expire never run. Hourly job **every 3 minutes** (mis-cron) committed for months.
- Manifest lists huge; many **small** data files (5–20 MB) in recent partitions.
- `rewrite_data_files` not scheduled; `expire_snapshots` commented out "until we need time travel."
- Trino: coordinator CPU in planning, workers idle, then a burst.
- Time travel "last 2 hours" still works — too well: you kept everything.
- Disk/S3 metadata: billions of objects in the prefix.

!!! question "Form a hypothesis"
    Slow scan of data vs slow **planning**? Which metric (worker bytes vs coordinator CPU / S3 GET on `metadata/`)?

<details>
<summary>Resolution — Iceberg snapshot pile</summary>

### Root cause

Every commit adds a snapshot. Iceberg planning walks metadata (manifests) to pick files. Unexpired snapshots + tiny files + missing compaction → **metadata amplification**. The lake is correct; it is unusable. Cold path of [observability](../architectures/observability.md) / [analytics](../architectures/analytics-platform.md).

This is not "Trino is slow at SQL" and not "need 200 workers" (they are idle in plan).

### Fix

1. **Expire snapshots** older than the time-travel policy (e.g. 7 d), retain last N.
2. **Rewrite manifests** / `rewrite_data_files` on hot partitions.
3. Stop the 3-minute commit storm; batch commits.
4. Orphan file removal **after** expire, carefully.

Emergency: query a **specific snapshot** you know is healthy; do not `SELECT` through current if metadata is pathological — still may plan slowly. Worst case: read known file list from a **new** table created from a recent snapshot's data files (surgical).

### Prevention

- Compaction + expire as **Airflow gold**, not a wiki.
- Alert snapshot count, files per partition, planning time.
- Time travel is a **retention policy**, not infinity ([security](../security/index.md) GDPR).
- Fewer, larger commits.

</details>

---

## Incident 6 — Trino: coordinator OOM on a wild join

### Alert

Trino cluster **down**. Coordinator `java.lang.OutOfMemoryError`. All users fail. Grafana on CH **unaffected**.

### Symptoms

- Last query in log:  
  `SELECT * FROM iceberg.events e JOIN postgres.prod.users u ON e.user_id = u.id`  
  with **no** time filter, `events` is 2 years, `users` 40 million rows.
- Query: distributed join, **broadcast** hinted or estimated **wrong** (stats missing on Iceberg). Coordinator or a worker tried to hold a huge build side; coordinator OOM often from **query plan / metadata / buffered results** or a join that was scheduled as single-node.
- `JOIN` on `cast(user_id AS varchar)` disabling pruning.
- Cost-based optimizer thought `events` was 0 rows (stale stats).
- Exchange `REPARTITION` vs `BROADCAST` in EXPLAIN: broadcast of a **large** side.
- Other queries died because **one coordinator**.

!!! question "Form a hypothesis"
    Worker scan vs coordinator? What EXPLAIN line is the smoking gun?

<details>
<summary>Resolution — Trino coordinator OOM</summary>

### Root cause

Unbounded lake ⋈ OLTP replica. Missing partition predicate. Bad stats → broadcast or a too-fat plan. Coordinator is a **SPOF** for planning and sometimes for results. This is why [CH vs Trino](../comparisons/clickhouse-vs-trino.md) says Trino is not the 100 ms UI and not a playground without governors.

### Fix

1. Restart coordinator (cluster is already dead).
2. Kill the query pattern: **require** `WHERE e.date > ...` via view or policy.
3. Disable broadcast for huge tables; set `join_distribution_type`.
4. Memory limits per query; **isolation** (separate Trino for ad-hoc vs gold).
5. Do not `SELECT *` the lake.

### Prevention

- Query max memory, timeout, **max scanned bytes**.
- Stats collection on Iceberg.
- Analysts default to **sampled** views.
- Two coordinators/clusters: exploratory vs production ETL.
- Education: join **day** of events to users, not history.
- [Security](../security/index.md): this query may also be an exfil; audit it.

</details>

---

## Cross-walk

| Incident | Architecture | Lab / sim |
|----------|--------------|-----------|
| Kafka one-partition lag | Analytics whale, fraud key, observability hot service | [Kafka lab](../labs/index.md), [partition sim](../simulations/kafka-partitions.html) |
| Spark join OOM | E-commerce enrich, fraud labels | [Spark lab](../labs/index.md), [shuffle sim](../simulations/spark-shuffle.html) |
| Flink no output | IoT rollups, session windows | [Flink lab](../labs/index.md) |
| CH 10× | Every Grafana | [CH lab](../labs/index.md), [ORDER BY sim](../simulations/clickhouse-order-by.html) |
| Iceberg planning | Cold paths | Lakehouse module |
| Trino OOM | Ad-hoc federation | [CH vs Trino](../comparisons/clickhouse-vs-trino.md) |

---

## After-action habit

For your own incidents, steal this structure: Alert → Symptoms (numbers) → Hypothesis pause → Root cause → Fix → Prevention. If prevention is "be careful," you have not finished. Add a **metric** and an **owner**.
