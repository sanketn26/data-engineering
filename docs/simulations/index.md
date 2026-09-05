# Interactive Simulations

Browser tools for failure modes that are hard to **feel** from prose: hot Kafka keys, shuffle skew, ClickHouse `ORDER BY`, TSDB cardinality. They are not dashboards of a real cluster. They are labs for your **mental model**.

Related: [how to study](../how-to-study.md), [labs](../labs/index.md), [incidents](../incidents/index.md).

---

## How to use (non-negotiable)

1. **Read the concept page first** (linked below). If you open the sim cold, you will slide sliders.
2. **Predict** what the default settings show. One sentence, out loud or on paper.
3. **Change one variable** (hot-key %, skew, `ORDER BY`, a label).
4. **Compare** to the prediction. Name the metric that would page you in production.
5. Only then try the "what to try" recipe on this page.

!!! tip "Predict first"
    The sim will happily confirm whatever you already believe if you skip step 2. The academy loop is predict → run → compare → explain.

These are single HTML files. They work offline. In MkDocs they are linked pages. You can also open them from `docs/simulations/` in the repo.

---

## Kafka partition simulator

**Open:** [kafka-partitions.html](kafka-partitions.html)

**Teaches:** Kafka parallelism is **partitions**, not consumer count. Ordering and state locality are **per key**. A hot key is a **hot partition**. Extra consumers idle while one partition's lag explodes.

**Read first:** [Kafka partitions](../kafka/partitions.md), [gotchas](../kafka/gotchas.md).

**Predict before touching sliders:**

- If consumers > partitions, how many consumers sit idle?
- If 70% of keys are one customer, can 12 consumers save you?

**What to try:**

1. Equal keys, 6 partitions, 6 consumers — lag ~flat.
2. 6 partitions, 12 consumers — idle consumers.
3. Hot key ratio 70% — **one** partition's lag grows. This is [incident 1](../incidents/index.md).
4. Increase partitions **without** changing the key — the hot key still hashes to **one** partition.

**Production metric:** lag **per partition**, produce bytes per partition, consumer CPU per subtask.

**Architecture:** [SaaS whale](../architectures/analytics-platform.md), [fraud user key](../architectures/fraud.md).

**Then run:** [Kafka lab](../labs/index.md) with a real broker.

---

## Spark shuffle visualiser

**Open:** [spark-shuffle.html](spark-shuffle.html)

**Teaches:** `groupBy`/`join` **move** data. Uniform keys → even reduce tasks. Skew → one reducer owns the stage. Salting splits a hot key on purpose.

**Read first:** [shuffle](../spark/shuffle.md).

**Predict:**

- Very skewed: what fraction of rows land on the busiest reducer?
- Does adding Spark executors split **one** key? (No.)

**What to try:**

1. Uniform — bar chart even; shuffle still **happens**.
2. Very skewed — one bar dominates; this is [incident 2](../incidents/index.md) without the OOM yet.
3. Salted — more bars, extra CPU, **no** 90% bar.

**Production metric:** Spark UI task duration / shuffle read **max vs median**.

**Then run:** [Spark lab](../labs/index.md) and watch stage 4040.

---

## ClickHouse ORDER BY explorer

**Open:** [clickhouse-order-by.html](clickhouse-order-by.html)

**Teaches:** the sparse primary index is the **physical** `ORDER BY`. If `WHERE` does not match the **prefix**, you scan granules you did not think you would. Same SQL, 100% vs 0.3% marks.

**Read first:** [ClickHouse](../olap/clickhouse.md).

**Predict:**

- Table `ORDER BY (customer_id, timestamp)`, query one tenant last 15 min — skip or scan?
- Flip to `ORDER BY (timestamp, customer_id)` — same query?

**What to try:**

1. Scenario "wrong `ORDER BY`" vs "right" — compare granules read.
2. Imagine the [observability](../architectures/observability.md) query `service + 15 min` on a time-first table — that is [incident 4](../incidents/index.md) shape A.

**Production metric:** `system.query_log` marks / rows read, not "CPU is high."

**Then run:** [ClickHouse lab](../labs/index.md) with two real tables.

---

## Cardinality calculator

**Open:** [cardinality-calculator.html](cardinality-calculator.html)

**Teaches:** TSDB series count is the **product** of label values. Adding `user_id` (100k) turns a healthy metric into a memory bomb. Time on the x-axis does not justify Prometheus.

**Read first:** [cardinality](../time-series/cardinality.md), [TSDB vs OLAP](../comparisons/tsdb-vs-olap.md).

**Predict:**

- 4 bounded labels (service, code, region, pod) — order of magnitude series?
- Same + `user_id` 1e5 — do you still have a TSDB?

**What to try:**

1. Start with 4 infra labels — stay under a comfortable series budget.
2. Add `user_id` or `device_id` — cross the line. That data belongs in **ClickHouse**.
3. Map to [IoT](../architectures/iot.md) 10M devices — Prometheus is for the **cluster**, not the fleet.

**Production metric:** Prometheus `prometheus_tsdb_head_series`, scrape duration, CH `uniq(user_id)` as a **column** cost not a series cost.

---

## What each sim is **not**

| Sim | Not a substitute for |
|-----|----------------------|
| Kafka partitions | Disk, ISR, `acks`, rebalances — [replication](../kafka/replication.md) |
| Spark shuffle | AQE, broadcast memory, disk spill — [lab](../labs/index.md) |
| CH ORDER BY | Too many **parts** (incident 4 shape B) — [lab](../labs/index.md) |
| Cardinality | CH `GROUP BY` memory — still a real limit, different cliff |

---

## Suggested afternoon

1. Kafka sim → Kafka lab → incident 1.
2. Shuffle sim → Spark lab → incident 2.
3. ORDER BY sim → CH lab → incident 4.
4. Cardinality sim → rewrite one Prom metric at work that should be an event.

If you only do the HTML, you will remember the pictures. If you do lab + incident, you will remember the **metric**.

---

## Contributing simulations

Keep them:

- One HTML file, no npm build.
- One concept, one failure mode.
- Sliders that map to **real knobs** (partitions, skew, labels).
- Dark-theme consistent with the site.

Do not add a sim that only animates a logo. If it cannot support a predict → compare loop, it is decoration.

---

## Predict prompts (copy onto paper)

**Kafka:** "With 8 partitions and 8 consumers, lag is even. I now set hot key 80%. I predict partition *i* (whichever hashes the hot key) will hold ~80% of messages and its consumer CPU will saturate; the other 7 will idle. Adding 8 more consumers will not change that."

**Spark:** "Very skewed → one reduce bar ~90%. Salted → N extra keys, bars even, more shuffle bytes. Uniform → even bars, shuffle still non-zero."

**ClickHouse:** "Tenant filter on `(customer_id, ts)` reads << all marks. Same filter on `(ts, customer_id)` reads all customers in the time range."

**Cardinality:** "Four infra labels stay under 1e6 series. Adding user_id 1e5 crosses a TSDB budget; I will refuse that label and use CH."

If your prediction was wrong, write **which assumption failed** (e.g. you thought consumers split partitions).

---

## Mapping to Grafana

When you later have a real cluster, recreate the sim with **real** panels:

- Kafka: `sum by (partition) (lag)`
- Spark: task duration histogram
- CH: `query_log.read_rows`
- Prom: `prometheus_tsdb_head_series`

The sim is a cartoon of those panels. The lab is a small live version. Production is the same picture with money on fire.

---

## Classroom / pairing

Two people: A predicts, B drives the slider. Swap. No silent sliding. Fifteen minutes per sim is enough; then the matching lab.

Do not run all four sims in a row without a module page — they will blur into "graphs go brrr."
