---
description: Browser labs for Kafka hot keys, Spark shuffle skew, ClickHouse ORDER BY, TSDB cardinality, and backpressure recovery you can predict, run, and compare.
---

# Interactive Simulations

Browser tools for failure modes that are hard to **feel** from prose: hot Kafka keys, shuffle skew, ClickHouse `ORDER BY`, TSDB cardinality, backpressure recovery time, stalled watermarks, ISR failure, metadata pruning, hash distribution, and dimension history. They are not dashboards of a real cluster. They are labs for your **mental model**.

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

## Backpressure & Little's Law calculator

**Open:** [backpressure-calculator.html](backpressure-calculator.html)

**Teaches:** a throughput mismatch does not vanish — it becomes a growing backlog. Little's Law turns "how long until we catch up" from a guess into arithmetic, and shows why a service rate that barely exceeds arrival still means a very slow recovery.

**Read first:** [Backpressure & Queueing](../foundations/backpressure.md).

**Predict before touching sliders:**

- Arrival 100k/s, service 70k/s for 20 minutes — roughly how many events back up?
- After scaling to 150k/s, does the system recover in minutes or hours?
- What happens if the "scaled" rate is set to exactly the arrival rate?

**What to try:**

1. Default mismatch — note the backlog number, then guess recovery time before scrolling to the verdict.
2. Set the post-scaling rate equal to arrival rate — watch recovery time diverge instead of hitting zero.
3. Set post-scaling rate just barely above arrival (e.g. +5%) — same backlog, very different recovery time from +50%.

**Production metric:** Kafka `records-lag`, Flink `backpressured_time` per operator, or a sink's write-latency trend — see [debugging](../foundations/backpressure.md#debugging) for which one separates which hypothesis.

**Then run:** the exercise at the bottom of [Backpressure & Queueing](../foundations/backpressure.md#exercise) with the numbers from this sim.

---

## Watermark simulator

**Open:** [watermark-simulator.html](watermark-simulator.html)

**Teaches:** a downstream event-time watermark is the **minimum** across every active source split. One idle split with no idleness policy freezes the whole downstream watermark — and therefore every window waiting on it — forever, even while every other split advances normally.

**Read first:** [Flink windows & watermarks](../flink/windows.md), [Flink time semantics](../flink/time.md).

**Predict before touching sliders:**

- Split A's watermark is way ahead, split B stops advancing. Does the downstream watermark advance, stall, or split the difference?
- Turning on an idleness timeout for B — does that change what happens to a **late event that still arrives on B**?

**What to try:**

1. Both splits advancing evenly — downstream watermark tracks the slower of the two, windows close on schedule.
2. Freeze split B, no idleness policy — downstream watermark stalls at B's last value; a window waiting to close never does.
3. Enable the idleness timeout past B's stall duration — downstream watermark starts advancing again, using only A.

**Production metric:** per-source-split watermark lag, Flink's `currentInputWatermark` metric, and `records_out` per window operator flatlining despite `RUNNING` status.

**Then run:** [Flink lab](../labs/index.md) `stalled_watermark.py`, then [`check_event_time.py`](../labs/index.md) to see the same mechanism asserted, not just visualized.

---

## Kafka ISR failure simulator

**Open:** [kafka-isr-simulator.html](kafka-isr-simulator.html)

**Teaches:** `acks`, `min.insync.replicas`, and current ISR membership jointly decide whether a write succeeds, blocks, or throws `NotEnoughReplicas` — and `acks=all` only ever means "wait for the *current* ISR," which can have shrunk to just the leader.

**Read first:** [Kafka replication & durability](../kafka/replication.md).

**Predict before touching sliders:**

- ISR is down to just the leader, `acks=all`, `min.insync.replicas=1`. Does this write have the same durability guarantee as `acks=1`?
- Kill the leader with an empty ISR and unclean election **disabled** — does the partition lose data, or go unavailable?

**What to try:**

1. Full ISR, `acks=all`, `min.insync.replicas=2` — healthy write, safe leader failover.
2. Shrink ISR to the leader only, same settings — writes start failing with `NotEnoughReplicas` rather than silently losing durability.
3. Kill the leader with unclean election **enabled** on an empty ISR — a replica with stale data becomes leader; flag the data-loss window explicitly.

**Production metric:** `UnderReplicatedPartitions`, `ISR shrink/expand` broker logs, `NotEnoughReplicasException` rate on producers.

**Then run:** [Kafka lab](../labs/index.md)'s broker-stop exercise, this time watching ISR membership instead of just consumer lag.

---

## Iceberg manifest explorer

**Open:** [iceberg-manifest-explorer.html](iceberg-manifest-explorer.html)

**Teaches:** Iceberg answers "what is the table right now" by reading a small tree of metadata (snapshot → manifest list → manifests → per-file stats), never by listing the object store — and a query prunes files using only that tree, before opening a single data file.

**Read first:** [Apache Iceberg](../lakehouse/iceberg.md), [Object Storage Internals](../foundations/object-storage.md).

**Predict before clicking:**

- After "commit a new write," does the previous snapshot stop being queryable?
- A `date =` filter that matches only 1 of 8 data files — how many files does the engine actually have to open to know that?

**What to try:**

1. Commit a second write — watch a new snapshot appear while the old one (and its manifests) stay intact for time travel.
2. Run a query with a narrow `date =` filter — watch most files go grey (skipped via manifest stats) without ever being opened.
3. Compare "files opened" to "total files" as you add more snapshots — pruning cost stays a metadata read, not a data scan.

**Production metric:** Iceberg's `plan.metadata-read` duration and manifest count in query plans; a growing manifest count with no compaction is the thing that eventually makes even metadata-only planning slow.

**Then run:** [Iceberg's own worked example](../lakehouse/iceberg.md) against a real table to see `metadata.json` and manifest files on disk.

---

## Parquet row-group explorer

**Open:** [parquet-row-group-explorer.html](parquet-row-group-explorer.html)

**Teaches:** predicate pushdown skips row groups using only the file footer's min/max stats — and that only works if the data was actually **sorted** on the filtered column before write. Partitioning and row-group statistics are two different pruning mechanisms; this simulator has no partitioning at all.

**Read first:** [Parquet Internals](../foundations/parquet-internals.md).

**Predict before touching the toggle:**

- Data sorted by `customer_id` before write, filter on one `customer_id` — how many of 8 row groups get opened?
- Flip to unsorted (random write order), same filter — does the count change, and why?

**What to try:**

1. Sorted mode, narrow filter — most row groups skip; only 1-2 opened.
2. Unsorted mode, same filter — nearly all row groups' min/max ranges span the whole domain, so almost none skip.
3. Compare this to a partitioned-but-unsorted file: partitioning alone would have pruned by directory, but rows **inside** the partition are still unsorted, so row-group pruning still fails at the file level.

**Production metric:** `explain()` on Spark/Trino showing `ReadSchema`/`PushedFilters` and the row-group count actually scanned vs. total.

**Then run:** [Spark's Catalyst page](../spark/optimizer.md) `explain("formatted")` example against a real Parquet table you control the write-order of.

---

## Bloom filter playground

**Open:** [bloom-filter-playground.html](bloom-filter-playground.html)

**Teaches:** a bloom filter trades a small, tunable false-positive rate for a large space saving over storing the real set — and that rate is a live function of bit-array size, hash-function count, and how full the filter already is, not a fixed constant you can ignore.

**Read first:** [Why Columnar Storage](../olap/columnar-storage.md) (skip indexes), [ClickHouse](../olap/clickhouse.md).

**Predict before adding items:**

- At a mostly-empty bit array, can a membership test for something never added ever return "possibly present"?
- What happens to the false-positive rate as you keep adding items without growing the bit array?

**What to try:**

1. Add a handful of items to a large bit array — false-positive rate stays near zero.
2. Keep adding items without resizing — watch the live false-positive rate climb per the formula, not your intuition.
3. Use "find a false positive" once the filter is fairly full — confirm a real false positive, then note it is a wasted read, never wrong data.

**Production metric:** ClickHouse/Parquet bloom-index false-positive rate in practice vs. the bytes saved — a filter sized for yesterday's cardinality silently degrades as a table grows.

---

## Consistent hashing visualizer

**Open:** [consistent-hashing-visualizer.html](consistent-hashing-visualizer.html)

**Teaches:** consistent hashing on a ring moves roughly `1/N` of keys when a node joins or leaves; naive `hash(key) % N` reshuffles nearly everything on the same event. Virtual nodes fix the load imbalance a small number of physical nodes has on a ring.

**Read first:** [Partitioning](../foundations/partitions.md) (hash partitioning), [Cassandra & ScyllaDB](../databases/cassandra.md).

**Predict before adding a node:**

- Consistent hashing, add one node to a 5-node ring — roughly what fraction of keys change owner?
- Naive mod-N hashing, same change — same fraction, or nearly all keys?

**What to try:**

1. Add/remove a node under consistent hashing — note the small, bounded number of reassigned keys.
2. Toggle naive mod-N hashing, repeat — nearly every key reassigns.
3. Raise virtual nodes per physical node — watch per-node load bars even out from lopsided to balanced.

**Production metric:** Cassandra/DynamoDB partition-count rebalance events and per-node request-rate variance after a scale-out.

---

## DynamoDB hot-key simulator

**Open:** [dynamodb-hot-key-simulator.html](dynamodb-hot-key-simulator.html)

**Teaches:** DynamoDB divides a table's total throughput evenly across its physical partitions, so a skewed logical key can throttle its one physical partition while the table's aggregate utilization looks fine and every other partition sits idle. Adding table-level throughput does not fix a key-design problem.

**Read first:** [DynamoDB](../databases/dynamodb.md).

**Predict before touching sliders:**

- Table utilization at 20% overall, one key gets 90% of traffic — throttled or fine?
- Does raising total table throughput fix that one hot partition?

**What to try:**

1. Low overall utilization, high skew — watch one partition redline while others sit idle.
2. Raise total table throughput — the hot partition's *budget* rises too, but if skew stays concentrated on one physical partition, it can still throttle.
3. Enable "shard the hot key across N suffixes" instead — the same logical entity's traffic spreads across multiple physical partitions and the throttle clears.

**Production metric:** `ThrottledRequests` and `ConsumedCapacity` per partition (not just per table) — AWS doesn't expose this directly, which is exactly why hot partitions are usually diagnosed from application-side latency first.

---

## SCD Type-2 timeline explorer

**Open:** [scd2-timeline-explorer.html](scd2-timeline-explorer.html)

**Teaches:** an SCD Type-2 dimension keeps every historical version of a row with `effective_from`/`effective_to`/`is_current`, so "what was true as of date X" is answerable — but only because every change appends a new row instead of overwriting the old one.

**Read first:** [Analytical Data Modelling](../foundations/data-modelling.md).

**Predict before recording a change:**

- After changing `plan` from "free" to "pro," does the old row disappear, or stay with a closed `effective_to`?
- Querying "as of" a date *before* the change — which row answers that, the current one or a historical one?

**What to try:**

1. Record one change — watch a new row append while the prior row's `effective_to` closes and `is_current` flips.
2. Set "as of" to a date before the change — confirm the historical row, not the current one, is selected.
3. Contrast with what an SCD Type-1 (in-place update) table would show for the same "as of" query: today's value, applied retroactively to history — the silent-rewrite failure mode.

**Production metric:** a fact-to-dimension join using `is_current = true` instead of an `effective_from`/`effective_to` range join on the fact's event time — this is the bug this simulator is built to make visible.

---

## What each sim is **not**

| Sim | Not a substitute for |
|-----|----------------------|
| Kafka partitions | Disk, ISR, `acks`, rebalances — [replication](../kafka/replication.md) |
| Spark shuffle | AQE, broadcast memory, disk spill — [lab](../labs/index.md) |
| CH ORDER BY | Too many **parts** (incident 4 shape B) — [lab](../labs/index.md) |
| Cardinality | CH `GROUP BY` memory — still a real limit, different cliff |
| Backpressure calculator | Real, bursty arrival rates — this assumes constant rates; a real recovery is messier |
| Watermark simulator | Real out-of-order jitter across thousands of keys, not two splits |
| Kafka ISR simulator | Real network partitions and split-brain timing — this is the steady-state logic, not the race conditions |
| Iceberg manifest explorer | Real catalog contention, concurrent-writer conflicts — [Iceberg](../lakehouse/iceberg.md) |
| Parquet row-group explorer | Dictionary/RLE compression ratios — this is pruning only, not storage size |
| Bloom filter playground | A real skip-index's interaction with compression and merges |
| Consistent hashing visualizer | Real network topology and rack/AZ awareness in placement |
| DynamoDB hot-key simulator | Adaptive capacity and burst credits, which soften but do not remove this failure |
| SCD2 timeline explorer | Concurrent late-arriving changes to the same entity — see [CDC](../foundations/cdc.md) ordering |

---

## Suggested afternoon

1. Kafka sim → Kafka lab → incident 1.
2. Shuffle sim → Spark lab → incident 2.
3. ORDER BY sim → CH lab → incident 4.
4. Cardinality sim → rewrite one Prom metric at work that should be an event.
5. Backpressure sim → estimate a real recovery time from a lag incident you've seen at work.
6. Watermark sim → Flink lab `stalled_watermark.py` → `check_event_time.py`.
7. Kafka ISR sim → re-read the Kafka lab's broker-stop step with ISR in mind.
8. Iceberg + Parquet explorers → pick one real table at work and predict its pruning before running `EXPLAIN`.
9. Bloom filter + consistent hashing → connect to whichever KV/wide-column store you actually operate.
10. DynamoDB + SCD2 → the two "silent until it's an incident" failure modes: a throttled partition and a fact joined to the wrong dimension version.

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

Do not run more than two or three sims in a row without a module page — they will blur into "graphs go brrr."
