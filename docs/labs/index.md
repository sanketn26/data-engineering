---
description: Predict-run-break labs for Kafka, Spark, Flink, ClickHouse, Cassandra, and Prometheus, each with an automated pass/fail check script.
---

# Labs

Friday, 4:55 PM. You clone `labs/kafka`, run `docker compose up -d`, produce a few hundred events, watch the consumer keep up, and close the laptop. Nothing broke.

Predict before you read on: if you skip the README's hypothesis section and go straight to `docker compose up`, what do you actually lose — the ten minutes it would have taken, or the lesson?

It's the lesson. Hands-on work here lives in **`labs/` at the repository root**, not under `docs/`, and each subdirectory is a small Compose (or a laptop PySpark) environment built around a README that follows **predict → run → break**, not run → shrug.

!!! tip "New: enter from the concept"
    The [practice map](../practice-map.md) pairs each core idea with a short
    simulation, this real-system lab, and an incident. If you are learning rather
    than looking up commands, start there. Each lab README now repeats its exact
    read → simulate → run → diagnose sequence.

You do not need a cloud account. You need Docker for Kafka, ClickHouse, and the optional Flink UI. Kafka/Spark scripts support Python 3.9+; the pinned PyFlink 1.18 wheel should use a supported Python 3.9–3.11 environment. About 8 GB RAM is enough; 16 GB if you run Kafka + ClickHouse at once.

Module pages still have extra exercises ([Spark labs](../spark/labs.md), [Kafka labs](../kafka/labs.md), [Flink labs](../flink/labs.md)). Start from **root `labs/`** so the Compose files stay in one place.

---

## How to run a lab

1. Read the README **hypothesis** section. Write what you expect to see.
2. `cd labs/<name> && docker compose up -d` (Spark: no cluster required).
3. Run the commands in order. Do not skip the "break" step — that is the incident.
4. Run that lab's `check_*.py` script. It makes the same claim you were asked to predict and either prints `PASS` with the measured numbers, or raises an `AssertionError` naming exactly what did not hold — you do not have to trust your own eyeballing of a chart or a log line.
5. Explain any gap between prediction and output. That gap is the lesson.
6. `docker compose down -v` so the next lab is clean.

!!! tip "Predict first"
    If you run the happy path and only then read the questions, you are executing a recipe. The academy's [study loop](../how-to-study.md) is the opposite.

!!! tip "The check script is the exit criterion"
    Every lab now has at least one `check_*.py` (`check_hot_partition.py`, `check_skew.py`, `check_order_by.py`, `check_event_time.py`) that turns the lab's central prediction into a real assertion against the running system. A green `PASS` is evidence you reproduced the failure mode; a page full of terminal output with no assertion is not the same thing.

---

## Lab map

| Lab | Directory | What you stand up | What you should feel |
|-----|-----------|-------------------|----------------------|
| **Kafka** | `labs/kafka/` | Single-broker **KRaft** Kafka | Produce/consume, lag, **hot partition** |
| **Spark** | `labs/spark/` | Local **PySpark** on the laptop | Shuffle, skew, join strategy, partition count |
| **Flink** | `labs/flink/` | Optional Compose JobManager+TaskManager; **PyFlink** conceptual path | Event time vs processing time, **idle watermark**, keyed state |
| **ClickHouse** | `labs/clickhouse/` | Official ClickHouse server | `ORDER BY` skip vs full scan, **too many parts** |
| **Cassandra** | `labs/cassandra/` | Official Cassandra server, single node | Partition key **is** the query plan, **unbounded wide partition** |
| **Time series** | `labs/time-series/` | Synthetic exporter + official Prometheus | Series count is a **product of label domains**, cardinality explosion |

Open the runnable lab directories on GitHub: [Kafka](https://github.com/sanketn26/data-engineering/tree/main/labs/kafka), [Spark](https://github.com/sanketn26/data-engineering/tree/main/labs/spark), [Flink](https://github.com/sanketn26/data-engineering/tree/main/labs/flink), [ClickHouse](https://github.com/sanketn26/data-engineering/tree/main/labs/clickhouse), [Cassandra](https://github.com/sanketn26/data-engineering/tree/main/labs/cassandra), and [Time series](https://github.com/sanketn26/data-engineering/tree/main/labs/time-series).

If you are browsing the documentation site, open the GitHub tree or your local clone — Compose files are **not** inside `docs/`.

---

## Kafka (`labs/kafka`)

**Compose:** Apache Kafka 3.7, KRaft combined broker+controller, port `9092`.

**You will:**

1. Create `user-events` with 6 partitions.
2. Produce the System A JSON events (SaaS analytics).
3. Consume in a group; watch offsets.
4. **Predict** lag with a slow consumer; measure with `kafka-consumer-groups`.
5. **Break:** produce with a hot key (`customer_id=cust_0042` at 80%). Watch **one** partition's lag climb. Extra consumers do not split that partition.

**Check:** `python check_hot_partition.py` asserts one partition holds a large majority of the traffic instead of asking you to eyeball it.

Pairs with [partitions](../kafka/partitions.md), [incident 1](../incidents/index.md), [partition simulator](../simulations/kafka-partitions.html).

---

## Spark (`labs/spark`)

**No Compose.** `pip install pyspark` and run the scripts in the README (or the longer set in [docs/spark/labs.md](../spark/labs.md)). Spark UI at `http://localhost:4040`.

**You will:**

1. `groupBy` and find shuffle read/write in the UI.
2. Build an 80% skewed `customer_id` and watch one task dominate.
3. Compare sort-merge vs broadcast join times.
4. Sweep `spark.sql.shuffle.partitions`.

**Break:** disable AQE and broadcast; join the skewed frame; predict which task dies or crawls.

**Check:** `python check_skew.py` reads the per-key row counts back from the aggregation output and asserts uniform mode is actually uniform and skew mode is actually skewed.

Pairs with [shuffle](../spark/shuffle.md), [incident 2](../incidents/index.md), [shuffle simulator](../simulations/spark-shuffle.html).

---

## Flink (`labs/flink`)

**Two tracks:**

- **Conceptual / PyFlink on the laptop** — enough to see watermarks and keyed state without a cluster.
- **Compose** (JobManager `8081` + TaskManager) if you want the Flink UI. Optional Kafka from `labs/kafka` on the same Docker network is extra credit, not required.

**You will:**

1. Run the same 20 events through processing-time vs event-time windows (timestamps 5 minutes in the past).
2. **Predict** which window they land in.
3. **Break:** one idle source split stalls the downstream minimum watermark so **no window output** — [incident 3](../incidents/index.md). A stale record on an active split is a different failure and does not move a max-based watermark backwards.
4. Keyed failed-login counter with `ValueState`.

**Check:** `python stalled_watermark.py` self-asserts the idleness-exclusion claim; `python check_event_time.py` collects both jobs' actual window boundaries and asserts event-time buckets land ~5 minutes in the past while processing-time buckets land ~now.

Pairs with [time](../flink/time.md), [windows](../flink/windows.md), [state](../flink/state.md).

---

## ClickHouse (`labs/clickhouse`)

**Compose:** `clickhouse/clickhouse-server`, HTTP `8123`, native `9000`.

**You will:**

1. Load the same SaaS events into two tables: `ORDER BY (customer_id, timestamp)` vs `ORDER BY (timestamp, customer_id)`.
2. **Predict** `marks` / rows read for a tenant dashboard query vs a global time query.
3. Compare with `EXPLAIN` and query log.
4. **Break:** insert **one row per INSERT** in a loop; watch `system.parts` explode and the same SELECT slow down — [incident 4](../incidents/index.md).

**Check:** `python check_order_by.py` reads ClickHouse's own `read_rows` for the tenant query on both tables and asserts the tenant-first design reads meaningfully fewer rows.

Pairs with [ClickHouse](../olap/clickhouse.md), [ORDER BY explorer](../simulations/clickhouse-order-by.html).

---

## Cassandra (`labs/cassandra`)

**Compose:** official `cassandra:4.1`, native protocol `9042`. Slower to boot than the other labs — 30-60s before the healthcheck is green.

**You will:**

1. Load the same SaaS events into two tables: `events_by_service` (partition key `service`, 4 values) vs `events_by_customer_day` (partition key `(customer_id, day_bucket)`).
2. **Predict** which design produces the larger single partition once 80% of traffic hits one service and one customer.
3. Inspect partition sizes with `nodetool tablehistograms`; both queries are fast single-partition reads regardless of which design is a bad idea.
4. **Break (optional):** push `--hot-ratio` and row count higher and watch `nodetool tablehistograms` show a dramatically larger max partition size — real, but slow on a laptop; the check script proves the same point instantly.

**Check:** `python check_wide_partition.py` queries Cassandra's own row counts and asserts the low-cardinality partition key absorbs a large majority of all rows, while the compound key bounds the same hot customer to one partition.

Pairs with [Cassandra & ScyllaDB](../databases/cassandra.md), [consistent hashing visualizer](../simulations/consistent-hashing-visualizer.html).

---

## Time series (`labs/time-series`)

**Compose:** a dependency-free synthetic Prometheus exporter + official `prom/prometheus`.

**You will:**

1. Scrape a metric with 4 bounded labels (service, region, status_code) — 60 series.
2. **Predict** the series count `prometheus_tsdb_head_series` should report (including Prometheus's own several hundred self-monitoring series).
3. **Break:** add a `user_id` label with 10,000 values and watch `prometheus_tsdb_head_series` jump by roughly that multiplier — the same arithmetic as the [cardinality calculator](../simulations/cardinality-calculator.html).

**Check:** `python3 check_cardinality.py --expect-min <N> --expect-max <M>` reads Prometheus's own head-series count via its HTTP API for whichever scenario you just ran.

Pairs with [Cardinality](../time-series/cardinality.md), [cardinality calculator](../simulations/cardinality-calculator.html).

---

## Shared dataset (all labs)

SaaS analytics event (System A):

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

Use this shape so Kafka, Spark, Flink, ClickHouse, and Cassandra are comparable. When you "hot key" or "skew join," it is **`cust_0042`** — the same whale as [analytics architecture](../architectures/analytics-platform.md). The time-series lab uses a synthetic metric instead, since Prometheus labels are not this event shape.

---

## Prerequisites

| Tool | Why |
|------|-----|
| Docker + Compose v2 | Kafka, ClickHouse, Cassandra, Prometheus, Flink UI |
| Python 3.9+ | `kafka-python` or `confluent-kafka`, PySpark, optional PyFlink, `cassandra-driver` |
| 8 GB RAM | 16 GB if stacking Compose files; Cassandra alone wants ~2 GB for its JVM |
| Browser | Spark UI 4040, Flink 8081, CH HTTP, Prometheus 9090 |

Windows: use WSL2. Apple Silicon: the images used in `labs/*/docker-compose.yml` publish `arm64` or emulate; Kafka/CH/Cassandra/Prometheus official images are fine on M-series. `cassandra-driver`'s source build can fail on very new Python versions — force the prebuilt wheel with `pip install --only-binary=:all: cassandra-driver` if that happens.

---

## What is **not** a Compose lab here

Iceberg time travel, Trino federation, Timescale continuous aggregates, and Neo4j rings are taught in their **modules** with SQL/Cypher you can run if you already have those systems. They are not stubbed as "coming soon" on this page — they are **not in `labs/` yet**. Use [simulations](../simulations/index.md) and [incidents](../incidents/index.md) (Iceberg snapshots, Trino OOM) as tabletop drills until you add Compose.

Do not wait for those to run Kafka/Spark/Flink/CH/Cassandra/Prometheus. Those six are the data plane (plus one metrics plane).

---

## After a lab

- Write one sentence: *the metric that would have paged me*.
- Open the matching [incident](../incidents/index.md) and do the hypothesis pause **without** looking at your lab notes first.
- Connect the observation to one architecture page; that step turns a useful
  experiment into a reusable design insight.

Root map next to Compose: `labs/README.md`.

---

## Suggested order (one weekend)

| When | Lab | Stop when you can say |
|------|-----|------------------------|
| Sat morning | Kafka | "More consumers will not save partition 7" |
| Sat midday | Cassandra | "The partition key is the query plan" |
| Sat afternoon | ClickHouse | "I can predict marks read from `ORDER BY`" |
| Sun morning | Spark | "I know which UI column is skew" |
| Sun midday | Time series | "Cost is #series, not #samples" |
| Sun afternoon | Flink | "Windows close on watermarks, not on hope" |

Then sit the matching [incidents](../incidents/index.md) **without** the lab notes open.

---

## Troubleshooting

| Symptom | Likely |
|---------|--------|
| Kafka client `Connection refused` | Compose not healthy; advertised listener is `localhost:9092` — do not use the container hostname from the host |
| PySpark `Java gateway` | Install JDK 11/17; `JAVA_HOME` |
| ClickHouse `too many parts` immediately | You are on the **break** step — that is success |
| Flink no output on event-time lab | Watermark never passed window end; read the README dummy-event note |
| Cassandra `cqlsh`/driver connection refused | Its healthcheck takes 30-60s — wait for `docker compose ps` to show `healthy`, not just `Up` |
| `prometheus_tsdb_head_series` query returns no data | Prometheus must scrape **itself**, not just the exporter — check `prometheus.yml` has a `prometheus` job targeting `localhost:9090` |
| Port 9092 / 8123 / 8081 / 9042 / 9090 busy | Another lab still up; `docker compose down` |

Do not change image tags casually. The Compose files pin versions that match the READMEs.

---

## Pairing with simulations

Do the HTML **before** Compose if you are new to the concept; after Compose if you want to check the mental model. Same whale key `cust_0042` everywhere so the stories compose.
