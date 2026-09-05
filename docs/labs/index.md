# Labs

Hands-on work lives in **`labs/` at the repository root**, not under `docs/`. Each subdirectory is a small Compose (or a laptop PySpark) environment plus a README that follows **predict → run → break**.

You do not need a cloud account. You need Docker for Kafka, ClickHouse, and the optional Flink UI. Kafka/Spark scripts support Python 3.9+; the pinned PyFlink 1.18 wheel should use a supported Python 3.9–3.11 environment. About 8 GB RAM is enough; 16 GB if you run Kafka + ClickHouse at once.

Module pages still have extra exercises ([Spark labs](../spark/labs.md), [Kafka labs](../kafka/labs.md), [Flink labs](../flink/labs.md)). Start from **root `labs/`** so the Compose files stay in one place.

---

## How to run a lab

1. Read the README **hypothesis** section. Write what you expect to see.
2. `cd labs/<name> && docker compose up -d` (Spark: no cluster required).
3. Run the commands in order. Do not skip the "break" step — that is the incident.
4. Explain any gap between prediction and output. That gap is the lesson.
5. `docker compose down -v` so the next lab is clean.

!!! tip "Predict first"
    If you run the happy path and only then read the questions, you are executing a recipe. The academy's [study loop](../how-to-study.md) is the opposite.

---

## Lab map

| Lab | Directory | What you stand up | What you should feel |
|-----|-----------|-------------------|----------------------|
| **Kafka** | `labs/kafka/` | Single-broker **KRaft** Kafka | Produce/consume, lag, **hot partition** |
| **Spark** | `labs/spark/` | Local **PySpark** on the laptop | Shuffle, skew, join strategy, partition count |
| **Flink** | `labs/flink/` | Optional Compose JobManager+TaskManager; **PyFlink** conceptual path | Event time vs processing time, **idle watermark**, keyed state |
| **ClickHouse** | `labs/clickhouse/` | Official ClickHouse server | `ORDER BY` skip vs full scan, **too many parts** |

Open the runnable lab directories on GitHub: [Kafka](https://github.com/sanketn26/data-engineering/tree/main/labs/kafka), [Spark](https://github.com/sanketn26/data-engineering/tree/main/labs/spark), [Flink](https://github.com/sanketn26/data-engineering/tree/main/labs/flink), and [ClickHouse](https://github.com/sanketn26/data-engineering/tree/main/labs/clickhouse).

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

Pairs with [time](../flink/time.md), [windows](../flink/windows.md), [state](../flink/state.md).

---

## ClickHouse (`labs/clickhouse`)

**Compose:** `clickhouse/clickhouse-server`, HTTP `8123`, native `9000`.

**You will:**

1. Load the same SaaS events into two tables: `ORDER BY (customer_id, timestamp)` vs `ORDER BY (timestamp, customer_id)`.
2. **Predict** `marks` / rows read for a tenant dashboard query vs a global time query.
3. Compare with `EXPLAIN` and query log.
4. **Break:** insert **one row per INSERT** in a loop; watch `system.parts` explode and the same SELECT slow down — [incident 4](../incidents/index.md).

Pairs with [ClickHouse](../olap/clickhouse.md), [ORDER BY explorer](../simulations/clickhouse-order-by.html).

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

Use this shape so Kafka, Spark, Flink, and ClickHouse are comparable. When you "hot key" or "skew join," it is **`cust_0042`** — the same whale as [analytics architecture](../architectures/analytics-platform.md).

---

## Prerequisites

| Tool | Why |
|------|-----|
| Docker + Compose v2 | Kafka, ClickHouse, Flink UI |
| Python 3.9+ | `kafka-python` or `confluent-kafka`, PySpark, optional PyFlink |
| 8 GB RAM | 16 GB if stacking Compose files |
| Browser | Spark UI 4040, Flink 8081, CH HTTP |

Windows: use WSL2. Apple Silicon: the images used in `labs/*/docker-compose.yml` publish `arm64` or emulate; Kafka/CH official images are fine on M-series.

---

## What is **not** a Compose lab here

Iceberg time travel, Trino federation, Timescale continuous aggregates, and Neo4j rings are taught in their **modules** with SQL/Cypher you can run if you already have those systems. They are not stubbed as "coming soon" on this page — they are **not in `labs/` yet**. Use [simulations](../simulations/index.md) and [incidents](../incidents/index.md) (Iceberg snapshots, Trino OOM) as tabletop drills until you add Compose.

Do not wait for those to run Kafka/Spark/Flink/CH. Those four are the data plane.

---

## After a lab

- Write one sentence: *the metric that would have paged me*.
- Open the matching [incident](../incidents/index.md) and do the hypothesis pause **without** looking at your lab notes first.
- If you cannot connect the lab to an architecture page, the lab was tourism.

Root map next to Compose: `labs/README.md`.

---

## Suggested order (one weekend)

| When | Lab | Stop when you can say |
|------|-----|------------------------|
| Sat morning | Kafka | "More consumers will not save partition 7" |
| Sat afternoon | ClickHouse | "I can predict marks read from `ORDER BY`" |
| Sun morning | Spark | "I know which UI column is skew" |
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
| Port 9092 / 8123 / 8081 busy | Another lab still up; `docker compose down` |

Do not change image tags casually. The Compose files pin versions that match the READMEs.

---

## Pairing with simulations

Do the HTML **before** Compose if you are new to the concept; after Compose if you want to check the mental model. Same whale key `cust_0042` everywhere so the stories compose.
