# Choosing a TSDB

**Slack thread, Monday.** "Should we standardize on InfluxDB, Timescale, or just remote_write everything into VictoriaMetrics?" Forty replies in, nobody has mentioned what any of the three teams actually write or query — one pages on service SLOs, one joins device readings to a customer table, one wants `user_id`-level latency charts.

Predict before you read on: is there one engine that serves all three well, or does the right answer depend entirely on the access pattern each team actually has?

There is no one right logo — the market is a pile of them — and the job is an access pattern: scrape-and-alert, IoT `{timestamp, device_id, sensor, value}`, or high-cardinality events that only look like metrics because they have a timestamp. Pick the engine from **how you write and read**, not from who sponsored the conference.

---

## Use case

Three questions this academy actually asks:

1. **SLO / paging:** `rate(http_requests_total{service="checkout"}[5m])` at 15 s scrape, 200 services, bounded labels.
2. **IoT:** 10 M devices, 30 s temperature, latest-value + 24 h chart + 1 y trend, SQL joins to a `customers` table.
3. **Product analytics / observability events:** hundreds of millions of events/day, `user_id` on the row, dashboard GROUP BY endpoint for one tenant.

One system that does all three well is a slide, not a deployment.

---

## Why this is hard

TSDBs share append-heavy writes and time predicates. They disagree on:

- **identity** (labels vs columns vs measurements);
- **query language** (PromQL vs SQL vs Flux);
- **pull vs push**;
- **how badly high cardinality hurts**;
- **retention / downsampling story**;
- **joins**.

ClickHouse is not a TSDB. Teams still use it as one when the access pattern is OLAP-shaped. Pretending it is Prometheus is how you lose alerting. Pretending Prometheus is ClickHouse is how you lose `user_id`.

---

## Intuition

```text
Need PromQL + Alertmanager + Grafana scrape?
    → Prometheus, scale-out with VictoriaMetrics / Mimir

Need SQL, devices, joins, moderate rate?
    → Timescale (Postgres you already run)

Need IoT push, flexible tags, you accept the license?
    → Influx — or skip to CH/Timescale

Need high-card columns, heavy GROUP BY, batched ingest?
    → ClickHouse (or Pinot if user-facing QPS)
```

If the sentence contains **label** and **user_id**, you already left the Prom family. If it contains **scrape** and **page me**, you already left ClickHouse as the primary.

---

## Internals by system

### Prometheus

**Write path:** pull. Prometheus scrapes `/metrics`. Short local retention (often 15 d). WAL + 2 h blocks + compaction.

**Read path:** PromQL range vectors, not SQL `GROUP BY`. Inverted index on labels. Memory ~ **active series**.

**Strength:** Kubernetes, exporters, Alertmanager, Grafana, a dialect SREs can write at 03:00.

**Weakness:** [cardinality](cardinality.md); long-term storage is **someone else** (remote_write); not a device join engine; not 300k IoT push/s without a gateway story.

```text
rate(http_requests_total{service="checkout", status=~"5.."}[5m])
histogram_quantile(0.95, sum by (le) (rate(http_request_duration_seconds_bucket{service="checkout"}[5m])))
```

**Do not:** `user_id` labels; 10 M `device_id` series on one Prom; 13-month local disk as the plan.

### VictoriaMetrics

Prometheus-compatible (PromQL / MetricsQL, remote_write, exporters). Better compression, cheaper high-churn, cluster mode.

**Use:** the same access pattern as Prometheus when Prom’s single-node series/disk ceiling is the problem.

**Still:** labels are identity. `user_id` remains wrong. MetricsQL is not SQL joins to billing.

### TimescaleDB

PostgreSQL + hypertables (chunk by time, optionally space by `device_id`) + compression + continuous aggregates + retention jobs.

```sql
CREATE TABLE readings (
    ts timestamptz NOT NULL,
    device_id text NOT NULL,
    sensor text NOT NULL,
    value double precision NOT NULL
);
SELECT create_hypertable('readings', 'ts',
    chunk_time_interval => INTERVAL '1 day');
SELECT add_dimension('readings', 'device_id', number_partitions => 16);

ALTER TABLE readings SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'device_id, sensor',
    timescaledb.compress_orderby = 'ts DESC'
);
```

**Strength:** SQL, JOINs to `customers`, BI tools, transactions for **small** metadata, `time_bucket_gapfill`.

**Weakness:** it is still Postgres. 3×10⁵ inserts/s needs batching, fewer indexes, compression, and honest hardware. Vacuum/chunk world. Not PromQL. Not 200 ms at 8k QPS user-facing.

**Access pattern:** IoT / app metrics where the team is SQL-native and rates are “serious but not ClickHouse-or-die.”

### InfluxDB

Push, tag/field model, good at irregular IoT. InfluxQL / Flux. Cardinality on **tags** is the same class of footgun as Prom labels (`user_id` as tag).

License: v3 BSL / product split — treat as a **vendor** decision, not a default open-source TSDB. For new greenfield, many teams pick Timescale or ClickHouse instead of betting the license.

**Access pattern:** IoT if already in-stack and tags are bounded. Not the default in this academy.

### ClickHouse as a TSDB

MergeTree, `ORDER BY (device_id, sensor, ts)`, batched inserts, MVs for [downsampling](downsampling.md), TTL, SQL including `quantile`, `argMax`, JOINs to small dims.

```sql
CREATE TABLE readings
(
    ts        DateTime64(3),
    device_id String,
    sensor    LowCardinality(String),
    value     Float64
)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(ts)
ORDER BY (device_id, sensor, ts)
TTL ts + INTERVAL 7 DAY;

SELECT device_id, argMax(value, ts)
FROM readings
WHERE sensor = 'temperature'
GROUP BY device_id;
```

**Strength:** high-cardinality **columns**, scan speed, rollups, one engine for events **and** measurements if you are disciplined.

**Weakness:** no PromQL/Alertmanager native; small inserts die (`too many parts`); you build recording/alerting; not scrape; `FINAL` habits from ReplacingMergeTree.

**Access pattern:** IoT + observability **events**; product analytics; anything that would have been “Prom + user_id.”

Pinot if the same events are **user-facing high QPS** ([Pinot](../olap/pinot.md)).

---

## Compare by access pattern

| Pattern | Prom / VM | Timescale | Influx | ClickHouse |
|---------|-----------|-----------|--------|------------|
| Scrape, page, Grafana SLO | **Yes** | Awkward | No | Build it yourself |
| 10 M devices, SQL, joins | Series bomb | **Yes, if batched** | Tags bounded | **Yes, if batched** |
| `user_id` on points | **No** | Columns OK | Tags **no** | **Columns OK** |
| 200 ms, 5k QPS tenant UI | No | No | Maybe not | Maybe + cache / **Pinot** |
| 1-year cheap trend | remote + downsample | cagg + compression | retention policies | MV + TTL |
| Ad-hoc JOIN Iceberg | No | No | No | CH copy or **Trino** |
| Counter `rate()` | Native | You write SQL | Derivative fns | You write SQL / MV |
| Ops complexity | Prom easy; VM cluster medium | Postgres you know | Vendor | Parts, Keeper |

Deeper OLAP vs TS: [TSDB vs OLAP](../comparisons/tsdb-vs-olap.md).

---

## How

**Pattern 1 — services:**

```text
app /metrics → Prometheus (2–4 weeks)
            → remote_write VictoriaMetrics (1 year, downsampled)
Alertmanager → PagerDuty
Grafana → PromQL
```

Keep label set reviewed. Recording rules for expensive dashboards.

**Pattern 2 — IoT SQL:**

```text
device → Kafka → batch 10k → Timescale hypertable
                           → or ClickHouse Kafka Engine / MV
cagg / MV 1m, 1h
TTL raw 7d
Grafana SQL or a thin API
```

Join:

```sql
SELECT c.name, avg(r.value)
FROM readings r
JOIN devices d ON d.id = r.device_id
JOIN customers c ON c.id = d.customer_id
WHERE r.ts > now() - interval '1 day'
  AND r.sensor = 'temperature'
GROUP BY c.name;
```

Timescale: natural. ClickHouse: `JOIN` the dim table (ReplacingMergeTree + `argMax`) **or** denormalize `customer_id` onto the reading at ingest (usually better).

**Pattern 3 — events with user_id:**

```text
app → Kafka → ClickHouse ORDER BY (customer_id, ts)
           → optional Pinot for the in-app tiles
Prometheus stays on service SLOs only
```

Do not remote_write these events into Prom.

---

## Gotchas

!!! production-gotcha "One cluster for metrics and events"
    Prom remote_write of unbounded-label events. Or CH used as scrape TSDB with one-row inserts from 2,000 exporters.

!!! production-gotcha "PromQL in Timescale via a compatibility layer as the strategy"
    You will debug two dialects and still own Postgres. Pick one primary language per workload.

!!! production-gotcha "Influx tags = Prom labels"
    Same explosion, different logo.

!!! production-gotcha "ClickHouse Kafka engine as the only HA ingest"
    It works until it does not (offsets, blocks, duplicates). Have a replay from Kafka and idempotent parts / Replacing by `(device_id, ts)` if needed.

!!! production-gotcha "Grafana against raw 10 M series"
    The TSDB choice does not matter; [downsampling](downsampling.md) does.

---

## Failure modes

| Engine | Classic death |
|--------|----------------|
| Prometheus | `head_series` / scrape payload |
| VM | You treated it as infinite Prom and added `user_id` |
| Timescale | Row-by-row insert, too many indexes, uncompressed chunks, JOIN exploding |
| Influx | TSI cardinality, license/ops surprise |
| ClickHouse | too many parts, wrong `ORDER BY (ts)`, `GROUP BY user_id` without filter, no TTL |

Debugging pointers: Prom `head_series`; Timescale `EXPLAIN` chunk exclusion; CH `system.parts` + `EXPLAIN indexes = 1` + `query_log.read_bytes`; VM `vm_rows_inserted`.

---

## Debugging

Ask four questions of any slow tile:

1. **How many series / groups?** (`count` of identities in the range.)
2. **Which clock?** event vs scrape ([time semantics](time-semantics.md)).
3. **Which layer?** raw vs 1m vs 1h ([downsampling](downsampling.md)).
4. **Which engine is actually serving?** Grafana datasource mixups (Prom vs SQL) are frequent.

ClickHouse:

```sql
SELECT read_rows, formatReadableSize(read_bytes), query
FROM system.query_log
WHERE type = 'QueryFinish' AND query LIKE '%readings%'
ORDER BY event_time DESC LIMIT 10;
```

Prometheus: query log + `count({__name__="http_requests_total"})`.

Timescale: `EXPLAIN (ANALYZE)` should show **chunks excluded**. If not, the time predicate is wrapped in a function.

---

## Scale 10× / 100× / 1000×

| | Prom/VM | Timescale | ClickHouse |
|--|---------|-----------|------------|
| **10× samples** | Disk / remote_write | Bigger chunks, compression | More parts/merges, still one node often |
| **10× series** | The real Prom cliff | Space partitioning | Sort key / partitions |
| **100×** | Sharded VM/Mimir | Citus-like / split | Shards, MV, TTL |
| **1000×** | Drop dimensions | Not the IoT 10 M×year raw store | Coarsen identity; CH is serving not the lake |

At 1000×, **Iceberg is the lake**; TSDBs/CH are hot tiers. Trino for humans on cold data.

---

## Trade-offs

| Choosing Prom-family | Choosing SQL-family (Timescale/CH) |
|----------------------|-------------------------------------|
| Alerting ecosystem | Joins, BI, high-card columns |
| Pull, service discovery | Push pipelines you own |
| Series identity | Row identity |
| Weak long-term unless you add it | You will build rollups anyway |

| Choosing CH over Timescale | Choosing Timescale over CH |
|----------------------------|----------------------------|
| Ingest + scan ceiling | Postgres tooling, TX, FK to dims |
| Ops is CH | Ops is PG |
| Mutations painful | Mutations familiar (still expensive at IoT volume) |

---

## Alternatives

| Need | Also consider |
|------|----------------|
| Managed Prom | Grafana Cloud, Amazon AMP, Google GMP |
| Managed CH | ClickHouse Cloud, Altinity |
| Wide events + search | OpenSearch — not a metrics TSDB |
| User-facing QPS | [Pinot](../olap/pinot.md) |
| Lake SQL | [Trino](../query-engines/trino.md) + Iceberg |

---

## Apply

Write the access pattern in one sentence. If it has **scrape and page**, start Prom/VM. If it has **device_id and SQL**, start Timescale or CH (CH if you already need it for events or the rate is ugly). If it has **user_id and dashboards**, CH/Pinot, **not** labels.

Then implement [downsampling](downsampling.md) before the disk is full. Cardinality review is a merge blocker for Prom PRs.

Related: [IoT](../architectures/iot.md), [observability](../architectures/observability.md), [ClickHouse](../olap/clickhouse.md).

---

## Exercise

Company today: Prometheus (15 d) for k8s; Postgres 2 TB of IoT rows (`readings` with indexes on `(device_id, ts)`), inserts 8k/s and climbing to 80k/s; product wants per-user API latency in Grafana **using the existing Prom**.

Pick a 12-month architecture for (a) paging, (b) IoT, (c) per-user latency. Name what you **stop** doing.

??? success "Answer"
    **(a) Paging:** keep Prometheus (HA pair) + Alertmanager. Add VictoriaMetrics (or Mimir) remote_write for >15 d and recording rules. Do **not** dump IoT or per-user into it.

    **(b) IoT:** stop using vanilla Postgres as the hot path at 80k inserts/s without Timescale (batch, hypertable, compression, drop extra indexes) **or** move ingest to ClickHouse with batched Kafka. Given 80k/s and likely growth, **ClickHouse** (or Timescale **only** if the team is PG-native and benchmarks batch insert + compression on **their** hardware). Raw TTL + 1m/1h cagg/MV. Postgres remains **devices/customers** OLTP, not 80k/s readings.

    **(c) Per-user latency:** **stop** the Prom plan. Kafka events → ClickHouse `ORDER BY (customer_id, timestamp)` or similar; Grafana SQL or a product API. Optional Pinot if this becomes customer-facing QPS. Prom stays on `{service, endpoint}` SLOs.

    Stop: unbounded labels; `INSERT` one IoT row per HTTP; 13-month Postgres heap of readings; Grafana on Prom for user-level tiles.
