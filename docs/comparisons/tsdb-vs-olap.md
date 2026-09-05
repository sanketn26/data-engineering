---
description: Why adding a high-cardinality label like user_id to Prometheus is a cardinality bomb, and when ClickHouse or Pinot is the right tool instead.
---

# TSDB vs OLAP

A pull request adds `user_id` as a new Prometheus label so support can filter latency by customer. It passes review — it's just a label, and Prometheus already stores time series. Two weeks later the TSDB's memory usage triples and queries start timing out. Predict before you read on: was the label the mistake, or was Prometheus the wrong tool for that question regardless of the label?

Prometheus was the wrong tool for that question: time on the x-axis does not mean "use a TSDB." A TSDB is a **data model** (metric + labels + samples) with a **cardinality contract**. An OLAP engine is a **wide table** with a sparse or inverted index. Mixing them is the most common observability self-own: `user_id` as a Prometheus label, or Prometheus as a product-analytics warehouse.

Related: [cardinality](../time-series/cardinality.md), [TSDBs](../time-series/tsdbs.md), [observability](../architectures/observability.md), [IoT](../architectures/iot.md), [cardinality calculator](../simulations/cardinality-calculator.html).

---

## The problem each solves

**TSDB** (Prometheus, VictoriaMetrics, Influx, Timescale in "metrics" mode): "rate of `http_requests_total` for `service=api` over 6 hours, 1-minute steps." Alerting is native. Cardinality must stay **bounded**.

**OLAP** (ClickHouse, Pinot): "count users per region per plan for 90 days" or "p95 latency per `user_id`." High cardinality columns are normal. Alerting is **not** native.

```
TSDB:  metric{label=value} sample
OLAP:  row with many typed columns
```

---

## Cardinality (the split)

Prometheus-style systems want **< ~100k** active series per metric as a *comfort* zone (implementations vary; the cliff is real). `user_id`, `request_id`, `device_id` × 10M are not labels. They are **columns**.

ClickHouse: 100M distinct `user_id` values is a compression and `GROUP BY` problem, not an index explosion of the same kind. Still not free — but it is the right class of engine.

Timescale sits in the middle: SQL + hypertables, better at joins, weaker than CH at huge ingest, friendlier than Prom at arbitrary SQL.

---

## Data model and queries

**TSDB**

```
http_requests_total{service="api",code="500"}
PromQL: rate(http_requests_total{service="api"}[5m])
```

**OLAP**

```sql
CREATE TABLE events (
    timestamp DateTime,
    customer_id UInt64,
    user_id UInt64,
    region LowCardinality(String),
    latency_ms Float64,
    status_code UInt16
) ENGINE = MergeTree
ORDER BY (customer_id, timestamp);
```

If your "metric" has ten unbounded dimensions, you already have events.

---

## Downsampling, ingest, alerting

| | TSDB | OLAP |
|--|------|------|
| Pyramid (raw → 1m → 1h) | Native (recording rules, CMAGs, VM rollup) | DIY: MVs, Flink, Airflow |
| Ingest | Pull scrape or specialised write | Batched inserts |
| Alerting | Alertmanager et al. | External probes |
| High-card events | **No** | Yes |
| Joins to business tables | Weak (except Timescale/SQL) | CH joins; or Trino |

IoT [architecture](../architectures/iot.md) often looks TSDB-shaped (device, sensor, value, time) but **device_id cardinality** pushes you to CH or Timescale, not Prometheus.

---

## Choose a TSDB when

- Infra and **app RED metrics** with bounded labels (`service`, `code`, `pod`).
- You need **alerting** as a product.
- Grafana "is the CPU high?"
- Cardinality budget is explicit and enforced.

## Choose ClickHouse (OLAP) for time-shaped data when

- Per-user, per-request, per-device, per-tenant **events**.
- SQL funnels, joins, wide rows.
- [SaaS analytics](../architectures/analytics-platform.md), [observability logs/traces](../architectures/observability.md), [fraud features](../architectures/fraud.md).

## Choose Timescale when

- You want **Postgres** (joins to OLTP-ish tables, extensions, ops).
- Rate is moderate (measure; tens of k samples/s is a discussion, 333k/s fleet is a CH discussion).
- Team is SQL-native and small.

## Choose neither when

- Dashboards that are really **product analytics** at huge QPS → CH or Pinot, not Prom.
- Ad-hoc lake joins → Trino.
- You were about to store logs in Prometheus.

!!! danger "Anti-pattern"
    `http_request_latency_ms{user_id="..."}`. Use a histogram with bounded labels, and put per-user latency in ClickHouse. Run the [cardinality calculator](../simulations/cardinality-calculator.html) with a `user_id` dimension.

---

## Running example: observability split

| Signal | Store | Why |
|--------|-------|-----|
| `container_cpu_seconds` | Prometheus/VM | Bounded, alerts |
| Request logs, traces, `user_id` | ClickHouse | Cardinality, SQL |
| 1 year of rolled-up RED | Prom long-term or CH/Iceberg | Cost; don't keep 1 Hz raw |
| Security audit rows | CH / lake | Not Prom |

**IoT:** 10M `device_id` series → CH or Timescale + downsample. Prometheus for the **ingest cluster's** CPU, not for each device.

---

## Decision checklist

```
Bounded labels + alerts?               → TSDB
High-card events + SQL?                → ClickHouse
Postgres-shaped, moderate rate?        → Timescale
Last-2s user-facing tiles, huge QPS?   → Pinot (OLAP serving)
Lake + federation?                     → Trino
Time on x-axis only?                   → Not a reason
```

---

## Summary

```
Infrastructure monitoring + alerting?         → TSDB
High-cardinality event analytics?             → ClickHouse
SQL time series, moderate scale?              → Timescale or ClickHouse
Sub-second fresh fixed dashboards?            → Pinot
Full flexibility + historical depth?          → ClickHouse + Iceberg
```

---

## Worked cardinality

Metric `http_requests_total` with labels:

- `service` 50
- `code` 8
- `method` 4
- `pod` 200 (rolling)

Series ≈ 50×8×4×200 = **320k** — already spicy for Prometheus, OK-ish for VM with care.

Add `user_id` 100,000: **32 billion** series. Not a TSDB. Put latency in CH with `user_id` as a column; keep Prom for `service,code`.

[Calculator](../simulations/cardinality-calculator.html): do this live in a review when someone "just adds a label."

---

## Timescale vs CH vs Prom (IoT)

[IoT](../architectures/iot.md) 333k samples/s, 10M `device_id`:

- Prom: no.
- Timescale: maybe with hypertables, compression, continuous aggs — **measure** WAL and chunk count. Joins to `assets` PG are the reason to try.
- CH: default at this card and rate.

If you already run Timescale for 10k devices and it is boring, do not migrate for a blog. Migrate when chunks, bloat, or ingest CPU say so.

---

## Alerting tax

PromQL + Alertmanager is a **decade** of runbooks. CH alerting is "run a query every 30 s and page." You will rebuild silence, grouping, inhibit, HA badly.

Keep alerts on a TSDB even if **events** live in CH. Export **service-level** rollups from CH/Flink **into** Prom (bounded labels) for error-rate alerts. Do not Prom-query CH.

---

## "But Grafana can query ClickHouse"

Yes. That does not make CH a TSDB. Recording rules, scrape discovery, `up` metric, and cardinality control are Prom's product. Grafana is a **client**.

---

## Review script

1. What are the label/column dimensions? Product of values?
2. Alerting or SQL funnels?
3. Bounded infra vs events?
4. If both, **two** stores.
5. Reject "everything is time series."

---

## Downsampling is not optional

TSDBs assume you will **not** keep 15s CPU forever. Recording rules / CMAGs / VM rollup are the product.

OLAP: you must **build** the pyramid (Flink, MVs, Airflow). CH TTL on raw without a rollup is how year charts die.

If your only store is Prom and you need 13 months of 1s data, you will cry. If your only store is CH raw 1 Hz IoT for a year, you will pay.

---

## Ingest models

Prom **pulls** scrapes. That is correct for `up` and service discovery. It is wrong for 333k device pushes — use a write path (VM remote_write, CH insert, Kafka).

CH **pushes** inserts. You own batching.

Mixing: apps push events to Kafka → CH; apps expose `/metrics` for Prom. Two channels on purpose.

---

## Query languages

PromQL is built for rates, increases, histograms. SQL is built for joins and funnels. Translating PromQL to SQL is possible and ugly. Translating funnels to PromQL is how you get `user_id` labels.

Pick the language of the **question**. Infrastructure people should not be forced into CH for `rate(cpu)`. Analysts should not be forced into PromQL for GMV.

Timescale: SQL for both, with the scale caveats above.

---

## Histogram vs events

RED metrics: use Prom histograms (`le` buckets) with bounded labels. Per-user latency: CH events. **Both.** The histogram answers SLO burn. The events answer "which customers."

---

## Choose-neither recap

Pinot for ultra-fresh high-QPS tiles. Trino for lake federation. Postgres for small. The x-axis being time never selected an engine by itself.

---

## FAQ

**VictoriaMetrics vs Prometheus?** VM often survives higher cardinality and ingest. The **label contract** remains. VM is not CH for `user_id` events.

**Influx?** Fine for small; not the academy default.

**Can CH replace Prom for `up`?** You will rebuild scrape discovery and Alertmanager badly. Don't.

**Can Prom replace CH for logs?** No.

**OpenTelemetry?** A **telemetry standard**. It does not pick Prom vs CH. Export metrics to a TSDB and traces/logs to CH/lake.

---

## Anti-patterns

- `request_id` as a Prom label.
- One year of raw 1 Hz in CH with no rollup.
- Alerting only from Grafana-on-CH with no HA, no inhibit.

---

## Decision table (copy)

| Need | Pick |
|------|------|
| `up`, CPU, SLO burn, Alertmanager | TSDB |
| Per-user/per-device events SQL | CH |
| Postgres-shaped moderate series | Timescale |
| High-QPS live tiles | Pinot (OLAP serving) |
| Lake joins | Trino |

---

## Worked: adding `customer_id` to a RED metric

Product wants error rate **per customer** in Grafana using the existing Prom stack. Series: `customers × services × codes`. At 10k customers you are in the danger zone; at 100k you are dead. Export **bounded** `customer_tier` (gold/silver/free) to Prom if you must alert; put per-customer error rate in CH. Do not "try VM first" as a way to sneak `customer_id` in.

## Worked: IoT factory dashboard

10k machines, 20 sensors, 1 Hz. Series = 200k if one metric name — VM/Prom **might** hold metrics-shaped floats with care. 10M devices: CH/Timescale. Same dashboard, different cardinality. [IoT](../architectures/iot.md).

## Recording rules vs MVs

Prom recording rules: precompute `rate()` at scrape resolution for alerts. CH MVs: precompute `count/sum` for tiles. Do not implement recording rules in CH SQL every 10 s as a Prom replacement (you skip `up` and grouping).

## Retention dual-write

People write the same latency to Prom **and** CH. Acceptable if Prom labels are bounded and CH has the event. Not acceptable if both copies include `user_id`. Duplicate **aggregates** (service-level) into Prom from Flink if you want one alert path.

## Reading list

[Cardinality](../time-series/cardinality.md), [TSDBs](../time-series/tsdbs.md), [downsampling](../time-series/downsampling.md), [calculator](../simulations/cardinality-calculator.html).

---

## Choose X / Y / neither (summary table)

| | TSDB | ClickHouse OLAP | Neither / other |
|--|------|-----------------|-----------------|
| When | Bounded labels + alerts | High-card events + SQL | Pinot tiles, Trino lake, Timescale if PG-shaped moderate |
| First failure | Series explosion | Parts / `GROUP BY` RAM | — |
| Label `user_id` | Never | Column OK | — |
