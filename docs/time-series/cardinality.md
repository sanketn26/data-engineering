# Cardinality

Cardinality is one of the most important — and most misunderstood — concepts in time-series systems. Getting it wrong has caused Prometheus outages at many companies.

---

## What Is a Time Series?

A time series is uniquely identified by its **metric name** + **label set**:

```
http_requests_total{service="api", region="us-east", status="200"}
```

This is one time series. If you add another label:

```
http_requests_total{service="api", region="us-east", status="200", method="GET"}
```

Still one time series, but with a different label set (and thus a different series ID).

---

## The Cardinality Explosion

Start simple:

```
http_requests_total{service="api", region="us-east"}
```

With 50 services and 10 regions: `50 × 10 = 500 series`.

Now add `status_code`:

```
http_requests_total{service="api", region="us-east", status_code="200"}
```

With ~20 status codes: `50 × 10 × 20 = 10,000 series`.

Seems manageable. Now add `endpoint`:

```
http_requests_total{service="api", region="us-east", status_code="200", endpoint="/api/orders"}
```

With 500 endpoints: `50 × 10 × 20 × 500 = 5,000,000 series`.

And now the catastrophic mistake — add `user_id`:

```
http_requests_total{service="api", region="us-east", status_code="200", user_id="u123456"}
```

With 1 million users: `50 × 10 × 20 × 500 × 1,000,000 = 5 × 10^12 series`.

**Five trillion time series. Prometheus is completely overwhelmed.**

---

## Why High Cardinality Breaks Prometheus

Prometheus stores one time series per unique label combination in memory (an inverted index). Memory usage grows linearly with the number of active time series.

At 5 million active time series, Prometheus uses ~20-30 GB of memory. At 50 million, it is using 200+ GB and likely struggling with WAL write latency.

Prometheus is designed for thousands to tens of millions of time series. Not hundreds of millions or billions.

---

## The Rule

> **Never use high-cardinality values as labels.**

Labels should have bounded cardinality: service names, regions, status codes, endpoints (if you control their count), environments.

Labels should NEVER be: user_id, session_id, request_id, IP address, any UUID, any free-form string.

---

## Interactive Exploration

Try computing series cardinality for your own workload:

```python
services = 50
regions = 10
status_codes = 20
endpoints = 500
users = 1_000_000  # ← try this with and without

# With user_id as label
with_users = services * regions * status_codes * endpoints * users
print(f"With user_id label: {with_users:,} series")

# Without user_id as label
without_users = services * regions * status_codes * endpoints
print(f"Without user_id label: {without_users:,} series")

# Memory estimate (rough): ~1 KB per active series in Prometheus
print(f"Memory (no users): {without_users * 1024 / 1e9:.1f} GB")
print(f"Memory (with users): {with_users * 1024 / 1e9:.0f} GB")
```

---

## High-Cardinality Data in Practice

If you need per-user metrics, use a different tool:

- **ClickHouse**: store individual events with `user_id` as a column, query with GROUP BY
- **Elasticsearch/OpenSearch**: for event-level logs with user context
- **Pinot**: for user-facing analytics dashboards

Prometheus is for **infrastructure metrics** (per-service, per-region, per-instance). It is not for **business metrics** (per-user, per-transaction).

---

## VictoriaMetrics and High-Cardinality

VictoriaMetrics handles higher cardinality than Prometheus through more efficient storage and indexing. Benchmarks show it handles 10–100× more series with the same memory footprint.

But the fundamental principle holds: high-cardinality labels still cause problems, just at larger scale. Don't use user_id as a label in VictoriaMetrics either.
