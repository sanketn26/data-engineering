# Time-series (cardinality) lab

A synthetic Prometheus exporter and one official `prom/prometheus` container. You will scrape a bounded-label metric, measure its real series count via Prometheus's own API, then **break** it by adding a `user_id` label and watching `prometheus_tsdb_head_series` multiply.

## Before you run

1. Read [Cardinality](../../docs/time-series/cardinality.md) through **Build the mental picture**.
2. Add `user_id` in the [cardinality calculator](../../docs/simulations/cardinality-calculator.html).
3. Predict both the application series count and Prometheus's total head-series count.

The simulation gives the product-of-labels model; this lab includes the extra
series Prometheus creates to monitor itself. The difference between those two
numbers is part of the observation. See the [practice map](../../docs/practice-map.md)
for the full sequence.

## Prerequisites

- Docker Compose v2
- No Python packages beyond the standard library — `exporter.py` and `check_cardinality.py` are both stdlib-only

## Predict (write this down)

1. The exporter's default label set is `service` (4 values), `region` (3 values), `status_code` (5 values), and **no** `user_id`. Roughly how many distinct `http_requests_total` series should Prometheus be holding?
2. If you add a `user_id` label with 10,000 distinct values to every series, how many series does the product-of-bounds arithmetic predict?
3. Does the *sample rate* (scrape interval) change either answer? Why is cost here "how many series," not "how many scrapes"?

## Run

```bash
cd labs/time-series
docker compose up -d --build
docker compose ps   # wait for prometheus to report healthy
```

Give it 15-20 seconds for a few 5-second scrapes to land, then check the number two ways:

```bash
curl -s 'http://localhost:9090/api/v1/query?query=prometheus_tsdb_head_series' | python3 -m json.tool
```

Or in a browser: open `http://localhost:9090/graph`, enter `prometheus_tsdb_head_series` in the expression box, and hit Execute (or Graph).

You can also look at the raw exposition text the exporter emits:

```bash
curl -s http://localhost:9105/metrics | head -20
curl -s http://localhost:9105/metrics | wc -l
```

**Compare to prediction 1.** With `SERVICES=4`, `REGIONS=3`, `STATUS_CODES=5`, and `USER_IDS=0` (the defaults), the exporter emits `4 * 3 * 5 = 60` distinct `http_requests_total` series per scrape. `prometheus_tsdb_head_series` reads noticeably higher than 60 — this stack's Prometheus scrapes **itself** (see `prometheus.yml`'s `prometheus` job) so it can answer `prometheus_tsdb_head_series` via PromQL at all, and Prometheus exposes several hundred metrics about its own internals (`up`, `scrape_duration_seconds`, dozens of `prometheus_tsdb_*` and `prometheus_engine_*` families, many of them histograms with multiple bucket series each). Expect **~600-700** at baseline, not 60 — a small, free lesson that monitoring your monitoring is not free either.

## Check your work

```bash
python3 check_cardinality.py --expect-min 400 --expect-max 1200
```

This reads `prometheus_tsdb_head_series` from Prometheus's HTTP API — the same number you just looked at by hand — and asserts it falls in a realistic range for the low-cardinality baseline (60 exporter series plus several hundred series from Prometheus's own self-scrape). It also unconditionally checks that `head_series > 0`, i.e. Prometheus is actually scraping something at all. If the count is 0, `docker compose ps` and the exporter logs are the first things to check.

## Break — raise cardinality

Turn on the `user_id` label with 10,000 distinct values. `docker-compose.yml` reads `USER_IDS` (and `SERVICES`/`REGIONS`/`STATUS_CODES`) from the shell environment via `${USER_IDS:-0}`, so you can override it inline and recreate just the exporter container:

```bash
USER_IDS=10000 docker compose up -d --build exporter
```

Equivalently, edit the `environment:` block for the `exporter` service in `docker-compose.yml` directly:

```yaml
services:
  exporter:
    environment:
      SERVICES: 4
      REGIONS: 3
      STATUS_CODES: 5
      USER_IDS: 10000   # was 0
```

then:

```bash
docker compose up -d --build exporter
```

Wait 15-20 seconds for a few scrapes, then re-check:

```bash
curl -s 'http://localhost:9090/api/v1/query?query=prometheus_tsdb_head_series' | python3 -m json.tool
python3 check_cardinality.py --expect-min 200000 --expect-max 650000
```

With `SERVICES=4`, `REGIONS=3`, `STATUS_CODES=5`, `USER_IDS=10000`, the product is `4 * 3 * 5 * 10000 = 600,000` series — a 10,000x jump from the baseline, exactly the "one unbounded label multiplies every other dimension" arithmetic from the [cardinality doc](../../docs/time-series/cardinality.md) and the [calculator](../../docs/simulations/cardinality-calculator.html). Note how long the `/metrics` scrape itself takes now (`time curl -s http://localhost:9105/metrics > /dev/null`) compared to the baseline — this is the same "80 MB `/metrics` payload" incident smell the docs page describes.

**Predict again before you look:** does doubling `STATUS_CODES` from 5 to 10 at `USER_IDS=10000` double the series count, or something else? (It doubles it — the product is linear in every bounded dimension; `user_id` is just the dimension nobody bounded.)

## Clean up

```bash
docker compose down -v
```

## Notes

- Cost here is **#series**, not #samples — a metric scraped once still allocates a head block and inverted-index entries per unique label combination.
- A single high-cardinality label multiplies **every other** dimension in the product, not just itself; that is why `user_id` is categorically different from adding one more bounded label like `region`.
- This kind of per-user fact belongs in an event table (ClickHouse, `ORDER BY (customer_id, ts)`) queried at read time, not a Prometheus label paid for on every scrape — see [ClickHouse](../../docs/olap/clickhouse.md) and the `labs/clickhouse/` lab for the columnar side of this trade-off.
