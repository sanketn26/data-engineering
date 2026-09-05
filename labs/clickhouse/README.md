# ClickHouse lab

One official ClickHouse container. You will load the **same** SaaS events into two tables that differ only in `ORDER BY`, then **break** ingest with tiny inserts until `system.parts` explodes.

Docs: [ClickHouse](../../docs/olap/clickhouse.md), [ORDER BY sim](../../docs/simulations/clickhouse-order-by.html), [incident 4](../../docs/incidents/index.md), [analytics architecture](../../docs/architectures/analytics-platform.md).

## Prerequisites

- Docker Compose v2
- `curl` (HTTP 8123) or `clickhouse-client` via `docker exec`

```bash
cd labs/clickhouse
docker compose up -d
# wait for health
curl -sS --user academy:academy 'http://localhost:8123/?query=SELECT%201'
docker exec -i dea-clickhouse clickhouse-client --user academy --password academy --multiquery < schema.sql
python load_events.py --rows 200000
```

Helper:

```bash
alias ch="docker exec -i dea-clickhouse clickhouse-client --user academy --password academy"
# or: curl --user academy:academy 'http://localhost:8123/' --data-binary @-
```

## Predict

Tenant dashboard query:

```sql
SELECT count(), avg(latency_ms)
FROM events
WHERE customer_id = 'cust_0042'
  AND timestamp >= now() - INTERVAL 15 MINUTE;
```

1. Table A `ORDER BY (customer_id, timestamp)` — roughly what fraction of granules / marks should this read if data is many customers × time?
2. Table B `ORDER BY (timestamp, customer_id)` — same query, more or less data read?
3. One-row `INSERT` in a loop 5,000 times: what happens to `system.parts` and to the SELECT?

## Run — two physical designs

```sql
CREATE TABLE events_by_tenant
(
    timestamp   DateTime,
    customer_id String,
    user_id     String,
    service     LowCardinality(String),
    endpoint    LowCardinality(String),
    region      LowCardinality(String),
    latency_ms  UInt32,
    status_code UInt16,
    bytes       UInt32
)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(timestamp)
ORDER BY (customer_id, timestamp);

CREATE TABLE events_by_time
AS events_by_tenant
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(timestamp)
ORDER BY (timestamp, customer_id);
```

The committed `load_events.py` generates the canonical dataset. The expanded snippet below shows its mechanics. Keep **many** customers and a few hours of timestamps so skip indexes have something to skip:

```python
import base64, json, random, urllib.parse, urllib.request
from datetime import datetime, timedelta, timezone

base = datetime.now(timezone.utc) - timedelta(hours=2)
rows_t, rows_s = [], []
for i in range(200_000):
    ts = base + timedelta(seconds=random.randint(0, 7200))
    cid = f"cust_{random.randint(0, 99):04d}"
    rec = (
        ts.strftime("%Y-%m-%d %H:%M:%S"),
        cid,
        f"user_{i}",
        random.choice(["api-gateway", "auth", "billing"]),
        "/v2/events",
        "eu-west-1",
        random.randint(10, 400),
        200,
        1024,
    )
    rows_t.append(rec)

def insert(table, rows):
    columns = ["timestamp", "customer_id", "user_id", "service", "endpoint",
               "region", "latency_ms", "status_code", "bytes"]
    body = "\n".join(json.dumps(dict(zip(columns, row))) for row in rows).encode()
    query = urllib.parse.quote(f"INSERT INTO {table} FORMAT JSONEachRow")
    req = urllib.request.Request(
        f"http://localhost:8123/?query={query}", data=body, method="POST"
    )
    credentials = base64.b64encode(b"academy:academy").decode()
    req.add_header("Authorization", f"Basic {credentials}")
    urllib.request.urlopen(req).read()

# insert in chunks of 10k
chunk = 10_000
for i in range(0, len(rows_t), chunk):
    insert("events_by_tenant", rows_t[i : i + chunk])
    insert("events_by_time", rows_t[i : i + chunk])
print("loaded", len(rows_t))
```

`JSONEachRow` avoids SQL string-quoting mistakes and matches the committed loader. For production, use a native client and batch by bytes/time rather than building large request bodies in memory.

Compare plans:

```sql
EXPLAIN indexes = 1
SELECT count(), avg(latency_ms)
FROM events_by_tenant
WHERE customer_id = 'cust_0042'
  AND timestamp >= now() - INTERVAL 15 MINUTE;

EXPLAIN indexes = 1
SELECT count(), avg(latency_ms)
FROM events_by_time
WHERE customer_id = 'cust_0042'
  AND timestamp >= now() - INTERVAL 15 MINUTE;
```

Also:

```sql
SET send_logs_level = 'trace';
-- look at query_log after
SELECT
    table,
    query_duration_ms,
    read_rows,
    read_bytes,
    result_rows
FROM system.query_log
WHERE type = 'QueryFinish' AND query LIKE '%cust_0042%'
ORDER BY event_time DESC
LIMIT 10;
```

**Compare to predictions 1–2.** Tenant-first key should read **fewer** rows for a tenant filter. Time-first may still restrict by the 15-minute range (partition + timestamp prefix) but **cannot** skip other customers inside that range.

Flip the query to "all customers, last 15 minutes" (`WHERE timestamp >= ...` only). **Predict** which table is less bad. Time-first can win **that** query. This is the product vs SRE `ORDER BY` fight.

## Break — too many parts

Run the deterministic committed exercise:

```bash
python break_parts.py
```

It temporarily stops merges on the disposable lab table, inserts one row per request, asserts that the configured guard rejects the pattern, and restarts merges in a `finally` block. The expanded commands below show the underlying mechanism.

```sql
CREATE TABLE tiny_inserts
(
    timestamp DateTime,
    customer_id String,
    latency_ms UInt32
)
ENGINE = MergeTree
ORDER BY (customer_id, timestamp)
SETTINGS parts_to_delay_insert = 20, parts_to_throw_insert = 40;
```

```python
import urllib.request
import base64
authorization = "Basic " + base64.b64encode(b"academy:academy").decode()
for i in range(3000):
    q = f"INSERT INTO tiny_inserts VALUES (now(), 'cust_0042', {i})".encode()
    request = urllib.request.Request("http://localhost:8123/", data=q)
    request.add_header("Authorization", authorization)
    urllib.request.urlopen(request)
    if i % 200 == 0:
        print("inserted", i)
```

```sql
SELECT count() AS parts, sum(rows) AS rows
FROM system.parts
WHERE table = 'tiny_inserts' AND active;

SELECT count(), avg(latency_ms) FROM tiny_inserts WHERE customer_id = 'cust_0042';
```

**Pass condition:** the server begins delaying or rejects an insert with `Too many parts` once the deliberately low lab threshold is reached. Background merges make the exact active-parts count nondeterministic; do not expect exactly 3,000. Inspect `system.part_log` as well as `system.parts`. Emergency: `OPTIMIZE TABLE tiny_inserts FINAL` (lab only). Real fix: **batch** inserts (seconds of rows per INSERT), which you already did in the 10k chunks above.

Watch `system.metrics` / logs for merge pressure if you push further.

## Clean up

```bash
docker compose down -v
```

## Notes

- `LowCardinality(String)` is not an index.
- `PARTITION BY date` does not replace `ORDER BY` for tenant filters inside a day.
- Grafana 10× is often this lab, not "need 20 nodes."
