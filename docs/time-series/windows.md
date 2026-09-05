# Time Windows

**2:03 PM.** A dashboard request comes in: "rolling 5-minute average temperature, updated every 30 seconds, across the whole fleet." An engineer writes it as a SQL window function directly over 90 days of raw readings from 10 million devices. The query never returns; the cluster's CPU sits at 100%.

Predict before you read on: (A) a bigger cluster, (B) an index on `timestamp`, (C) precompute tumbling windows first and slide over those instead of raw, or (D) switch this chart to Prometheus?

It's (C) — a stream of `{timestamp, device_id, sensor, value}` never ends, a chart is a finite picture, and windows are how you cut the stream into finite aggregates without pretending you loaded "all rows." This is the storage/query side; the streaming side (watermarks, allowed lateness) is [Flink windows](../flink/windows.md) — same shapes, different engines.

---

## Use case

IoT fleet + SaaS observability:

- **Tumbling 1 minute:** average temperature per device for the live chart.
- **Sliding 5 minute / 1 minute:** rolling p95 latency for anomaly detection.
- **Session:** “device awake” bursts separated by 15 minutes of silence.
- **Range lookback:** PromQL `rate(http_requests_total[5m])` at each Grafana step.

Hundreds of millions of points/day. If you compute sliding windows from raw at read time over 90 days, you will not like the bill.

---

## Why this is hard

Every window type has a different **fan-out** and a different **close** rule.

- Tumbling: each sample in **one** bucket. Cheap. Aligns to clock.
- Sliding: each sample in **size/slide** buckets. A 1 h window sliding by 1 s is a 3600× multiply.
- Session: buckets are per-entity and data-dependent. Hard to pre-aggregate globally.
- Prom range vectors: not SQL `GROUP BY`; they are **lookbacks** at evaluation instants.

Late event time ([time semantics](time-semantics.md)) means a “closed” tumbling bucket can still receive points. Rollups that already flushed will **disagree** with raw unless you recompute.

---

## Intuition

Draw the time axis. Put ticks every minute. Tumbling windows are the cells between ticks. Sliding windows are a stencil you drag. Session windows are rubber bands that snap when there is a gap.

```text
Events:      *  * *     *            * *
Tumbling 5:  [  W1  ][  W2  ][  W3  ][ W4 ]
Sliding 5/2: [ W1 ] [ W2 ] [ W3 ] [ W4 ]
             [  W1  ] [  W2  ] [  W3  ]
Session 3:   [burst]   [one]         [burst]
```

Rates need **two** times: the window in which deltas are taken, and the step at which you emit. Mixing them is how Grafana shows a different shape than SQL.

---

## Internals

### Tumbling (fixed, non-overlapping)

Aligned to an origin (Unix epoch, or `time_bucket(..., origin)`).

```sql
-- Timescale
SELECT time_bucket('1 minute', ts) AS minute,
       device_id,
       avg(value) AS avg_v,
       max(value) AS max_v,
       count(*)   AS n
FROM readings
WHERE sensor = 'temperature'
  AND ts >= now() - interval '6 hours'
GROUP BY minute, device_id
ORDER BY minute;

-- ClickHouse
SELECT toStartOfMinute(ts) AS minute,
       device_id,
       avg(value), max(value), count()
FROM readings
WHERE sensor = 'temperature'
  AND ts >= now() - INTERVAL 6 HOUR
GROUP BY minute, device_id
ORDER BY minute;
```

**Cost:** one hash key per `(bucket, device)`. 10 M devices × 1-minute buckets × 6 h = 3.6×10⁹ keys if you group globally — you will not. Filter, or pre-aggregate.

### Sliding (hopping)

Window size `S`, slide `H`. Each event in `S/H` windows.

```sql
-- Rolling 5-minute average, emit every row (expensive)
SELECT
    ts,
    device_id,
    avg(value) OVER (
        PARTITION BY device_id
        ORDER BY ts
        RANGE BETWEEN INTERVAL '5 minutes' PRECEDING AND CURRENT ROW
    ) AS rolling_5m
FROM readings
WHERE device_id = 'd-9';
```

Fleet-wide sliding at query time is how you melt Timescale. **Precompute tumbling 1-minute**, then slide over **minutes** (12× cheaper than sliding over raw 5 s samples).

PromQL sliding is the native model:

```text
avg_over_time(temperature{device_id="d-9"}[5m])
```

Evaluated every `step`. Cost is per-series, which is why cardinality kills you.

### Session

Gap `G`. New window when `ts - prev_ts > G` per entity.

```sql
-- Session starts, then running sum of ids
SELECT
    device_id,
    ts,
    value,
    sum(is_start) OVER (PARTITION BY device_id ORDER BY ts) AS session_id
FROM (
    SELECT
        *,
        if(
            ts - lag(ts) OVER (PARTITION BY device_id ORDER BY ts) > 900
            OR lag(ts) OVER (PARTITION BY device_id ORDER BY ts) IS NULL,
            1, 0
        ) AS is_start
    FROM readings
    WHERE device_id = 'd-9'
);
```

Session windows **resist** continuous aggregates: you cannot merge two hourly rollups into sessions without raw gaps. Keep sessions on hot raw only, or compute in Flink and store session facts.

### Range lookback vs buckets

| | SQL `time_bucket(5m)` | Prom `metric[5m]` |
|--|------------------------|-------------------|
| Alignment | Grid | Anchored at eval time |
| Overlap | None | Yes, every step |
| Late data | Re-run query | Depends on scrape presence |

A Grafana panel with step=15s and `rate[5m]` draws ~20 overlapping windows per 5 minutes. SQL `GROUP BY toStartOfMinute` draws 5 points. Do not expect pixel-identical charts.

### What to store per window

| Statistic | Keeps | Hides |
|-----------|-------|--------|
| `avg` | Trend | 5 s spike |
| `max`/`min` | Spikes, drops | How long they lasted |
| `count` / `sum` | Volume, rates | Shape |
| `p95` | Tail | Need histograms / t-digest state, **not** avg of p95s |
| `last` | Gauge state | Everything before last |

**Percentiles do not merge** by averaging p95 of minutes to get p95 of hours. Store sketches (`quantilesState` in ClickHouse, t-digest) or raw.

---

## How

**Latest value per device** (not really a window — a group):

```sql
-- Timescale
SELECT DISTINCT ON (device_id)
    device_id, ts, value
FROM readings
WHERE sensor = 'temperature'
ORDER BY device_id, ts DESC;

-- ClickHouse
SELECT device_id, argMax(value, ts) AS last_v, max(ts) AS last_ts
FROM readings
WHERE sensor = 'temperature'
GROUP BY device_id;
```

`ORDER BY (device_id, sensor, ts)` makes `argMax` a tail read per device, not a full scan.

**Anomaly: value > mean + 2σ over 1 h tumbling windows** — better as a 1-minute rollup then SQL:

```sql
WITH min1 AS (
    SELECT
        toStartOfMinute(ts) AS m,
        device_id,
        avg(value) AS v
    FROM readings
    WHERE sensor = 'temperature'
      AND ts >= now() - INTERVAL 2 HOUR
    GROUP BY m, device_id
)
SELECT
    m, device_id, v,
    avg(v) OVER w AS mu,
    stddevPop(v) OVER w AS sd
FROM min1
WINDOW w AS (
    PARTITION BY device_id
    ORDER BY m
    ROWS BETWEEN 59 PRECEDING AND CURRENT ROW
)
WHERE sd > 0 AND abs(v - mu) > 2 * sd;
```

**Prom counter rate in SQL** (tumbling 1 m, reset-aware) belongs in ETL, not in the dashboard query.

**Materialize tumbling, derive sliding:**

```sql
-- ClickHouse MV: 1-minute states
CREATE MATERIALIZED VIEW readings_1m
ENGINE = AggregatingMergeTree
PARTITION BY toYYYYMMDD(m)
ORDER BY (device_id, sensor, m)
AS SELECT
    toStartOfMinute(ts) AS m,
    device_id,
    sensor,
    avgState(value) AS avg_s,
    maxState(value) AS max_s,
    countState()    AS n_s
FROM readings
GROUP BY m, device_id, sensor;
```

A 5-minute sliding chart reads 5 of these minute rows, not 10 raw samples × 30 s.

---

## Gotchas

!!! production-gotcha "Sliding over raw at 90-day range"
    `RANGE BETWEEN INTERVAL '1 hour' PRECEDING` on 10 M devices of raw is a self-join in disguise. Roll up first.

!!! production-gotcha "avg of p95"
    Hourly p95 averaged to daily p95 is not a percentile. Store sketches or compute from raw/hot.

!!! production-gotcha "toStartOfInterval on a String timestamp"
    No index, no partition prune. Native datetime types.

!!! production-gotcha "Session in a continuous aggregate"
    Caggs are tumbling (or hierarchical tumbling). Sessions need raw or a stream job.

!!! production-gotcha "Prom step too large"
    `query_range` step of 5 m with `rate[5m]` undersamples. Step too small with high cardinality: Prom CPU incident.

---

## Failure modes

| Symptom | Cause |
|---------|--------|
| Chart smoother than reality | Only `avg` tumbling; window too wide |
| CPU 100% on one dashboard | Sliding window / cartesian of devices × buckets |
| Alert lag | Window not closed; waiting for lookback to fill after scrape fail |
| Double-sized rate at deploy | Reset handling missing |
| Empty buckets dropped, line connects across outage | UI interpolation; use gapfill/null |

---

## Debugging

- Compute **expected bucket count**: 6 h × 1-minute × 1 device = 360. If the query returns 360,000, you forgot `device_id` filter or grouped wrong.
- ClickHouse `EXPLAIN` / `read_rows`: should be ~ raw in the time range, or ~ rollup rows. If you read 30 days for a 1-hour sliding chart, the window is in the wrong layer.
- Prom: `count({__name__=~".+"})` and query log; `rate` with `irate` confusion (`irate` = last two samples, noisier).
- Compare tumbling SQL vs Prom lookback on **one series** for the same incident.

Flink EXPLAIN is the wrong tool here unless the window is in the job. For CH: `system.query_log.read_bytes`. For Timescale: `EXPLAIN ANALYZE` and chunk exclusion (`chunks excluded`).

---

## Scale 10× / 100× / 1000×

| Scale | Window strategy |
|-------|-----------------|
| **10×** | 1-minute cagg/MV; dashboards never hit raw except drill-down |
| **100×** | Hierarchical: 1 m → 1 h → 1 d; sliding only on 1 m table; session on hot 24 h |
| **1000×** | Pre-aggregate in the stream (Flink) keyed by device; TSDB stores windows, not raw; drop raw fast |

Window **type** that does not scale: fleet-wide sliding on raw. Window that does: tumbling rollups with a bounded key (`device_id` after a filter, or `customer_id`).

---

## Trade-offs

| Window | Accuracy vs cost |
|--------|------------------|
| Tumbling 1 m | Cheap, clock-aligned, can miss a spike on the boundary (split across two buckets — **max** of two minutes still sees it if you keep max) |
| Sliding | Better for “last 5 m” UX; expensive; Prom-native |
| Session | Matches product “usage burst”; hard to pre-agg |
| Lookback rate | Correct for counters; not a SQL bucket |

---

## Alternatives

| Tool | Window story |
|------|----------------|
| Flink | Event-time windows + watermarks **before** storage |
| Prometheus | Range vectors, recording rules as pre-windows |
| Timescale cagg | Tumbling SQL, hierarchical |
| ClickHouse MV | Tumbling states, `AggregatingMergeTree` |
| Grafana | Client-side reduce — do not rely on it at 10 M devices |

---

## Apply

For each tile, write: **window type, size, slide, statistic, source table (raw vs 1m vs 1h)**. If two tiles share a tumbling 1 m, that is one MV, not two queries on raw.

If the PM wants a rolling 5-minute p95 on 10 M devices for 30 days, that is a [downsampling](downsampling.md) + sketch problem, not a Grafana `range()`.

Related stream processing: [Flink windows](../flink/windows.md) if you must close event-time windows **before** they hit the TSDB (sessionization, exactly-once facts). Related storage: [ClickHouse MVs](../olap/clickhouse.md).

---

## Exercise

10 M devices, temperature every 30 s. Tile: “rolling 5-minute average, updated every 30 s” for **one** device (device page) vs **fleet p95 of those per-device averages** (ops wall).

Can you serve both from raw ClickHouse? What windows and rollups do you build? Why is fleet p95 of averages not p95 of raw samples?

??? success "Answer"
    **Device page:** raw (or 30 s data) with a sliding SQL window or client-side roll of the last 10 samples. 5 minutes × 1 device × 2 samples/s wait, 30 s interval = 10 points. Trivial. `ORDER BY (device_id, sensor, ts)` + time filter. Do **not** scan the fleet.

    **Ops wall:** do **not** slide on raw 10 M devices. Build 30 s or 1-minute tumbling per device (`avg`/`max`). Fleet tile should use a **further** rollup: e.g. 1-minute `quantile` **across devices** of the per-device mean, stored as a sketch or as a pre-aggregated fleet table. Updating every 30 s means the MV/cagg delay must be ≤ 30 s or you query the last few raw minutes merged with the rollup.

    **p95 of per-device averages ≠ p95 of raw samples.** The first asks “how hot is a typical device’s 5-minute mean.” The second asks “how hot is a typical **sample**” (dominated by devices that report more often, and includes intra-window spikes). Product must pick. Storing only `avg` in the 1-minute table **destroys** raw p95; keep `max` and/or a quantile state if the wall needs spikes.
