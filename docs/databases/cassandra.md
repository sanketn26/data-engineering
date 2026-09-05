# Cassandra & ScyllaDB

The observability pipeline needs to **append ~400,000 spans per second**, keep the last 24 hours queryable as “this service, this time range,” survive a zone loss, and ack writes in a few milliseconds. There is no join. There is no ad-hoc `WHERE payload LIKE`. There is no single primary to fail over.

Cassandra (and Scylla as a shard-per-core runtime for the same model) is the write-optimised, query-bound store for that shape. It is a poor warehouse and a poor session cache. Model it as **partition key + clustering key** or do not use it.

---

## Use case

**Observability writes** in this academy: every span is `(service, ts, trace_id, payload)`. The ops UI reads **one service, last 15 minutes**. Multi-region: a local read should not wait for another continent.

Secondary academy uses: **IoT per-device time series** (not the device *registry* identity document — that is KV), and **e-commerce inbox / event log per `user_id`** when the query is always “latest N events for this user.”

---

## Why this is hard

A relational primary serialises writes through WAL and B-trees. At hundreds of thousands of writes per second you fight:

- One leader’s disk and CPU
- Index maintenance on every insert
- Failover measured in seconds to minutes
- Range queries that want a different physical order than the primary key

Cassandra’s answer — **log-structured merge, no leader for data, hash partitions** — creates new hard problems: **hot partitions, tombstones, unbounded wide rows, repair, and quorum math**. The CQL looks like SQL. The execution is not SQL.

---

## Intuition

```mermaid
flowchart LR
    W["Write"]
    C["Any coordinator node"]
    R["Replicas for this token"]
    CL["CommitLog + memtable"]
    SS["Flush SSTables"]
    M["Compaction"]
    W --> C --> R --> CL --> SS --> M
```

- Hash(`partition key`) → token → replica set (RF copies).
- Inside a partition, rows are **sorted by clustering key** — a wide row.
- Writes append (commitlog + memtable). Reads **merge** memtable + SSTables + cache.
- You only get fast reads if you **already know the partition key**.

Think: a huge distributed sorted map. Key = partition. Value = sorted list of clustering cells. There is no mystery secondary index that makes `WHERE event_type = 'purchase'` cheap.

---

## Internals

### Tokens, replicas, coordinators

Every node owns token ranges (vnodes). `PRIMARY KEY ((service, bucket), ts)` hashes **both** `service` and `bucket` as the partition key. The coordinator is **any node you contacted**; it forwards to the replica set.

Replication factor 3, simple strategy vs network topology: in production you use **NetworkTopologyStrategy** and **LOCAL_QUORUM** so a write does not wait for the far DC.

### Partition key vs clustering key

```sql
PRIMARY KEY ((pk1, pk2), ck1, ck2)
--          ^^^^^^^^^^^  ^^^^^^^^
--          partition     clustering (order inside the partition)
```

| | Partition key | Clustering key |
|--|---------------|----------------|
| Purpose | Which nodes store the data | Order and range scans **inside** one partition |
| Uniqueness | Together with CK, identifies a row | |
| Anti-pattern | Low cardinality (`country`, `service` alone) | Unbounded growth (`ts` forever on one PK) |

**Wide row:** many clustering cells under one partition. Useful: last 1 hour of spans for `api-gateway` in bucket `2024011513`. Lethal: all spans for `api-gateway` since 2019 in one partition (compaction, repair, and reads fall over).

### Write path

1. Coordinator sends to replicas.
2. Each replica appends **commitlog**, puts in **memtable**.
3. When memtable fills, flush **immutable SSTable**.
4. **Compaction** merges SSTables, drops garbage (eventually).

No B-tree leaf split on the write path. That is why writes are fast — and why deletes are **tombstones**, not holes.

### Read path

Coordinator reads from enough replicas to satisfy consistency. Each replica checks memtable, row cache (if any), Bloom filters, partition index, then SSTables. Results **merge**. More SSTables → slower reads until compaction catches up.

### Consistency: QUORUM

`QUORUM = floor(RF/2)+1`. With RF=3, QUORUM=2.

| If you write with | And read with | You get |
|-------------------|---------------|---------|
| `QUORUM` | `QUORUM` | Strong for that row (same DC story aside) |
| `LOCAL_QUORUM` | `LOCAL_QUORUM` | Strong **in the local DC** — what you want multi-region |
| `ONE` | `ONE` | Fast, can be stale, can miss a write |
| `ALL` | anything | Fragile; one dead replica blocks |

`R + W > RF` is the textbook overlap rule. **LOCAL_QUORUM** is the production default for multi-DC, not `ALL`.

### Tombstones

A delete writes a tombstone. TTL expiry writes a tombstone. Inserting `null` can write a tombstone. Collections (list append/remove) generate many.

Reads must **see tombstones** until `gc_grace_seconds` has passed **and** compaction has dropped them — otherwise a replica that missed the delete can resurrect data during repair.

!!! danger "Tombstone storms"
    A partition that deleted 10 million cells will time out on read (`TombstoneOverwhelmingException`) long after the product thought data was gone. TTLing a high-churn table without bounding partition size is a standard outage.

### Compaction

| Strategy | Fit |
|----------|-----|
| Size-tiered (STCS) | Write-heavy, default-ish; can leave large SSTables |
| Leveled (LCS) | Read-heavy, more write amp |
| Time-window (TWCS) | **Time-series with TTL** — observability spans. One window per interval; drop whole SSTables |

For the observability use case, **TWCS + TTL + time-bucketed partition keys** is the design. STCS on an infinite `service` partition is how disks fill with overlapping SSTables.

### LWT (lightweight transactions)

`UPDATE … IF` / `INSERT IF NOT EXISTS` runs **Paxos** for that partition: multiple round trips, serial consistency, throughput far below a normal write.

Use for: “allocate this device_id once,” “compare-and-set firmware.” Do not use for: every span insert, every session touch. LWT is not a substitute for Postgres transactions across keys.

### ScyllaDB — shard-per-core, not a new model

Scylla speaks CQL and Cassandra drivers. Internals: **one shard per CPU core**, shared-nothing, no JVM GC pauses, explicit CPU pinning. You still design partition keys. You still get tombstones and hot partitions. You get more throughput per box and tail latency that does not sawtooth with GC.

Treat Scylla as a **drop-in runtime** for a Cassandra data model, not as a reason to skip modelling. Compaction, TWCS, and QUORUM remain your problem. Repair/streaming details differ; operations are not copy-paste of 2015 Cassandra runbooks — but this academy will not re-document Scylla’s admin surface.

---

## How

### Observability table (do this)

```sql
CREATE KEYSPACE obs WITH replication = {
  'class': 'NetworkTopologyStrategy',
  'dc1': 3
};

CREATE TABLE obs.spans_by_service (
    service   text,
    bucket    int,              -- unix hour
    ts        timeuuid,
    host      text,
    trace_id  text,
    duration  int,
    payload   blob,
    PRIMARY KEY ((service, bucket), ts)
) WITH CLUSTERING ORDER BY (ts DESC)
  AND compaction = {
    'class': 'TimeWindowCompactionStrategy',
    'compaction_window_unit': 'HOURS',
    'compaction_window_size': 1
  }
  AND default_time_to_live = 86400
  AND gc_grace_seconds = 86400;
```

```sql
-- write
INSERT INTO obs.spans_by_service (service, bucket, ts, host, trace_id, duration, payload)
VALUES ('api-gateway', 1705312800, now(), 'gwy-12', 'abc', 23, 0x...);

-- read last 15 minutes for one service: at most 2 buckets
SELECT ts, trace_id, duration
FROM obs.spans_by_service
WHERE service = 'api-gateway'
  AND bucket IN (1705312800, 1705309200)
  AND ts >= minTimeuuid('2024-01-15 10:45:00')
  AND ts <= maxTimeuuid('2024-01-15 11:00:00');
```

### IoT telemetry (per device, bucketed)

```sql
CREATE TABLE iot.readings (
    device_id text,
    day       date,
    ts        timestamp,
    metric    text,
    value     double,
    PRIMARY KEY ((device_id, day), metric, ts)
) WITH CLUSTERING ORDER BY (metric ASC, ts DESC)
  AND default_time_to_live = 604800
  AND compaction = {'class': 'TimeWindowCompactionStrategy',
                    'compaction_window_unit': 'DAYS',
                    'compaction_window_size': 1};
```

The **registry** (`device_id → firmware, owner`) is a separate one-row-per-device table, not this wide time series.

### E-commerce event log per user

```sql
CREATE TABLE shop.user_events (
    user_id  text,
    ts       timeuuid,
    kind     text,
    payload  text,
    PRIMARY KEY (user_id, ts)
) WITH CLUSTERING ORDER BY (ts DESC);
```

Add a **bucket** (`user_id, week`) the moment a power user can write unbounded events. Celebrity users are hot partitions — shard: `PRIMARY KEY ((user_id, shard), ts)` with `shard = hash(event_id) % N` on write, fan-out N reads.

### LWT example (device claim)

```sql
INSERT INTO iot.devices (device_id, owner)
VALUES ('dev-9', 'alice')
IF NOT EXISTS;
```

Measure latency. If this is on the 50k QPS path, you designed wrong.

---

## Gotchas

!!! warning "CQL is not SQL"
    No joins, no `OR` across partitions, no unconstrained `ALLOW FILTERING` in production, no `COUNT(*)` of the table.

!!! warning "ALLOW FILTERING"
    Means “scatter-gather and hope.” Forbidden on the hot path.

!!! warning "Secondary indexes"
    Cassandra 2i are per-node, QPS-sensitive, and a hotspot magnet. Materialise a second table instead.

!!! warning "Batches"
    `BATCH` is for **the same partition** (atomicity of a few rows). Multi-partition batches are a coordinator tax, not a transaction.

!!! warning "Unbounded partitions"
    Aim for tens of MB, not tens of GB. Bucket time. Cap list/collection sizes.

!!! warning "Repair neglected"
    Without repair, deleted data resurrects after `gc_grace_seconds`. TWCS tables with TTL still need an operational story.

---

## Failure modes

| Failure | What you see | Cause |
|---------|--------------|-------|
| Hot partition | One node CPU/disk 100%, others bored | PK cardinality too low (`service` only, `country`) |
| Tombstone timeout | Read errors, huge scan histograms | Deletes/TTL on a fat partition |
| Read latency after write burst | SSTable count high | Compaction lag; wrong strategy |
| `UnavailableException` | Writes fail | Not enough replicas up for CL |
| `WriteTimeout` with LWT | Paxos unfinished | Contention on one PK |
| Disk full | Flush fails, node down | Compaction leftover + snapshots + commitlog |
| Resurrection | Deleted row returns | Missed repair, short `gc_grace` |

Multi-DC: using `QUORUM` instead of `LOCAL_QUORUM` makes a remote DC outage stall local ingest.

---

## Debugging

1. **Nodestats / Scylla manager:** latency, pending compactions, SSTable count per table.
2. **`nodetool tablehistograms` / tracing:** `TRACING ON` in cqlsh for a slow `SELECT` — count of tombstones and SSTables touched.
3. **Per-partition size:** `nodetool tablehistograms` and estimated partition size. Find the celebrity key.
4. **Coordinator vs replica:** if coordinator CPU is high, clients are not token-aware (driver should send to a replica).
5. **Query in logs:** any `ALLOW FILTERING` or unbound `IN` lists.

For Scylla, shard-aware drivers matter: a poorly pinned client turns shard-per-core into cross-shard chatter.

---

## Scale: 10× / 100× / 1000×

**10× (4M spans/s peak).** Add nodes; **linear** only if new tokens take load. If `api-gateway` is 60% of traffic and PK is `(service, hour)`, that hour’s partition still lives on RF nodes — **bucket smaller** (minute) or shard `service`.

**100×.** TWCS windows, TTL, and bucket size are mandatory. Streaming/rebuild time dominates ops; prefer Scylla or carefully sized Cassandra with automation. Never let a partition exceed a fraction of one shard’s disk. Analytics has already moved to ClickHouse; if not, this is the incident.

**1000×.** You are sharding the *key* (`(service, minute, shard)`), isolating tenants, and treating Cassandra as a **durability buffer**, not a query engine. Compaction throughput and repair become a dedicated practice. LWT is banned except on tiny control tables. Consider whether a log (Kafka) + OLAP (ClickHouse) removed the need for a 24 h CQL read path at all.

Cluster size does not fix a hot partition. That ceiling is **one node’s** disk and CPU.

---

## Trade-offs

You gain: write throughput, multi-DC availability, predictable single-partition reads, operational independence from a primary.

You give up: ad-hoc query, cheap indexes, multi-row ACID, easy deletes, “small cluster” friendliness (repair, compaction, JVM or shard tuning).

Scylla trades JVM pauses for **core-count and shard-aware ops**. It does not trade away modelling.

---

## Alternatives

| Workload | Often better |
|----------|----------------|
| Session cache, TTL, tiny values | Redis |
| AWS-only KV + sort key, no nodes | [DynamoDB](dynamodb.md) |
| Dashboards, aggregates | [ClickHouse](../olap/clickhouse.md) |
| Device registry, low QPS | Postgres |
| Metrics with rollups | [TSDB](../time-series/index.md) |
| Unknown query shapes | Postgres + warehouse |

---

## Apply

You will see this when someone “puts events in Cassandra so we can query them later,” when `service` is the only PK, when TTL’d tables time out on read, or when LWT is proposed to “make it consistent like Postgres.”

List queries. Bucket time. Measure partition size. Use `LOCAL_QUORUM`. Put analytics elsewhere.

---

## Exercise

??? question "Design spans_by_service for 400k writes/s"
    `api-gateway` is 40% of traffic. RF=3, two DCs. Reads: last 15 minutes per service, p99 50 ms. Retention 24 h.

    1. Write the PRIMARY KEY. Why is `PRIMARY KEY (service, ts)` wrong?
    2. Compaction strategy and TTL? `gc_grace_seconds`?
    3. Consistency level for write and read?
    4. What breaks at 10× if gateway traffic stays 40%?
    5. Product wants `WHERE trace_id = ?`. What do you do instead of a secondary index?

??? success "Answer"
    1. `PRIMARY KEY ((service, bucket), ts)` with `bucket` = hour or minute. `(service, ts)` as PK (only `service` partitioned) puts all gateway spans for all time on one partition — unbounded wide row and a permanent hotspot.

    2. TWCS with window ≈ bucket (1 hour). TTL 86400. `gc_grace_seconds` on the order of a day (or less if you understand repair/TWCS drop behaviour) — do not leave 10 days of tombstones on a high-churn table.

    3. `LOCAL_QUORUM` / `LOCAL_QUORUM`. Not `ALL`. Not `ONE` if operators will believe the UI.

    4. The hot `(service, bucket)` still maps to RF nodes. 10× load on that partition is 10× on those disks. Split bucket to minutes and/or add a shard: `(service, bucket, shard)`.

    5. Materialise `spans_by_trace (trace_id, ts)` as a **second table** written by the pipeline (or a CDC/consumer). Do not add 2i on `trace_id` of a 400k/s table.
