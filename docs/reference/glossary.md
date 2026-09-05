---
description: Operational definitions for Kafka acks, Spark AQE, delivery semantics, and other academy terms — precise, not tutorial explanations.
---

# Glossary

Operational definitions as this academy uses them. If a term is not here, it is either ordinary English or only defined in its module. Related: [selection framework](selection-framework.md).

---

## A

**Ack (acknowledgement).** Kafka producer confirmation. `acks=all` waits for in-sync replicas; `acks=1` leader only; `acks=0` no wait. `acks=1` + leader death can **lose** acknowledged-looking data.

**AQE (Adaptive Query Execution).** Spark 3+ re-plans at runtime from shuffle stats. Can split skewed joins and coalesce tiny partitions. Not a substitute for a 180 GB single key ([incident 2](../incidents/index.md)).

**At-least-once.** Every record processed ≥1 time; **duplicates** possible. Default useful contract if the sink is **idempotent**.

**At-most-once.** ≤1 time; **loss** possible. Rarely what you want for money.

**Avro / Protobuf (in a registry).** Contracts for Kafka payloads. Compatibility modes catch poison schema changes that JSON `dict.get` will not.

---

## B

**Backpressure.** Slow consumer forces the source to slow. Flink does this natively with credit-based flow. Kafka just **lags** if you do not read.

**Broadcast join.** Spark sends a small side to every executor; **no shuffle of the big side**. Memory-bound. Wrong on a "small" side that is 80 GB.

**Broker.** Kafka process that stores partitions. Not a processor. Not a queue API.

---

## C

**Cardinality.** Distinct values of a key or label set. **High cardinality** (`user_id`) kills TSDB indexes; OLAP stores it as a column. See [calculator](../simulations/cardinality-calculator.html).

**CDC.** Change data capture: inserts/updates/deletes as events (Debezium). Sinks must **merge**, not `SUM` amounts.

**Checkpoint (Flink).** Consistent snapshot of operator state + positions. Recovery rewinds to it. Success ≠ windows are emitting ([incident 3](../incidents/index.md)).

**Columnar storage.** Disk layout by column. Analytics read fewer bytes; tiny point updates are the wrong workload.

**Compaction.** Merge small files/parts (Iceberg rewrite, CH merges, Kafka log compact is **different** — latest key only).

**Consumer group.** Set of Kafka consumers sharing `group.id`. **One** member per partition. Scaling past partition count does nothing.

---

## D

**DAG.** Directed acyclic graph: Spark stages or Airflow jobs. Cycles mean you designed a loop, not a pipeline.

**Data skew.** Uneven key distribution. One Kafka partition, one Spark task, one Flink subtask does all the work.

**DLQ (dead-letter queue).** Topic/table for poison messages after N failures. Without it, a bad record is an infinite restart.

---

## E

**Event time.** Timestamp when the event **happened**. Windowing on event time needs **watermarks**.

**Exactly-once.** End-to-end no loss no dup **if** source, processor, and sink participate (e.g. Flink + transactional Kafka). A HTTP POST sink is not exactly-once because you said the words. Prefer **idempotent** writes.

**Exchange.** Trino/Spark data movement between stages. Expensive. Cousin of shuffle.

---

## F

**Freshness.** How old the **visible** data is (`max(ts)` vs now, or Kafka lag in seconds). A green job can publish stale zeros.

**Granule.** ClickHouse index grain (~8192 rows). Sparse primary index stores one entry per granule. Queries skip granules using `ORDER BY`.

---

## H

**Hot partition.** One Kafka partition gets most traffic (hot key). Lag on **that** partition only. Extra consumers do not split it.

**Hypertable.** Timescale time-partitioned table (chunks). Not ClickHouse.

---

## I

**Idempotent.** Applying twice = once. The practical cousin of exactly-once for CH inserts, Iceberg MERGE, payment scores keyed by `transaction_id`.

**ISR (in-sync replicas).** Kafka replicas caught up with the leader. `min.insync.replicas` + `acks=all` define durability.

**Iceberg snapshot.** Committed table state. Time travel reads old snapshots. Unexpired snapshots **slow planning** ([incident 5](../incidents/index.md)).

---

## L

**Lag (Kafka).** Log-end offset minus consumer committed offset (per partition). Growing lag = consumer slower than produce **or** stuck. Sum lag can hide one hot partition.

**Late event.** Arrives after the watermark passed its timestamp. Side output / allowed lateness / drop.

**Lineage.** Dataset parent/child graph for paging and blast radius. OpenLineage is the event API; a catalogue is the store.

**Log compaction.** Kafka keeps latest value **per key**. Changelog / compacted CDC. Not a substitute for a database.

---

## M

**Manifest (Iceberg).** File listing data files + stats for pruning. Snapshot → manifest list → manifests → Parquet.

**MergeTree.** ClickHouse engine family: immutable **parts**, background merge, `ORDER BY` = sparse index.

**Micro-batch.** Spark Structured Streaming: a short batch, repeatedly. Latency floor of hundreds of ms to seconds.

---

## O

**Offset.** Monotonic id **inside a Kafka partition**. There is no global offset for a topic.

**OLAP.** Scan/aggregate heavy analytics. Opposite of OLTP point updates.

**OLTP.** Transactional point reads/writes (Postgres checkout). Do not replace with a lake.

**ORDER BY (ClickHouse).** Physical sort key, not a SQL nicety. Prefix must match filters or you scan ([incident 4](../incidents/index.md)).

**OpenLineage.** Standard for job run lineage events.

---

## P

**Part (ClickHouse).** Directory of column files from an insert (or merge). Too many parts → slow reads/writes.

**Partition (Kafka).** Ordered log shard; unit of parallelism and ordering.

**Partition (Spark/Iceberg/CH).** Different beasts: Spark task input; Iceberg layout; CH `PARTITION BY` (usually time) **plus** `ORDER BY`.

**Partition pruning.** Skip files/partitions whose stats cannot match `WHERE`.

**Predicate pushdown.** Filter evaluated in storage (Parquet/CH) so you do not ship rows.

**Processing time.** Wall clock when the operator sees the event. Easy; wrong for late devices.

---

## R

**Rebalance.** Kafka group membership change; consumption pauses. Storms = repeated rebalances (slow `poll`, crash loops).

**Replay.** Re-read Kafka or rebuild from lake. Requires idempotent sinks or you double-count.

**Replication factor.** Kafka copies of a partition. `3` with `min.insync=2` is a common durability pair.

**RLS (row-level security).** Engine-enforced row filter (tenant). App-only filters will leak.

---

## S

**Savepoint.** User-owned Flink checkpoint for upgrades. Not deleted like automatic checkpoints.

**Schema evolution.** Adding/renaming fields without breaking readers. Needs a registry or table format, not hope.

**Shuffle.** Redistribute by key for join/group. Spark's tax. Skew shows up here.

**Skipping index.** Extra CH index (minmax, bloom) per granule. Secondary to `ORDER BY`.

**SLO / SLA.** Freshness or latency **number** with an owner. "Realtime" is not an SLO.

**Star-tree.** Pinot pre-agg index over declared dimensions. High QPS, low flexibility.

---

## T

**Tenant isolation.** `customer_id` in keys, `ORDER BY`, RLS, quotas. Shared table ≠ shared everything.

**Tombstone.** Kafka compacted-topic null value meaning **delete this key**.

**TTL.** Auto-expire (CH table, Kafka retention, Iceberg expire). Security control as much as cost.

**Tungsten.** Spark off-heap / codegen. Why `collect()` to pandas still hurts: you left Tungsten.

---

## W

**Watermark.** Event-time notion of "we will not see older than this" (with slack). Windows **close** on it. Idle sources must not stall it forever.

**WAL.** Write-ahead log (Postgres, etc.). CDC reads it; a slow Debezium sink **fills disk on the primary**.

---

## Z

**Z-order.** Multi-column clustering (Delta ZORDER, related ideas in CH). Helps multi-dimension filters; not magic for all predicates.

---

## Additional terms used in architectures

**Allowed lateness.** Flink window still accepts events after the watermark for a configured duration; then drops or side-outputs.

**Bloom skip index.** ClickHouse secondary index: "this granule might contain the trace_id." False positives exist; false negatives should not. Does not replace putting time+service in `ORDER BY`.

**Broadcast (Trino).** Join strategy that sends one side to all workers. Wrong when that side is the lake.

**Consumer lag seconds.** Lag in **time** (event ts vs now) vs lag in **offsets**. Both matter; dashboards care about time.

**Contract (data).** Versioned schema + SLO + owner + semantics in git. Catalogue displays it; git is source.

**Dead partition (Kafka).** No produce; Flink watermark min-over-partitions stalls without idleness.

**Fan-out join.** Join keys not unique; row count explodes. Quality: compare counts before/after.

**Gold table.** Dataset that pages a human. See [metadata](../metadata/index.md).

**Idle cull.** JupyterHub scale-to-zero for unused user pods. Cost control.

**KRaft.** Kafka consensus without ZooKeeper. Lab broker is KRaft combined mode.

**Materialised view (CH).** Insert trigger that writes an aggregate table. Not a Prom recording rule, same idea.

**Noisy neighbour.** One tenant/key consumes a shared partition or node. Quotas + isolation.

**Outbox.** Transactional table in OLTP that CDC turns into business events you designed, vs raw row CDC.

**Poison message.** Record that crashes the consumer. DLQ or you stall.

**Pushdown.** Filter/agg executed in the connector/source (Parquet, PG). Trino without pushdown pulls oceans.

**ReplacingMergeTree.** CH engine that keeps latest row per `ORDER BY` key eventually. Queries may need `FINAL` or a collapsing understanding.

**Savepoint vs checkpoint.** Savepoint is operator-owned upgrade artifact; checkpoints are automatic recovery.

**Star schema.** Fact + dimensions. Pinot likes this; CH can join dims if small.

**Two-phase commit sink.** Flink EOS with Kafka transactions. JDBC sinks often are not participants.

**Whale tenant.** `cust_0042` in labs: one customer is a double-digit fraction of traffic. Architecture, not a row.
