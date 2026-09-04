# Glossary

Key terms used throughout the academy. Each definition explains the concept in context, not just what it is but why it matters.

---

## A

**Ack (acknowledgement)**: a confirmation from a Kafka broker that a message was received and written. `acks=all` waits for all in-sync replicas; `acks=1` waits for the leader only; `acks=0` fires and forgets.

**AQE (Adaptive Query Execution)**: a Spark feature (3.0+) that re-optimises a query plan at runtime based on actual partition statistics observed during execution. Fixes skewed joins and over-partitioned shuffles automatically.

**At-least-once delivery**: a guarantee that every message is delivered at least once. Duplicates are possible. Contrast with at-most-once and exactly-once.

**At-most-once delivery**: a guarantee that a message is delivered at most once. Loss is possible but duplicates are not.

---

## B

**Backpressure**: a mechanism by which a slow consumer signals upstream producers to slow down. Flink has native backpressure; without it, fast producers fill memory buffers until they crash.

**Broadcast join**: a Spark join strategy where the smaller table is sent to every executor. Avoids a shuffle. Controlled by `spark.sql.autoBroadcastJoinThreshold`.

**Bucket**: a pre-partitioned file layout in Spark/Hive where rows are written to a fixed number of buckets by hash. Eliminates shuffle for JOIN and GROUP BY on the bucket key.

---

## C

**CAP theorem**: a distributed systems theorem stating you can have at most two of: Consistency, Availability, Partition tolerance. During a network partition, you must choose between consistency (return error) and availability (return stale data).

**Cardinality**: the number of distinct values in a column or label set. High cardinality (millions of distinct user IDs) causes issues in TSDBs and certain indexing strategies.

**CDC (Change Data Capture)**: capturing every insert, update, and delete from a database as a stream of change events. Debezium is the standard CDC tool for relational databases.

**Checkpoint**: a consistent snapshot of Flink operator state written to durable storage. Enables recovery from failures without replaying the entire input.

**Columnar storage**: storing data column-by-column rather than row-by-row. Analytical queries that read a few columns out of many benefit enormously (read only what you need).

**Compaction**: merging many small files into fewer large files in a lakehouse or ClickHouse. Reduces read amplification and query latency. Controlled by Iceberg's `rewrite_data_files` procedure or ClickHouse's OPTIMIZE TABLE.

**Consumer group**: a set of Kafka consumers sharing a group ID. Each partition is consumed by exactly one member of the group. Enables parallel consumption and horizontal scaling.

---

## D

**DAG (Directed Acyclic Graph)**: a graph with directed edges and no cycles. Spark represents query plans as DAGs. Airflow represents workflow dependencies as DAGs.

**Data skew**: uneven distribution of data across partitions. One partition has significantly more data than others, causing one task/worker to be the bottleneck.

**Exactly-once semantics**: a guarantee that each event is processed exactly once, even in the presence of failures. Requires coordination between source, processor, and sink.

---

## E

**Event time**: the timestamp when an event actually occurred at the source. Contrast with processing time.

**Exchange**: in Trino/Spark, an operation that moves data between workers/executors. Equivalent to a shuffle. The most expensive operation in distributed query execution.

---

## F

**Fan-out**: one message triggering multiple downstream actions or copies. In Kafka, multiple consumer groups each reading the same topic is a fan-out pattern.

**Fault tolerance**: the ability of a system to continue operating when some components fail. Kafka achieves this via replication; Flink via checkpoints; Spark via RDD lineage.

---

## G

**Granule**: ClickHouse's unit of indexing. A granule is ~8192 rows. The sparse primary index stores one entry per granule, not per row. Queries skip granules that don't match the WHERE clause.

---

## H

**Hot partition**: a Kafka partition that receives significantly more traffic than others. Causes one consumer to fall behind while others are idle.

**Hyptertable**: TimescaleDB's time-partitioned table abstraction. Automatically splits data into chunks by time range.

---

## I

**Idempotent**: an operation that produces the same result regardless of how many times it is applied. Essential for safe retries in data pipelines.

**In-sync replicas (ISR)**: the set of Kafka replicas that are fully caught up with the partition leader. Acks are only confirmed once all ISR members acknowledge.

**Iceberg snapshot**: a point-in-time state of an Iceberg table. Every commit creates a new snapshot. Time travel queries a specific snapshot.

---

## L

**Lag**: in Kafka, the difference between the latest offset produced and the latest offset committed by a consumer. Growing lag means the consumer is falling behind.

**Late event**: in stream processing, an event that arrives after the watermark has passed the event's timestamp. Flink allows configuring an allowed lateness window before dropping late events.

**Log compaction**: a Kafka retention mode that keeps only the latest value for each key in a topic. Used for changelog or CDC topics where you want current state, not full history.

---

## M

**Manifest file**: in Iceberg, a file listing the data files in a snapshot along with their statistics. Used for partition pruning and skipping.

**MergeTree**: ClickHouse's primary table engine family. Data is written as immutable parts and merged in the background. The ORDER BY clause determines the sort key and sparse index.

**Micro-batch**: Spark Structured Streaming's approach to streaming — collect events over a short interval (100ms to 30s) and process as a batch. Lower latency than batch, higher than event-by-event streaming.

---

## O

**Offset**: a sequential ID for each message within a Kafka partition. Consumers track which offset they have processed.

**OLAP (Online Analytical Processing)**: analytical workloads that scan large amounts of data for aggregations, filtering, and GROUP BY. Designed for read performance over write performance.

---

## P

**Partition pruning**: the query optimizer skipping partitions that cannot contain rows matching the WHERE clause. Works for both Spark/Iceberg physical partitions and ClickHouse granules.

**Predicate pushdown**: pushing filter conditions (WHERE clauses) down to the storage layer so only matching rows are read. Reduces I/O significantly in columnar systems.

**Processing time**: the timestamp when an event is processed by the stream processor. May be much later than event time due to network delays or upstream delays.

---

## R

**Rebalance**: when Kafka reassigns partitions to consumers in a consumer group. Triggered by consumer joins, leaves, or crashes. During rebalance, consumption stops.

**Replication factor**: how many copies of each Kafka partition are maintained across brokers. `replication.factor=3` means 3 copies, tolerable to lose 2 brokers.

**Retention**: how long data is kept. Kafka retention is time-based or size-based per topic. Iceberg tables can use TTL or expiration procedures. ClickHouse uses TTL expressions.

---

## S

**Savepoint**: a manually triggered, user-initiated Flink checkpoint. Used for planned maintenance, version upgrades, or job migration. Unlike checkpoints, savepoints are not deleted automatically.

**Schema evolution**: changing the structure of a table (adding/removing/renaming columns) without breaking existing readers or writers.

**Shuffle**: redistributing data across workers/partitions by hash of a key. Required for GROUP BY, JOIN, and DISTINCT in distributed systems. The most expensive operation.

**Skipping index**: in ClickHouse, a secondary index that stores min/max, bloom filter, or other statistics per granule. Allows skipping granules that cannot match a WHERE clause.

**Snapshot isolation**: a transaction isolation level where each read sees a consistent snapshot of data as of a point in time, without blocking writers.

**Star-tree index**: a Pinot pre-aggregated index that stores rollup combinations of dimensions. Dramatically speeds up fixed-dimension aggregation queries at high concurrency.

---

## T

**Tombstone**: in Kafka log-compacted topics, a message with a null value that signals a key should be deleted from the compacted log.

**Tungsten**: Spark's off-heap memory management and code generation layer. Bypasses JVM GC for large binary operations. Part of the Spark optimizer stack.

---

## W

**Watermark**: in stream processing, a marker indicating that all events with timestamps earlier than the watermark value have arrived (or are assumed to have arrived). Used to trigger window computations with event time.

**Write-ahead log (WAL)**: a log where changes are recorded before being applied to the main storage. Enables recovery to a consistent state after failure. Used by PostgreSQL, Kafka, and others.

---

## Z

**Z-order**: a space-filling curve that interleaves bits from multiple dimensions. Used by Delta Lake OPTIMIZE ZORDER and ClickHouse to co-locate rows with similar values across multiple columns, improving multi-dimensional filter performance.
