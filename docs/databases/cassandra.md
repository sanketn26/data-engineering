# Cassandra & ScyllaDB

## The Problem

You need to write 500,000 records per second across multiple regions, with no single point of failure, and each write must be confirmed in <10ms.

No relational database handles this well.

---

## How Cassandra Works

Cassandra uses **consistent hashing** to distribute data across nodes with no master node. Every node can accept reads and writes.

Data is partitioned by a **partition key**. The hash of the partition key determines which node "owns" that data. With a replication factor of 3, three nodes store each partition.

### Writes Are Fast

Cassandra writes go to:
1. A commit log (sequential disk write — fast)
2. A MemTable (in-memory structure)

When the MemTable fills, it is flushed to an immutable SSTable on disk. Background compaction merges SSTables.

There is no B-tree to maintain, no locking, no index updates on write.

### Reads Are Slower

A read must check multiple SSTables (and the MemTable). Bloom filters help skip SSTables that definitely don't contain the key.

This is the fundamental Cassandra trade-off: **write-optimised, read is more complex**.

---

## Data Modelling: Query-First

Cassandra's data model must be designed around your queries. You cannot add ad-hoc queries after the fact.

**Step 1**: List all queries you need to answer.
**Step 2**: Design one table per query pattern.

This is the opposite of relational design (normalise first, query with JOINs).

```sql
-- Query: "Get all events for user X in the last hour"
CREATE TABLE user_events (
    user_id UUID,
    timestamp TIMESTAMP,
    event_type TEXT,
    data TEXT,
    PRIMARY KEY (user_id, timestamp)
) WITH CLUSTERING ORDER BY (timestamp DESC);
```

The partition key is `user_id` — all events for one user are on the same node, enabling efficient single-partition reads.

---

## ScyllaDB

ScyllaDB is a drop-in Cassandra replacement written in C++ (not Java). Benefits:
- Significantly lower latency (no JVM GC pauses)
- Higher throughput per node
- Compatible with Cassandra drivers and CQL

For new deployments, ScyllaDB is often preferred over Cassandra for performance-sensitive workloads.
