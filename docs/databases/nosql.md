# NoSQL Databases

## When Relational Is Not Enough

Relational databases are excellent for structured data with well-defined schemas, transactional requirements, and moderate scale. They struggle when:

- Write throughput exceeds what a single-leader database can handle
- Schema is dynamic or semi-structured
- The access pattern is always by primary key (no complex queries needed)
- Global distribution with low-latency reads everywhere is required

NoSQL databases make specific trade-offs to address these limitations.

---

## The NoSQL Taxonomy

### Key-Value: Redis, DynamoDB

**Problem**: "I know the key and need the value immediately."

Access pattern: GET/SET by primary key only. No joins. No range scans (or limited).

```python
# Redis
redis_client.set("user:u001:session", json.dumps(session_data), ex=3600)
session = json.loads(redis_client.get("user:u001:session"))
```

**Strengths**: microsecond latency, trivially scalable, simple mental model.

**Weaknesses**: no ad-hoc queries, data model must anticipate all access patterns.

---

### Document: MongoDB

**Problem**: "My application data naturally belongs together and evolves structurally."

Documents (JSON-like) group related data:

```json
{
  "_id": "order_001",
  "customer": {"id": "c001", "name": "Alice"},
  "items": [
    {"product_id": "p001", "qty": 2, "price": 29.99},
    {"product_id": "p002", "qty": 1, "price": 49.99}
  ],
  "total": 109.97,
  "status": "shipped"
}
```

Whole order in one read. No JOIN required.

**Strengths**: flexible schema, natural fit for hierarchical data, good for heterogeneous data.

**Weaknesses**: no transactions across documents (historically), can lead to data duplication, eventual consistency in some configurations.

---

### Wide-Column: Cassandra, ScyllaDB, HBase

**Problem**: "I need extremely high distributed writes and I know my query patterns."

Data is organised by a **partition key** (row distribution) and **clustering key** (row ordering within partition).

```sql
-- Cassandra CQL
CREATE TABLE user_events (
    user_id UUID,
    timestamp TIMESTAMP,
    event_type TEXT,
    data TEXT,
    PRIMARY KEY (user_id, timestamp)
) WITH CLUSTERING ORDER BY (timestamp DESC);
```

Reads are fast only if they match the primary key pattern. Cassandra cannot efficiently answer "find all events where event_type = 'purchase'" — it would require a full table scan.

**Strengths**: extremely high write throughput, multi-region active-active, linear scalability.

**Weaknesses**: you must design your schema around your queries. Secondary indexes are expensive. Not suitable for ad-hoc queries.

---

## CAP Theorem

A distributed system can guarantee at most two of three:
- **Consistency**: all nodes see the same data at the same time
- **Availability**: every request receives a response
- **Partition tolerance**: system continues operating despite network partitions

In practice, partition tolerance is non-negotiable (networks fail). So the real choice is:

- **CP** (Consistent + Partition tolerant): return error or block during partition. Examples: HBase, ZooKeeper.
- **AP** (Available + Partition tolerant): return potentially stale data during partition. Examples: Cassandra (tunable), DynamoDB (default).

---

## PACELC Extension

CAP only addresses partitions. PACELC adds the latency-consistency trade-off when the system is functioning normally:

> If Partition: choose A or C. Else (normal operation): choose L (latency) or C (consistency).

DynamoDB: PA/EL — Available during partition, Low latency normally (with eventual consistency).
HBase: PC/EC — Consistent during partition, Consistent normally (at higher latency).
