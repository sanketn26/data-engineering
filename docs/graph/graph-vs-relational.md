# Graph vs Relational

The most important thing to walk away knowing:

> **Graph databases are not better for anything involving relationships. They become valuable when multi-hop traversal and relationship patterns are first-class access patterns.**

Most data has relationships. PostgreSQL handles them perfectly well with foreign keys and JOINs.

---

## When PostgreSQL Is Better

### Case 1: Single-hop lookups

"Find all orders for customer X."

```sql
SELECT * FROM orders WHERE customer_id = 'c001';
```

This is one index lookup. A graph traversal adds no value. PostgreSQL with an index is faster and simpler.

### Case 2: Simple two-table JOINs

"Find all customers who placed orders last month."

```sql
SELECT DISTINCT c.name
FROM customers c
JOIN orders o ON c.customer_id = o.customer_id
WHERE o.order_date >= '2024-01-01';
```

One JOIN. PostgreSQL handles this efficiently. No graph needed.

### Case 3: Aggregation over flat data

"How many orders per country last week?"

```sql
SELECT country, count(*) FROM orders
WHERE order_date >= current_date - 7
GROUP BY country;
```

PostgreSQL or ClickHouse. Graph databases do not add value for aggregation queries.

### Case 4: Most OLTP workloads

Transactional systems with well-defined schemas, primary keys, foreign keys, and ACID requirements: use a relational database. PostgreSQL handles this workload better than any graph database.

---

## When Graph Databases Win

### Case 1: Multi-hop traversal with variable depth

"Find all accounts connected within N hops to this suspicious IP."

With N=3: SQL requires 3 self-joins or CTEs. With N=5: SQL becomes prohibitively complex and slow. Cypher handles this in 2 lines regardless of depth.

### Case 2: Path analysis

"What is the shortest path between Account A and Account B?"

Relational: manually implement Dijkstra or BFS in application code or complex SQL. Graph: built-in `shortestPath()`.

### Case 3: Pattern matching

"Find all users who share a device with another user who also shares an IP with a known fraudster."

This is a specific structural pattern in the graph. Cypher's pattern matching syntax expresses it naturally. SQL does not.

### Case 4: Highly connected data where relationships are often queried

If the schema has many-to-many relationships and queries frequently navigate them, graph database performance advantage grows.

---

## The Decision Framework

Before choosing a graph database, answer these questions:

1. **Do my queries involve traversal of more than 2–3 hops?** If no: use relational.
2. **Do I need to find paths, shortest paths, or connected components?** If no: use relational.
3. **Is the structure of relationships itself interesting, not just their existence?** If no: use relational.
4. **Would expressing the query in SQL require complex recursive CTEs?** If no: use relational.

If you answered "yes" to most of these, a graph database may be justified. It adds operational complexity — a new system to learn, operate, scale, and monitor. Justify it.

---

## Hybrid Architectures

In the fraud detection system, the actual production architecture often uses both:

- **PostgreSQL**: authoritative record of transactions, users, merchants
- **Neo4j**: graph replica built from CDC events, used for graph traversal queries
- **ClickHouse**: aggregated fraud analytics, dashboards

The graph database handles traversal. The relational database handles the transactional record. The OLAP database handles reporting.

Use each tool for what it is good at.
