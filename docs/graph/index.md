# Graph Databases

## When Relationships Become First-Class

Most data systems treat relationships as foreign keys — an afterthought. When your core business problem is about traversing relationships — fraud networks, recommendation graphs, access control hierarchies — a graph database makes the access pattern first-class.

---

## What This Module Covers

| Topic | What You Will Learn |
|-------|-------------------|
| [Graph Thinking](graph-thinking.md) | When to think in graphs, and when not to |
| [Graph Modelling](graph-modelling.md) | How to design a good graph schema |
| [Neo4j & Cypher](neo4j.md) | Practical graph database usage |
| [Graph Algorithms](graph-algorithms.md) | BFS, shortest path, PageRank, community detection |
| [Graph vs Relational](graph-vs-relational.md) | When PostgreSQL is better |

---

## Running Use Case

The **Fraud Detection Platform**:

```
Users → Devices → IPs → Transactions → Merchants
```

Analysts need to identify fraud rings: clusters of accounts connected through shared devices, IPs, or merchants. Multi-hop traversal across millions of entities.

Also:
- **IAM hierarchy**: User → Role → Permission → Resource
- **Data lineage**: Dataset → Job → Dataset → Dashboard
- **Recommendations**: User → Product → User
