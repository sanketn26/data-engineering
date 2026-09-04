# Graph Modelling

## The Hardest Part of Graph Databases

> The hardest part of graph databases is often the model, not Cypher syntax.

A poorly designed graph model leads to:
- Queries that are slow despite the "graph advantage"
- Supernodes that make traversals catastrophically slow
- Models that cannot express the queries the business needs

---

## Model the Domain, Not the Schema

Bad approach: convert your relational tables directly to nodes.

```
# Bad: treating join tables as nodes
User → [Transaction] → Merchant
```

Better approach: ask "what questions will I ask?" and model for those.

If the primary question is "which users are connected to which merchants via transactions?", the relationship should be direct:

```
(User)-[:TRANSACTED_WITH {amount, timestamp}]->(Merchant)
```

If you also need to query transaction details, keep them as node properties or a separate Transaction node.

---

## Multiple Valid Models for the Same Domain

Consider: a user makes a transaction at a merchant, using a specific card, from a specific device.

### Model A: Transaction as a Relationship

```
(User)-[:PAID {amount: 100, card_id: "c123", device_id: "d456"}]->(Merchant)
```

- Fast: "show me all merchants User A paid"
- Harder: "find all transactions where card_id = 'c123'" (must scan all PAID relationships)

### Model B: Transaction as a Node

```
(User)-[:MADE]->(Transaction {amount, timestamp})-[:AT]->(Merchant)
(Transaction)-[:WITH]->(Card)
(Transaction)-[:FROM]->(Device)
```

- Slower hop: "merchants paid by User A" requires two hops
- But: "all transactions with Card c123" is just `MATCH (c:Card)<-[:WITH]-(t:Transaction)`
- More flexible: add properties to Transaction as needed

**The right model depends on your queries.** There is no universally correct answer.

---

## Supernodes: The Silent Killer

A supernode is a node with an extremely high degree (many relationships).

Example: a celebrity user who appears in every "popular content" recommendation. Or an IP address used by a VPN with millions of users.

```
(VPN IP: 203.0.113.100) ← CONNECTED_FROM — (Transaction_1)
                        ← CONNECTED_FROM — (Transaction_2)
                        ← CONNECTED_FROM — (Transaction_3)
                        ...
                        ← CONNECTED_FROM — (Transaction_50,000,000)
```

Any traversal that touches this node must scan 50 million relationships. A query that was 10ms without the supernode takes minutes with it.

**Solutions**:
- Limit traversal depth explicitly (`[:CONNECTED_FROM*1..3]` not `*`)
- Detect and flag supernodes, handle them separately
- Redesign the model to avoid the supernode

---

## Relationship Direction and Types

Always give relationships **explicit, meaningful directions**:

```
(User)-[:MADE]->(Transaction)  ← User makes a transaction
(Transaction)-[:AT]->(Merchant)  ← Transaction occurs at a merchant
(User)-[:USES]->(Device)  ← User uses a device
```

Direction enables efficient lookups:
- "Find all transactions made by User X" → follow outgoing `[:MADE]` from User
- "Find all users who used Device Y" → follow incoming `[:USES]` to Device

Use descriptive relationship types. `[:RELATED_TO]` is useless. `[:USES]`, `[:TRANSACTED_WITH]`, `[:CONNECTED_FROM]` are clear.

---

## Practical Modelling Checklist

1. List the top 5–10 queries you need to answer
2. For each query, identify the traversal pattern
3. Design the model so each query is at most 3–5 hops
4. Identify potential supernodes and plan for them
5. Consider relationship properties for attributes that belong to the connection, not the entities
