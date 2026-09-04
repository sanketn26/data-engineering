# Neo4j & Cypher

## Getting Started

```bash
# Run Neo4j locally
docker run -p 7474:7474 -p 7687:7687 \
  -e NEO4J_AUTH=neo4j/password \
  neo4j:5
```

Open `http://localhost:7474` for the browser UI.

---

## Cypher Basics

Cypher is Neo4j's query language. Its syntax visually matches graph structure.

### Creating Nodes

```cypher
// Create nodes
CREATE (alice:User {user_id: 'u001', name: 'Alice', email: 'alice@example.com'})
CREATE (bob:User {user_id: 'u002', name: 'Bob'})
CREATE (iphone:Device {device_id: 'd001', type: 'iPhone', fingerprint: 'abc123'})
CREATE (ip1:IP {ip_id: 'ip001', address: '203.0.113.42'})
CREATE (merchant1:Merchant {merchant_id: 'm001', name: 'CoffeeShop'})
```

### Creating Relationships

```cypher
MATCH (alice:User {user_id: 'u001'})
MATCH (iphone:Device {device_id: 'd001'})
CREATE (alice)-[:USES]->(iphone)
```

### Pattern Matching

```cypher
// All users who use a specific device
MATCH (u:User)-[:USES]->(d:Device {device_id: 'd001'})
RETURN u.name, u.user_id

// All devices used by Alice
MATCH (u:User {name: 'Alice'})-[:USES]->(d:Device)
RETURN d.type, d.fingerprint
```

### Filtering

```cypher
MATCH (u:User)-[:MADE]->(t:Transaction)-[:AT]->(m:Merchant)
WHERE t.amount > 1000
  AND t.timestamp > datetime('2024-01-01')
RETURN u.name, m.name, t.amount
ORDER BY t.amount DESC
LIMIT 10
```

### Variable-Length Traversal

```cypher
// Find all users within 3 hops of a suspicious IP
MATCH (ip:IP {address: '203.0.113.42'})
     <-[:ORIGINATED_FROM]-(t:Transaction)
     <-[:MADE]-(u:User)
MATCH path = (u)-[*1..3]-(suspicious:User)
RETURN DISTINCT suspicious.user_id, suspicious.name
```

### Aggregation

```cypher
// Top merchants by transaction volume
MATCH (t:Transaction)-[:AT]->(m:Merchant)
RETURN m.name, count(t) as txn_count, sum(t.amount) as total_volume
ORDER BY total_volume DESC
LIMIT 10
```

### Shortest Path

```cypher
// Shortest path between two users
MATCH (a:User {user_id: 'u001'}), (b:User {user_id: 'u999'})
MATCH path = shortestPath((a)-[*]-(b))
RETURN path, length(path) as hops
```

---

## Indexes and Constraints

Always create indexes on frequently queried properties:

```cypher
// Create constraint (also creates index)
CREATE CONSTRAINT user_id_unique FOR (u:User) REQUIRE u.user_id IS UNIQUE;
CREATE CONSTRAINT device_id_unique FOR (d:Device) REQUIRE d.device_id IS UNIQUE;

// Regular index (non-unique)
CREATE INDEX ip_address FOR (ip:IP) ON (ip.address);
```

Without indexes, `MATCH (u:User {user_id: 'u001'})` scans all User nodes.

---

## EXPLAIN and PROFILE

```cypher
EXPLAIN MATCH (u:User {user_id: 'u001'})-[:USES]->(d:Device)
RETURN d
```

Look for:
- `NodeIndexSeek` — uses an index (good)
- `AllNodesScan` — scans all nodes (bad, add an index)
- `Expand` — follows relationships

```cypher
PROFILE MATCH (u:User {user_id: 'u001'})-[:USES]->(d:Device)
RETURN d
```

PROFILE executes the query and shows actual row counts at each step. Use it to identify where most work is happening.

---

## Python Driver

```python
from neo4j import GraphDatabase

driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "password"))

with driver.session() as session:
    # Create fraud detection graph
    session.run("""
        CREATE (u:User {user_id: $uid, name: $name})
        CREATE (d:Device {device_id: $did, fingerprint: $fp})
        CREATE (u)-[:USES]->(d)
    """, uid="u001", name="Alice", did="d001", fp="abc123")

    # Find connected users
    result = session.run("""
        MATCH (ip:IP {address: $addr})<-[:ORIGINATED_FROM]-(:Transaction)<-[:MADE]-(u:User)
        RETURN u.user_id, u.name
    """, addr="203.0.113.42")

    for record in result:
        print(record["u.name"])
```

---

## Production Gotchas

!!! warning "Unbounded Traversal"
    `MATCH (u)-[*]-()` without a depth limit will traverse the entire graph. Always specify depth: `[*1..5]`.

!!! warning "Supernodes"
    An IP used by a large VPN may have millions of incoming relationships. Any query that touches it will be slow. Detect supernodes (`MATCH (n) RETURN n, degree(n) ORDER BY degree(n) DESC LIMIT 20`) and handle them specially.

!!! warning "Cartesian Products"
    ```cypher
    MATCH (u:User), (d:Device)  -- no relationship specified
    RETURN u, d
    ```
    This creates a cartesian product — millions of rows. Always specify the relationship pattern in MATCH.
