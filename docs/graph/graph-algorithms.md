# Graph Algorithms

## Why Algorithms Matter

Storing a graph is easy. Extracting insight from it requires algorithms.

Each algorithm solves a specific class of business problem. Understanding which algorithm to apply to which problem is the practical skill.

---

## Breadth-First Search (BFS)

**What**: explore all nodes at distance 1, then all at distance 2, then 3...

**Business problem**: "Find all accounts reachable from this suspicious account within 3 hops."

```cypher
// Neo4j's variable-length traversal uses BFS internally
MATCH path = (start:User {user_id: 'u001'})-[:CONNECTED_TO*1..3]-(reachable)
RETURN DISTINCT reachable.user_id, length(path) as distance
```

BFS guarantees finding the shortest path. It explores layer by layer.

---

## Depth-First Search (DFS)

**What**: follow one path as deep as possible before backtracking.

**Business problem**: "Find any path between two accounts." Less useful for finding shortest paths than BFS.

---

## Shortest Path

**What**: find the minimum-length path between two nodes.

**Business problem**: "How many hops separate these two accounts? Are they directly connected?"

```cypher
MATCH (a:User {user_id: 'u001'}), (b:User {user_id: 'u999'})
MATCH path = shortestPath((a)-[*]-(b))
RETURN length(path)
```

**Weighted shortest path** (Dijkstra): when relationships have weights (e.g., transaction amounts, similarity scores), finds the minimum-weight path.

---

## Connected Components

**What**: find all groups of nodes that are connected to each other but not to nodes outside the group.

**Business problem**: "Find all fraud rings — clusters of accounts that share devices, IPs, or merchants."

```cypher
// Using Neo4j Graph Data Science library
CALL gds.wcc.stream('fraud-graph')
YIELD nodeId, componentId
RETURN gds.util.asNode(nodeId).user_id AS userId, componentId
ORDER BY componentId
```

If Component 42 contains 500 accounts, all 500 are connected through some chain of relationships. That is a fraud ring.

---

## PageRank

**What**: rank nodes by the importance of their incoming connections. Originally designed for web pages.

**Business problem**: "Which users are most influential in our social graph? Which merchants are most connected?"

PageRank assigns high scores to nodes that are pointed to by other high-scoring nodes. A node connected to many important nodes gets a high score.

```cypher
CALL gds.pageRank.stream('social-graph')
YIELD nodeId, score
RETURN gds.util.asNode(nodeId).name AS name, score
ORDER BY score DESC LIMIT 10
```

---

## Centrality Metrics

**Betweenness centrality**: how often does a node appear on the shortest path between other nodes? High betweenness = a bridge node. Removing it disconnects parts of the graph.

**Closeness centrality**: how close is a node to all other nodes? High closeness = can reach others quickly.

**Business problem**: "Find the key connectors in a fraud ring — the nodes whose removal would break up the ring."

---

## Community Detection (Louvain)

**What**: partition the graph into communities that have denser connections internally than externally.

**Business problem**: "Identify customer segments based on shared behaviour, without predefined labels."

```cypher
CALL gds.louvain.stream('customer-graph')
YIELD nodeId, communityId
RETURN communityId, count(*) as members
ORDER BY members DESC
```

Louvain optimizes **modularity** — the degree to which connections are denser within communities than would be expected by chance.

---

## Practical Application

| Business Question | Algorithm |
|------------------|-----------|
| "Are these two accounts connected?" | BFS/Shortest Path |
| "How many hops apart are they?" | Shortest Path |
| "Find all accounts in this fraud ring" | Connected Components |
| "Who are the key connectors?" | Betweenness Centrality |
| "Which users are most influential?" | PageRank |
| "Find customer segments organically" | Community Detection (Louvain) |
| "Recommend products based on similarity" | Graph embeddings, collaborative filtering on graph |

---

## Graph Algorithms at Scale

Neo4j's **Graph Data Science** library runs algorithms on in-memory graph projections — loading the relevant subgraph into memory for efficient computation.

For very large graphs (billions of edges), consider:
- **GraphX** (Spark): distributed graph processing, for Pregel-style algorithms
- **DGL** (Deep Graph Library): for graph neural networks
- **NetworkX** (Python): for small graphs in analysis scripts

In the fraud detection use case: run Connected Components to identify fraud rings, then use PageRank within each ring to identify the key accounts.
