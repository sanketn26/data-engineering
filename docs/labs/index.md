# Labs

Hands-on exercises using Docker Compose and synthetic data. You do not need a cloud account.

---

## Prerequisites

- Docker and Docker Compose installed
- 8GB RAM recommended (16GB for multi-service labs)
- Python 3.9+

---

## Labs by Module

### Foundations
No dedicated lab — concepts are exercised within tool-specific labs.

### Spark
- [Spark Labs](../spark/labs.md) — shuffle observation, skew detection, join strategies, partition tuning

### Kafka
- [Kafka Labs](../kafka/labs.md) — producer/consumer, consumer lag, broker failure simulation

### Flink
- [Flink Labs](../flink/labs.md) — watermarks, windows, stateful processing, checkpoints

### Lakehouse
Coming soon — Iceberg time travel, schema evolution, compaction

### Query Engines
Coming soon — Trino federation, explain plans

### OLAP
Coming soon — ClickHouse ORDER BY comparison, sparse index visualisation

### Time Series
Coming soon — TimescaleDB continuous aggregates, downsampling pipeline

### Graph
Coming soon — Neo4j fraud ring detection, algorithm performance comparison

---

## Lab Structure

Every lab follows the same pattern:

1. **Start the environment**: `docker compose up -d`
2. **Run the data generator**: creates synthetic events matching the five production systems
3. **Execute the exercise**: follow the numbered steps
4. **Observe the outcome**: check the metrics, explain plans, or visualisations
5. **Break something**: each lab has a "now cause this failure" section

---

## Shared Datasets

All labs use the same synthetic schemas from the five running production systems:

**System A events (SaaS Analytics)**:
```json
{
  "timestamp": "2024-01-15T10:30:00Z",
  "customer_id": "cust_0042",
  "user_id": "user_98712",
  "service": "api-gateway",
  "endpoint": "/v2/events",
  "region": "eu-west-1",
  "latency_ms": 45,
  "status_code": 200,
  "bytes": 1024
}
```

The same dataset flows through every tool so you can compare how Spark, Flink, ClickHouse, and Trino each handle the same workload.

---

## Getting the Lab Files

Labs are in the `labs/` directory at the root of this repository. Each lab has its own subdirectory with a `docker-compose.yml` and a `README.md` with step-by-step instructions.
