# Apache Kafka

## The Problem

Hundreds of services generate events. Some generate thousands per second. Downstream services need to process those events — but at their own pace, with their own retry logic, without being coupled to the producers.

How do producers and consumers evolve independently without losing data?

---

## What This Module Covers

| Topic | What You Will Learn |
|-------|-------------------|
| [The Log Abstraction](log.md) | Why Kafka uses an append-only log |
| [Partitions & Consumers](partitions.md) | How parallelism and ordering work |
| [Replication & Durability](replication.md) | How Kafka survives broker failures |
| [Exactly-Once Semantics](exactly-once.md) | Idempotent producers and transactions |
| [Production Gotchas](gotchas.md) | Hot partitions, consumer lag, rebalance storms |
| [Labs](labs.md) | Hands-on: producer, consumer, lag, failure |

---

## The Central Intuition

Kafka is not a message queue. It is a **distributed, replicated, append-only log**.

Producers append records to the log. Consumers read from a position in the log. The log is retained for a configurable duration. Multiple consumers can read the same records independently, at different speeds, from different positions.

This architecture decouples producers from consumers in a way that traditional queues cannot.

---

## Running Use Case

The **Security / Observability Platform** generates:
- Application logs: 500,000 lines/sec
- Metrics: 2,000,000 data points/sec
- Security events: 50,000/sec

Total: ~2.5 million records per second. This needs to be ingested without data loss, processed by multiple downstream systems (alerting, storage, enrichment), and retained for replay if downstream systems fail.
