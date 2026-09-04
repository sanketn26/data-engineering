# How to Study

This material is dense. Here is how to use it effectively.

---

## Do Not Read Passively

Every page contains questions embedded in the text. Stop when you see them. Think before reading the answer. The questions are not rhetorical — they are the actual reasoning you will need in production.

Example:

> What happens if one customer generates 40% of all events in a partitioned Kafka topic?

Before reading further: think. Where does that imbalance show up? Which consumer gets overloaded? What metric would alert you?

*Then* read the explanation.

---

## Follow the Reasoning, Not the Tool

When studying ClickHouse, do not try to memorise the list of table engines. Instead, understand *why* ClickHouse writes immutable parts and merges them in the background. Once you understand that, most of the operational consequences follow automatically.

The question to ask about any concept: *"What problem forced someone to design it this way?"*

---

## Run the Labs

Reading about shuffle is not the same as watching one slow task dominate a Spark job execution graph. The hands-on labs are not optional supplements — they are where the intuition becomes visceral.

Every lab uses Docker Compose and synthetic datasets. You do not need a cloud account or a cluster. You need a laptop.

---

## Use the Recommended Progression

If you are new to data systems internals, follow the phases in order:

1. [Foundations](foundations/index.md) — build the mental models before touching any tool
2. [Kafka](kafka/index.md) + [Spark](spark/index.md) — the core data plane
3. [Flink](flink/index.md) — stream processing and time semantics
4. [Lakehouse](lakehouse/index.md) — storage and table formats
5. [Trino](query-engines/trino.md) + [ClickHouse](olap/clickhouse.md) — query engines
6. Specialised: [Time Series](time-series/index.md), [Graph](graph/index.md)
7. [Ray](distributed-python/ray.md) — distributed Python
8. Platform: [Airflow](airflow/index.md), [Metadata](metadata/index.md), [Quality](quality/index.md)
9. [Architectures](architectures/index.md) — end-to-end system design

If you are experienced and targeting a specific gap, jump directly.

---

## Treat Every "Production Is On Fire" Section Seriously

These are incident scenarios with real telemetry. Your job is to form a hypothesis before looking at the resolution. These scenarios reflect the kind of reasoning that separates engineers who can operate systems from engineers who can only build demos.

---

## The Standard

You have understood a concept when you can explain *why someone had to invent it*, not just what it does.
