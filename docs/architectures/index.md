# Architectures

End-to-end system designs for the five running use cases.

---

## The Purpose of Architecture Pages

These pages are not "here is the one correct architecture." They are:

1. A demonstration of how individual technologies combine into systems
2. A worked example of the technology selection framework
3. A scaling exercise: what changes at 1 GB/day vs 1 PB/day?

Use them to practice architectural reasoning before encountering these decisions at work.

---

## The Five Systems

| Architecture | Technologies | Key Challenges |
|-------------|-------------|----------------|
| [Observability Platform](observability.md) | Kafka, Flink, ClickHouse, Iceberg, Trino | Scale, cardinality, retention tiers |
| [E-Commerce Platform](ecommerce.md) | CDC, Kafka, Lakehouse, Graph DB | Consistency, CDC, multi-modal data |
| [IoT Platform](iot.md) | Kafka, TSDB, downsampling | High-frequency writes, time windows |
| [Fraud Detection](fraud.md) | Kafka, Flink, Neo4j, ClickHouse | Graph traversal, real-time scoring |
| [SaaS Analytics](analytics-platform.md) | Full stack | Multi-tenancy, cost, scale |
