---
title: Versions & Primary Sources
description: Lab pins, compatibility policy, review date, and authoritative documentation.
---

# Versions & Primary Sources

**Last technical review:** 2026-09-04

Conceptual lessons describe durable mechanisms. Code and configuration are versioned examples; verify them against the primary documentation before production use.

| System | Executable lab baseline | Primary documentation |
|---|---|---|
| Spark | PySpark 3.5.1 | [Spark documentation](https://spark.apache.org/docs/latest/) |
| Kafka | Apache Kafka 3.7.1 | [Kafka documentation](https://kafka.apache.org/documentation/) |
| Flink | Flink/PyFlink 1.18.1 | [Flink documentation](https://nightlies.apache.org/flink/flink-docs-stable/) |
| Airflow | Conceptual/API examples | [Airflow documentation](https://airflow.apache.org/docs/) |
| Iceberg | Format v2 concepts unless stated | [Iceberg documentation](https://iceberg.apache.org/docs/latest/) and [specification](https://iceberg.apache.org/spec/) |
| Hudi | CoW/MoR concepts | [Hudi documentation](https://hudi.apache.org/docs/overview/) |
| Delta Lake | Protocol-dependent features identified in text | [Delta documentation](https://docs.delta.io/latest/) |
| Trino | Conceptual/API examples | [Trino documentation](https://trino.io/docs/current/) |
| ClickHouse | Server 24.8 in lab | [ClickHouse documentation](https://clickhouse.com/docs) |
| Pinot | Conceptual/API examples | [Pinot documentation](https://docs.pinot.apache.org/) |
| Ray | Conceptual/API examples | [Ray documentation](https://docs.ray.io/en/latest/) |
| Cassandra | Conceptual/CQL examples | [Cassandra documentation](https://cassandra.apache.org/doc/latest/) |
| DynamoDB | Conceptual/API examples | [DynamoDB documentation](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/) |
| Neo4j | Conceptual/Cypher examples | [Neo4j documentation](https://neo4j.com/docs/) |
| Prometheus | Capacity must be measured | [Prometheus documentation](https://prometheus.io/docs/) |

## Maintenance policy

- Pin executable labs so output is reproducible.
- Do not call a version “latest” in prose.
- Put version-sensitive caveats next to the example.
- Re-run `make validate` on dependency changes.
- Re-review a module when its lab baseline changes or after 12 months.
- Prefer specifications and official documentation over vendor comparison pages.

These pins are teaching baselines, not recommendations to deploy old releases. Production selection must include support status, security fixes, connector compatibility, and an upgrade plan.
