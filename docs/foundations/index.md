# Phase 0: Foundations

Before learning Kafka, Spark, Flink, or ClickHouse, you need mental models that those technologies build upon.

Engineers who skip foundations often learn tools in isolation and struggle to reason about systems they haven't encountered before. Engineers who understand foundations can approach an unfamiliar technology and quickly form accurate hypotheses about how it works.

---

## What You Will Build Here

| Concept | Why It Matters |
|---------|----------------|
| [Data at Scale](scale.md) | Understanding what breaks, and when, as data grows |
| [Partitioning](partitions.md) | The single most important concept in distributed data systems |
| [Data Movement](data-movement.md) | Why moving data is often the most expensive thing you do |
| [Distributed Execution](distributed-execution.md) | How jobs, stages, tasks, and workers relate |
| [Batch vs Stream](batch-vs-stream.md) | Not a binary choice — a spectrum with real trade-offs |

---

## The Central Insight

Every distributed data system is fundamentally answering the same question:

> **Given more data than one machine can efficiently process, how do we divide the work, execute it in parallel, and combine the results?**

The answers to that question — partitioning, shuffling, checkpointing, replication — appear over and over in every technology in this academy.

Once you understand *why* those mechanisms exist, each new technology becomes a specific implementation of patterns you already understand.
