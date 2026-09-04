# Start Here

Before you open the first lesson, understand what this academy is and is not.

---

## What This Is

A serious engineering resource for people who want to understand how data systems actually work at scale — not memorise feature lists, not pass a certification, not build a demo.

The objective:

> Given a data workload — its scale, latency requirements, access patterns, reliability requirements, and cost constraints — you can derive an appropriate architecture, choose sensible technologies, explain their trade-offs, predict how they will fail, debug them in production, and evolve the architecture as scale increases.

---

## What This Is Not

- It is not a tutorial for beginners
- It is not a documentation mirror
- It is not a "learn tool X in N days" course
- It is not a checklist to memorise for interviews

---

## The Founding Intuition

Every major data technology was built because someone had a problem they could not solve with existing tools.

Kafka was not invented because "microservices need messaging." It was invented because databases were not designed to handle millions of writes per second with decoupled producers and consumers and durable replay.

ClickHouse was not invented because queries should be fast. It was invented because analytical workloads that scan billions of rows for aggregations perform terribly on row-oriented storage.

Flink was not invented because streaming is exciting. It was invented because systems need to reason about time — event time, not processing time — and maintain consistent state across distributed workers under failure.

If you understand *why* these systems exist, you will be able to reason about systems you have never used before.

---

## Five Running Production Systems

Throughout the entire academy, you will work with five fictional but realistic production systems. The same datasets flow through different technologies so you can compare them directly.

### System A — SaaS Analytics Platform

Millions of users generate product events:

```
{timestamp, customer_id, user_id, service, endpoint, region, latency_ms, status_code, bytes}
```

You will see this workload in Kafka (ingestion), Spark (transformation), Iceberg (historical storage), Trino (ad-hoc querying), and ClickHouse (dashboard queries).

### System B — Security / Observability Platform

Billions of events per day: logs, metrics, traces, security events. High ingestion rate, high cardinality, late events, real-time detection, historical investigation.

### System C — E-Commerce Platform

Orders, payments, users, inventory, clickstream, recommendations. CDC, batch and streaming, lakehouse, graph relationships.

### System D — IoT Platform

Millions of devices sending sensor readings every 30 seconds. Time series, windows, downsampling, retention.

### System E — Fraud Graph

Users → Devices → IPs → Transactions → Merchants. Graph traversal, connected components, fraud ring detection.

---

## The Teaching Loop

Every lesson follows this progression:

1. **USE CASE** — what concrete problem are we solving?
2. **WHY** — why is this problem hard at scale?
3. **INTUITION** — what mental model makes the solution feel obvious?
4. **WHAT** — what concept or technology addresses it?
5. **INTERNALS** — what actually happens inside the system?
6. **ARCHITECTURE** — where does it live in a real system?
7. **HOW** — how do I build or use it?
8. **GOTCHAS** — where do experienced engineers still get this wrong?
9. **FAILURE MODES** — how does it break in production?
10. **DEBUGGING** — how would I investigate that failure?
11. **SCALE** — what changes at 10×, 100×, 1000× scale?
12. **TRADE-OFFS** — what am I giving up by choosing this?
13. **ALTERNATIVES** — when would a different approach be better?
14. **HOW TO APPLY** — how do I recognise this pattern at work?
15. **EXERCISE** — can I reason through a novel problem?

---

## Before You Proceed

Ask yourself: *can I currently answer these questions without looking anything up?*

- Why is data partitioned in the first place?
- What creates a shuffle in a distributed computation?
- Why does consumer lag increase?
- What is the difference between event time and processing time?
- Why does ClickHouse sort data on disk?
- When would graph modelling outperform relational modelling?

If most of these feel unclear, start with [Phase 0: Foundations](foundations/index.md).

If you are comfortable with distributed systems fundamentals, jump to the technology most relevant to your current work.

---

→ [How to Study](how-to-study.md)
→ [Phase 0: Foundations](foundations/index.md)
