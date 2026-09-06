---
title: Start Here
description: What this academy is, who it is for, and the five production systems that run through every module.
---

# Start Here

You do not need to understand the whole data stack before beginning. You only
need one familiar workload and the willingness to ask, “what breaks next?” The
course keeps returning to that question until the moving parts feel like
consequences, rather than facts to memorise.

!!! tip "If you have only one hour"
    Read [Data at Scale](foundations/scale.md) through **Build the mental
    picture**, try the short check at the end, and then explore the
    [partitioning simulation](simulations/kafka-partitions.html). That is a
    complete first session. You do not need to configure a cluster today.

## What you will learn

This is a guided course in reasoning about data systems at scale. Product names
appear only after the workload has created a reason for them to exist.

The objective:

> Given a data workload — its scale, latency requirements, access patterns, reliability requirements, and cost constraints — you can derive an appropriate architecture, choose sensible technologies, explain their trade-offs, predict how they will fail, debug them in production, and evolve the architecture as scale increases.

That is the eventual destination, not an expectation for day one. Each lesson
teaches one piece of the reasoning and gives you a small way to check it.

## What you can safely skip at first

- Exact configuration defaults and version-specific syntax
- Optional labs that do not match your learning path
- The **Under the hood** section while the mental picture is still new
- Product comparisons until you understand the workload they are comparing

Come back to those sections on a second pass. Skipping detail temporarily is
sequencing, not a gap in understanding.

## What you should know already

The examples assume you can read basic Python and SQL and have seen a database,
a command line, and JSON. Docker is needed for some labs, but not for the first
pass through any concept.

If production terminology is new, keep the [glossary](reference/glossary.md)
open and take the **Foundations first** route. If you already operate these
systems, choose a shorter route around the gap you want to close.

!!! note "A quick placement check"
    If *offset*, *partition*, *join*, *index*, or *SLA* are unfamiliar, begin
    with [Foundations first](learning-paths.md#foundations-first). The linked
    lessons introduce the operational meaning as it becomes useful.

## The idea that holds the course together

Every major data technology was built because someone had a problem they could not solve with existing tools.

Kafka was not invented because “microservices need messaging.” It was invented because databases were not designed for millions of writes per second with decoupled producers, independent consumers, and durable replay.

ClickHouse was not invented because “queries should be fast.” It was invented because analytical workloads that scan billions of rows for a handful of columns perform terribly on row-oriented storage.

Flink was not invented because streaming is fashionable. It was invented because systems need to reason about **event time**, not processing time, and keep consistent state across workers that crash.

If you understand *why* these systems exist, you can reason about systems you have never used.

## Five stories you will keep revisiting

The same datasets flow through different technologies so each new idea has a
familiar home. You are not expected to memorise all five now. Pick the one
closest to your experience; the others will become useful comparisons later.

### System A — SaaS analytics platform

Millions of users generate product events:

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

You will see this workload in Kafka (ingestion), Spark (transformation), Iceberg (historical storage), Trino (ad-hoc querying), and ClickHouse (dashboard queries). The interesting property is **skew**: one enterprise customer can be 40% of volume.

### System B — Security / observability platform

Billions of events per day: logs, metrics, traces, security events. High ingestion, high cardinality, late events, real-time detection, historical investigation. This is where Kafka partitions, Flink watermarks, ClickHouse `ORDER BY`, and Iceberg's cold path earn their keep.

### System C — E-commerce platform

Orders, payments, users, inventory, clickstream, recommendations. CDC from OLTP, batch plus streaming, lakehouse modelling, graph relationships for fraud and “bought together.”

### System D — IoT platform

Millions of devices sending `{timestamp, device_id, sensor, value}` every 30 seconds. Time series, windows, downsampling, retention tiers. Cardinality explosions live here.

### System E — Fraud graph

Users → Devices → IPs → Transactions → Merchants. Traversal, connected components, fraud-ring detection. Relational joins get embarrassing; graph modelling does not.

## What a lesson feels like

Every substantial lesson follows the same conversation:

1. **Meet the situation.** A concrete workload gives the topic a reason to matter.
2. **See the mental picture.** An analogy, diagram, or small example makes the idea predictable.
3. **Look under the hood.** The implementation explains where the behaviour comes from. This is optional on a first pass.
4. **Use and operate it.** A worked example connects the model to code, failure, and debugging.
5. **Check the idea.** A small question, simulation, or lab tells you whether the model has stuck.

Three levels of understanding, same as the sister academies:

| Level | When | Focus |
|-------|------|--------|
| **1 — Intuition** | Explaining to a teammate | Analogies, diagrams, workloads |
| **2 — Engineering** | Design review | Algorithms, storage, execution |
| **3 — Production** | On-call / Staff | Bottlenecks, cost, recovery, debugging |

## Choose your next step

Use these questions only to choose a route; they are not an entrance exam:

- Why is data partitioned in the first place?
- What creates a shuffle in a distributed computation?
- Why does consumer lag increase on **one** partition?
- What is the difference between event time and processing time?
- Why does ClickHouse sort data on disk?
- When would graph modelling outperform relational modelling?

If most feel unclear, that is exactly what
[Phase 0: Data Systems Foundations](foundations/index.md) is for. If you already
operate these systems, use [Learning paths](learning-paths.md) to jump to one
specific gap.

- **New to the internals:** [Begin with Data at Scale](foundations/scale.md)
- **Here for a production problem:** [Choose an on-call path](learning-paths.md#on-call-streaming)
- **Designing a platform:** [Follow the Staff path](learning-paths.md#staff-data-platform)
- **Want the study rhythm first:** [How to study](how-to-study.md)
- **Ready to make an idea visible:** [Use the practice map](practice-map.md)
