---
title: Start Here
description: What this academy is, who it is for, and the five production systems that run through every module.
---

# Start Here

Before you open the first lesson, understand what this academy is and is not.

## What this is

A serious engineering resource for people who want to understand how data systems work at scale — not memorise feature lists, not pass a certification, not build a weekend demo.

The objective:

> Given a data workload — its scale, latency requirements, access patterns, reliability requirements, and cost constraints — you can derive an appropriate architecture, choose sensible technologies, explain their trade-offs, predict how they will fail, debug them in production, and evolve the architecture as scale increases.

That sentence is the exam. Every module exists to make it answerable.

## What this is not

- A tutorial for beginners
- A documentation mirror of Spark / Kafka / ClickHouse
- A “learn tool X in N days” course
- A checklist to memorise for interviews

If you want product docs, read the product docs. If you want to know **why the product looks like that**, stay here.

## Who this is for

Experienced data engineers, senior software / backend / platform engineers, ML engineers who own pipelines, SREs who get paged for lag, Staff-track engineers who must justify a stack.

**You should already know:** Python, SQL, Linux, Docker, Git, basic databases, basic cloud, and what a production incident feels like.

**You will not be taught:** what an API is, what JSON is, what `SELECT` means, what a container is.

**You will be taught:** partitioning, shuffle, logs, time, state, table formats, columnar layout, cardinality, and the operational consequences of each.

!!! warning "Prerequisite check"
    If the words *offset*, *partition*, *join*, *index*, and *SLA* are unfamiliar in an operational sense, this academy will feel like it starts in the middle — because it does. Build those foundations first.

## The founding intuition

Every major data technology was built because someone had a problem they could not solve with existing tools.

Kafka was not invented because “microservices need messaging.” It was invented because databases were not designed for millions of writes per second with decoupled producers, independent consumers, and durable replay.

ClickHouse was not invented because “queries should be fast.” It was invented because analytical workloads that scan billions of rows for a handful of columns perform terribly on row-oriented storage.

Flink was not invented because streaming is fashionable. It was invented because systems need to reason about **event time**, not processing time, and keep consistent state across workers that crash.

If you understand *why* these systems exist, you can reason about systems you have never used.

## Five running production systems

The same datasets flow through different technologies so you can compare them directly. Do not treat them as flavour text — they are the workload you will be asked to design for.

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

## The teaching loop

Every substantial lesson uses five acts. Not every page needs fifteen repeated headings; orientation and reference pages use the shape that best serves their job.

1. **PROBLEM** — workload, learner outcomes, and why the constraint matters.
2. **MODEL** — the intuition and vocabulary needed to reason.
3. **MECHANISM** — internals and a runnable or worked example.
4. **PRODUCTION** — failure, debugging, scale, trade-offs, and alternatives where relevant.
5. **ASSESSMENT** — an observable exit check or contribution to the capstone.

Three levels of understanding, same as the sister academies:

| Level | When | Focus |
|-------|------|--------|
| **1 — Intuition** | Explaining to a teammate | Analogies, diagrams, workloads |
| **2 — Engineering** | Design review | Algorithms, storage, execution |
| **3 — Production** | On-call / Staff | Bottlenecks, cost, recovery, debugging |

## Before you proceed

Ask yourself, without looking anything up:

- Why is data partitioned in the first place?
- What creates a shuffle in a distributed computation?
- Why does consumer lag increase on **one** partition?
- What is the difference between event time and processing time?
- Why does ClickHouse sort data on disk?
- When would graph modelling outperform relational modelling?

If most of these feel unclear, start with [Phase 0: Foundations](foundations/index.md).

If you already operate these systems and have a specific gap, use [Learning paths](learning-paths.md).

→ [How to Study](how-to-study.md)
→ [Phase 0: Foundations](foundations/index.md)
→ [Capstone and rubric](capstone.md)
→ [Versions and primary sources](reference/version-matrix.md)
