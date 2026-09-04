# Data at Scale

## The Problem

You have a dataset. You need to process it. How hard can it be?

It depends entirely on how big the dataset is.

---

## Scaling Through the Orders of Magnitude

### 1 GB

A single machine handles this comfortably. A modern laptop with 16 GB of RAM can load a 1 GB CSV into a pandas DataFrame and run aggregations in seconds.

**Nothing interesting happens yet.**

---

### 100 GB

Now you have a choice. You *could* still use one machine — but only if you process the data in chunks, because 100 GB does not fit in RAM.

Problems start to emerge:

- Sequential disk reads become a bottleneck
- A single Python process takes hours
- One machine failure means starting over

**You start thinking about parallelism.**

> Before reading further: what would you do with 100 GB on a single 16 GB machine? How would you process it efficiently?

Processing in chunks works, but you only get the parallelism of the CPU cores on one box. One machine, 8 cores — you get 8× at most.

---

### 10 TB

One machine is no longer adequate. Even reading 10 TB from a fast SSD at 500 MB/s takes over 5 hours. A network disk would be worse.

Now you need multiple machines.

**Distributed processing enters the picture.**

But distributed processing introduces new problems that did not exist on one machine:

- **Coordination**: who tells each machine what to do?
- **Data locality**: should you move data to the compute, or compute to the data?
- **Failure**: what happens when one of your 20 workers crashes halfway through?
- **Skew**: what if 90% of the data lands on one worker?

Each of these problems has a specific answer in Spark, Flink, and the other systems you will study.

---

### 1 PB/day

Now you are generating a petabyte every day. The architecture looks completely different from the 1 GB case.

You need:

- **Distributed ingestion** — no single writer can keep up
- **Partitioning** — data must be pre-divided so workers process independent slices
- **Streaming** — you cannot wait until the day is over to begin processing
- **Tiered storage** — keeping 1 PB hot in a fast query engine is prohibitively expensive
- **Data formats** — text CSV is out; columnar compressed formats become mandatory
- **Compaction** — many small files accumulate and degrade query performance

**The architecture you designed for 1 GB will fail catastrophically here.**

---

## The Three Fundamental Tensions

Every data architecture you will design in this academy navigates these trade-offs:

### Compute vs Storage

- More storage = cheaper data retention, but slower querying
- More compute = faster processing, but higher cost
- The two are increasingly decoupled (object storage + ephemeral compute)

### Throughput vs Latency

- Batch processing optimises for throughput — process large volumes efficiently
- Stream processing optimises for latency — process each event quickly
- These are not free — high throughput often comes at the cost of batching delay

### Local vs Distributed Processing

- Local processing: no coordination overhead, simple, fast for small data
- Distributed processing: horizontal scale, but shuffle/coordination/fault-tolerance complexity

---

## What Breaks Next?

At every scale transition, ask:

| Resource | What breaks at scale? |
|----------|----------------------|
| **Memory** | Dataset no longer fits in RAM → need streaming/chunking |
| **Single CPU** | Processing is too slow → need parallelism |
| **Single disk** | I/O becomes the bottleneck → need SSDs, RAID, or distributed storage |
| **Single machine** | Machine cannot process the data → need distributed systems |
| **Single region** | Latency or availability requirements → need multi-region |
| **Single data format** | Row-oriented formats waste I/O on analytics → need columnar |
| **Single query engine** | Different access patterns need different engines |

---

## The Concrete Lesson

**Do not design a 1 PB architecture for a 1 GB problem.**

Every additional component you add — Kafka, distributed compute, a lakehouse — has operational cost, complexity, and failure modes. A PostgreSQL database handles 100 GB of events with careful indexing and partitioning. A small startup running Kafka + Flink + Iceberg + Trino for 10 GB/day has added enormous complexity for zero benefit.

The correct architecture is determined by the scale you actually have, not the scale you fantasise about having someday.

---

## How to Apply This at Work

When you encounter a new data pipeline, ask:

1. What is the data volume today?
2. What is the realistic growth trajectory (not the optimistic one)?
3. What is the latency requirement — milliseconds, seconds, minutes, hours?
4. What access patterns are expected — point lookups, aggregations, full scans?
5. Is the current architecture appropriate for today's scale?
6. What breaks first if data triples?

A pipeline that runs fine at 1 GB/day may fail silently at 100 GB/day — not with an error, but with incorrect results due to timeouts, memory pressure, or skipped data.

---

## Reasoning Exercise

You are processing customer event data on a single Spark job on one machine. The job takes 2 hours for 50 GB. Data is growing at 20% monthly.

1. In 6 months, how large will the data be?
2. At what point will the single-machine approach fail?
3. What is the first bottleneck you expect — CPU, memory, or disk?
4. Sketch the simplest distributed architecture that could handle 500 GB.
