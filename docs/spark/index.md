# Apache Spark

## The Problem

One machine cannot efficiently process a 5 TB dataset.

You could read the file sequentially in chunks, but you are limited to the I/O bandwidth and CPU cores of one machine. A server with 32 cores reading at 500 MB/s from an SSD takes nearly 3 hours just to read 5 TB — before any computation.

Distributed processing was invented to solve this. Spark is one answer to it.

---

## What This Module Covers

| Topic | What You Will Learn |
|-------|-------------------|
| [The Spark Mental Model](mental-model.md) | Driver, executors, DAGs, lazy evaluation |
| [The Shuffle](shuffle.md) | Why shuffles are expensive and how to minimise them |
| [Catalyst & Tungsten](optimizer.md) | How Spark optimises your queries |
| [Production Gotchas](gotchas.md) | Where experienced engineers still go wrong |
| [Labs](labs.md) | Hands-on: skew, joins, partitioning |

---

## The Central Intuition

Divide the dataset into partitions. Assign each partition to a worker. Workers process their partitions independently. When the result requires data from multiple workers — a GROUP BY, a JOIN — redistribute the data (shuffle) and finish.

The shuffle is the expensive part. Minimise it.

---

## Running Use Case

Throughout this module, we use the **SaaS Analytics Platform** event dataset:

```
{timestamp, customer_id, user_id, service, endpoint, region, latency_ms, status_code, bytes}
```

10 million events per day. Customers range from individuals to enterprises with millions of events daily. This skew will be important.
