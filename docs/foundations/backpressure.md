---
title: Backpressure & Queueing
description: Reason about producer/consumer throughput mismatch with Little's Law before it becomes a 3 AM lag page.
---

# Backpressure & Queueing

**Time:** 35 minutes reading + 30 minutes exercise<br>
**Prerequisites:** [Data at Scale](scale.md), [Partitioning](partitions.md)<br>
**Outcomes:** compute backlog growth from a throughput mismatch; choose between admission control, load shedding, and autoscaling; read Little's Law off any queue in the academy.

A producer writes 100k events/s. Kafka accepts all of it — a topic does not push back on the writer. A processor downstream can only handle 70k/s. A sink after that can only accept 40k/s.

```text
Producer  100k/s
   |
   v
 Kafka    (accepts everything — a topic buffers, it does not refuse)
   |  100k/s
   v
Processor 70k/s
   |
   v
Sink      40k/s
```

Where does the missing 60k/s go? It does not vanish. It becomes **lag** — a growing backlog sitting in Kafka, in the processor's internal buffers, or in retry queues — until something either slows the producer down (backpressure) or starts dropping work (shedding). This is the same mechanism whether the "queue" is a Kafka topic, a Flink operator's input buffer, a thread pool's work queue, or an API's request queue. Learn it once here; every product-specific page (Kafka lag, Flink checkpoints, Airflow pools) is this same shape wearing a different label.

## Little's Law

For any stable queue:

\[
L = \lambda W
\]

- \(L\) — average number of items in the system (queue depth / backlog).
- \(\lambda\) — arrival rate.
- \(W\) — average time an item spends in the system.

The useful direction for on-call is usually solving for backlog growth when arrival exceeds service rate:

```text
Arrival (λ_in)  = 100,000 events/s
Service (λ_out) =  80,000 events/s

Backlog growth  = λ_in - λ_out
                = 20,000 events/s
                = 72,000,000 events/hour
```

If you know current lag and the sustained arrival/service rates, you can estimate **recovery time** after scaling the consumer:

```text
Current lag = 500,000,000 events
New service rate after scaling = 150,000 events/s (arrival stays 100,000/s)
Drain rate = 150,000 - 100,000 = 50,000 events/s
Recovery time ≈ 500,000,000 / 50,000 = 10,000 s ≈ 2.8 hours
```

This is the arithmetic behind "how long until the dashboard catches up" — a question every on-call engineer gets asked and too often answers with a guess.

## The three responses to a mismatch

| Response | Mechanism | Cost |
|----------|-----------|------|
| **Backpressure** (propagate the slowdown upstream) | Flink/Kafka Streams: an operator's input buffer fills, it stops pulling from its upstream, which stops pulling from *its* upstream, back to the source | Producer or source connector slows down or blocks — safe, but can stall an unrelated upstream if it shares resources |
| **Bounded buffering + lag** | Kafka topic (bounded by retention, not by a live signal to the producer) | Consumer falls behind; data is safe until retention expires, but staleness grows unbounded if nothing intervenes |
| **Load shedding / admission control** | Reject or sample requests at the edge (rate limiter, load balancer, queue-full response) | Data loss by design — must be paired with a policy for *what* to drop (newest, oldest, low-priority) |

Kafka is the middle case: it does not backpressure the producer in the classic sense (a topic keeps accepting writes up to disk/retention limits), so a slow consumer shows up as **lag**, not as the producer being throttled. Flink and Kafka Streams operators, by contrast, implement true backpressure — a slow sink propagates backward through the topology via bounded internal buffers, eventually slowing the source connector's read rate. Confusing these two mechanisms is why teams "fix" Kafka lag by adding consumers when the real ceiling is downstream (the sink), or "fix" a stalled Flink job by adding Kafka partitions when the real ceiling is the operator's own state size.

## Levers, in the order most teams should reach for them

1. **Fix the real bottleneck first.** Measure per-stage throughput (producer, broker, each processing stage, sink) before touching any config — see [Debugging](#debugging) below. Scaling the wrong stage just moves the queue.
2. **Batch sizing.** Larger batches amortize per-record overhead (network round trips, serialization) at the cost of latency. A sink batching 10k rows/write instead of 1 row/write can be the entire fix.
3. **Consumer/processor autoscaling.** Add parallelism — but only up to the number of Kafka partitions, and only if the new bottleneck (network, downstream sink) doesn't just move one hop over.
4. **Admission control / rate limiting.** Cap arrival at the edge so the system never enters the growing-backlog regime — appropriate when late data is worse than dropped data (an API, not a ledger).
5. **Load shedding.** Explicit, policy-driven dropping (sample, drop low-priority, drop oldest) when 1–4 aren't enough and *some* answer beats a fully stalled pipeline. Silent drops (buffer overflow with no metric) are the failure mode, not the fix.

## Debugging

Backpressure symptoms look similar across systems, but the metric that separates the hypotheses differs:

| Symptom | Check | Distinguishes |
|---------|-------|----------------|
| Kafka consumer lag rising | `records-lag` per partition | One hot partition (see [partitions](partitions.md)) vs uniformly slow consumer |
| Flink job "backpressured" in UI | `busy_time` / `backpressured_time` per operator, working **backward** from the sink | Which operator is the true bottleneck — everything upstream of it will show as backpressured even though it is healthy |
| API latency climbing under load | queue depth at the load balancer / thread pool | Saturated downstream dependency vs undersized thread pool |
| Airflow pool exhaustion | pool slot usage, task queue time | Too few slots vs tasks that should not share a pool ([orchestration](../airflow/index.md)) |

!!! production-gotcha "Backpressure in Flink points at the wrong operator if you read it forward"
    A Flink UI showing every operator "backpressured" does not mean every operator is slow. It means everything **upstream of the actual bottleneck** is blocked waiting for it. Start at the sink and walk backward to find the first operator that is *busy*, not backpressured — that is the real ceiling.

## Failure modes

- Scaling consumers when the sink, not the consumer, is the bottleneck — lag moves from "Kafka" to "processor's internal queue," which is harder to see.
- An unbounded in-memory queue "to avoid dropping data" that turns a slow sink into an OOM instead of a controlled lag metric.
- Load shedding with no metric on *what* was shed — silent data loss looks identical to "everything is fine" until an audit.
- Autoscaling with no maximum, so a downstream outage causes the consumer fleet to scale to a size the sink then cannot survive when it recovers (the "thundering herd on recovery" failure).
- Treating Kafka retention as backpressure — it is a deadline, not a signal; the producer never learns to slow down.

## Exercise

An ingestion API accepts events at a sustained 100k/s during business hours. It writes to Kafka, which a Flink job consumes at 100k/s in steady state — the pipeline is healthy. A downstream ClickHouse sink hiccups and drops to 40k/s for 20 minutes before recovering.

1. Using Little's Law, how much backlog (events) accumulates during the 20-minute hiccup?
2. If ClickHouse recovers to exactly 100k/s (not higher), how long does it take to drain the backlog? What does the on-call dashboard show during that time?
3. Would scaling the Flink job's parallelism help during the hiccup? Why or why not?
4. Propose one change that would have capped the backlog instead of letting it grow for the full 20 minutes.

??? success "Exit check"
    (1) Arrival 100k/s, service 40k/s during the hiccup → backlog grows at 60k/s × 1200s = 72,000,000 events. (2) At exactly 100k/s recovery, drain rate is 0 (arrival = service) — the backlog **never drains**, it just stops growing; lag holds flat at 72M until service exceeds 100k/s. The dashboard shows persistent, non-zero, non-growing lag, which is easy to misread as "recovered" if you only alert on lag *increasing*. (3) No — Flink's consumption rate is already matched to Kafka's arrival rate; the bottleneck is the ClickHouse sink, and adding Flink parallelism just means more idle-waiting-on-sink tasks. (4) A backpressure-aware sink connector (bounded buffer that slows the Flink-to-ClickHouse write path) converts the incident into a controlled, visible slowdown instead of an unbounded backlog — or an alert on sink write latency that pages before 20 minutes elapse, catching it earlier than a lag-only alert would.
