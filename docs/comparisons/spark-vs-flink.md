# Spark vs Flink

Both Spark and Flink process data at scale. They make different trade-offs. Understanding those trade-offs prevents you from picking the wrong one.

---

## The Core Difference

**Spark** was designed as a batch engine. Structured Streaming was added later. Batching is first-class; streaming is built on micro-batches.

**Flink** was designed as a streaming engine. Batch is a special case of streaming (a bounded stream). Streaming is first-class.

This design origin shapes everything.

---

## Mental Model

**Spark**: divide data into partitions, run transformations in stages, shuffle between stages. Even in Structured Streaming, Spark processes data in discrete micro-batches.

**Flink**: process each event as it arrives. State is maintained across events. Time is a first-class concept. Checkpoints capture consistent state snapshots.

---

## Latency

| Metric | Spark Structured Streaming | Flink |
|--------|---------------------------|-------|
| Minimum latency | ~100ms (micro-batch) | <10ms (event-by-event) |
| Typical streaming latency | 1–30 seconds | 10–500ms |
| Batch latency | Minutes to hours | Minutes to hours |

If you need sub-second latency, Flink. If minutes are acceptable, either works.

---

## State Management

**Spark**: stateful streaming exists (`flatMapGroupsWithState`, `mapGroupsWithState`) but it's awkward. State is managed per micro-batch with limited expressiveness.

**Flink**: state is a first-class primitive. `ValueState`, `ListState`, `MapState`. RocksDB backend for state that doesn't fit in memory. TTL per state entry. State is what makes Flink powerful for session windows, fraud detection, and complex event processing.

---

## Time Semantics

**Spark**: watermarks supported, but they work on micro-batch boundaries. Event time is supported but less precise.

**Flink**: event time, ingestion time, and processing time. Watermarks are first-class. Late event handling (allow/drop/side output) is expressive. This is where Flink was purpose-built.

---

## Exactly-Once

**Spark**: achievable end-to-end with idempotent sinks and Kafka offsets checkpointed in HDFS/S3.

**Flink**: native exactly-once via two-phase commit protocol integrated into checkpoints. Kafka source + Kafka/JDBC/HDFS sinks with transactional exactly-once.

---

## Ecosystem and APIs

**Spark**:
- Python (PySpark), Scala, Java, R
- DataFrame API is excellent and widely used
- SQL support is mature (Spark SQL)
- MLlib for ML on the same cluster
- Delta Lake, Iceberg, Hudi all have first-class Spark support

**Flink**:
- Java and Python (PyFlink)
- DataStream API (low-level, powerful)
- Table API and Flink SQL (improving rapidly)
- ML via Alink or export to dedicated ML platforms

---

## Operational Complexity

**Spark**: simpler mental model for most engineers. Debugging via Spark UI is mature. Many engineers already know PySpark.

**Flink**: steeper learning curve. Checkpoint configuration, state backend tuning, watermark strategies, and backpressure management require deeper understanding. But it pays off at low-latency requirements.

---

## When to Use Spark

- Batch processing is your primary workload
- Latency of minutes is acceptable
- Your team knows PySpark and doesn't want to relearn
- You need ML and data processing on the same cluster
- You're reading from a lakehouse (Iceberg, Delta, Hudi)
- You need the richest SQL support

## When to Use Flink

- You need sub-second streaming latency
- Complex stateful logic: sessions, fraud detection, complex event processing
- Event time reasoning with out-of-order events is critical
- Exactly-once end-to-end is required and you need it to be robust
- Your workload is continuous enrichment or alerting

---

## Both Together

Many production systems use both:

```
Kafka → Flink (real-time enrichment + alerting, <1s latency)
      → Spark (batch transformations + lakehouse writes, hourly)
```

This is not waste — they serve different SLAs. The Flink job powers the real-time dashboard. The Spark job powers the historical reports.

---

## Decision Checklist

```
Latency requirement < 1 second?          → Flink
Complex stateful logic?                  → Flink
Primarily batch + some streaming?        → Spark Structured Streaming
Team knows PySpark?                      → Spark (unless latency demands otherwise)
Heavy ML on same data?                   → Spark
Reading/writing lakehouses?              → Spark (better native support)
```
