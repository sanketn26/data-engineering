# Flink vs Kafka Streams vs Spark Structured Streaming

## The Question Is Not "Which Is Best"

The question is: "Which fits this workload?"

Each has a different design philosophy, deployment model, and performance profile.

---

## Mental Models

### Apache Flink
A distributed stream processing system. Processing happens in a separate cluster. Can be deployed on Kubernetes, YARN, or standalone. Designed for correctness, exactly-once semantics, and large-scale stateful processing.

### Kafka Streams
A Java library. Runs embedded in your application — no separate cluster. State stored in RocksDB on the application's machines. Suited for microservices that need to process Kafka data.

### Spark Structured Streaming
Micro-batch processing (or continuous mode) built on Spark. Reuses Spark's DataFrame API and optimizer. Suited when your team already uses Spark for batch.

---

## Comparison by Dimension

| Dimension | Flink | Kafka Streams | Spark Structured Streaming |
|-----------|-------|---------------|---------------------------|
| **Latency** | Milliseconds | Milliseconds | Seconds (micro-batch) |
| **State management** | External cluster + RocksDB | Embedded RocksDB | Executor memory or external |
| **Exactly-once** | Yes (with Kafka) | Yes (with Kafka) | Yes (with supported sources) |
| **Deployment** | Separate cluster | Library in app | Spark cluster |
| **Event time** | First-class, mature | Supported | Supported |
| **SQL support** | Flink SQL (mature) | KSQL (separate) | Spark SQL (excellent) |
| **Scaling** | Independent of app | Co-located with app | With Spark cluster |
| **Learning curve** | High | Medium | Low (if Spark already known) |

---

## When to Use Flink

- Sub-second latency is required
- Complex event patterns (CEP)
- Large stateful operations (GB–TB of state)
- Exactly-once end-to-end is critical
- Independent scaling of processing infrastructure

---

## When to Use Kafka Streams

- Your application is a Java service
- You want to process Kafka data without a separate infrastructure component
- State size is manageable (GB, not TB)
- Operational simplicity matters more than ultimate performance

---

## When to Use Spark Structured Streaming

- Your team already uses Spark for batch
- You want to reuse the same DataFrame code for both batch and streaming
- Latency of 30–60 seconds is acceptable
- You need Spark's broad ecosystem (Delta Lake, MLlib, etc.)

---

## Real Workload Comparison

### "5-minute windowed counts with exactly-once" (Observability Platform)

- **Flink**: natural fit. Event-time windows, watermarks, exactly-once with Kafka, sub-second latency
- **Kafka Streams**: possible, but deployment inside observability service adds complexity
- **Spark SS**: possible at minutes of latency, not seconds

### "Enrichment: join event stream with user profile table" (E-Commerce)

- **Flink**: broadcast state or regular stream-table join
- **Kafka Streams**: KTable join, naturally fits the library model
- **Spark SS**: stream-static join, works well

### "Complex ETL: clean and aggregate data before loading to warehouse" (Batch-like)

- **Flink**: works but stateless ETL is not its strength
- **Spark SS**: strong, especially with Delta Lake sink
- **Kafka Streams**: limited SQL, not ideal for complex transformations
