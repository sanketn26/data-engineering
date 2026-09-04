# Production Gotchas

## 1. Consumer Lag Growing Silently

Lag can grow slowly over time — each message processed 10ms slower than produced, multiplied across millions of messages.

At 1,000 messages/sec, a 10ms processing overhead means lag grows by 10 messages/sec. After a day, you have 864,000 messages of lag.

**Monitor lag continuously.** Alert when lag exceeds a threshold relevant to your latency SLA.

---

## 2. Rebalance Storms

A rebalance storm occurs when consumers repeatedly trigger rebalances by:
- Taking too long to process messages (`max.poll.interval.ms` exceeded)
- Frequent crash-restart cycles
- Slow consumer group joining (e.g., loading a large model at startup)

During a rebalance, *all* consumers in the group stop. If consumers keep crashing and rejoining, you get continuous rebalances with near-zero net throughput.

**Fix**:
- Increase `max.poll.interval.ms` for slow-processing consumers
- Use `session.timeout.ms` carefully
- Implement smooth startup (avoid expensive initialisation before the first poll)

---

## 3. Poison Messages

A message your consumer cannot process — perhaps due to a schema violation, null pointer, or corrupted data.

Without handling:
1. Consumer processes message → exception → consumer restarts
2. Consumer reads same message → exception → restart
3. Loop forever, zero progress

**Fix**: implement a dead-letter queue. Messages that fail after N retries are written to a separate topic for investigation.

```python
for message in consumer:
    try:
        process(message)
        consumer.commit()
    except Exception as e:
        if should_dead_letter(message, e):
            dead_letter_producer.send('dlq-user-events', message.value)
            consumer.commit()
        else:
            raise
```

---

## 4. Schema Evolution Without Coordination

Producer adds a new field to a JSON event. Consumer does not expect it. Consumer throws `KeyError`. Cascade failure.

**Fix**: use a schema registry (Confluent Schema Registry or AWS Glue Schema Registry) with Avro/Protobuf. Schema evolution rules (backward, forward, full compatibility) are enforced at the schema registry level.

---

## 5. Hot Partitions

Partitioning by `customer_id` where one customer generates 40% of traffic means one partition — and one consumer — handles 40% of all work.

**Symptoms**:
- Consumer lag concentrated on one partition
- One consumer CPU at 100% while others are idle
- Throughput bottleneck on one broker (the leader for the hot partition)

**Fix**: compound keys, random suffix on hot keys, or round-robin with enrichment downstream.

---

## 6. Disk Saturation

Kafka writes all data to disk. At high ingestion rates, disk can saturate before memory or CPU.

**Symptoms**:
- Broker I/O wait high
- Producer latency increasing
- Replication lag growing

**Fix**: SSDs (or NVMe), multiple disks for Kafka's log directories, or reduce retention.

---

## 7. Oversized Messages

Kafka has a configurable `max.message.bytes` limit (default 1 MB). Sending a 10 MB JSON blob will fail.

**Fix**: split large payloads, compress, or store in object storage and send a reference in Kafka.

---

## Production Is On Fire 🔥

> Consumer lag for the `alert-processor` group increased from 30 seconds to 45 minutes over the past 2 hours.

Before reading further — what would you investigate?

<details>
<summary>Investigation approach</summary>

**Step 1**: Is it all partitions or specific ones?
```bash
kafka-consumer-groups.sh --describe --group alert-processor
```
- If all partitions: consumer is globally slow
- If specific partitions: hot partition, network issue to specific broker, or consumer assigned to slow partitions crashed

**Step 2**: Is producer throughput normal?
Check `messages-in-per-second` on the topic. If it spiked, consumers may simply be overwhelmed.

**Step 3**: Is the consumer crashing?
Check consumer logs for exceptions and restart counts.

**Step 4**: Is there a rebalance storm?
Check group coordinator logs. Frequent rebalances with zero net commits is diagnostic.

**Step 5**: Is this a downstream dependency problem?
Alert-processor writes to a database. If the database is slow, consumer processing time increases, causing lag.

**The most common cause**: a downstream dependency (database, API, external system) slowed down, causing consumer processing time to increase, causing lag to grow.

</details>

---

## How to Apply This at Work

Every week, check your Kafka topics for:

1. Lag growth trends (not just current lag)
2. Consumer group rebalance frequency
3. Disk usage growth rate on brokers
4. Under-replicated partition count (should be zero)
5. Producer error rates (failed sends, timeouts)
