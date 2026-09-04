# Exactly-Once Semantics

## The Delivery Guarantees

Kafka supports three delivery semantics:

| Guarantee | What It Means | How |
|-----------|--------------|-----|
| **At most once** | May lose messages, no duplicates | No retries, commit before processing |
| **At least once** | No message loss, may duplicate | Retry on failure, commit after processing |
| **Exactly once** | No loss, no duplicates | Idempotent producers + transactions |

Most production systems use **at-least-once** with idempotent downstream processing (i.e., processing the same event twice produces the same result).

---

## Idempotent Producers

Without idempotency, a producer retry creates duplicates:

```
1. Producer sends record (sequence 5) → network timeout
2. Producer doesn't know if broker received it
3. Producer retries → broker receives it twice → duplicate
```

With idempotent producers, Kafka deduplicates retries:

```python
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    enable_idempotence=True  # assigns a producer ID + sequence numbers
)
```

The broker tracks each producer's latest sequence number per partition and rejects duplicates.

---

## Kafka Transactions

Transactions allow you to write to multiple partitions atomically — all succeed or all fail.

Use case: read from topic A, transform, write to topic B. Either both the write and the offset commit succeed, or neither does.

```python
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    transactional_id='my-transactional-producer'
)
producer.init_transactions()

try:
    producer.begin_transaction()
    producer.send('output-topic', value=b'processed-event')
    producer.send_offsets_to_transaction(
        offsets={TopicPartition('input-topic', 0): OffsetAndMetadata(42, '')},
        group_metadata=consumer.consumer_group_metadata()
    )
    producer.commit_transaction()
except Exception:
    producer.abort_transaction()
```

Consumers must set `isolation.level=read_committed` to only read committed records.

---

## When Exactly-Once Matters

- Financial transactions (cannot duplicate a payment)
- Aggregations that cannot tolerate re-counting
- Event sourcing where duplicate events corrupt state

**The cost**: transactions add latency (coordination overhead). For high-throughput pipelines where idempotent consumers are possible, at-least-once is usually the right choice.
