# Kafka lab

Single-broker **KRaft** Kafka on your laptop. You will produce System A events, watch a consumer group, then **break** partitioning with a hot key.

Docs: [Kafka module](../../docs/kafka/index.md), [labs index](../../docs/labs/index.md), [incident: hot partition](../../docs/incidents/index.md), [partition sim](../../docs/simulations/kafka-partitions.html).

## Prerequisites

- Docker Compose v2
- Python 3.9+ (`pip install -r requirements.txt`)
- ~1 GB RAM for the broker

CLI tools run **inside** the container (`/opt/kafka/bin`). Python clients run on the **host** (`localhost:9092`).

## Predict (write this down)

Before `compose up`:

1. A topic with 6 partitions and **1** consumer: how many partitions does that consumer read?
2. Same topic, **12** consumers in one group: how many are idle?
3. If 80% of messages use `key=cust_0042`, can 6 consumers keep lag flat on every partition?

## Start

```bash
cd labs/kafka
docker compose up -d
docker compose ps   # wait until healthcheck is healthy
```

Create the topic (6 partitions, RF=1 because there is one broker):

```bash
docker exec dea-kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create --topic user-events --partitions 6 --replication-factor 1

docker exec dea-kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --describe --topic user-events
```

## Run — produce and consume

The committed scripts are the canonical path:

```bash
python produce_events.py --count 10000
python consume_events.py --max-records 10000
```

Save as `produce_uniform.py` (or run from a Python REPL):

```python
from kafka import KafkaProducer
import json, time, random

producer = KafkaProducer(
    bootstrap_servers=["localhost:9092"],
    key_serializer=lambda k: k.encode(),
    value_serializer=lambda v: json.dumps(v).encode(),
)

services = ["api-gateway", "auth", "billing", "search"]
customers = [f"cust_{i:04d}" for i in range(100)]

for i in range(10_000):
    cid = random.choice(customers)
    event = {
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "customer_id": cid,
        "user_id": f"user_{random.randint(1, 10_000)}",
        "service": random.choice(services),
        "endpoint": "/v2/events",
        "region": random.choice(["eu-west-1", "us-east-1"]),
        "latency_ms": random.randint(10, 400),
        "status_code": random.choice([200, 200, 200, 200, 500]),
        "bytes": 1024,
    }
    producer.send("user-events", key=cid, value=event)

producer.flush()
print("produced 10000 uniform keys")
```

```python
from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    "user-events",
    group_id="lab-consumer",
    bootstrap_servers=["localhost:9092"],
    value_deserializer=lambda v: json.loads(v.decode()),
    auto_offset_reset="earliest",
    enable_auto_commit=True,
)

count = 0
for msg in consumer:
    count += 1
    if count % 1000 == 0:
        print(f"n={count} partition={msg.partition} offset={msg.offset}")
    if count >= 10_000:
        break
consumer.close()
```

Describe the group:

```bash
docker exec dea-kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 --describe --group lab-consumer
```

**Compare to prediction 1.** LAG should be ~0 on all six partitions after the consumer finishes.

## Run — lag with a slow consumer

Create topic `lag-test` (3 partitions). Produce 30,000 messages quickly, then consume with `time.sleep(0.01)` (≈100 msg/s). In another terminal, watch:

```bash
docker exec dea-kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 --describe --group slow-consumer
```

**Predict:** does LAG fall, rise, or stay while you sleep? At 100 msg/s vs a burst produce, lag **starts high and drains**. If you produced *and* continued producing faster than 100/s, lag would grow.

## Break — hot partition

**Predict again:** 20,000 messages, `key=cust_0042` with probability 0.8, else uniform `cust_XXXX`. Six partitions, group `hot-lab` with **6** consumers (six Python processes or one consumer — start with **one** consumer first, then six).

```bash
python produce_events.py --count 20000 --hot-ratio 0.8
python consume_events.py --group hot-lab --max-records 20000 --sleep-ms 10
```

```python
from kafka import KafkaProducer
import json, time, random

producer = KafkaProducer(
    bootstrap_servers=["localhost:9092"],
    key_serializer=lambda k: k.encode(),
    value_serializer=lambda v: json.dumps(v).encode(),
)

for i in range(20_000):
    cid = "cust_0042" if random.random() < 0.8 else f"cust_{random.randint(0, 99):04d}"
    producer.send(
        "user-events",
        key=cid,
        value={"n": i, "customer_id": cid, "service": "api-gateway"},
    )
producer.flush()
print("produced 20000 hot-keyed messages")
```

Count keys per partition (consumer with `group_id` unique, `auto_offset_reset=earliest`, no commit needed for a tally):

```python
from kafka import KafkaConsumer
from collections import Counter

c = KafkaConsumer(
    "user-events",
    group_id="tally-hot",
    bootstrap_servers=["localhost:9092"],
    auto_offset_reset="earliest",
    consumer_timeout_ms=15000,
)
parts = Counter()
keys = Counter()
for msg in c:
    parts[msg.partition] += 1
    keys[msg.key] += 1
print("per partition", parts)
print("top keys", keys.most_common(3))
```

You should see **one partition** with a large majority. `kafka-consumer-groups --describe` during a **slow** consume of this data: LAG on **one** partition stays high.

Start a second consumer in the **same** group. **Predict:** does partition lag split? **No.** The hot partition is still assigned to **one** member.

## Break — broker stop (optional)

```bash
docker compose stop kafka
# consumer throws; restart:
docker compose start kafka
# consumer resumes from last committed offset
```

With a single broker, `acks=all` cannot protect you from **this** process dying with unflushed data. That is a teaching point, not a production durability story (need RF=3).

## Clean up

```bash
docker compose down -v
```

## What to write in your notes

- The metric that pages you: **lag on one partition**, not sum lag.
- The fix that does **not** work: more consumers in the group.
- The fix that does: change the **key** (or isolate the whale tenant). See [analytics architecture](../../docs/architectures/analytics-platform.md).
