---
description: "Hands-on Kafka labs: reproduce lag, hot partitions, and broker failure on a laptop before touching production settings."
---

# Kafka labs

Your team lead asks you to justify raising `service-events` from 12 partitions to 48 before Friday's deploy. You say it will fix the lag. She asks how you know, given that increasing partitions reshuffles every key going forward. The only honest answer right now is "I read that it should help."

Predict before you read on: before you touch a partition count, an ISR setting, or a consumer group in production, could you reproduce the failure — hot key, poison message, broker loss — on a laptop first, and name what the metrics would show before you ran it?

These labs are meant to be run with Docker and `kafka-python`. If you cannot run Docker here, read each step and write down what you **expect** before the "now break it" sections. The point is the failure, not the happy path.

**Prerequisites:** Docker, Python 3.9+, ~2 GB RAM. Commands assume a shell in an empty working directory.

Related reading: [the log](log.md), [partitions](partitions.md), [replication](replication.md), [gotchas](gotchas.md). Flink labs that consume the same topics: [Flink labs](../flink/labs.md).

---

## Lab setup (single broker, then three)

A single broker is enough for produce/consume/lag/poison. Replication needs three.

```bash
pip install 'kafka-python>=2.0.2'

cat > docker-compose.yml << 'EOF'
services:
  kafka-1:
    image: confluentinc/cp-kafka:7.5.0
    hostname: kafka-1
    ports:
      - "9092:9092"
    environment:
      KAFKA_NODE_ID: 1
      KAFKA_PROCESS_ROLES: broker,controller
      KAFKA_CONTROLLER_QUORUM_VOTERS: 1@kafka-1:29093
      KAFKA_LISTENERS: PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:29093
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092
      KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER
      KAFKA_INTER_BROKER_LISTENER_NAME: PLAINTEXT
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
      KAFKA_LOG_DIRS: /var/lib/kafka/data
      CLUSTER_ID: MkU3OEVBNTcwNTJENDM2Qk
    volumes:
      - kafka1-data:/var/lib/kafka/data

volumes:
  kafka1-data:
EOF

docker compose up -d
# wait until kafka-topics works
until docker compose exec kafka-1 kafka-topics --bootstrap-server localhost:9092 --list; do sleep 2; done
```

If this image+KRaft combo misbehaves in your environment, the older ZooKeeper pair from Confluent's 7.5 examples is fine — the labs care about offsets and failures, not KRaft. A single-node ZooKeeper+broker compose is equivalent for labs 1–4.

Create the topic used everywhere:

```bash
docker compose exec kafka-1 kafka-topics --bootstrap-server localhost:9092 \
  --create --topic service-events --partitions 6 --replication-factor 1
```

Event shape for all producers:

```text
{timestamp, customer_id, user_id, service, endpoint, region, latency_ms, status_code, bytes}
```

---

## Lab 1: produce and consume

**Goal:** see keys stick to partitions, and see two groups read the same bytes.

```python
# lab1_producer.py
from kafka import KafkaProducer
import json, time, random

producer = KafkaProducer(
    bootstrap_servers=["localhost:9092"],
    value_serializer=lambda v: json.dumps(v).encode(),
    key_serializer=str.encode,
    acks="all",
)

services = ["api", "auth", "payment", "search"]
customers = [f"cust_{i}" for i in range(100)]
# one whale — we will use this in lab 5
customers += ["cust_whale"] * 40

for i in range(10_000):
    cid = random.choice(customers)
    event = {
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S.000Z", time.gmtime()),
        "customer_id": cid,
        "user_id": f"u_{random.randint(1, 5000)}",
        "service": random.choice(services),
        "endpoint": "/login" if random.random() < 0.2 else "/orders",
        "region": random.choice(["eu-west-1", "us-east-1"]),
        "latency_ms": random.randint(10, 500),
        "status_code": random.choice([200, 200, 200, 200, 401, 500]),
        "bytes": random.randint(200, 4000),
    }
    producer.send("service-events", key=cid, value=event)

producer.flush()
print("produced 10000")
```

```python
# lab1_consumer.py
from kafka import KafkaConsumer
import json, collections, sys

group = sys.argv[1] if len(sys.argv) > 1 else "lab-consumer"

consumer = KafkaConsumer(
    "service-events",
    group_id=group,
    bootstrap_servers=["localhost:9092"],
    value_deserializer=lambda v: json.loads(v.decode()),
    auto_offset_reset="earliest",
    enable_auto_commit=False,
)

counts = collections.Counter()
n = 0
for message in consumer:
    counts[message.partition] += 1
    n += 1
    if n % 1000 == 0:
        print(group, n, "partitions", dict(counts))
    if n >= 10_000:
        consumer.commit()
        break

print("done", group, dict(counts))
```

Run:

```bash
python lab1_producer.py
python lab1_consumer.py alert-processor
python lab1_consumer.py warehouse-loader
```

**Expected:** both groups print ~10_000. Partition histograms should look similar (same key mapping). Offsets in each group are independent — the second group still sees all records.

```bash
docker compose exec kafka-1 kafka-consumer-groups --bootstrap-server localhost:9092 \
  --describe --group alert-processor
```

---

## Lab 2: observe consumer lag

**Goal:** feel lag as a rate mismatch, **per partition**.

```python
# lab2_slow_consumer.py
from kafka import KafkaProducer, KafkaConsumer
import json, time

producer = KafkaProducer(
    bootstrap_servers=["localhost:9092"],
    value_serializer=lambda v: json.dumps(v).encode(),
)
for i in range(50_000):
    producer.send("lag-test", value={"n": i, "customer_id": f"cust_{i % 20}"})
producer.flush()
print("produced 50k")

consumer = KafkaConsumer(
    "lag-test",
    group_id="slow-consumer",
    bootstrap_servers=["localhost:9092"],
    auto_offset_reset="earliest",
    enable_auto_commit=True,
)

n = 0
for _ in consumer:
    n += 1
    time.sleep(0.01)  # 100 rec/s
    if n % 1000 == 0:
        print("processed", n)
    if n >= 5000:
        break
```

Create the topic (`--partitions 3`) then run the script. In another terminal:

```bash
watch -n 2 'docker compose exec kafka-1 kafka-consumer-groups \
  --bootstrap-server localhost:9092 --describe --group slow-consumer'
```

**Expected:** CURRENT-OFFSET crawls; LAG stays huge. If you only printed the sum, you would miss that all partitions are equally behind (slow consumer, not a hot key).

Leave the consumer running longer if you want a hockey-stick chart. Kill it and restart: with auto-commit you resume near the last timer commit, **not** necessarily at the last processed record — that is the at-least-once / at-most-once blur from [exactly-once](exactly-once.md).

---

## Lab 3: now break it — poison message

**Goal:** one bad JSON stalls a partition; a DLQ lets the group move on.

```python
# lab3_poison_produce.py
from kafka import KafkaProducer
import json

p = KafkaProducer(bootstrap_servers=["localhost:9092"])
for i in range(20):
    body = json.dumps({"customer_id": "cust_1", "status_code": 200, "i": i}).encode()
    p.send("poison-lab", key=b"cust_1", value=body)
p.send("poison-lab", key=b"cust_1", value=b"this is not json {")
for i in range(20, 40):
    body = json.dumps({"customer_id": "cust_1", "status_code": 200, "i": i}).encode()
    p.send("poison-lab", key=b"cust_1", value=body)
p.flush()
```

```python
# lab3_fragile_consumer.py
from kafka import KafkaConsumer
import json

c = KafkaConsumer(
    "poison-lab",
    group_id="fragile",
    bootstrap_servers=["localhost:9092"],
    auto_offset_reset="earliest",
    enable_auto_commit=False,
    max_poll_records=1,
)
for m in c:
    json.loads(m.value)  # throws on the pill
    print("ok", m.offset)
    c.commit()
```

Run producer, then consumer. **Expected:** prints `ok` for the first 20 offsets, then a `JSONDecodeError` loop if you restart. Lag on that partition never clears.

Now the DLQ version:

```python
# lab3_dlq_consumer.py
from kafka import KafkaConsumer, KafkaProducer
import json

dlq = KafkaProducer(bootstrap_servers=["localhost:9092"])
c = KafkaConsumer(
    "poison-lab",
    group_id="with-dlq",
    bootstrap_servers=["localhost:9092"],
    auto_offset_reset="earliest",
    enable_auto_commit=False,
    max_poll_records=1,
)
for m in c:
    try:
        json.loads(m.value)
        print("ok", m.offset)
    except Exception as e:
        print("dlq", m.offset, e)
        dlq.send("poison-lab.dlq", key=m.key, value=m.value)
        dlq.flush()
    c.commit()
```

**Expected:** offsets 0–40 commit; one record in `poison-lab.dlq`. Per-key order for `cust_1` skipped the poison — acceptable for logs, not for payments.

---

## Lab 4: now break it — kill the broker (single node)

**Goal:** see that RF=1 is data loss / downtime, and that consumers resume from **committed** offsets.

Terminal A: run `lab1_consumer.py` in a loop (raise the `n >= 10000` break, or run without the break). Terminal B: produce slowly:

```python
# lab4_slow_produce.py
from kafka import KafkaProducer
import json, time

p = KafkaProducer(
    bootstrap_servers=["localhost:9092"],
    value_serializer=lambda v: json.dumps(v).encode(),
    key_serializer=str.encode,
    acks="all",
)
i = 0
while True:
    p.send("service-events", key="cust_1", value={"i": i, "customer_id": "cust_1"})
    p.flush()
    print("sent", i)
    i += 1
    time.sleep(0.2)
```

Then:

```bash
docker compose stop kafka-1
# producer: timeouts / NoBrokersAvailable
# consumer: stop fetching
docker compose start kafka-1
```

**Expected:** after restart, consumption resumes at the last **commit**. If the consumer used `enable_auto_commit=True`, you may skip or duplicate around the crash. With RF=1, any data that was only in page cache and not flushed *can* vanish — this is why [replication](replication.md) exists.

Try the same produce loop with `acks=0` versus `acks=all` and compare how many `sent` lines have no counterpart in the consumer after the kill. Single-node `acks=all` only waits for **that** broker; it is not durability against disk loss.

---

## Lab 5: hot partition (conceptual if you skip coding)

Reuse `lab1_producer.py` (the whale). Consume with a group of **two** processes (`group_id='skew'`) and print per-partition counts.

```bash
# two terminals
python lab1_consumer.py skew
python lab1_consumer.py skew
```

**Expected:** one member is busy if `cust_whale` hashed into its partitions; the other is relatively idle. `kafka-consumer-groups --describe` shows uneven LAG if you slow the consumer. Adding a third process does **not** split the whale's partition.

---

## Lab 6: three-broker replication (optional hardware)

Compose three KRaft/broker nodes on ports 9092/9093/9094, RF=3, `min.insync.replicas=2`. Produce with `acks=all`. Then:

```bash
docker compose stop kafka-2
docker compose exec kafka-1 kafka-topics --bootstrap-server localhost:9092 \
  --describe --topic service-events
```

**Expected:** ISR shrinks; `UnderReplicatedPartitions` would be > 0 on a real JMX scrape; produces still succeed (min ISR 2). Stop a second broker: produces fail with `NotEnoughReplicas`.

This is the lab version of the [replication exercise](replication.md).

---

## Lab 7: idle partition (preview of Flink watermarks)

If you already run [Flink](../flink/index.md), produce **only** to partition 0 of a 3-partition topic (custom partitioner or keyed with a key that hashes to 0). A Flink event-time job whose watermark is `min` across Kafka partitions will **stall** because partitions 1 and 2 are idle.

Kafka-only observation: `GetOffsetShell` shows log-end 0 on idle partitions. The Flink-side break is in [Flink labs](../flink/labs.md) ("stall a watermark").

---

## What to write down

After the labs, you should be able to fill this without notes:

| Question | Your answer |
|----------|-------------|
| Two groups, one topic: who sees the records? | |
| Lag on one partition only: can you add a consumer? | |
| Poison record: skip or halt? | |
| Kill RF=1 broker: what is RPO? | |
| `acks=all`, ISR=1, min.ISR=2: does produce succeed? | |

If any cell is fuzzy, re-read the matching deep-dive rather than adding more producer flags.

---

## Exercise

You ran lab 3's fragile consumer with `enable_auto_commit=True` and `max.poll.interval.ms=5000`, processing with a 10s sleep after each record (including the poison). Describe the cluster's behaviour for 2 minutes.

??? question "Answer"
    Each poll (or the processing after it) exceeds `max.poll.interval.ms`. The member is kicked, a rebalance runs, the same partition is assigned (maybe to the same process after restart). Auto-commit may already have committed offsets **before** the poison was successfully processed, or not, depending on the auto-commit interval versus when the exception happened. Typical mess: rebalance storm, duplicate processing of the records before the pill, and the pill still blocking if commit never moved past it. This is why poison + auto-commit + long processing is a [gotcha](gotchas.md) triple.
