# Kafka Labs

**Prerequisites**: Docker

```bash
# Start Kafka with Docker Compose
cat > docker-compose.yml << 'EOF'
version: '3'
services:
  zookeeper:
    image: confluentinc/cp-zookeeper:7.5.0
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181

  kafka:
    image: confluentinc/cp-kafka:7.5.0
    depends_on: [zookeeper]
    ports:
      - "9092:9092"
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: "true"
EOF

docker-compose up -d
pip install kafka-python
```

---

## Lab 1: Producer and Consumer

```python
# lab1_producer.py
from kafka import KafkaProducer
import json, time, random

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode(),
    key_serializer=str.encode
)

services = ['api', 'auth', 'payment', 'search']
customers = [f'customer_{i}' for i in range(100)]

for i in range(10000):
    event = {
        'timestamp': time.time(),
        'customer_id': random.choice(customers),
        'service': random.choice(services),
        'latency_ms': random.randint(10, 500),
        'status_code': random.choice([200, 200, 200, 200, 500])
    }
    producer.send(
        'user-events',
        key=event['customer_id'],
        value=event
    )

producer.flush()
print("Produced 10,000 events")
```

```python
# lab1_consumer.py
from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'user-events',
    group_id='lab-consumer',
    bootstrap_servers=['localhost:9092'],
    value_deserializer=lambda v: json.loads(v.decode()),
    auto_offset_reset='earliest',
    enable_auto_commit=True
)

count = 0
for message in consumer:
    count += 1
    if count % 1000 == 0:
        print(f"Consumed {count} messages. Latest: partition={message.partition}, offset={message.offset}")
    if count >= 10000:
        break

consumer.close()
```

---

## Lab 2: Observe Consumer Lag

```python
# lab2_slow_consumer.py - deliberately slow
from kafka import KafkaConsumer, KafkaProducer
import json, time

# First produce 50,000 messages quickly
producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                          value_serializer=lambda v: json.dumps(v).encode())
for i in range(50000):
    producer.send('lag-test', value={'n': i})
producer.flush()
print("Produced 50k messages. Now starting slow consumer...")

# Now consume at 100 messages/sec (production rate might be 5000/sec)
consumer = KafkaConsumer('lag-test', group_id='slow-consumer',
                         bootstrap_servers=['localhost:9092'],
                         auto_offset_reset='earliest')

count = 0
for msg in consumer:
    count += 1
    time.sleep(0.01)  # 10ms processing = 100 msgs/sec
    if count % 1000 == 0:
        print(f"Processed {count}")
    if count >= 5000:
        break
```

In a separate terminal, observe lag:
```bash
watch -n 2 kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --describe --group slow-consumer
```

---

## Lab 3: Simulate Broker Failure (Single Node)

```bash
# Produce to a topic
python lab1_producer.py

# In another terminal, stop Kafka mid-consumption
docker-compose stop kafka

# Observe: consumer throws exception
# Start Kafka again
docker-compose start kafka

# Consumer reconnects and resumes from last committed offset
```

Observe: with `acks=1`, did any messages get lost? With `acks=all`, the behavior differs — try both.
