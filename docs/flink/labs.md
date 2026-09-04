# Flink Labs

## Lab Setup

```bash
# Docker Compose for Flink + Kafka
cat > docker-compose-flink.yml << 'EOF'
version: '3'
services:
  zookeeper:
    image: confluentinc/cp-zookeeper:7.5.0
    environment: {ZOOKEEPER_CLIENT_PORT: 2181}

  kafka:
    image: confluentinc/cp-kafka:7.5.0
    depends_on: [zookeeper]
    ports: ["9092:9092"]
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1

  jobmanager:
    image: flink:1.18-scala_2.12
    ports: ["8081:8081"]
    command: jobmanager
    environment: {FLINK_PROPERTIES: "jobmanager.rpc.address: jobmanager"}

  taskmanager:
    image: flink:1.18-scala_2.12
    depends_on: [jobmanager]
    command: taskmanager
    environment:
      FLINK_PROPERTIES: |
        jobmanager.rpc.address: jobmanager
        taskmanager.numberOfTaskSlots: 4
EOF

docker-compose -f docker-compose-flink.yml up -d
```

---

## Lab 1: Event Time vs Processing Time

**Goal**: observe the difference between event time and processing time windows.

Generate events with timestamps that are 5 minutes in the past, and observe how event-time windows produce correct counts while processing-time windows do not.

```python
# lab_time_semantics.py
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.window import TumblingEventTimeWindows, TumblingProcessingTimeWindows
from pyflink.common import Types, Time, Duration, WatermarkStrategy
import json, time

# Run with PyFlink
# pip install apache-flink

env = StreamExecutionEnvironment.get_execution_environment()

# Generate events with timestamps 5 minutes in the past
now = int(time.time() * 1000)
five_min_ago = now - 5 * 60 * 1000

events_data = [
    {'user_id': 'u1', 'event': 'login_failed', 'ts': five_min_ago + i * 1000}
    for i in range(20)  # 20 failed logins in the 5-minutes-ago window
]

# Create a stream from collection
stream = env.from_collection(
    [(e['user_id'], e['event'], e['ts']) for e in events_data],
    type_info=Types.ROW([Types.STRING(), Types.STRING(), Types.LONG()])
)

# With PROCESSING TIME: these events will go into the current minute window
# With EVENT TIME: these events correctly go into the 5-minutes-ago window

watermark_strategy = (WatermarkStrategy
    .for_bounded_out_of_orderness(Duration.of_seconds(5))
    .with_timestamp_assigner(lambda row, _: row[2]))

event_time_stream = stream.assign_timestamps_and_watermarks(watermark_strategy)

# Count per user per minute using event time
counts = (event_time_stream
    .key_by(lambda row: row[0])
    .window(TumblingEventTimeWindows.of(Time.minutes(1)))
    .apply(lambda key, window, rows, out: out.collect(
        f"User {key}: {sum(1 for _ in rows)} events in window {window}"
    )))

counts.print()
env.execute("Time Semantics Lab")
```

---

## Lab 2: Stateful Failed Login Detection

```python
# lab_stateful.py - Count failed logins, alert if > 5 in session
from pyflink.datastream import StreamExecutionEnvironment, KeyedProcessFunction
from pyflink.datastream.state import ValueStateDescriptor
from pyflink.common import Types, Duration, WatermarkStrategy

class FailedLoginCounter(KeyedProcessFunction):
    def open(self, ctx):
        self.count = self.get_runtime_context().get_state(
            ValueStateDescriptor("count", Types.INT()))

    def process_element(self, row, ctx):
        event_type = row[1]
        current = self.count.value() or 0

        if event_type == 'login_failed':
            new_count = current + 1
            self.count.update(new_count)
            if new_count > 5:
                yield f"ALERT: {row[0]} has {new_count} failed logins"
        elif event_type == 'login_success':
            self.count.update(0)  # reset

env = StreamExecutionEnvironment.get_execution_environment()

# Mix of failed and successful logins
events = [
    ('user1', 'login_failed'), ('user1', 'login_failed'),
    ('user2', 'login_failed'), ('user1', 'login_failed'),
    ('user1', 'login_failed'), ('user1', 'login_failed'),
    ('user1', 'login_failed'),  # 6th failure → alert
    ('user1', 'login_success'), # reset
    ('user1', 'login_failed'),  # back to 1, no alert
]

stream = env.from_collection(events, type_info=Types.ROW([Types.STRING(), Types.STRING()]))

alerts = stream.key_by(lambda row: row[0]).process(FailedLoginCounter())
alerts.print()
env.execute("Failed Login Detector")
```
