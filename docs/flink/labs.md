---
description: Hands-on Flink labs for stalling a watermark, killing a TaskManager, and verifying window and checkpoint recovery in the Flink UI, not on paper.
---

# Flink labs

Time.md's exercise asked you to predict what happens to a fraud window when one Kafka partition goes idle. Windows.md asked you to predict whether a tumbling or sliding window catches a login-failure burst that straddles a bucket boundary. A prediction on paper is cheap — Lab 3 (stall a watermark) and Lab 9 (sliding versus tumbling) are where you find out if yours was right, by running the failure and reading it off the actual Flink UI instead of reasoning about it from a chair.

Run the committed scripts under [`labs/flink`](https://github.com/sanketn26/data-engineering/tree/main/labs/flink), then use the longer experiments below. The failures (**stall a watermark**, **kill a TaskManager**) must produce the stated pass condition; a paper walkthrough is preparation, not completion.

**Prerequisites:** Docker, Python 3.9–3.11 for the pinned PyFlink wheel, ~4 GB RAM. Kafka labs' `service-events` topic is reused ([Kafka labs](../kafka/labs.md)). PyFlink 1.18.1 APIs are the tested teaching baseline; see [versions and primary sources](../reference/version-matrix.md) before changing it.

```bash
pip install 'apache-flink==1.18.1'
```

---

## Lab setup

```bash
cd labs/flink
pip install -r requirements.txt
python event_time.py
python stalled_watermark.py
docker compose up -d  # optional UI at 8081
```

Mini-cluster in-process (no Docker TM) is enough for labs 1–2:

```python
env = StreamExecutionEnvironment.get_execution_environment()
env.set_parallelism(2)
```

Open http://localhost:8081 if you used the compose file.

---

## Lab 1: event time versus processing time

**Goal:** 20 failed logins whose `timestamp` is five minutes ago must land in the **past** window under event time, and in **now** under processing time.

```python
# lab_time_semantics.py
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.window import (
    TumblingEventTimeWindows,
    TumblingProcessingTimeWindows,
)
from pyflink.common import Types, Time, Duration
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.datastream.functions import ProcessWindowFunction
import time

class CountWindow(ProcessWindowFunction):
    def process(self, key, context, elements):
        n = sum(1 for _ in elements)
        window = context.window()
        yield f"user={key} n={n} window={window.start}-{window.end}"

env = StreamExecutionEnvironment.get_execution_environment()
env.set_parallelism(1)

now = int(time.time() * 1000)
five_min_ago = now - 5 * 60 * 1000

# (user_id, event, ts_ms) — same shape as auth failures
rows = [
    ("u1", "login_failed", five_min_ago + i * 1000) for i in range(20)
]

stream = env.from_collection(
    rows,
    type_info=Types.TUPLE([Types.STRING(), Types.STRING(), Types.LONG()]),
)

wm = (
    WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_seconds(5))
    .with_timestamp_assigner(lambda row, ts: row[2])
)
event_time_stream = stream.assign_timestamps_and_watermarks(wm)

# EVENT TIME: should attach to the window covering five_min_ago
et = (
    event_time_stream.key_by(lambda row: row[0])
    .window(TumblingEventTimeWindows.of(Time.minutes(1)))
    .process(CountWindow(), output_type=Types.STRING())
)
et.print("event-time")

# PROCESSING TIME: same records, wall-clock windows (run this in a second job
# or comment the event-time pipeline to compare)
# pt = (
#     stream.key_by(lambda row: row[0])
#     .window(TumblingProcessingTimeWindows.of(Time.minutes(1)))
#     .process(CountWindow(), output_type=Types.STRING())
# )
# pt.print("processing-time")

env.execute("Lab 1 time")
```

**Expected (event time):** one window around `five_min_ago`, `n=20` (or split across two 1-minute buckets if the 20 seconds cross a minute boundary).

**Expected (processing time):** counts in the window that contains **wall clock now**. Fraud using this clock is wrong.

The committed `event_time.py` contains this exact API shape. If it fails on the pinned environment, treat that as a course defect and report the full traceback.

---

## Lab 2: keyed state for failed logins

```python
# lab_stateful.py
from pyflink.datastream import StreamExecutionEnvironment, KeyedProcessFunction
from pyflink.datastream.state import ValueStateDescriptor
from pyflink.common import Types

class FailedLoginCounter(KeyedProcessFunction):
    def open(self, ctx):
        self.count = ctx.get_state(ValueStateDescriptor("count", Types.INT()))

    def process_element(self, row, ctx: KeyedProcessFunction.Context):
        user, event_type = row[0], row[1]
        n = self.count.value() or 0
        if event_type == "login_failed":
            n += 1
            self.count.update(n)
            if n > 5:
                yield f"ALERT {user} n={n}"
        elif event_type == "login_success":
            self.count.clear()

env = StreamExecutionEnvironment.get_execution_environment()
env.set_parallelism(1)

events = [
    ("user1", "login_failed"),
    ("user1", "login_failed"),
    ("user2", "login_failed"),
    ("user1", "login_failed"),
    ("user1", "login_failed"),
    ("user1", "login_failed"),
    ("user1", "login_failed"),  # 6th → alert
    ("user1", "login_success"),
    ("user1", "login_failed"),  # back to 1
]

stream = env.from_collection(
    events, type_info=Types.TUPLE([Types.STRING(), Types.STRING()])
)
stream.key_by(lambda r: r[0]).process(FailedLoginCounter()).print()
env.execute("Lab 2 state")
```

**Expected:** one `ALERT user1 n=6`. `user2` never alerts. After success, a single failure is silent.

This is the [state](state.md) lecture in nine rows.

---

## Lab 3: now break it — stall the watermark with an idle partition

**Goal:** prove that `min` watermarks across Kafka partitions freeze windows.

1. Create topic `login-events` with **3 partitions**.
2. Produce a steady stream of *current* event-time JSON **only with a key that hashes to partition 0** (print `message.partition` from a Python consumer until you find a key; `user_idle_test` may not be it — brute force keys).

```python
# lab3_busy_partition.py
from kafka import KafkaProducer
import json, time

p = KafkaProducer(
    bootstrap_servers=["localhost:9092"],
    key_serializer=str.encode,
    value_serializer=lambda v: json.dumps(v).encode(),
)
key = "only-p0"  # adjust until kafka-console-consumer shows partition 0
while True:
    p.send(
        "login-events",
        key=key,
        value={
            "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S.000Z", time.gmtime()),
            "customer_id": "cust_1",
            "user_id": key,
            "service": "auth",
            "endpoint": "/login",
            "region": "eu-west-1",
            "latency_ms": 10,
            "status_code": 401,
            "bytes": 100,
            "ts_ms": int(time.time() * 1000),
        },
    )
    p.flush()
    time.sleep(0.5)
```

3. Run a Flink job: KafkaSource on `login-events`, **no** `with_idleness`, bounded out-of-orderness 5s, tumbling **10s** event-time windows, print counts.

**Expected:** Kafka UI/consumer shows data on one partition, zeros on the others. Flink **windows do not emit** (watermark stuck at the idle partitions). Checkpoint size of the window operator slowly grows.

4. Add `.with_idleness(Duration.of_seconds(10))` and rerun.

**Expected:** windows start emitting. You have reproduced the [idle source](time.md) incident.

If you cannot run Kafka, write the same experiment as a `from_collection` of two streams unioned — one silent — and reason about `min` watermarks; then still add idleness.

---

## Lab 4: now break it — poison / bad timestamp

Inject one event with `ts_ms=0` (or `timestamp=1970-01-01`) into an otherwise healthy event-time job.

**Expected:** a watermark generator that uses `max(event_time) - bound` may **not** jump backward (max ignores 0 after it has seen real times), but a custom generator that averages, or a sort, might. More commonly, the 1970 record is **late** for every window and hits the late counter / side output. Filter `ts_ms` to `[now-1d, now+1h]` before assigning watermarks.

This is the cousin of Kafka's [poison pill](../kafka/gotchas.md): one record does not crash the JVM, it **corrupts time**.

---

## Lab 5: now break it — kill a TaskManager

With the Docker compose cluster:

1. Submit a checkpointing job (interval 10s) that uses keyed state (lab 2, but from Kafka so it is long-running).
2. Watch http://localhost:8081 → Checkpoints until one **COMPLETED**.
3. `docker compose -f docker-compose-flink.yml stop taskmanager`
4. Job goes to FAILING/RESTARTING (or you start a new TM).
5. `docker compose -f docker-compose-flink.yml start taskmanager`

**Expected:** restore from last successful checkpoint; Kafka offsets rewind; **duplicate alerts** if the sink is `print()` / non-idempotent. Checkpoint duration and Kafka lag by partition jump, then recover. This is [checkpoints](checkpoints.md).

If HA is not configured, killing the **JobManager** loses the job — contrast with killing a TM.

---

## Lab 6: backpressure (conceptual)

Add `time.sleep(0.05)` in a map at parallelism 1 while producing 1k events/s.

**Expected:** Flink UI backpressure badge on the map; checkpoint alignment time grows; if sleep is bad enough, checkpoints **fail by timeout**. Remove the sleep or scale the operator. Do not "fix" this by disabling checkpoints.

---

## Lab 7: checkpoint files (what "restore" means)

Enable checkpointing to a local dir in mini-cluster:

```python
env.enable_checkpointing(5_000)
env.get_checkpoint_config().set_checkpoint_storage("file:///tmp/flink-cps")
```

Run lab 2 from a finite collection (too short) or a looping source. After a checkpoint appears under `/tmp/flink-cps`, inspect `_metadata`. You will not read the binary state by eye; you *will* see that a checkpoint is a directory with operator ids, not a magic JVM snapshot.

Cancel the job. Restore:

```bash
# from a real cluster
flink run -s file:///tmp/flink-cps/<job-id>/chk-12 -c your.Job job.jar
```

Mini-cluster Python restore is fiddly; on paper, name what would restore: the `ValueState` counts, not the `print()` sink. That is why lab 5 duplicates alerts.

---

## Lab 8: Kafka source offsets versus consumer groups

With `KafkaSource` and checkpoints, Flink commits offsets **on checkpoint** if configured. Disable checkpointing and observe: after restart, behaviour depends on `set_starting_offsets` (`committedOffsets`, `latest`, `earliest`).

**Expected:** without checkpoints, "start from committed" is only as good as group commits; you are back in [Kafka at-least-once](../kafka/exactly-once.md). With checkpoints, the JobManager's snapshot is the cursor that matters. Mixing a hand-managed consumer group with a Flink job on the same `group.id` is a good way to steal partitions from yourself — use a dedicated group id.

---

## What to write down

| Observation | Which page explains it |
|-------------|------------------------|
| Past events in current window | [time](time.md) processing time |
| No window output, Kafka lag 0 on some partitions | Idle watermark |
| ALERT after 6 failures, reset on success | [state](state.md) |
| Duplicate ALERT after TM kill | At-least-once sink + replay |
| Checkpoint timeout | Backpressure / alignment |

---

## Lab 9: sliding versus tumbling on paper (then in code)

Generate 11 `login_failed` events for `u_split` at times `10:04:50` through `10:05:40` (one every 5s).

- Tumbling 5-minute windows `[10:00,10:05)` and `[10:05,10:10)`: counts 3 and 8 — **no alert** if threshold is 10.
- Sliding 5-minute, slide 1 minute: at least one window contains all 11 — **alert**.

Implement both in the lab 1 harness with synthetic timestamps. This is the [windows](windows.md) fraud-edge case.

---

## Exercise

You run lab 3 with idleness 10s. Partition 1 and 2 are idle. At 12:00 a producer starts sending **buffered** events for partition 1 with timestamps 11:00–11:05.

What do the 11:00 tumbling minutes do? What should the late side output contain? How would you have designed the partner integration instead?

??? question "Answer"
    Watermarks already advanced (idleness) to ~12:00 minus bound. 11:00–11:05 events are **late** by ~55 minutes. Tumbling windows for 11:00 are gone unless `allowed_lateness` covers an hour (it should not). Side output should receive those records if configured; otherwise they are dropped. Design: separate topic/job for the buffered partner, or a replay pipeline into the lake, not a shared watermark with the live fraud job ([time](time.md) exercise, same moral).
