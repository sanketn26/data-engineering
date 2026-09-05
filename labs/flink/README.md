# Flink lab

Two tracks: **PyFlink on the laptop** (enough for time + state) and an **optional Compose** UI. You will predict event-time vs processing-time windows, then **break** watermarks so a windowed job produces **no output** — [incident 3](../../docs/incidents/index.md).

Docs: [Flink labs](../../docs/flink/labs.md), [time](../../docs/flink/time.md), [windows](../../docs/flink/windows.md), [IoT architecture](../../docs/architectures/iot.md).

## Prerequisites

```bash
pip install -r requirements.txt
# Java 11/17 required
```

Optional UI (JobManager 8081):

```bash
docker compose up -d
```

### Committed `docker-compose.yml`

```yaml
services:
  jobmanager:
    image: flink:1.18.1-scala_2.12
    ports: ["8081:8081"]
    command: jobmanager
    environment:
      FLINK_PROPERTIES: |
        jobmanager.rpc.address: jobmanager
  taskmanager:
    image: flink:1.18.1-scala_2.12
    depends_on: [jobmanager]
    command: taskmanager
    environment:
      FLINK_PROPERTIES: |
        jobmanager.rpc.address: jobmanager
        taskmanager.numberOfTaskSlots: 4
```

PyFlink mini-cluster (default `StreamExecutionEnvironment`) does **not** need this Compose. Use Compose when you want the Flink Web UI while submitting a job.

Run the committed event-time job with `python event_time.py`. Run the dependency-free failure model with `python stalled_watermark.py`.

## Predict

1. 20 events whose **event timestamps** are 5 minutes ago, processed **now**:
   - Processing-time 1-minute tumbling windows: which minute do they land in?
   - Event-time 1-minute windows with a bounded watermark: which minute?
2. If **one** Kafka partition (or one source split) stops emitting and its last watermark is old, do event-time windows close?
3. Keyed `ValueState` failed-login count: after `login_success`, does the next failure alert?

## Run — event time vs processing time

```python
import time
from pyflink.common import Types, Time, Duration
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.functions import ProcessWindowFunction
from pyflink.datastream.window import (
    TumblingEventTimeWindows,
    TumblingProcessingTimeWindows,
)
from pyflink.common.watermark_strategy import WatermarkStrategy

class CountWindow(ProcessWindowFunction):
    def process(self, key, context, elements):
        yield f"event-time key={key} count={sum(1 for _ in elements)} " \
              f"win={context.window().start}-{context.window().end}"

env = StreamExecutionEnvironment.get_execution_environment()
env.set_parallelism(1)

now = int(time.time() * 1000)
five_min_ago = now - 5 * 60 * 1000
rows = [("u1", "login_failed", five_min_ago + i * 1000) for i in range(20)]

stream = env.from_collection(
    rows,
    type_info=Types.ROW_NAMED(
        ["user_id", "event", "ts"],
        [Types.STRING(), Types.STRING(), Types.LONG()],
    ),
)

wm = (
    WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_seconds(5))
    .with_timestamp_assigner(lambda row, _: row[2])
)
event_time = stream.assign_timestamps_and_watermarks(wm)

counts = (
    event_time.key_by(lambda row: row[0])
    .window(TumblingEventTimeWindows.of(Time.minutes(1)))
    .process(CountWindow(), output_type=Types.STRING())
)
counts.print()
env.execute("lab-event-time")
```

Re-run using `TumblingProcessingTimeWindows` **without** the watermark assigner (or ignore event ts). **Compare to prediction 1:** processing-time buckets **now**; event-time buckets **five minutes ago**.

Because `from_collection` is bounded, it emits a terminal watermark and closes remaining windows. A long-running Kafka source does not terminate, so it needs advancing per-split watermarks and an idleness policy.

## Break — stalled watermark (Kafka partitions)

Use the runnable KafkaSource exercise in `../../docs/flink/labs.md` Lab 3, or run `python stalled_watermark.py` in this directory for a deterministic model of two source splits. Split 0 advances with current events; split 1 retains an old watermark and then goes silent. The downstream minimum remains old until the idle timeout excludes split 1.

An old event on the **same active split** does not move a bounded-out-of-orderness watermark backwards: that strategy tracks the maximum timestamp seen. A future timestamp can jump it forward; an idle split can hold the downstream minimum back. Keep these as separate failure modes.

This is the IoT overnight stall: a quiet source split vs the downstream `min` watermark.

## Run — keyed state (failed logins)

Use the `FailedLoginCounter` in [docs/flink/labs.md](../../docs/flink/labs.md) Lab 2. **Predict** the alert on the 6th failure; reset on success.

## Optional: Kafka source

If `labs/kafka` is up, consume `user-events` with a Flink Kafka source (add the connector jar). That is extra credit; the watermark incident does **not** require Kafka.

## Notes

- Checkpoints succeeding while `records_out=0` is the on-call trap.
- Do not add a second processor "to fix" a watermark bug.
