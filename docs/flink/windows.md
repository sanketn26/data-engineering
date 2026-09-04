# Windows & Watermarks

## Why Windows Exist

A stream is unbounded. You cannot compute an aggregate over "all data" because the stream never ends.

Windows bound a stream into finite chunks so you can compute results.

---

## Window Types

### Tumbling Windows

Fixed-size, non-overlapping windows. Each event belongs to exactly one window.

```
Window 1: [10:00 – 10:01)
Window 2: [10:01 – 10:02)
Window 3: [10:02 – 10:03)
```

```
Events: ●●●●●●●●●●●●●●●●●●
         ←W1→←W2→←W3→←W4→
```

**Use case**: "error count per minute per service" — each minute gets an independent count.

### Sliding Windows

Fixed-size windows that overlap. Each event may belong to multiple windows.

```
Window 1 (size=5min, slide=1min): [10:00 – 10:05)
Window 2: [10:01 – 10:06)
Window 3: [10:02 – 10:07)
...
```

**Use case**: "rolling 5-minute average latency, updated every minute." The 5-minute window slides by 1 minute.

**Cost**: each event is counted in `size/slide` windows. A 60-minute window sliding by 1 minute means each event is counted 60 times — expensive.

### Session Windows

Variable-size windows based on activity. A session window closes after a period of inactivity.

```
User active: ●●●●  [inactivity gap > 30s]  ●●● [gap > 30s]  ●
              ←──Session 1──→               ←Session 2→      S3
```

**Use case**: user session analytics — a user's session is the span of activity bounded by inactivity gaps.

### Global Windows

No automatic windowing — you define a custom trigger. Used when you need precise control.

---

## Watermarks and Window Closure

A tumbling 1-minute event-time window `[10:00, 10:01)` closes when the watermark advances past `10:01`.

With a 30-second out-of-order tolerance:
- The watermark lags behind the maximum observed event time by 30 seconds
- When Flink sees an event at `10:01:30`, it sets watermark to `10:01:00`
- The `[10:00, 10:01)` window closes when watermark reaches `10:01`
- In practice, this means the `[10:00, 10:01)` window closes when you see event timestamps around `10:01:30`

**Trade-off**: larger watermark lag = more complete windows (fewer late events dropped) but higher result latency.

---

## Window Triggers

Triggers define when a window's contents are evaluated. Beyond watermark-based closure:

- **Processing-time trigger**: emit every N seconds regardless of event time (for approximate early results)
- **Count trigger**: emit after N elements
- **Custom triggers**: arbitrary logic

Early firing is useful when windows are large but you want approximate results sooner.

---

## Practical Example: Failed Login Detection

```python
# Count failed logins per user in a 5-minute tumbling event-time window
# Alert if count > 10
# Allow events up to 1 minute late

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.window import TumblingEventTimeWindows
from pyflink.common import Time, Duration
from pyflink.datastream.strategy import WatermarkStrategy

env = StreamExecutionEnvironment.get_execution_environment()

watermark_strategy = (WatermarkStrategy
    .for_bounded_out_of_orderness(Duration.of_minutes(1))
    .with_timestamp_assigner(lambda event, _: event['timestamp_ms']))

stream = (env
    .add_source(kafka_source)
    .assign_timestamps_and_watermarks(watermark_strategy))

alerts = (stream
    .filter(lambda e: e['event_type'] == 'login_failed')
    .key_by(lambda e: e['user_id'])
    .window(TumblingEventTimeWindows.of(Time.minutes(5)))
    .aggregate(CountAggregateFunction())
    .filter(lambda result: result.count > 10))

alerts.print()
env.execute("Failed Login Detector")
```

---

## Reasoning Exercise

You are computing a 1-hour sliding window with a 5-minute slide.

1. How many windows does an event at 10:23:15 belong to?
2. If your watermark lag is 2 minutes, when does the `[10:00, 11:00)` window close?
3. If events can arrive up to 5 minutes late, what watermark lag should you use?
4. For 10 million events/hour with 1-hour windows sliding every minute, how does memory scale?
