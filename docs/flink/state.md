# Stateful Processing

## Why State Is Necessary

Some computations require memory of past events:

- "How many times has this user logged in today?" — requires a running count
- "What was the previous value of this metric?" — requires the last seen value
- "Is this the same session as the last event?" — requires session state
- "Has this user exceeded their rate limit?" — requires a rolling window counter

A **stateless** stream processor handles each event independently. A **stateful** stream processor maintains information across events.

---

## Types of State in Flink

### Keyed State

State scoped to a key. Most common type.

```python
class FailedLoginCounter(KeyedProcessFunction):
    def open(self, context):
        # State descriptor: one counter per user_id key
        self.count_state = self.get_runtime_context().get_state(
            ValueStateDescriptor("failed_login_count", Types.INT())
        )

    def process_element(self, event, ctx):
        current = self.count_state.value() or 0
        if event['type'] == 'login_failed':
            self.count_state.update(current + 1)
        elif event['type'] == 'login_success':
            self.count_state.update(0)  # reset on success

        if (self.count_state.value() or 0) > 10:
            ctx.output(alert_tag, f"User {event['user_id']} exceeded failed login threshold")
```

Each key (user_id) has its own independent state. State for user A does not interfere with state for user B.

### Operator State

State not tied to a key — shared across all instances of an operator. Used for:
- Kafka offsets (per operator instance)
- Buffering records for broadcast

### Broadcast State

Special state that is distributed to all parallel instances of an operator. Used when you need all instances to share the same data (e.g., configuration, rules for enrichment).

---

## State Backends

State needs to be stored somewhere. Flink supports multiple state backends:

| Backend | Storage | Best For |
|---------|---------|----------|
| HashMapStateBackend | JVM heap | Fast, small state, development |
| EmbeddedRocksDBStateBackend | RocksDB on local disk | Large state that doesn't fit in memory |

For production with large state: **RocksDB**.

```python
env.set_state_backend(EmbeddedRocksDBStateBackend())
```

RocksDB stores state on local disk and uses an LRU cache in memory. State can exceed memory. The trade-off: RocksDB reads/writes are ~10-100× slower than in-memory.

---

## State Expiration (TTL)

Unbounded state growth is dangerous. If you track state per user_id and you have 100 million users, state grows indefinitely.

Use **state TTL** to expire stale state:

```python
from pyflink.datastream.state import StateTtlConfig

ttl_config = (StateTtlConfig
    .new_builder(Duration.of_hours(24))
    .set_update_type(StateTtlConfig.UpdateType.OnCreateAndWrite)
    .set_state_visibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
    .build())

state_descriptor = ValueStateDescriptor("count", Types.INT())
state_descriptor.enable_time_to_live(ttl_config)
```

---

## Reasoning Exercise

You are processing 10 million unique users/day. Your keyed state stores a float per user. Using RocksDB, each key-value pair takes approximately 50 bytes.

1. How much state accumulates after 1 day without TTL?
2. With a 7-day TTL, what is the steady-state size?
3. What RocksDB configuration might be necessary?
4. At what state size would you worry about checkpoint duration?
