---
description: Diagnosing Flink RocksDB state growth from key skew, missing TTL, and ListState misuse, before a stateful counter job floods disk.
---

# Stateful processing

A TaskManager disk-usage alert fires: the RocksDB directory has hit 380 GB and is still climbing, on a job that does nothing more exotic than count failed logins per `user_id`.

A. Key skew — one whale `customer_id` is holding most of the state.
B. No TTL — state for users who logged in successfully months ago is still sitting there.
C. The job stores a `ListState` of every raw event instead of a small counter.
D. A broadcast dimension table is too large and is duplicated on every TaskManager.

Predict which one before reading on — then work out how many bytes per key you would expect if the diagnosis is right.

## Use case

Windows are state with an opinionated API. Plenty of jobs need memory of the past **without** a window:

- "How many failed logins has this `user_id` had since the last success?"
- "What was the previous `latency_ms` for this `endpoint`?" (delta)
- "Is this IoT `device_id` in the same session?"
- E-commerce: join a click stream with a **broadcast** table of fraud rules that update every minute.

A **stateless** map/filter cannot do this. A **stateful** operator stores bytes keyed by something, checkpoints them ([checkpoints](checkpoints.md)), and restores after a crash.

---

## Why this is hard at scale

State is a database you accidentally wrote inside the pipeline.

10 million users × 100 bytes looks like 1 GB. Then you add a `MapState` of 30 days of days. Then you forget TTL. Then RocksDB holds 400 GB on each TaskManager's local SSD, checkpoints take 25 minutes, and a rescale means 400 GB reshuffle.

Keyed state is sharded by key hash onto parallel subtasks — the same skew story as [Kafka partitions](../kafka/partitions.md). One `customer_id` whale holds a giant `ListState` on one slot.

---

## Intuition

After `key_by(user_id)`, each operator subtask owns a slice of keys. For each key you may keep:

- `ValueState` — one value (the count)
- `ListState` — append-only list (careful)
- `MapState` — map inside the key (e.g. endpoint → count)
- `ReducingState` / `AggregatingState` — incremental

Timers (`register_event_time_timer`) fire when the watermark (or processing time) reaches a timestamp — how you implement "reset after 5 minutes" without a window.

Operator state is **not** keyed: each subtask has its own list (Kafka offsets in older sources, buffers). Broadcast state is a replicated map every parallel instance sees (rules, dimension tables).

---

## Internals: keyed vs operator vs broadcast

### Keyed state

```python
from pyflink.datastream import KeyedProcessFunction
from pyflink.datastream.state import ValueStateDescriptor
from pyflink.common import Types

class FailedLoginCounter(KeyedProcessFunction):
    def open(self, runtime_context):
        self.count = runtime_context.get_state(
            ValueStateDescriptor("failed_login_count", Types.INT())
        )

    def process_element(self, event, ctx: KeyedProcessFunction.Context):
        n = self.count.value() or 0
        if event["endpoint"] == "/login" and event["status_code"] in (401, 403):
            n += 1
            self.count.update(n)
            if n > 10:
                yield f"ALERT {event['user_id']} count={n}"
        elif event["status_code"] == 200 and event["endpoint"] == "/login":
            self.count.clear()
```

State for `u_99102` never collides with `u_12`. Parallelism can increase **if you restore from a savepoint** (Flink redistributes keys). Changing the **key** (`user_id` → `customer_id`) makes old state unreachable.

### Operator state

Per-subtask, not per-key. Used by sources (split assignment), custom connectors, random buffers. On restore, Flink redistributes operator **list** state with a routing scheme (even-split, union). Getting this wrong duplicates Kafka splits.

### Broadcast state

A control stream (fraud rules, "this `customer_id` is a whale") is broadcast to all parallel instances of a `BroadcastProcessFunction`. The keyed event stream reads the broadcast map. Rules must fit in memory **on every TaskManager**.

---

## Internals: state backends

| Backend | Where | Best for |
|---------|--------|----------|
| `HashMapStateBackend` | JVM heap | Small state, low latency, dev |
| `EmbeddedRocksDBStateBackend` | Local disk + block cache | State larger than heap; production default at scale |

```python
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.state_backend import EmbeddedRocksDBStateBackend, HashMapStateBackend

env = StreamExecutionEnvironment.get_execution_environment()
env.set_state_backend(EmbeddedRocksDBStateBackend(True))  # incremental checkpoints
```

RocksDB: each state access may be a disk read (with memory cache). Expect **10–100×** slower than heap for random `ValueState` access. Incremental checkpoints only work with RocksDB (and a few others); they upload SST diffs rather than the full state each time.

Heap backend: one GC pause is a latency incident; a 20 GB heap of state will not go well.

!!! warning "Local disk is not the source of truth"
    RocksDB on the TaskManager is a cache of state. **Checkpoints** on S3/HDFS are the durability. Lose a disk without a checkpoint and you replay from the last successful checkpoint, not from "whatever was on that SSD".

---

## Internals: TTL and timers

Unbounded keyed state is a leak.

```python
from pyflink.datastream.state import StateTtlConfig, ValueStateDescriptor
from pyflink.common import Duration, Types

ttl = (
    StateTtlConfig.new_builder(Duration.of_hours(24))
    .set_update_type(StateTtlConfig.UpdateType.OnCreateAndWrite)
    .set_state_visibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
    .cleanup_full_snapshot()
    .build()
)
desc = ValueStateDescriptor("count", Types.INT())
desc.enable_time_to_live(ttl)
```

TTL cleanup is not instant; RocksDB compaction / snapshot cleanup eventually drops it. Timers you register are also state. A timer per event (instead of per key) is a well-known OOM.

Event-time timers fire on watermark progress — idle Kafka partitions that stall watermarks also stall timers ([time](time.md)).

---

## How: session-ish fraud without a window

```python
class RollingFailures(KeyedProcessFunction):
    """Count failures; expire the count 5 minutes after the last failure (event time)."""

    def open(self, ctx):
        self.count = ctx.get_state(ValueStateDescriptor("n", Types.INT()))
        self.last_ts = ctx.get_state(ValueStateDescriptor("t", Types.LONG()))

    def process_element(self, event, ctx: KeyedProcessFunction.Context):
        ts = event["ts_ms"]
        n = self.count.value() or 0
        if event["status_code"] in (401, 403):
            n += 1
            self.count.update(n)
            self.last_ts.update(ts)
            ctx.timer_service().register_event_time_timer(ts + 5 * 60 * 1000)
            if n > 10:
                yield event
        else:
            self.count.clear()

    def on_timer(self, timestamp, ctx: KeyedProcessFunction.OnTimerContext):
        last = self.last_ts.value()
        if last is not None and timestamp >= last + 5 * 60 * 1000:
            self.count.clear()
            self.last_ts.clear()
```

This is exact "last 5 minutes" only if you also decrement on timer for **each** failure (a list of timestamps). The sketch above is a reset-after-quiet approximation — know which one product wanted.

---

## Gotchas

- **`ListState` of raw events** for a 1-hour window: you stored the stream twice. Use `AggregateFunction`.
- **Changing state descriptor names** without a migration: restore finds empty state. Use `@type_info` / `StateMigration` / savepoint with schema evolution (POJOs/Avro, not untyped Python tuples if you care).
- **Python / PyFlink state** crosses to the JVM; hot paths at 2M/s are Java/Scala operators.
- **TTL OnReadAndWrite** vs OnCreateAndWrite: a noisy read-only key lives forever with the former.
- **Broadcast a 5 GB dimension table:** every TM holds 5 GB. Use a regular keyed join or an external store.

---

## Failure modes

| Failure | Symptom |
|---------|---------|
| No TTL | Checkpoint size monotonic ↑ |
| Hot key | One TM disk / CPU hot |
| Heap backend + big state | GC death spiral |
| Restore with different parallelism without savepoint | Job refuses / state unused |
| Timers on processing time during catch-up | All timers fire at once |

---

## RocksDB vs heap: a concrete latency picture

Heap `ValueState` is a hash lookup — tens of nanoseconds if the object is hot. RocksDB is a `get` that may hit block cache (microseconds) or SSD (tens to hundreds of microseconds). A `KeyedProcessFunction` that does five state reads per event at 100k events/s/core is trivial on heap and a CPU+IO budget on RocksDB.

That is why fraud counts (tiny value, huge cardinality) still *work* on RocksDB, while "store the last 100 events as `ListState`" does not. Access pattern × cardinality × backend is the capacity model.

Measure `ValueState` access time in the Flink metrics (RocksDB `actual-user-key` histograms if enabled) before you "optimise" the algorithm. Many jobs are sink-bound; RocksDB is innocent.

---

## Debugging

| Metric | Meaning |
|--------|---------|
| Checkpoint size / duration **per operator** | Who owns the state |
| RocksDB `num-running-compactions`, `block-cache-usage` | Disk backend health |
| `state.backend.rocksdb.memory.managed` | Whether Flink manages the cache |
| Per-subtask `numRecordsIn` | Key skew |
| Number of registered timers | Timer leak |

If checkpoint size of `FailedLoginCounter` is 80% of the job, dump key cardinality: unique `user_id` per day vs TTL.

---

## Scale: 10× / 100× / 1000×

Assume 8 bytes count + 32 bytes key overhead ≈ 50 bytes/key (the exercise in the original notes).

| Scale | Keys | Rough state | Backend |
|-------|------|-------------|---------|
| **10×** | ~1M users | ~50 MB | Heap is fine |
| **100×** | ~10M | ~0.5 GB + RocksDB amplification (often 3–10×) | RocksDB, incremental CP |
| **1000×** | ~100M devices | tens of GB **per job**, not per TM if well sharded | RocksDB, TTL, maybe split jobs |

7-day TTL at 10M users/day ≈ 70M keys × 50 B ≈ 3.5 GB *logical* plus RocksDB files. Checkpoint duration becomes the SLO.

---

## Trade-offs

| Choice | Gain | Cost |
|--------|------|------|
| Heap backend | Latency | Size cap, GC |
| RocksDB | Size | Latency, ops |
| TTL | Bounded disk | Forgotten keys |
| Broadcast | Fast rule updates | Memory × parallelism |
| Query external DB per event | Tiny Flink state | Latency, load on DB, harder EOS |

---

## Alternatives

- **Kafka Streams** state stores (also RocksDB, changelog topics) — same idea, embedded.
- **Redis / DynamoDB** as the store, Flink stateless — operationally simpler at modest QPS; you lose local checkpoint atomicity unless you design idempotent writes.
- **Spark** `mapGroupsWithState` / `flatMapGroupsWithState` — micro-batch state; see [comparison](comparison.md).

---

## Joins are state

A stream-stream interval join buffers records from both sides until the watermark says they cannot match. That buffer **is** keyed state. A 5-minute join window at 200k clicks/s is a large RocksDB.

A stream-table join against a compacted `user-profile` topic keeps the latest profile per `user_id` as `ValueState` (or a `KTable` in Kafka Streams). Cardinality = users, not events — usually the right shape.

Broadcast join: rules file 2 MB, every TM holds it. Fine. Broadcast a 20 GB item catalogue: you have designed a distributed out-of-memory error.

Querying Postgres from `process_element` is **not** Flink state. It is an RPC. It will not be snapshotted. It will amplify QPS by the event rate. Cache with TTL *in state* if you must, and accept staleness.

---

## Rescaling

Heap and RocksDB keyed state can be redistributed on restore from a **savepoint** (or from a retained checkpoint when the deployment supports it). The unit is a **key group**, not an individual key. Runtime parallelism may be any positive value up to `maxParallelism`; it does **not** need to divide it evenly. Set `maxParallelism` deliberately before the first state you must preserve, because changing it changes key-to-group assignment and normally prevents compatible restore.

Operator state (source splits) rescales with a different contract. Never assume a custom source survived a 4→12 rescale without a test.

---

## Changelog thinking (why Kafka Streams feels similar)

Every keyed state update can be emitted as a changelog: `(key, new_value)` or a retraction. Kafka Streams does this onto compact topics so a restarted instance can rebuild RocksDB. Flink does it onto checkpoint storage instead (unless you use queryable state / SQL upsert to Kafka).

The implication for **this** academy: if you need another job to read "current failed-login count per user", sink a compacted changelog topic from Flink rather than poking TaskManager memory. That topic is the [compaction](../kafka/log.md) use case — latest value per `user_id`.

Do not log every state update to a JSON debug topic at 2M/s. You will DDoS Kafka and learn nothing.

---

## Serialisation of state

Heap backend keeps Java objects (or PyFlink's representation). RocksDB needs **bytes**. A change to the Python tuple layout without a TypeSerializer migration yields silent corruption or restore failure.

Prefer explicit `Types.INT()`, `Types.STRING()`, POJOs, Avro for anything you will restore in six months. `pickle` in state is a future incident.

Queryable state (Flink) lets other services hit a TM over RPC for the current count. It is convenient and operationally fragile (you are now serving production reads from the processor). The robust pattern is still: compacted Kafka changelog or a serving DB populated by the sink.

For IoT, shard state by `device_id` prefix onto **separate jobs** if a single RocksDB instance cannot compact fast enough. That is a 1000× move, not a 10× one. At 10×, TTL and a single job are enough.

---

## How: RocksDB job config worth copying

```python
config = Configuration()
config.set_string("state.backend.incremental", "true")
config.set_string("state.backend.rocksdb.memory.managed", "true")
config.set_string("state.checkpoints.dir", "s3://data-platform/flink/checkpoints")
env = StreamExecutionEnvironment.get_execution_environment(config)
env.set_state_backend(EmbeddedRocksDBStateBackend(True))
```

Local RocksDB directories belong on NVMe, not on a shared network volume. NFS-backed `rocksdb` is a classic "why is everything 20× slower" ticket.

---

## How to apply this at work

Inventory every `get_state` / `ValueStateDescriptor` in the job. For each: key, estimated cardinality, bytes per key, TTL, backend. If nobody can estimate cardinality, you do not have a state budget.

---

## Exercise

10 million unique users/day. Keyed `ValueState` of one float per user (~50 bytes/pair including overhead). RocksDB.

1. State after 1 day, no TTL?
2. Steady-state with 7-day TTL?
3. When do you worry about checkpoint duration?
4. What RocksDB knobs matter first?

??? question "Answer"
    1. ~10e6 × 50 B = **~500 MB** logical. RocksDB files, indexes, and replication in checkpoints often land at **1–3 GB**. Fine.

    2. ~70e6 keys × 50 B = **~3.5 GB** logical, teens of GB on disk. Still a single-job size if TMs have local NVMe.

    3. When a **full** checkpoint cannot finish inside `checkpoint timeout` or blocks the next checkpoint (unaligned/aligned barriers backing up). Rule of thumb: if upload to S3 at 200 MB/s of a 40 GB full snapshot is minutes, you **must** use incremental checkpoints and you should keep state well under a size that makes restore longer than your RTO. Worry before 10+ minutes of checkpoint duration.

    4. Managed memory (`state.backend.rocksdb.memory.managed: true`), incremental checkpoints, local SSD (not NFS), and TTL cleanup. Do not cargo-cult `block-cache` sizes until you see cache hit rate.
