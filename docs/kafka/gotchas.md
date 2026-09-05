# Production gotchas

These are the Kafka failures that show up after the happy-path tutorial. Each one is a real pattern on SaaS analytics, observability, e-commerce, IoT, or fraud — using the same `{timestamp, customer_id, user_id, service, endpoint, region, latency_ms, status_code, bytes}` events.

---

## Use case

The same five systems share Kafka and then fail in different ways. This page is the incident catalogue: lag, rebalances, poison pills, schemas, hot partitions, disks, EOS misconfig, retention skips. Read it after [partitions](partitions.md) and [replication](replication.md); use it as the runbook during [labs](labs.md).

---

## Why this is hard at scale

Kafka's happy path (append, fetch, commit) hides a pile of independent clocks: producer retries, ISR, consumer poll, retention, schema, sink latency. At 10×, one of them is red and you can see it. At 100×, three are red and the first metric you open (lag) is a *symptom*. The rest of this page is the differential diagnosis.

---

## Intuition

Treat every incident as one of: **the log is wrong** (acks, ISR, unclean, retention), **the cursor is wrong** (commits, reset, rebalance), **the work is skewed** (hot key, poison, slow sink), **the contract is wrong** (schema, EOS isolation). Metrics below map onto those four.

---

## Internals (where to look)

Brokers: URP, ISR shrink, disk, request p99. Groups: lag **by partition**, rebalance generation, `max.poll.interval.ms`. Clients: produce error rate, commit rate, DLQ rate. Schema registry: compatibility failures. None of these replace `kafka-consumer-groups.sh --describe` in the first five minutes.

---

## How: a consumer skeleton that avoids half the list

```python
consumer = KafkaConsumer(
    "service-events",
    group_id="alert-processor",  # unique per job
    enable_auto_commit=False,
    auto_offset_reset="none",
    max_poll_records=100,
    isolation_level="read_committed",  # if producers transact
)
```

Pair with `acks=all` producers, a DLQ, and a schema. Details in the numbered sections.

---

## 1. Consumer lag growing silently

Lag can grow at a few messages per second and still ruin an SLO.

At 1,000 messages/s, an extra 10 ms of processing (a slower `INSERT`) means the consumer finishes 100 messages/s *less* than ingest. Lag grows ~900/s. In a day you have tens of millions of unprocessed records and you are racing [retention](log.md).

**Monitor lag per partition, as a trend**, not a single "current lag" gauge that someone averages across the group.

Alert on:

- lag *age* (seconds behind the log end timestamp), not only offset count — 10k tiny metrics ≠ 10k fat traces
- first derivative: lag that was 2k and is 200k an hour later
- the **hottest** partition, not the sum

---

## 2. Rebalance storms

A rebalance storm is membership churn that never settles:

- processing one poll longer than `max.poll.interval.ms`
- crash loops (OOM, poison message)
- loading a 4 GB model in `main()` *after* join but *before* the first successful poll
- eager assignor + rolling deploy of 40 pods

During an eager rebalance **all** members stop. If they keep dying, net throughput is ~0 while CPU looks "busy".

**Fix:**

- Cooperative sticky assignor + `group.instance.id` ([partitions](partitions.md))
- `max.poll.records` small enough to finish inside `max.poll.interval.ms`
- Initialise heavy resources **before** subscribing
- Do not catch-and-restart on every bad record (see poison pills)

```python
# Heartbeats continue while you process, but poll must return.
# If each record can take 200ms, 500 records/poll needs >100s interval.
consumer = KafkaConsumer(
    "service-events",
    group_id="alert-processor",
    max_poll_records=50,
    max_poll_interval_ms=300_000,
    session_timeout_ms=45_000,
    heartbeat_interval_ms=15_000,
)
```

---

## 3. Poison pills

A record the consumer cannot process: schema break, `null` where you assumed a dict, 20 MB JSON, a `user_id` that wedges a downstream lock.

Without a policy:

1. Process → exception → process restarts
2. Same offset → same exception
3. Rebalance storm, zero progress on that partition, lag grows **only there**

**Fix:** retry with jitter, then **dead-letter** the record and commit past it. Page on DLQ rate. A DLQ you never read is `/dev/null`.

```python
from kafka import KafkaProducer

dlq = KafkaProducer(bootstrap_servers=["localhost:9092"])

for message in consumer:
    try:
        process(message)
        consumer.commit()
    except TransientError:
        raise  # let the framework retry; do not commit
    except Exception as e:
        if failures[message.offset] >= 5:
            dlq.send(
                "service-events.dlq",
                key=message.key,
                value=message.value,
                headers=[("error", str(e).encode()), ("partition", str(message.partition).encode())],
            )
            dlq.flush()
            consumer.commit()
        else:
            failures[message.offset] += 1
            raise
```

!!! production-gotcha "DLQ and ordering"
    Skipping a poison record **breaks per-key order**. For e-commerce checkout that may be unacceptable — halt the partition and fix the record. For observability logs, skip. Write the policy down.

---

## 4. Schema evolution (not "just use a registry")

A producer adds `device_id` to the JSON. One consumer does `event["endpoint"]` and is fine. Another does a strict pydantic model and 500s. A third writes Parquet and the warehouse job's schema drifts.

A **schema registry** (Confluent, Glue, Apicurio) does not save you by existing. It saves you if:

1. Producers **fail** to register incompatible schemas (compatibility mode is enforced on write).
2. Consumers are built on a **reader schema** and tolerate allowed changes.
3. You picked a compatibility mode that matches how you deploy.

| Compatibility | Allows | Typical use |
|---------------|--------|-------------|
| BACKWARD | New schema can read old data | Consumers first, then producers (add optional field) |
| FORWARD | Old schema can read new data | Producers first (add field with default in reader) |
| FULL | Both | Strict platforms |
| NONE | Anything | A wiki, not a contract |

JSON-in-Kafka with no registry is a schema; it is just an **unenforced** one. Avro/Protobuf **without** compatibility checks is theatre.

Practical rules for this academy's event:

- Add fields with defaults; never reuse field names; never change `user_id` from string to int.
- Do not delete `latency_ms` until every consumer is gone (or mark deprecated and keep writing it).
- Enum `status_code` is an int; do not suddenly send `"OK"`.
- Breaking change → **new topic** (`service-events.v2`) and dual-write, not a clever registry mode.

---

## 5. Hot partitions

Key = `customer_id`. One SaaS tenant is 40% of bytes. One IoT firmware version reconnects 2M devices against one `device_id` prefix. One fraudster hammers one `user_id`.

Symptoms:

- Lag on **one** partition
- One consumer at 100% CPU, others idle
- One broker hot: it leads that partition ([replication](replication.md))

Adding consumers does nothing (one member per partition). Fix the key, isolate the whale, or speed up that path. See [partitions](partitions.md).

---

## 6. Disk saturation and page-cache eviction

Kafka's fast path is sequential write + read from **page cache**. At observability volume, disk saturates before CPU.

Symptoms: high `iowait`, producer p99 climbs, replica fetch lags, URP flaps, consumers read the tail slowly even when "lag is small" (they are waiting on the broker).

**Fix:** NVMe, multiple `log.dirs`, shorter retention on hot topics, compression at the producer, stop running a 7-day replay consumer on the same disks as the live tail.

---

## 7. Oversized messages

Default `max.message.bytes` / `message.max.bytes` is on the order of 1 MiB. A 10 MB debug payload fails the produce. A 900 KB JSON that compresses poorly still wrecks fetch and GC.

**Fix:** store the blob in object storage; put `{timestamp, customer_id, user_id, ..., uri}` in Kafka. Compress. Split. Do not raise the broker limit to 50 MB "to be safe" — you will pay in page cache and heap.

---

## 8. `acks=1` on a topic you thought was durable

Covered in [replication](replication.md). The gotcha is organisational: a shared producer library defaults to `acks=1` for "latency", and the payments team never notices until a broker dies.

---

## 9. Compacting a topic that has unique keys

Someone sets `cleanup.policy=compact` on `service-events` because "disk". Keys are UUIDs or `(user_id, timestamp)`. Compaction never drops anything useful; disk still grows; the cleaner burns CPU. Compaction is for **changelogs**. Retention delete is for **logs**. ([log](log.md))

---

## 10. Transactions without `read_committed`

[Exactly-once](exactly-once.md) is undone if the warehouse consumer uses default isolation and ingests aborted records. Then you "fixed duplicates" by creating ghosts.

---

## 11. `auto_offset_reset` as a silent skip

After retention deletes unread data, the next poll throws `OffsetOutOfRange`. With `auto_offset_reset=latest` the group **jumps to the tail** and pretends the gap never happened. Warehouse tables are missing a Tuesday. Fraud missed an attack. Nobody pages because lag returned to zero.

**Fix:** `auto_offset_reset=none` (or fail the process) on any group where a gap is an incident. Restore from the lake. `earliest` is not safer if earliest is already *after* the missing range.

---

## 12. Compression and batching left at defaults

`linger.ms=0` and no compression: millions of tiny produces, poor disk sequentiality, replication amplification. Observability producers should batch (`linger.ms=10–50`, `batch.size` 32–64 KiB) and compress (`lz4` or `zstd`). E-commerce checkout in the request path may keep `linger.ms=0` but can still compress.

The gotcha is **mixing** both on one producer instance: a shared library used by checkout and log shippers.

---

## 13. Connect / sink tasks and `max.poll.interval.ms`

Kafka Connect sinks are consumers. A flush to S3 that takes longer than `max.poll.interval.ms` looks like a poison pill. Rebalance, duplicate files, or skipped offsets depending on exactly-once connector config. Treat Connect like any consumer group: interval, batch size, DLQ.

---

## Production is on fire

> Consumer lag for `alert-processor` went from 30 seconds to 45 minutes in two hours. `service-events` ingest looks "a bit high". PageDuty is quiet except this one group.

Before reading further — what do you inspect, in order?

<details>
<summary>Investigation approach</summary>

**Step 1 — Per-partition lag, not the sum**

```bash
kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
  --describe --group alert-processor
```

- All partitions equally behind → the *group* is slow (or ingest spiked everywhere).
- One/few partitions → hot key, one stuck member, or one sick leader broker.

**Step 2 — Ingest actually spiked?**

Broker `MessagesInPerSec` / `BytesInPerSec` for `service-events`. A 5× launch-day spike with no extra consumers is lag by arithmetic. Scale consumers only if partitions allow it.

**Step 3 — Rebalance storm?**

`last-rebalance-seconds-ago` flapping; logs full of `Revoking` / `Assigned`. Check poison exceptions, OOM, deploys, `max.poll.interval.ms`.

**Step 4 — ISR / broker health**

`UnderReplicatedPartitions` non-zero, disk full, one broker `LeaderCount` spiked. Consumers of partitions whose leader is sick look "slow".

**Step 5 — Downstream**

Alerting writes to a DB or PagerDuty. If *that* p99 went from 5 ms to 200 ms, Kafka is a mirror of a sink problem.

**Most common cause:** sink latency, then poll interval, then lag, then a rebalance that makes it worse. Kafka is innocent.

</details>

---

## 14. Cross-cluster "exactly-once" with MirrorMaker 2

MM2 is at-least-once replication of records. Offsets do not mean the same thing on the destination without offset translation. Disaster-recovery runbooks that say "just switch producers to cluster B" without offset maps will duplicate or skip. Treat MM2 as a **second at-least-once sink**, not as RAID-1.

---

## Debugging cheat sheet

| Symptom | First metrics |
|---------|----------------|
| Lag all partitions | Ingest rate, consumer process time, sink p99 |
| Lag one partition | Key skew, that member's logs, leader broker disk |
| Zero throughput, CPU busy | Rebalances, poison pill |
| Produce timeouts | ISR, `UnderMinIsrPartitionCount`, disk, `acks` |
| "Lost messages" | Retention vs lag, `acks=1`, unclean election, `auto_offset_reset` after OOR |
| Duplicate outputs | Missing idempotence/txn, `read_uncommitted`, non-idempotent sink |
| Disk growth | Retention, unique-key compaction, one hot partition dir |

Broker JMX / Prometheus names vary by exporter; the *ideas* are URP, ISR shrink, bytes in/out, request p99, log flush, consumer lag **by partition**.

---

## Failure modes (index)

| Mode | First check |
|------|-------------|
| Loss | retention vs lag, `acks`, unclean election, `auto_offset_reset` |
| Dupes | retries without idempotence, txn isolation, non-idempotent sink |
| Stall | poison, rebalance storm, URP, hot partition |
| Lie | schema, processing-time Flink, compaction of unique keys |

---

## Trade-offs

Aggressive DLQ (availability of the partition) versus halt-on-poison (order). Long retention (replay) versus disk. Cooperative rebalance (deploy smoothness) versus simple assignors. There is no cluster-wide default that fits checkout and logs.

---

## Alternatives

Fixing these in the application (idempotent sinks, bounded processing) is better than raising every timeout to 30 minutes. Moving the workload off Kafka (SQS for jobs, S3 for files) is better than turning Kafka into a work queue with 7-day retry.

---

## Scale notes (same gotchas, bigger blast radius)

| Scale | What amplifies |
|-------|----------------|
| **10×** | Silent lag, one whale tenant |
| **100×** | Page cache fights, URP during bounce, schema breaks hitting 40 consumers at once |
| **1000×** | A bad deploy rebalances thousands of partitions; a 1 MB payload cap mistake is a cluster incident |

---

## How to apply this at work

Every week, not only during incidents:

1. Lag **trend** and **max partition lag** per group
2. Rebalance count
3. Broker disk growth rate vs retention
4. `UnderReplicatedPartitions` (should be 0)
5. Produce error rate (timeouts, `NotEnoughReplicas`)
6. DLQ rate and age of oldest DLQ record
7. Schema registry compatibility failures (or the JSON "unknown field" error rate)

If you cannot name the consumer groups on a topic, you do not yet operate that topic.

For the five systems: analytics cares about whale keys; observability about disk and retention; e-commerce about `acks` and poison-halt; IoT about reconnect storms and compaction; fraud about lag *age* on the scorer group. Same Kafka, different pages.

A useful weekly habit: pick one topic, dump `--describe` for every group, and ask whether retention still exceeds the slowest group's lag-age. That single question catches gotchas 1, 9, and 11 before they become restorations from the lake.

---

## Worked incident: IoT reconnect storm

Firmware bug: 2 million devices reconnect, each sending a 50 KB snapshot keyed by `device_id` that all hash into a **few** partitions (IDs were sequential). Brokers leading those partitions hit disk 100%. URP flaps. Other topics on the same brokers stall (page cache gone). Fraud lag on an unrelated topic pages first.

You debug fraud, then notice `BytesInPerSec` on `device-state` is 40× normal on three partitions. Fix is key design + partition isolation + producer quota, not "scale the fraud consumer group".

---

## Exercise

SaaS analytics, 48 partitions, key `customer_id`. Group `warehouse-loader` lag is 2 hours on partitions 0–47 **except** partition 12, which is 18 hours. Retention is 24 hours. A new field `bytes` was added to JSON yesterday. The Java warehouse job is fine; a Python side consumer in the **same group** `warehouse-loader` started this morning "to debug".

1. Why is partition 12 special?
2. What is the Python debugger doing to the warehouse?
3. What happens in 6 hours if nobody acts?

??? question "Answer"
    1. Either the leader of p12 is sick (check URP, that broker's disk) **or** a hot `customer_id` hashes to 12 **or** the member assigned p12 is the Python process (slow, or crashing on the new field). The Java job being "fine" globally does not mean every partition is assigned to Java.

    2. Same `group_id` → the debugger is a **member**. It stole some partitions (maybe including 12). Records it consumes are **not** seen by the warehouse. Debug consumers need their own group id, or better `assign` a replica cluster / a copy topic.

    3. Retention will delete unread segments on p12. The warehouse will get `OffsetOutOfRange` and, with `auto_offset_reset=latest`, **skip** the rest of that tenant's day. Restore from the lake if you have it; otherwise that partition's data is gone. Act by stopping the debugger, reassigning, and (if needed) resetting offsets to a timestamp still inside retention.
