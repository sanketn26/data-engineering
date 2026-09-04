# Replication & Durability

## The Problem

A broker holds your data. That broker dies.

Without replication, all data on that broker is lost — and any consumers reading from partitions that were on that broker are stuck.

---

## Replication Factor

Each partition is replicated across multiple brokers. The **replication factor** determines how many copies exist.

With replication factor 3:

```
Partition 0:
  Leader: Broker 1 (primary — handles reads and writes)
  Follower: Broker 2 (replica)
  Follower: Broker 3 (replica)
```

All writes go to the leader. Followers continuously fetch and replicate.

---

## ISR: In-Sync Replicas

The **ISR** (In-Sync Replica set) is the set of replicas that are fully caught up with the leader.

A replica is "in sync" if it has fetched up to the leader's latest offset within a configurable lag window (`replica.lag.time.max.ms`).

If a follower falls behind, it is removed from the ISR. If it catches up again, it rejoins.

```
ISR for Partition 0: [Broker1, Broker2, Broker3]
Broker 3 falls behind:
ISR becomes: [Broker1, Broker2]
```

---

## Acknowledgements and Durability

The producer's `acks` setting controls the durability guarantee:

| `acks` | Meaning | Durability | Latency |
|--------|---------|------------|---------|
| `0` | Fire and forget — no wait | None (data can be lost) | Lowest |
| `1` | Wait for leader acknowledgement | Data survives leader failure *if* at least one replica fetched | Medium |
| `all` (`-1`) | Wait for all ISR replicas to acknowledge | Data survives any single broker failure | Highest |

**For production**: always use `acks=all` with `min.insync.replicas=2`.

```properties
# Producer
acks=all

# Broker / Topic
min.insync.replicas=2
```

With `min.insync.replicas=2` and `acks=all`, Kafka refuses to accept writes if fewer than 2 replicas are in sync. This prevents silent data loss.

---

## What Happens When a Broker Dies

1. Followers detect the leader has stopped responding
2. The controller (another broker) detects the failure via ZooKeeper/KRaft
3. Controller elects a new leader from the ISR
4. Producers and consumers are notified of the new leader
5. Processing resumes from the new leader

**With replication factor 3**: losing one broker is transparent to producers and consumers (brief pause during leader election, typically seconds).

**With replication factor 1**: losing the broker loses the partition. Data is gone. Do not use replication factor 1 in production.

---

## Unclean Leader Election

Normally, a new leader is elected only from ISR members — guaranteeing no data loss.

If all ISR members are unavailable, you face a choice:

- **Wait for an ISR member to come back** (preserves data, reduces availability)
- **Elect a non-ISR replica as leader** (restores availability, but may lose data committed between when the non-ISR replica fell behind and when the leader died)

This is controlled by `unclean.leader.election.enable`. The default is `false` (prefer consistency over availability). Set to `true` only if availability is strictly more important than data loss.

---

## Replication Factor in Practice

| Use Case | Recommended Replication Factor |
|---------|-------------------------------|
| Development/testing | 1 |
| Production, tolerating one failure | 3 |
| High availability, two concurrent failures | 5 |

Most production Kafka clusters use replication factor 3.

!!! warning "Production Gotcha"
    Replication factor 3 with 3 brokers means every broker holds every partition. Losing one broker reduces availability but not to zero. However, you have no "spare" capacity — losing a second broker before the first is replaced can result in leadership election from a replica that may be slightly behind. Use at least 4 brokers for serious production workloads.
