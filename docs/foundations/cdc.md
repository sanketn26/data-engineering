---
title: Change Data Capture
description: Snapshot, stream, order, replay, and reconcile database changes safely.
---

# Change Data Capture

**Time:** 55 minutes reading + 45 minutes exercise<br>
**Prerequisites:** database transactions, Kafka partitions, idempotency<br>
**Outcomes:** design snapshot-to-stream handoff; preserve per-key order; handle deletes and schema changes; reconcile a sink.

CDC is not “send database rows to Kafka.” It is a protocol for reproducing committed database history from a snapshot plus a position in the transaction log.

## Contract

For every change retain:

```text
source_table, primary_key, operation, before, after,
source_commit_position, source_commit_time, transaction_id,
connector_ingest_time, schema_version
```

The source log position—not arrival time—is the ordering authority. Partition Kafka by the source primary key when consumers require per-row order.

## Snapshot plus stream

The unsafe sequence is “copy table, then start the log.” Changes committed between those actions disappear. A connector must establish a log position and snapshot under a database-specific consistency protocol, then stream changes after that position.

During the overlap, snapshot rows and live updates can arrive close together. Sinks must compare source positions or source versions so an older snapshot row cannot overwrite a newer streamed update.

## Operations

| Operation | Sink action |
|---|---|
| Insert | Insert if source position is newer |
| Update | Merge current state; optionally append history |
| Delete | Tombstone or equality/position delete, then physical erasure per policy |
| Primary-key change | Delete old key + insert new key |
| Truncate | Explicit administrative event; never silently interpret as row deletes |

Kafka log compaction preserves the latest keyed record, but retention, downstream snapshots, and physical GDPR deletion are separate concerns.

## Schema evolution

Additive nullable fields are the easy case. Renames, type narrowing, semantic changes, and table splits require a versioned contract and consumer migration. Database DDL appearing in the WAL does not prove every sink can apply it.

Deploy in this order:

1. Make consumers tolerate both schemas.
2. Publish the compatible producer/source change.
3. Backfill or dual-read where needed.
4. Observe old-schema traffic reach zero.
5. Remove compatibility code.

## Idempotency and ordering

Key a merge by source primary key and compare an ordering field from the source log. Connector ingest time is not safe: retries and backfills can arrive after newer events.

For append-only history, key each change by a stable identity such as `(source_partition, source_position, event_index)`. For current state, retain the greatest committed source position per key.

## Outbox vs raw row CDC

Raw CDC exposes storage-shaped events: a business action may update five tables. An outbox row written in the same OLTP transaction publishes one intentional business event. Use raw CDC for replication and analytical state; prefer an outbox for stable domain events consumed by independent services.

## Reconciliation

CDC is incomplete without proof:

- Source count/checksum by key range versus sink current state.
- Maximum source position applied per partition.
- Delete count and tombstone age.
- Snapshot progress and streaming lag measured separately.
- Quarantine count for incompatible schema.

Periodically repair from a bounded source snapshot. A replay procedure that has never been tested is not recovery.

## Failure modes

- WAL retention fills source disk while the connector is down.
- Snapshot row overwrites a newer streamed update.
- Primary-key update leaves the old key alive.
- DDL reaches Kafka before a consumer understands it.
- Repartitioning changes per-key order during migration.
- A sink uses ingestion timestamp for last-write-wins.

## Exercise

An `orders` snapshot runs for six hours. Order 42 changes from `PAID` to `SHIPPED` during hour two. The snapshot row reaches the sink after the live update. Specify the fields and merge predicate that keep `SHIPPED`, and the metrics that prove no key range was skipped.

??? success "Exit check"
    Compare source transaction positions, not connector arrival time. The sink keeps the greatest committed position for order 42. Track snapshot key-range completion, streamed source positions, lag, deletes, and a source-to-sink reconciliation.
