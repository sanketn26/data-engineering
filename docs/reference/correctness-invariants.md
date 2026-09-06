---
description: Exactly-once is not a platform checkbox — trace an event through every hop and name the invariant that proves it was neither lost nor duplicated.
---

# Data Pipeline Correctness

Postmortem, 11 AM. `revenue_agg` double-counted eleven minutes of orders after a Flink restart. Kafka replayed from the last committed offset — correctly, that is exactly what it is supposed to do. The sink was not idempotent. Nobody had asked "what happens if this exact batch is written twice?" because the team's mental model of correctness was "Kafka is exactly-once" — a claim about one hop, applied to the whole pipeline.

A. The Kafka producer config was wrong. B. Flink checkpointing was misconfigured. C. Nobody named the invariant that should have held at the sink. D. This needs a bigger cluster.

It's (C). Every other answer patches one hop and leaves the same class of bug at the next one. This page exists because "exactly-once" is not a single guarantee you buy from a platform — it is a **chain of invariants**, one per hop, and the chain is only as strong as its weakest, unstated link. This synthesizes correctness mechanisms already covered per-system in [CDC](../foundations/cdc.md), [Kafka exactly-once](../kafka/exactly-once.md), and [batch vs stream](../foundations/batch-vs-stream.md); this page's job is to show them as one chain instead of three separate stories.

---

## The seven-stage chain

```text
Produced ──▶ Accepted ──▶ Persisted ──▶ Processed ──▶ Written ──▶ Queryable ──▶ Reported
```

At every arrow, ask the same question: **what proves this event was neither lost nor duplicated crossing this boundary?** "It probably wasn't" is not an invariant. A number or a mechanism is.

| Stage | Boundary | What could go wrong | Invariant that proves it didn't |
|---|---|---|---|
| **Produced → Accepted** | App/producer → broker | Producer retries after a timeout it never actually failed | Idempotent producer (`enable.idempotence=true`) — retries dedupe at the broker by producer-id + sequence number |
| **Accepted → Persisted** | Broker write → replication | Leader accepts, dies before replicating; write is lost | `acks=all` + `min.insync.replicas` ensures a committed offset survived on more than one broker |
| **Persisted → Processed** | Consumer read → transform | Consumer crashes mid-batch; reprocesses on restart | Consumer offset commit **after** output is durable, not before — see [Kafka delivery semantics](../kafka/exactly-once.md#internals-delivery-table) |
| **Processed → Written** | Transform → sink | Same processed batch written twice after a restart | Idempotent sink (unique key + `ON CONFLICT DO NOTHING`, or `ReplacingMergeTree` + dedupe on read) or a transactional sink (two-phase commit) |
| **Written → Queryable** | Sink write → visible to readers | A partial write is visible mid-commit; a reader sees half a batch | Atomic commit at the storage layer — an Iceberg snapshot, a ClickHouse part becoming active, not appending rows one at a time |
| **Queryable → Reported** | Table → dashboard/metric | Correct rows, wrong aggregation (double-counted join, wrong denominator) | Row-count and value reconciliation between source and reported metric — see [incident: pipeline green, data wrong](../incidents/index.md) |

Read the table left to right once per incident. Most postmortems stop at the first plausible story ("Kafka replayed, so we double-counted") instead of walking the whole chain to find which specific hop had no invariant.

---

## The mechanisms, mapped to the chain

None of these are new — each is covered in depth on its own page. This is the index that shows which hop each one actually defends:

- **Idempotency** (Produced→Accepted, Processed→Written): making "apply this twice" equal to "apply it once." A unique event id plus `ON CONFLICT DO NOTHING`, an idempotent producer, or a `ReplacingMergeTree`/upsert sink. See [Kafka exactly-once — at-least-once plus an idempotent sink](../kafka/exactly-once.md#how-at-least-once-plus-an-idempotent-sink-the-usual-design).
- **Deduplication** (Persisted→Processed, Written→Queryable): removing duplicates that already happened, by key and (usually) a version/watermark — the read-side complement to idempotency when you cannot control the write path.
- **Replay** (any stage, backward): reprocessing a bounded range from a durable source — Kafka offsets, an Iceberg snapshot, a CDC source position. A replay path that has never been exercised is not a recovery plan; see [CDC — reconciliation](../foundations/cdc.md#reconciliation).
- **Ordering** (Accepted→Persisted→Processed): per-key ordering guarantees (Kafka partition, CDC transaction log order) — the reason replaying out of order silently corrupts state that assumes monotonic updates.
- **Checkpoints** (Processed): a stream processor's periodic durable snapshot of state + input position, so "restart" means "resume," not "recompute from zero and hope nothing downstream noticed." See [Flink checkpoints](../flink/checkpoints.md).
- **Transactional outbox** (Produced→Accepted): writing the business fact and the "publish this" intent in the same OLTP transaction, so the event cannot exist without the state change that caused it (or vice versa). See [CDC — outbox vs raw row CDC](../foundations/cdc.md#outbox-vs-raw-row-cdc).
- **Late events** (Processed): events that arrive after the watermark has passed their event time. The invariant here is a *chosen* trade-off, not a bug — see [Flink windows](../flink/windows.md) for allowed lateness.
- **Poison messages / DLQs** (Processed): one malformed or unprocessable record must not stall every other key sharing its partition or worker. The invariant is isolation — a poison record's failure is scoped to itself, with a queue to inspect it later, not a silent skip and not a stalled pipeline. See [Kafka gotchas — poison pills](../kafka/gotchas.md#3-poison-pills).
- **Reconciliation** (Queryable→Reported, and as a periodic backstop across every hop): comparing an independently computed count/sum against the pipeline's output on a schedule, because every mechanism above can silently fail and reconciliation is the invariant that catches the ones you didn't anticipate.
- **Exactly-once boundaries** (naming where the chain actually holds): Kafka's own exactly-once semantics cover producer→topic and, with transactions, consumer-read→producer-write *within Kafka*. The moment the chain crosses into an external sink, database, or API call, exactly-once requires that system's own cooperation (idempotent writes or two-phase commit) — Kafka cannot make an external side effect exactly-once by itself. See [Kafka exactly-once — internals: transactions and what EOS actually guarantees](../kafka/exactly-once.md#internals-transactions-and-what-eos-actually-guarantees).

---

## Worked example: the postmortem above, walked correctly

1. **Produced → Accepted**: producer idempotence on, acks=all. Not the fault — confirmed by broker logs showing no duplicate producer sequence numbers.
2. **Accepted → Persisted**: ISR held at 2/3 through the restart. Not the fault.
3. **Persisted → Processed**: Flink resumed from its last checkpoint, which was *before* the last eleven minutes of committed offsets — correct, expected replay behavior after a crash.
4. **Processed → Written**: the ClickHouse sink used a plain `INSERT`, not `ReplacingMergeTree` keyed on event id, and no dedup step downstream. **This is the hop with no invariant.** The replayed eleven minutes were written a second time, verbatim.
5. **Fix**: key the sink table on event id with a dedup mechanism (or use an idempotent, id-aware writer) so that "Flink replayed eleven minutes" is a no-op at the sink, exactly as designed for at-least-once processing.

The postmortem's real finding is not "Flink replayed data" — replay is correct, expected behavior. It's "stage 4 of the chain had no invariant," and that is the sentence that should appear in the incident write-up, not "add retries" or "add monitoring."

---

## Check your understanding { #exercise }

A CDC pipeline (Debezium → Kafka → Flink → Iceberg → dbt → ClickHouse) shows a customer's order count as 2× the source database's count, but only for orders created in the last hour, and only for one customer.

1. Walk the seven-stage chain. At which stage(s) is duplication *possible* given this pipeline shape, and which invariant (if any) already covers each one?
2. The bug is scoped to "last hour" and "one customer" — what does that scoping rule out, and what does it point toward?
3. Name the specific reconciliation query that would have caught this before a human noticed the dashboard was wrong.

??? success "Exit check"
    (1) Debezium→Kafka (idempotent producer covers duplicate publish); Kafka→Flink (consumer offset semantics, not duplication-prone by itself); Flink→Iceberg (checkpoint replay *can* duplicate if the Iceberg writer is not id-keyed or transactional per checkpoint); Iceberg→dbt→ClickHouse (a dbt incremental model re-running over an overlapping window, or a ClickHouse `INSERT` without a `ReplacingMergeTree`/dedup key, can duplicate on any rerun). (2) "Last hour only" rules out a systemic double-count (that would show in all data, all customers) and points at something replay- or rerun-adjacent — a checkpoint restore, a backfill, or a dbt incremental window overlap — combined with a write path that is not idempotent at the specific stage that reran. "One customer" further suggests either a keyed retry landing on one partition, or a manual backfill scoped to that customer. (3) `SELECT customer_id, count(*) FROM source.orders WHERE created_at > now() - interval 1 hour GROUP BY customer_id` compared against the same aggregation on the ClickHouse table, alerting when the ratio deviates from 1.0 — a reconciliation check run on a schedule, not only when someone happens to notice the dashboard.

---

Related: [CDC](../foundations/cdc.md), [Kafka exactly-once](../kafka/exactly-once.md), [Flink checkpoints](../flink/checkpoints.md), [batch vs stream](../foundations/batch-vs-stream.md), [incidents](../incidents/index.md), [metadata — declared vs observed truth](../metadata/index.md#contracts-vs-catalogues-declared-truth-vs-observed-truth).
