---
title: Transformation Engineering
description: Build tested, incremental, deployable SQL models rather than a DAG of unowned queries.
---

# Transformation Engineering

**Time:** 55 minutes reading + 60 minutes exercise<br>
**Prerequisites:** SQL, [data modelling](data-modelling.md)<br>
**Outcomes:** choose incremental boundaries; separate orchestration from transformation; test contracts; deploy models safely.

Airflow decides **when** a transformation runs. Spark, Trino, a warehouse, or dbt executes the transformation. The model must remain correct under retry, late data, backfill, and concurrent readers — that is, it must be **idempotent**: rerunning the same interval, whether on schedule or as a retry, produces the same output rather than duplicating or corrupting it. [Airflow idempotency](../airflow/idempotency.md) covers the orchestration side of this in depth later; here it drives the merge behavior below.

## Layer contracts

Use names only when their contracts are explicit:

- Raw/bronze: source-shaped, replayable, access restricted.
- Clean/silver: typed, deduplicated, stable keys and time semantics.
- Product/gold: declared grain, owned metric semantics, consumer SLO.

Do not create layers merely to copy data. Every boundary should add a contract, ownership, or useful physical layout.

## Full refresh vs incremental

Full refresh is simplest and often correct for small dimensions. Incremental models need a boundary:

```sql
WHERE source_updated_at >= last_successful_watermark - INTERVAL '2' DAY
```

The overlap captures late changes; a merge key makes replay safe. The watermark advances only after the output commit and quality checks succeed. Processing only `updated_at > max(target.updated_at)` without overlap loses late and equal-timestamp records.

## Model tests

| Contract | Test |
|---|---|
| Grain | unique composite key |
| Required field | non-null plus sentinel checks |
| Relationship | bounded orphan rate with source-lag allowance |
| Metric | reconciliation to an independent source |
| Freshness | visible consumer timestamp, not DAG success |
| Incremental equivalence | sampled/full rebuild equals incremental result |

Unit tests cover SQL logic with fixtures. Data tests cover runtime assumptions. Reconciliation covers end-to-end truth. One category does not replace the others.

## Safe deployment

1. Build into a versioned table or snapshot.
2. Run schema, grain, volume, and reconciliation checks.
3. Atomically publish a view/pointer or table-format snapshot.
4. Retain the previous version for rollback.
5. Backfill with the same model code and explicit interval.

Never mutate a gold table in place for a risky release without a rollback artifact.

## Semantic ownership

A metric definition includes grain, filters, time zone, late-event policy, and owner. Centralize reusable metrics, but do not hide physical costs: a semantic layer that emits an unbounded lake join can still take down Trino.

## CI/CD

Pull requests should compile models, resolve dependencies, lint SQL, run unit fixtures, detect destructive schema changes, and build only modified descendants in an isolated schema. Production promotion uses the same artifact tested in CI.

## Failure modes

- Incremental filter skips late updates.
- A retry appends rather than merges.
- A fan-out join passes because only null tests exist.
- Development points at production output tables.
- `warn` tests have no alert owner.
- Backfill code differs from scheduled code.

## Exercise

Design an incremental `fct_order_item` model with two-day late updates, refunds, and a daily publish deadline. Specify its unique key, watermark storage, merge behavior, tests, backfill interface, and rollback.

??? success "Exit check"
    A strong design uses the declared order-item grain, overlaps the source watermark, merges by a stable key and source version, advances state after atomic publish, reconciles money, and runs scheduled and backfill paths through the same model.
