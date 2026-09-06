---
title: Analytical Data Modelling
description: Choose grain, facts, dimensions, keys, and history before choosing an engine.
---

# Analytical Data Modelling

**Time:** 60 minutes reading + 45 minutes exercise<br>
**Prerequisites:** SQL joins, primary keys, [batch vs stream](batch-vs-stream.md)<br>
**Outcomes:** define a fact-table grain; prevent fan-out; choose an SCD strategy; model late-arriving changes.

Code review, 4:52 PM. A join between `fct_order_item` and `payment_attempt` just shipped, and this morning's GMV number is running 2.3x high. The SQL is clean — every join key exists, every column resolves, nothing errors.

Before you scroll to the diff, pick one: is the bug (A) a wrong join key, (B) a grain mismatch — payments join at attempt grain while orders are at item grain, so retries fan out the join — or (C) a missing filter on refunds?

It's (B), and no engine would have caught it: an engine cannot rescue an ambiguous grain. Before Spark, Iceberg, dbt, or ClickHouse, decide what one row means and which business changes must remain historically true.

## Workload

E-commerce needs daily GMV by customer plan **as the plan was when the order was placed**. Orders mutate, refunds arrive later, and customers change plans.

Start with the consumer:

```text
metric: net_gmv
grain: one order item
event time: order_created_at
corrections: refunds and cancellations restate the affected order
dimensions: customer, product, campaign
```

## Grain before columns

Write this sentence before DDL:

> One row in `fct_order_item` represents one sellable item on one accepted order.

Then enforce it with a key such as `(order_id, order_item_id)`. If another table has one row per payment attempt, joining it directly to order items produces item × attempt fan-out. Aggregate payments to order grain first or model a bridge explicitly.

```sql
WITH payment_by_order AS (
  SELECT order_id, sum(captured_amount) AS captured_amount
  FROM payment_attempt
  WHERE status = 'CAPTURED'
  GROUP BY order_id
)
SELECT sum(i.net_amount), sum(p.captured_amount)
FROM fct_order_item i
JOIN payment_by_order p USING (order_id);
```

Always compare row counts and key uniqueness before and after a join.

## Facts and dimensions

| Shape | Purpose | Examples |
|---|---|---|
| Transaction fact | One business event | order item, payment, shipment |
| Periodic snapshot | State at a regular interval | daily inventory balance |
| Accumulating snapshot | Milestones on one lifecycle | order placed→paid→shipped |
| Dimension | Descriptive context | customer, product, campaign |
| Bridge | Explicit many-to-many relation | order↔promotion |

Facts should contain additive measures where possible. Ratios and percentiles are not additive: retain numerator/denominator or mergeable sketches, not averages of averages.

## Surrogate and natural keys

Natural keys identify the business object (`customer_id`). Surrogate keys identify a historical dimension version (`customer_sk`). Keep both. A warehouse-generated surrogate key must be stable across retries; use a deterministic lookup or merge, not an unguarded sequence in a replay.

Unknown and late dimensions need an explicit policy:

- Insert an “unknown” dimension row and restate later.
- Hold the fact until the dimension arrives, within a bounded SLA.
- Store the natural key on the fact so reconciliation remains possible.

Silent inner-join loss is never a policy.

## Slowly changing dimensions

| Strategy | Meaning | Use |
|---|---|---|
| Type 1 | Overwrite current value | Corrections where history is irrelevant |
| Type 2 | New effective-dated row | “Plan at order time,” audit history |
| Type 3 | Retain limited previous value | Rare, fixed before/after comparison |

Type 2 invariant:

```text
customer_sk, customer_id, plan, valid_from, valid_to, is_current
```

Intervals for one natural key must not overlap. Join facts using event time:

```sql
ON f.customer_id = d.customer_id
AND f.order_created_at >= d.valid_from
AND f.order_created_at <  d.valid_to
```

This range join can be expensive. Resolve `customer_sk` during ingestion when correctness and latency allow, or maintain an engine-specific current dimension for serving.

## Mutability and corrections

Do not confuse an immutable event log with a correct analytical fact. A cancellation is a new event, but the consumer may need one current order row. Pick one contract:

- Event fact: append every transition; derive current state.
- Current-state table: merge by key and ordering/version field.
- Ledger: append compensating entries; sum remains auditable.

Record event time, ingestion time, source transaction position, and stable event ID. “Latest Kafka arrival wins” is unsafe when replays interleave with live traffic.

## How it fails { #failure-modes }

- A fact has no declared grain, so every consumer invents one.
- Many-to-many joins multiply money.
- Type 1 overwrite changes last quarter’s report.
- Overlapping Type 2 intervals match a fact twice.
- Late dimensions disappear through an inner join.
- Daily snapshots are summed across days as though they were transactions.

## Practice the idea

Open the [SCD Type-2 timeline explorer](../simulations/scd2-timeline-explorer.html).
Choose an event time first, predict which customer version should join, and only
then move a validity boundary. This makes the half-open interval rule visible.

## Check your understanding { #exercise }

Design orders, payments, refunds, and customer plan history. State the grain and key of every table. Then write how you calculate net GMV by the customer’s plan at purchase time.

??? success "Exit check"
    A defensible model has transaction facts at explicit grains, prevents payment×item fan-out, uses effective-dated customer history or a purchase-time plan key, and treats refunds as corrections or compensating facts. It states how late dimensions and replay are reconciled.

Next: [CDC](cdc.md) turns mutable source rows into ordered changes; [Transformation Engineering](transformation-engineering.md) builds this model repeatably.
