---
title: Capstone & Rubric
description: Three cumulative production-data-engineering assessments with an evidence-based rubric.
---

# Capstone & Rubric

The academy is complete when you can produce and defend evidence, not when every page has been opened. Complete one architecture capstone, one incident diagnosis, and one migration exercise.

## Deliverable 1 — architecture decision record

Choose SaaS analytics, observability, e-commerce, IoT, or fraud. Submit:

1. Workload table: volume, velocity, retention, consumers, latency, correctness, privacy, recovery, budget.
2. Logical model: grains, keys, mutability, time semantics, schema contract.
3. V1 with the fewest moving parts.
4. Capacity and unit-cost worksheet.
5. Failure table with signal, immediate mitigation, durable correction.
6. A 10× evolution plan and explicit “not yet” list.
7. One rejected alternative with evidence.

## Deliverable 2 — incident diagnosis

Ask a peer to choose an incident without revealing its resolution. In 30 minutes produce:

- ranked hypotheses;
- the first five metrics/queries and what each result would mean;
- containment that does not destroy evidence;
- root-cause proof;
- one prevention control with an owner.

## Deliverable 3 — backfill and migration

Migrate one mutable source from a daily append job to CDC-backed current state. Include snapshot/stream handoff, source ordering field, idempotent merge, schema rollout, reconciliation, rollback, and GDPR deletion behavior.

## Rubric

Score each category 0–3. A pass is at least 15/18 with no zero.

| Category | 0 | 1 | 2 | 3 |
|---|---|---|---|---|
| Requirements | Missing | Vague adjectives | Most constraints quantified | Consumer SLOs and failure budget quantified |
| Data correctness | No grain/key | Grain only | Grain, key, time, replay | Corrections, deletes, reconciliation proven |
| Architecture | Product list | Unjustified V1 | Workload-derived V1 | Alternatives and 10× trigger explicit |
| Operations | “Add monitoring” | Generic metrics | Failure-specific signals | Containment, recovery, ownership tested |
| Cost | Ignored | Instance prices | Major cost terms | Unit economics and sensitivity analysis |
| Communication | Diagram only | Partial narrative | Reproducible decision record | Assumptions, evidence, uncertainty clear |

## Feedback protocol

The reviewer must challenge three assumptions and introduce one changed constraint. The learner revises the decision record rather than defending the original stack. Store both versions; the delta is evidence of judgement.

If working alone, wait one day, draw a random change card, and rerun the decision:

- traffic becomes 10× and one tenant owns 60%;
- GDPR deletion SLO becomes 24 hours;
- platform team shrinks to one engineer;
- dashboard freshness becomes five seconds;
- budget drops 40%;
- a second query engine must read the historical table.
