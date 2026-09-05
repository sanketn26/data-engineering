---
title: Data Contracts
description: Version schema, semantics, and freshness as a producer/consumer agreement instead of discovering breakage in production.
---

# Data Contracts

**Time:** 45 minutes reading + 45 minutes exercise<br>
**Prerequisites:** [Change Data Capture](cdc.md), [Kafka log](../kafka/log.md)<br>
**Outcomes:** distinguish syntactic from semantic breakage; choose a compatibility mode; design a producer-owned contract; test it in CI.

A Kafka producer adds a field. A Spark job crashes on the new column. An analyst's dashboard silently starts double-counting because `status` gained a new enum value nobody told them about. None of these are schema bugs in the traditional sense — the JSON still parses. They are **contract** violations: the meaning consumers relied on stopped holding, even though the wire format looked fine.

A data contract is the versioned agreement between a producer and its consumers:

```text
Producer
   |
   |-- schema (fields, types)
   |-- semantics (what a field means, valid ranges, enum meaning)
   |-- freshness (how stale this may be)
   |-- ownership (who to page)
   +-- compatibility guarantees (what changes are safe)
           |
           v
        Consumer
```

Tools — a schema registry, Avro/Protobuf/JSON Schema, dbt contracts, OpenLineage — **implement** this. They are not the contract itself. The contract is the promise; the tool enforces or documents it.

## Syntactic vs semantic change

Most schema-evolution advice stops at syntax. Both kinds break consumers, but only the first kind is checkable by a registry.

| Kind | Example | Caught by |
|------|---------|-----------|
| Syntactic | Field renamed, type changed, field removed | Schema registry compatibility check |
| Semantic | `status` enum gains `REFUNDED_PARTIAL`; `amount` changes currency; `user_id` starts including bots | Nothing automatic — needs a human-reviewed contract change and consumer sign-off |

A field going from non-null to **nullable** is a syntactic relaxation but often a semantic landmine: "nullable" and "optional in the business sense" are not the same claim. A consumer that treats `null` as "not yet known" breaks if the producer starts using `null` to mean "not applicable."

## Compatibility modes

Schema registries (Confluent, AWS Glue, Apicurio) enforce one of these per subject:

| Mode | New schema can | Use when |
|------|-----------------|----------|
| **Backward** | Be read by consumers on the *old* schema | Consumers upgrade after producers (the common case) |
| **Forward** | Read data written with the *new* schema, using the *old* schema | Producers upgrade after some consumers (rare, riskier) |
| **Full** | Both | You cannot control deploy order at all |
| **None** | Anything | Never, except a throwaway topic |

Backward-compatible changes: add an optional field with a default, add an enum value **if consumers already have a default/other-case branch**, widen a numeric type (int → long, with reader awareness). Breaking changes: remove a required field, rename a field, change a field's type incompatibly, add a required field with no default.

!!! production-gotcha "Enum evolution is a silent breaking change"
    Adding a new enum value is syntactically backward-compatible under Avro/Protobuf rules, but a consumer with an exhaustive `switch` on the old enum values will either throw or — worse — silently fall into a default branch that mis-classifies the new value. Require every enum consumer to code an explicit "unknown" branch **before** the producer is allowed to add a value.

## Producer-driven vs consumer-driven contracts

- **Producer-driven**: the producer declares the schema and compatibility mode; consumers adapt. Works when there are many consumers and one producer team — the common Kafka topic shape.
- **Consumer-driven** (contract testing, e.g. Pact-style): each consumer declares the fields and shapes *it actually uses*; the producer's CI runs all consumer contracts before a change ships. Works when a small number of consumers depend on subtle behavior the producer can't otherwise see (an internal API, a small set of downstream services).

Most data platforms are producer-driven because consumers (analysts, other teams' pipelines) are too numerous and too loosely coupled to register a contract test per consumer. That makes the **registry's compatibility mode** the practical substitute for consumer-driven testing — it is the only automated check most consumers get.

## Where the contract lives

Put it next to the producer, versioned in git, not only in a UI:

```yaml
# contracts/orders.yaml
subject: orders-value
owner: checkout-team
compatibility: BACKWARD
schema_format: avro
freshness_slo: 5m
semantics:
  status:
    type: enum
    values: [PENDING, PAID, SHIPPED, REFUNDED, REFUNDED_PARTIAL]
    note: REFUNDED_PARTIAL added 2025-03; consumers must have a default branch
  amount:
    type: decimal
    currency: fixed_usd_cents
```

This is the same shape as [metadata's contract sketch](../metadata/index.md#v1-contract-yaml-sketch) — a catalogue **displays** this file at runtime; it does not replace it. If the contract lives only in a UI, it drifts the first time someone edits it under deadline pressure.

## Contract testing in CI

1. Schema registry check: does the proposed schema satisfy the subject's compatibility mode? (Automated, blocks merge.)
2. Semantic diff: did an enum, unit, or business meaning change? (Requires a human reviewer on the PR — a linter can flag "enum values changed," not judge if it's safe.)
3. Consumer smoke test: replay a sample of new-format messages through the most fragile known consumer (the one with an exhaustive switch, the one with a strict Avro reader schema) in CI, not production.
4. Deploy order: consumers that must tolerate both schemas deploy **before** the producer ships a backward-compatible change; a forward-only change requires the reverse.

This is the same deploy-order discipline as [CDC schema evolution](cdc.md#schema-evolution) — a data contract is CDC's schema-evolution problem generalized to every producer, not only databases feeding a log.

## Failure modes

- Nullable field silently changes meaning (not-yet-known vs not-applicable) with no consumer notified.
- New enum value reaches a consumer with an exhaustive switch and no default branch — crash or silent misclassification.
- Compatibility mode set to `NONE` on a shared topic "temporarily" and never revisited.
- Contract lives only in a wiki page that nobody updates after the third schema change.
- A currency, unit, or timezone change ships as a "just a rename" PR with no semantic review.
- Two teams both claim to own the same field's contract; neither one is accountable when it breaks.

## Exercise

`orders-value` (Avro, `BACKWARD` compatibility) is read by three consumers: a Flink job with a strict reader schema, a Spark batch job that projects only `order_id, status, amount`, and an analyst's Trino query that does `CASE status WHEN 'PAID' THEN ... WHEN 'SHIPPED' THEN ... ELSE 'unknown' END`. The checkout team wants to add `status = REFUNDED_PARTIAL` and change `amount` from integer cents to decimal with explicit currency.

Specify: which change is safe under `BACKWARD` compatibility as-is, which needs consumer changes first, and the deploy order for each.

??? success "Exit check"
    Adding `REFUNDED_PARTIAL` is schema-backward-compatible but a **semantic** break for the Flink job (strict reader schema — likely rejects the unknown value) and safe for the Trino query only because it already has an `ELSE` branch; the Spark job is unaffected since it doesn't branch on `status`. Deploy order: confirm/patch the Flink reader schema and Trino default branch first, then ship the enum addition. The `amount` type change (int → decimal, implicit → explicit currency) is a breaking **semantic** change regardless of registry rules — it needs a new field (`amount_v2` or a currency-qualified type) and a dual-write/migrate/cutover sequence, not a same-field edit.
