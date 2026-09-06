---
title: Platform Delivery & Infrastructure as Code
description: Promote data-platform changes safely across environments with tested artifacts and rollback.
---

# Platform Delivery & Infrastructure as Code

**Time:** 45 minutes reading + 30 minutes release exercise<br>
**Prerequisites:** Git, containers, orchestration, security<br>
**Outcomes:** design environment promotion; separate infrastructure and data migrations; plan rollback; test disaster recovery.

3:00 AM, cutover window. The engineer running the ClickHouse `ORDER BY` migration flips the table pointer, and dashboards go blank — the old table was already dropped, so there's nothing to roll back to.

What's the single missing step?

A. No dual-write/backfill period before cutover.
B. Terraform applied straight against `main`/`latest` instead of a pinned artifact.
C. No Flink savepoint taken before the paired job upgrade.
D. Rollback was never tested, only planned.

Predict which before reading on (the migration recipe later in this page answers it directly). A production platform is not the sum of its Terraform resources — delivery must coordinate infrastructure, engine configuration, schemas, jobs, permissions, and stateful migrations, and the scenario above is what happens when one of those is treated as optional.

## Change classes

| Change | Safe mechanism | Rollback concern |
|---|---|---|
| Stateless job | immutable image + canary | source offsets/output duplication |
| Additive schema | compatibility-first deployment | old readers |
| Partition/ORDER BY | new table + dual write/backfill | cutover and reconciliation |
| Kafka partitions | compatibility event | key history/order |
| Flink state | savepoint + stable operator UIDs | serializer/max parallelism |
| IAM/network | reviewed IaC plan | lockout and break-glass |

## Pipeline

1. Format, lint, compile, and unit-test.
2. Build an immutable artifact and generate an IaC plan.
3. Run integration tests against ephemeral services.
4. Deploy to staging with production-shaped state where needed.
5. Canary or dual-run; compare outputs and SLOs.
6. Promote the tested artifact; record provenance.
7. Verify rollback while the old state/data remains available.

Never make production fetch mutable `main` or `latest`.

## Environment and secrets

Keep configuration declarative and environment-specific values separate from code. Store secret references—not secret values—in Git. Test least-privilege identities in CI/staging; an administrator credential hides missing grants until production.

## Disaster recovery

Backups are inputs to a restore drill. Measure RPO and RTO for catalogs, Kafka metadata/data, table metadata, orchestrator state, and encryption keys. Restoring object files without the catalog or keys is not recovery.

## Check your understanding { #exercise }

Plan a ClickHouse `ORDER BY` migration plus a Flink job upgrade that changes state schema. Write the artifact, dual-run, reconciliation, cutover, savepoint, and rollback sequence.

??? success "Exit check"
    Use a new ClickHouse table and dual-write/backfill before an atomic consumer cutover. Take and test a Flink savepoint with stable UIDs and compatible serializers. Keep old outputs and the previous artifact until reconciliation and rollback SLOs pass.
