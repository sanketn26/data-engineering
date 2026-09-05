---
title: Platform Delivery & Infrastructure as Code
description: Promote data-platform changes safely across environments with tested artifacts and rollback.
---

# Platform Delivery & Infrastructure as Code

**Time:** 45 minutes reading + 30 minutes release exercise<br>
**Prerequisites:** Git, containers, orchestration, security<br>
**Outcomes:** design environment promotion; separate infrastructure and data migrations; plan rollback; test disaster recovery.

A production platform is not the sum of its Terraform resources. Delivery must coordinate infrastructure, engine configuration, schemas, jobs, permissions, and stateful migrations.

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

## Exercise

Plan a ClickHouse `ORDER BY` migration plus a Flink job upgrade that changes state schema. Write the artifact, dual-run, reconciliation, cutover, savepoint, and rollback sequence.

??? success "Exit check"
    Use a new ClickHouse table and dual-write/backfill before an atomic consumer cutover. Take and test a Flink savepoint with stable UIDs and compatible serializers. Keep old outputs and the previous artifact until reconciliation and rollback SLOs pass.
