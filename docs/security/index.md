---
description: Threat-model a data platform like a bank, not a blog's Postgres — RBAC gaps, PII masking, and audit logging across concentrated sensitive data.
---

# Data Security

11:15 AM. An analyst on JupyterHub runs `SELECT email FROM events LIMIT 100` against the "curated" schema in Trino. It returns real emails. No error, no audit ping — just PII on a screen that was supposed to be masked by default.

Predict before you read on: which control was missing?

A. RBAC never restricted the analyst's role to a masked view.
B. The masking view exists but `events` (the raw table) was grantable too.
C. Audit logging would have caught it after the fact, but nothing would have prevented it.
D. This is a break-glass path working as designed, and the ticket trail is the control.

A data platform **concentrates** what every product database holds in isolation — PII, payments, auth logs, health-adjacent telemetry, internal metrics — so a breach here is not "one app's users," and the scenario above (most likely A or B) is why this page threat-models the platform like a bank, not like a blog's Postgres, mapping controls to threats rather than listing vendors.

Related: [metadata](../metadata/index.md), [notebooks](../notebooks/index.md), [analytics tenancy](../architectures/analytics-platform.md), [fraud](../architectures/fraud.md).

---

## Threat model (data platform)

| Threat | Example | What actually stops it |
|--------|---------|------------------------|
| **Exfil via SQL** | Analyst `SELECT email FROM events` | Column masking, authorised views, audit, no raw notebooks |
| **Exfil via notebook/Spark** | Write to personal S3 | Egress controls, job IAM, review |
| **Tenant leak** | Missing `customer_id` filter | RLS **in the store**, not only the UI |
| **Producer impersonation** | Random client writes Kafka | SASL, ACLs, mTLS |
| **Consumer over-read** | One group reads `payments` and `debug` | Topic ACLs, separate clusters for PCI |
| **Ransomware / disk theft** | Snapshot walk-off | Encryption at rest, KMS, backup policy |
| **Insider + logs** | Support engineer | JIT access, audit, masking |
| **Supply chain** | Malicious PyPI in the image | Pin images, no arbitrary `pip` on prod |
| **Replay / undelete** | Kafka + Iceberg time travel restores PII | Retention + legal hold process |
| **Schema as attack** | Giant string field with payloads | Validation, size limits |

If your design only says "S3 is encrypted," you have modelled **disk theft** and nothing else.

---

## Encryption

**At rest:** object storage default encryption + KMS CMKs; Kafka disk; CH/Postgres volumes. This is table stakes.

**In transit:** TLS everywhere. Kafka `SASL_SSL`; CH TLS; Trino HTTPS; JDBC TLS. Plaintext "inside the VPC" is how a single compromised pod sniffs passwords.

**Keys:** KMS/Vault. Never in git, images, or Airflow Variables UI screenshots. Rotate. Separate keys per env and per **residency**.

**Application-level encryption** for a few super-sensitive columns (PAN tokens, secrets) when the store's RBAC is not enough. You then cannot `GROUP BY` the plaintext — that is the point.

---

## Authentication and RBAC

**Humans:** SSO (OIDC/SAML). No local CH passwords in a wiki.

**Jobs:** service principals with **least privilege**. Spark jobs that only write `s3://lake/curated/events/` must not hold `s3:*`.

**RBAC:** roles = job functions (`analyst_product`, `oncall_obs`, `etl_orders`), not individuals.

```sql
CREATE ROLE analyst;
GRANT SELECT ON SCHEMA analytics TO ROLE analyst;
GRANT ROLE analyst TO USER alice;
```

**ABAC / row filters:** tenant, region, "not PII." Trino/CH row policies. Spark + Iceberg: grant **on tables**, plus views for masked columns.

Lakehouse grants (Iceberg + catalog): treat the catalog like a database GRANT system. Object-store IAM is **defense in depth**, not the user-facing model (users should not get raw S3 list on the whole bucket).

---

## Column masking and row security

```sql
CREATE VIEW events_masked AS
SELECT
    event_id,
    timestamp,
    service,
    CASE WHEN is_pii_viewer() THEN email ELSE '***' END AS email,
    CASE WHEN is_pii_viewer() THEN user_id ELSE NULL END AS user_id
FROM events_raw;
```

Row-level:

```sql
-- tenant isolation
-- USING (customer_id = current_user_customer_id())
```

Rules:

- Product UIs **and** SQL engines enforce tenant filters ([analytics](../architectures/analytics-platform.md)).
- Masked view is default; raw table is a **break-glass role**.
- Aggregates can still leak (unique `user_id` counts on tiny sets). Policy must cover that for health/finance data.

---

## Kafka ACLs (minimum that is real)

| Principal | Allow | Deny |
|-----------|-------|------|
| Ingest gateway | `WRITE` on `product-events` | Read `payments-cdc` |
| Flink scorer | `READ` transactions, `WRITE` scores | Admin |
| Analysts | nothing on Kafka | "Just in case" console on prod |
| Spark lake job | `READ` specific topics | `WRITE` to prod topics |

`allow everyone` on a "internal" cluster is not an ACL strategy. Compacted topics with PII are **stores**. Treat them like databases.

Schema registry: authenticated, ACLs on subjects, compatibility `FORWARD`/`FULL` so a producer cannot ship a poison schema that crashes every consumer (availability is a security property too).

---

## Lakehouse grants

- Bucket policy: jobs write only their prefix.
- Table grants in Glue/Nessie/Polaris/Unity: `SELECT` on curated, not raw.
- Time travel: `SELECT` on historical snapshots can **undelete** GDPR. Restrict who can query old snapshots; expire per policy.
- Compaction jobs need write; analysts do not.

---

## Audit

If you cannot answer "who read `email` last week," you do not have a control, you have a hope.

Log:

- Trino/CH/Postgres query text (or hash + columns touched).
- Kafka ACL denials.
- S3 data access (careful: volume).
- Catalogue/admin changes.
- Notebook spawns and egress ([notebooks](../notebooks/index.md)).

Retain audits longer than the data if law requires. Protect audit logs from the people they watch.

---

## PII, GDPR, residency

**Classify** in the catalogue: direct identifiers, quasi, sensitive categories.

**Minimise:** do not land PAN, raw email, or access tokens in the lake if the product does not need them. Hash/tokenise at the gateway ([analytics](../architectures/analytics-platform.md) Flink mask).

**Erasure path** (must be tested):

1. OLTP
2. CDC → lake deletes (Iceberg/Hudi)
3. ClickHouse mutations
4. Search, graph, object exports
5. Backups: documented delay

**Residency:** EU tenants → EU Kafka, CH, S3. A `region` column on a US cluster is not residency.

**Retention:** TTL is a security control. Keeping 2 years of raw "for ML" is a threat-model choice; price it and name the owner.

PCI/fraud: PAN never in CH/Iceberg; tokens only. [Fraud](../architectures/fraud.md) scores are still sensitive.

---

## Secrets

Runtime fetch from Secrets Manager / Vault / cloud native. Rotate. Airflow Connections encrypted. No secrets in `spark-submit` CLI (they show up in `ps` and Spark UI).

---

## Worked threat: "just a notebook"

An analyst on JupyterHub with a Spark session and `s3://lake/raw/**` can:

1. `SELECT` PII.
2. Write a CSV to `~/` and download.
3. Push to an external IP.

Controls: masked views only, no raw IAM, network policy deny egress, download disabled or DLP, sample-size limits, audit. See [notebooks](../notebooks/index.md). If you cannot fund this, **do not** give notebooks lake credentials.

---

## V1 checklist (ship these)

- [ ] Encryption at rest + TLS
- [ ] SSO humans, service accounts for jobs
- [ ] Kafka ACLs per app, not `ANONYMOUS`
- [ ] Schema registry auth
- [ ] PII tagged on gold tables
- [ ] Masked views as default SQL
- [ ] Tenant predicate in CH/Trino
- [ ] Audit on interactive SQL
- [ ] Secrets in a manager
- [ ] Deletion drill on **one** user
- [ ] Retention TTLs that match policy

**Do not add yet:** homomorphic encryption, a second identity provider, custom KMS.

---

## Failure modes

| Failure | Symptom | Absorb |
|---------|---------|--------|
| RLS bug | Cross-tenant ticket | Dual control: app + DB; canary tenant test |
| ACL `ALLOW *` leftover | Silent | Periodic ACL dump diff |
| Time travel restores PII | Legal | Snapshot expire + role |
| Spark UI leaks SQL with secrets | Screenshots | Redact; no secrets in SQL |
| DLQ full of PII | World-readable topic | ACL + TTL on DLQ |

---

## Apply this at work

1. Draw data stores. For each, name who can read PII.
2. Pick one threat from the table. Trace a path. Put a control on that path.
3. Run a GDPR delete in staging end-to-end.
4. Remove one overly broad Kafka ACL this week.

Security that exists only in an architecture slide will not show up in an incident — the [incidents](../incidents/index.md) in this academy are availability-shaped; add **access** review to your real drills.

---

## Kafka ACL sketch (topic families)

```
product-events     WRITE: ingest-gw      READ: flink-enrich, spark-lake
product-events-dlq WRITE: flink-enrich   READ: oncall-role
payments-cdc       WRITE: debezium       READ: flink-fraud, spark-finance
scores             WRITE: flink-fraud    READ: spark-labels
```

Admin: platform only. `kafka-acls` dumped to git weekly; diff in CI. ANONYMOUS disabled.

---

## Iceberg / object store

- Prefix `s3://lake/raw/` — ETL roles only.
- `s3://lake/curated/` — Trino analyst role via catalog grants, not `s3:ListBucket` on `*`.
- `s3://lake/tmp/` — TTL 3 days, no PII (policy + scanner).

Spark UI still shows SQL. Assume analysts can see queries; no secrets in predicates.

---

## ClickHouse grants

```sql
CREATE USER analyst IDENTIFIED BY ...;
GRANT SELECT ON analytics.events_masked TO analyst;
GRANT SELECT ON analytics.tiles_1m TO analyst;
-- no SELECT on events_raw
-- no INSERT/ALTER
```

Row policy: `customer_id IN (SELECT cid FROM user_tenants WHERE user = currentUser())` for support roles. Product app uses a **service** user per tenant or a session setting you set **server-side**.

---

## Break-glass

On-call `raw_reader` role: 4-hour JWT, ticket required, all queries audited, Slack notify. If break-glass is the daily path, grants are wrong.

---

## Network

- Kafka/CH/Trino not on public IPs.
- Notebooks: no egress.
- Spark drivers in private subnets.
- TLS even inside the VPC.

A "trusted network" is not a tenant boundary.

---

## Review script for a new dataset

1. PII columns tagged?
2. Who can SELECT raw?
3. Kafka topic ACL?
4. Retention vs legal?
5. GDPR path tested?
6. Masked view default?
7. Audit on?

If three answers are "later," the dataset stays in bronze and **not** in Hub IAM.

---

## Threat: compromised Spark job

A dependency on PyPI runs in the ETL image and reads `s3://lake/raw`. Controls: pin hashes, private index, image scanning, runtime IRSA **scoped to one prefix**, network deny to the public internet from jobs, detect unexpected `ListBucket` on other prefixes. Treat job IAM as production admin.

---

## Threat: Trino wild join as exfil

Incident 6 is availability. The same query is **exfil** if it writes to an external connector or returns 40M emails to a JDBC client. Controls: no write connectors for analysts, result size limits, column masks, audit, no `INSERT` to personal catalogues.

---

## Kafka at-rest and compacted PII

Compacted topics retain **latest** PII forever until tombstone. GDPR on compacted `user-profile` requires a tombstone and retention of deleted keys. Treat compacted topics as databases in the erasure drill.

---

## Encryption ≠ access control

SSE-S3 stops a stolen disk. It does not stop a Hub user with `s3:*`. Always pair KMS with IAM/RLS. People confuse these in reviews — call it out.

---

## On-call access

Pager role: read logs/metrics, restart jobs, **not** SELECT email. Separate `incident_data` role if you must dump a user. Time-bound.

---

## Checklist recap (print)

- TLS + at-rest + KMS
- SSO + job principals
- Kafka ACLs + registry auth
- CH/Trino/Iceberg grants
- Masked defaults
- Tenant RLS
- Audit interactive SQL
- Secrets manager
- Erasure drill
- Notebook IAM ≠ ETL IAM
