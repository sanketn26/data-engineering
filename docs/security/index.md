# Data Security

## Why Security Is a First-Class Concern in Data Engineering

A data platform, by definition, aggregates data from many systems. It often holds:
- Personal user data (PII)
- Financial transactions
- Health records
- Authentication events
- Internal business metrics

A security breach in a data platform is typically far more damaging than a breach of a single application system.

---

## Encryption

**At rest**: all object storage (S3, GCS) should have server-side encryption enabled. ClickHouse, PostgreSQL, Kafka — ensure disk encryption. This protects against physical disk theft.

**In transit**: all connections should use TLS/SSL. Kafka with SASL_SSL, ClickHouse HTTPS/TLS, all API endpoints HTTPS.

**Key management**: use a KMS (AWS KMS, GCP Cloud KMS, HashiCorp Vault) for managing encryption keys. Never hardcode keys.

---

## Authentication and Authorization

**Authentication**: who are you?
- Service accounts with certificates/tokens
- OAuth 2.0 for human access
- SAML/SSO for enterprise authentication

**Authorization**: what are you allowed to do?

**RBAC** (Role-Based Access Control): assign roles to users, permissions to roles.

```sql
-- Trino RBAC example
CREATE ROLE analyst;
GRANT SELECT ON SCHEMA analytics TO ROLE analyst;
GRANT ROLE analyst TO USER alice;
```

**ABAC** (Attribute-Based Access Control): more granular — access depends on attributes of the user, resource, and environment.

---

## Column-Level and Row-Level Access

Not every user should see every column:

```sql
-- Trino: mask PII for non-privileged users
CREATE VIEW events_masked AS
SELECT
    event_id,
    timestamp,
    service,
    CASE WHEN is_pii_viewer() THEN email ELSE '***' END AS email,
    CASE WHEN is_pii_viewer() THEN user_id ELSE NULL END AS user_id
FROM events_raw;
```

Row-level security restricts which rows a user can see:

```sql
-- Only show each customer their own data
CREATE ROW LEVEL SECURITY POLICY customer_isolation
ON events
USING (customer_id = current_user_customer_id());
```

---

## PII and GDPR

**PII classification**: identify all columns containing personal data (name, email, phone, IP, location, user ID, device ID, cookie). Tag them in your data catalogue.

**Right to erasure (GDPR Article 17)**: when a user requests deletion, you must remove their data from:
- All production systems
- Data lakes (Iceberg/Hudi support row-level deletes)
- Backups (complex — requires retention policies)
- Analytics systems

**Data residency**: some regulations require data not to leave a jurisdiction. This affects where your Kafka brokers, object storage, and compute run.

**Retention limits**: don't keep personal data longer than necessary. Implement TTL policies.

---

## Secrets Management

Never store credentials in code, configuration files, or environment variables visible to other processes.

Use:
- AWS Secrets Manager / GCP Secret Manager
- HashiCorp Vault
- Kubernetes Secrets (with encryption at rest)

```python
# Fetch at runtime from secrets manager
import boto3

def get_db_password():
    client = boto3.client('secretsmanager', region_name='us-east-1')
    response = client.get_secret_value(SecretId='prod/db/password')
    return response['SecretString']
```

---

## Practical Security Checklist

For any new data system:
- [ ] Encryption at rest enabled
- [ ] All connections use TLS
- [ ] Service accounts with least-privilege permissions
- [ ] PII columns identified and tagged
- [ ] Column-level masking for sensitive data
- [ ] Access audit logging enabled
- [ ] Secrets in secrets manager, not in code
- [ ] Deletion mechanism tested for GDPR compliance
- [ ] Data retention policies configured
