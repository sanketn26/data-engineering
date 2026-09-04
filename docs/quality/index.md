# Data Quality

## The Silent Failure Mode

A data pipeline can be operationally healthy — all jobs green, no exceptions, data flowing — while producing completely wrong results.

An upstream schema change silently coerces a column to NULL. A timezone bug shifts all timestamps by 5.5 hours. A join on a non-unique key unexpectedly fans out, doubling row counts. None of these throw exceptions. All produce downstream dashboards that show incorrect numbers.

**Data quality** is the practice of detecting these failures before they affect decisions.

---

## Dimensions of Data Quality

| Dimension | Question | Example Check |
|-----------|---------|---------------|
| **Completeness** | Are all expected records present? | Row count vs yesterday |
| **Freshness** | Is data recent enough? | `max(timestamp) > now - 30min` |
| **Validity** | Do values conform to expected formats? | `status_code IN (200, 201, 400, 500)` |
| **Uniqueness** | Are there unexpected duplicates? | `count(*) = count(distinct order_id)` |
| **Consistency** | Do related fields agree? | `order_total = sum(item.price * item.qty)` |
| **Distribution** | Are statistical properties within expected bounds? | `avg(latency_ms) < 500` |

---

## Great Expectations

A Python framework for defining data expectations:

```python
import great_expectations as gx

context = gx.get_context()
validator = context.get_validator(batch_request=..., expectation_suite_name="events_suite")

# Define expectations
validator.expect_column_to_exist("customer_id")
validator.expect_column_values_to_not_be_null("timestamp")
validator.expect_column_values_to_be_between("latency_ms", min_value=0, max_value=60000)
validator.expect_column_unique_value_count_to_be_between("status_code", min_value=1, max_value=20)

# Validate
results = validator.validate()
if not results.success:
    raise ValueError("Data quality checks failed")
```

---

## dbt Tests

If you use dbt for transformations, built-in tests are easiest:

```yaml
# models/schema.yml
models:
  - name: clean_events
    columns:
      - name: event_id
        tests:
          - unique
          - not_null
      - name: status_code
        tests:
          - accepted_values:
              values: [200, 201, 400, 404, 500, 502, 503]
      - name: customer_id
        tests:
          - not_null
          - relationships:
              to: ref('customers')
              field: customer_id
```

---

## Anomaly Detection

Rule-based checks miss unknown problems. Anomaly detection learns normal behaviour and alerts on deviations.

Simple approach: alert when a metric is more than 3 standard deviations from a rolling mean.

```python
# Check if today's row count is anomalous
import pandas as pd

# Historical row counts (last 30 days)
history = pd.Series([1_000_000, 1_050_000, 980_000, ...])
today = 50_000  # suspiciously low

z_score = (today - history.mean()) / history.std()
if abs(z_score) > 3:
    alert(f"Row count anomaly: {today} (z-score: {z_score:.1f})")
```

---

## Where Quality Checks Belong in the Pipeline

```
Raw Data → [Quality Check 1: schema, nulls, types]
         → Transform
         → [Quality Check 2: business rules, row count, distribution]
         → Serve
         → [Quality Check 3: downstream impact, freshness]
```

Fail fast. A quality check at ingestion is infinitely cheaper than a corrupted production dashboard.
