# Idempotency in Airflow Pipelines

Idempotency is the single most important property of a production data pipeline. A pipeline is idempotent if running it twice for the same time period produces the same result as running it once.

---

## Why It Matters

Things that go wrong in production:
- A task fails halfway through a write
- You deploy a bug fix and need to reprocess last month
- A source system sends duplicate data
- You manually trigger a backfill after a monitoring outage

In every case, you need to be able to re-run the pipeline and get a correct result. Without idempotency, re-running creates duplicates, mixed data, or incorrect aggregations.

---

## The Non-Idempotent Pipeline

```python
def load_events(date, **context):
    df = spark.read.parquet(f"s3://events/dt={date}/")
    # WRONG: appending to a table that already has today's data
    df.write.mode("append").parquet(f"s3://output/events/")
```

If this task fails and retries, you get duplicate data for `date`.

---

## Making Writes Idempotent

**Pattern 1: Overwrite the partition**

```python
def load_events(date, **context):
    df = spark.read.parquet(f"s3://events/dt={date}/")
    # Overwrite only this partition, not the entire table
    df.write \
      .mode("overwrite") \
      .option("partitionOverwriteMode", "dynamic") \
      .partitionBy("dt") \
      .parquet("s3://output/events/")
```

Running this twice for the same `date` produces the same result. The partition is replaced, not appended to.

**Pattern 2: Delete then insert (for databases)**

```python
def load_events(date, **context):
    conn.execute(f"DELETE FROM events WHERE dt = '{date}'")
    # Now safe to insert — no duplicates possible
    insert_rows(df, table="events")
```

**Pattern 3: MERGE / UPSERT**

For tables that need updates (CDC targets):

```sql
MERGE INTO target t
USING source s ON (t.id = s.id AND t.dt = '{{ ds }}')
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
```

---

## Idempotent Aggregations

Avoid:
```python
# NOT idempotent: adds to existing counts
UPDATE daily_stats SET event_count = event_count + :new_count
WHERE dt = :date
```

Use instead:
```python
# Idempotent: replaces the computed value
INSERT INTO daily_stats (dt, event_count)
VALUES (:date, :computed_count)
ON CONFLICT (dt) DO UPDATE SET event_count = EXCLUDED.event_count
```

---

## The Execution Date Convention

Airflow's execution date convention supports idempotent backfills:

```python
def process(**context):
    date = context["ds"]  # Always the same value for the same DAG run
    source_path = f"s3://events/dt={date}/"
    output_path = f"s3://output/events/dt={date}/"
    # Read from fixed input, write to fixed output
    # Running twice → same result
```

The execution date is fixed per DAG run. If you trigger a backfill for January 15, every task in that run sees `ds = "2024-01-15"` regardless of when it actually executes.

---

## Testing Idempotency

Before deploying a pipeline to production, test it:

1. Run the pipeline for a date
2. Check the output row count: `COUNT(*)`
3. Run the pipeline again for the same date
4. Check the output row count again

If the row count is the same both times, the pipeline is idempotent. If it doubled, it is not.

---

## Common Violations

**Reading from `current_date()`** instead of execution date:
```python
# WRONG: produces different results depending on when it runs
df.filter(df.dt == current_date())

# RIGHT: uses fixed execution date
df.filter(df.dt == execution_date)
```

**Writing to paths without date partitions**:
```python
# WRONG: second run overwrites different data
write_to("s3://output/latest/")

# RIGHT: deterministic output path per run
write_to(f"s3://output/dt={execution_date}/")
```

**Incrementing counters in place**:
```python
# WRONG: not idempotent
UPDATE stats SET count = count + 1 WHERE user_id = ?
```
