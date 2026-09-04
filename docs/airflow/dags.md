# Airflow DAGs

A DAG (Directed Acyclic Graph) in Airflow is a collection of tasks and their dependencies. It defines *what* runs and in *what order*, not *how fast* or *on what data*.

---

## What Makes a Good DAG

A DAG should be:

1. **Idempotent** — running the same DAG run twice produces the same result
2. **Atomic** — each task either fully succeeds or fully fails (no partial writes)
3. **Isolated** — tasks do not share mutable state through files or databases mid-run

Most Airflow problems stem from violating one of these three properties.

---

## DAG Structure

```python
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.python import PythonOperator

default_args = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": True,
}

with DAG(
    dag_id="daily_events_pipeline",
    default_args=default_args,
    schedule_interval="0 2 * * *",  # 2 AM daily
    start_date=datetime(2024, 1, 1),
    catchup=False,  # Do not backfill missed runs
    tags=["events", "daily"],
) as dag:

    validate_source = PythonOperator(
        task_id="validate_source_data",
        python_callable=check_source_partition_exists,
        op_kwargs={"date": "{{ ds }}"},
    )

    transform = SparkSubmitOperator(
        task_id="transform_events",
        application="s3://code/transform_events.py",
        application_args=["--date", "{{ ds }}"],
        conf={
            "spark.executor.memory": "4g",
            "spark.executor.cores": "2",
        },
    )

    load_clickhouse = PythonOperator(
        task_id="load_to_clickhouse",
        python_callable=insert_daily_partition,
        op_kwargs={"date": "{{ ds }}"},
    )

    validate_source >> transform >> load_clickhouse
```

---

## Key Concepts

### `schedule_interval`

Airflow uses cron expressions or presets (`@daily`, `@hourly`). The `execution_date` (now called `logical_date`) is the *start* of the interval, not the time the DAG runs.

For a `@daily` DAG scheduled at midnight, the `execution_date` for the run that fires on Jan 2 is **Jan 1**. This is the "data interval" convention — the DAG processes data *for* that date.

This confuses almost everyone initially.

### `catchup`

If `catchup=True` and a DAG has a `start_date` in the past, Airflow will backfill every missed run. This is often not what you want for production pipelines. Set `catchup=False` unless you explicitly need backfill.

### `max_active_runs`

Limits how many concurrent DAG runs can execute. Important for pipelines that write to shared tables — you don't want two runs writing the same partition simultaneously.

```python
max_active_runs=1  # Only one run at a time
```

---

## Templating

Airflow's Jinja templating lets tasks use the execution date:

```python
"{{ ds }}"           # execution date as YYYY-MM-DD
"{{ ds_nodash }}"    # execution date as YYYYMMDD
"{{ execution_date }}" # datetime object
"{{ prev_ds }}"      # previous execution date
"{{ next_ds }}"      # next execution date
```

Use this to make tasks date-aware without hardcoding dates.

---

## XCom: Passing Data Between Tasks

XCom lets tasks share small values:

```python
def extract(**context):
    row_count = count_source_rows()
    context["ti"].xcom_push(key="row_count", value=row_count)

def validate(**context):
    row_count = context["ti"].xcom_pull(task_ids="extract", key="row_count")
    if row_count == 0:
        raise ValueError("No rows found in source")
```

**XCom is not for large data.** It stores values in the Airflow metadata database (Postgres/MySQL). Do not push DataFrames, large files, or anything over a few KB. For large data, write to S3 and pass the path.

---

## Sensors

Sensors wait for an external condition before proceeding:

```python
from airflow.sensors.s3_key_sensor import S3KeySensor

wait_for_upstream = S3KeySensor(
    task_id="wait_for_upstream_data",
    bucket_name="my-data-lake",
    bucket_key="events/dt={{ ds }}/success",
    timeout=3600,        # Give up after 1 hour
    poke_interval=60,    # Check every minute
    mode="reschedule",   # Release worker slot while waiting
)
```

Use `mode="reschedule"` (not `"poke"`) so the sensor releases its worker slot while waiting. `"poke"` mode holds the slot for the full wait duration.

---

## Task Groups

Organise related tasks visually:

```python
from airflow.utils.task_group import TaskGroup

with TaskGroup("quality_checks") as quality_checks:
    check_nulls = PythonOperator(task_id="check_nulls", ...)
    check_duplicates = PythonOperator(task_id="check_duplicates", ...)
    check_schema = PythonOperator(task_id="check_schema", ...)

transform >> quality_checks >> load
```

---

## Production Gotchas

**Avoid mutable global state in tasks.** Tasks may run on different workers. Don't write temporary files to local disk and expect the next task to find them.

**Don't import heavy libraries at DAG parse time.** Airflow parses DAGs continuously. Heavy imports at module level slow down the scheduler.

**Use `retry_delay` and `retries`.** Transient failures (network, downstream unavailability) should retry automatically.

**Set `execution_timeout` on long-running tasks.** Otherwise a hung task blocks forever.

```python
from datetime import timedelta

transform = SparkSubmitOperator(
    task_id="transform",
    execution_timeout=timedelta(hours=2),
    ...
)
```
