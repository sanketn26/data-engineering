# Airflow Production Gotchas

Common mistakes that cause production incidents. Every item here represents a real failure mode.

---

## 1. The Scheduler Bottleneck

**What happens**: Airflow scheduler is slow, tasks don't start on time, backlog builds up.

**Why**: Airflow 1.x had a single-threaded scheduler that parsed all DAGs serially. Even in Airflow 2.x, a poorly written DAG can slow down the entire scheduler loop.

**Fix**:
- Avoid heavy imports at DAG file level
- Use `@dag` decorator with lazy imports inside tasks
- Set `min_file_process_interval` to reduce re-parsing frequency
- Use `dag_discovery_safe_mode = False` if you control all DAG files
- Consider Airflow 2.x's improved scheduler with HA mode

---

## 2. Operator Confusion: Orchestrator vs Executor

**What happens**: engineers put heavy logic directly in PythonOperators.

```python
# WRONG: heavy data processing in Airflow task
def process_events(**context):
    df = pd.read_csv("s3://events/large_file.csv")  # 50GB file
    result = df.groupby("user_id").sum()
    result.to_parquet("s3://output/")

task = PythonOperator(task_id="process", python_callable=process_events)
```

Airflow is an **orchestrator**. The Airflow worker does not have the memory to process 50GB. The worker should submit a Spark job.

**Fix**:
```python
task = SparkSubmitOperator(
    task_id="process",
    application="s3://code/process_events.py",
    application_args=["--date", "{{ ds }}"],
)
```

---

## 3. Unbounded Sensor Polling

**What happens**: sensors using `mode="poke"` hold a worker slot indefinitely.

```python
# WRONG: holds worker slot while waiting
wait = S3KeySensor(
    task_id="wait",
    mode="poke",        # Holds slot
    poke_interval=60,   # Checks every 60s
    timeout=86400,      # Waits up to 24 hours
)
```

With `mode="poke"`, the Celery worker is occupied for the full wait period. If you have 10 DAGs waiting for upstream data with 10 workers, all workers are occupied.

**Fix**:
```python
wait = S3KeySensor(
    task_id="wait",
    mode="reschedule",  # Releases worker slot between checks
    poke_interval=300,
    timeout=86400,
)
```

---

## 4. Missing Retry Logic

**What happens**: transient failures (network blip, source unavailable) cause DAG failures that require manual intervention.

**Fix**: always configure retries on tasks that interact with external systems:

```python
default_args = {
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,  # Backoff between retries
}
```

---

## 5. Catchup=True on New DAGs

**What happens**: you deploy a new daily DAG with `start_date=datetime(2023, 1, 1)`. Airflow immediately queues 365+ backfill runs, overwhelming the executor.

**Fix**: set `catchup=False` on new DAGs unless you explicitly need the backfill. Trigger backfills manually when needed:

```bash
airflow dags backfill my_dag --start-date 2024-01-01 --end-date 2024-01-31
```

---

## 6. Timezone Confusion

**What happens**: DAG scheduled at `0 2 * * *` runs at unexpected times because timezones are mismatched.

**Fix**: always use timezone-aware datetimes:

```python
from datetime import datetime
import pendulum

start_date = pendulum.datetime(2024, 1, 1, tz="UTC")
```

Airflow's scheduler operates in UTC internally. Be explicit.

---

## 7. XCom Abuse

**What happens**: tasks push large objects to XCom, bloating the metadata database.

**Fix**: XCom is for metadata (row counts, S3 paths, status flags), not data. If a task needs to pass a large result to the next task, write it to S3 and push the path via XCom.

---

## 8. No Data Validation Between Tasks

**What happens**: a Spark job writes 0 rows due to a filter bug. The downstream load task succeeds (loads 0 rows). Dashboards show blank data. No alert fired.

**Fix**: add validation tasks between major processing steps:

```python
def validate_output(**context):
    date = context["ds"]
    count = spark.sql(f"SELECT COUNT(*) FROM output WHERE dt='{date}'").collect()[0][0]
    if count == 0:
        raise ValueError(f"Zero rows in output for {date}")
    if count < MIN_EXPECTED_ROWS:
        raise ValueError(f"Only {count} rows, expected >{MIN_EXPECTED_ROWS}")

validate = PythonOperator(task_id="validate_output", python_callable=validate_output)
transform >> validate >> load
```

---

## 9. DAG Dependencies Without ExternalTaskSensor

**What happens**: DAG B must run after DAG A. Engineers solve this with an arbitrary sleep or a time-based schedule offset. DAG A occasionally runs late and DAG B reads stale data.

**Fix**: use `ExternalTaskSensor`:

```python
from airflow.sensors.external_task import ExternalTaskSensor

wait_for_dag_a = ExternalTaskSensor(
    task_id="wait_for_upstream",
    external_dag_id="dag_a",
    external_task_id="final_task",
    mode="reschedule",
    timeout=3600,
)
```

---

## 10. No Alerting

**What happens**: a DAG fails silently. Engineers discover the issue hours later when a stakeholder reports missing data.

**Fix**: configure email or Slack alerts on failure:

```python
from airflow.operators.slack_webhook_operator import SlackWebhookOperator

def alert_slack(context):
    SlackWebhookOperator(
        task_id="slack_alert",
        http_conn_id="slack",
        message=f"DAG {context['dag'].dag_id} failed on {context['ds']}",
    ).execute(context)

default_args = {
    "on_failure_callback": alert_slack,
}
```
