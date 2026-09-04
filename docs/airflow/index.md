# Apache Airflow

## The Problem

Your data pipeline contains 25 dependent jobs:

1. Extract orders from PostgreSQL
2. Extract payments from Stripe API
3. Extract user events from Kafka
4. Join orders and payments
5. Enrich with user data
6. Compute daily metrics
7. Load to data warehouse
8. Trigger downstream reports
9. ...and 17 more

How do you coordinate them reliably? What happens when step 4 fails? How do you rerun steps 5–8 after fixing the bug in step 4? How do you know which step is currently running?

---

## Orchestration Is Not Data Processing

Before going further, understand this distinction:

**Orchestration**: scheduling, coordinating, monitoring, and retrying jobs.

**Data processing**: actually reading, transforming, and writing data.

Airflow is an orchestrator. It should schedule Spark jobs, trigger dbt models, call APIs, and monitor outcomes. It should not itself process a 1 TB dataset by reading it into a Python function.

When Airflow is used to process data directly — reading a CSV in Python, transforming it with pandas — the bottleneck is the single Airflow worker executing that code. This is a common and expensive antipattern.

**Use Airflow to run Spark. Don't use Airflow as Spark.**

---

## Core Concepts

### DAGs

A **DAG** (Directed Acyclic Graph) defines a workflow: tasks and their dependencies.

```python
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta

with DAG(
    'daily_analytics',
    start_date=datetime(2024, 1, 1),
    schedule_interval='@daily',
    default_args={'retries': 3, 'retry_delay': timedelta(minutes=5)},
    catchup=False
) as dag:

    extract_orders = BashOperator(
        task_id='extract_orders',
        bash_command='python /opt/scripts/extract_orders.py --date {{ ds }}'
    )

    process_events = SparkSubmitOperator(
        task_id='process_events',
        application='/opt/spark_jobs/process_events.py',
        conf={'spark.executor.memory': '4g'}
    )

    compute_metrics = SparkSubmitOperator(
        task_id='compute_metrics',
        application='/opt/spark_jobs/compute_metrics.py'
    )

    # Dependencies
    extract_orders >> process_events >> compute_metrics
```

---

## Schedulers and Executors

**Scheduler**: reads DAG definitions, determines which tasks are ready to run (dependencies met, schedule time reached), and sends them to the executor.

**Executors** determine where tasks run:

| Executor | How Tasks Run | Use Case |
|----------|--------------|----------|
| SequentialExecutor | One at a time in the scheduler process | Development only |
| LocalExecutor | Multiple processes on one machine | Small-scale production |
| CeleryExecutor | Distributed across multiple worker machines | Medium-large scale |
| KubernetesExecutor | Each task in a new Kubernetes pod | Cloud-native, isolation |

---

## Idempotency

Airflow tasks should be **idempotent**: running the same task twice produces the same result.

Why: tasks can be retried automatically. If a task is not idempotent, a retry may corrupt data (e.g., double-counting rows).

**Non-idempotent** (avoid):
```python
# Appends regardless of whether today's data already exists
df.write.mode("append").parquet(f"s3://data/events/date={execution_date}")
```

**Idempotent** (prefer):
```python
# Overwrites — safe to rerun
df.write.mode("overwrite").parquet(f"s3://data/events/date={execution_date}")
```

---

## Backfills

When you deploy a new DAG or fix a bug in an existing one, you often need to reprocess historical dates. This is a **backfill**.

Airflow supports backfills natively:

```bash
airflow dags backfill daily_analytics \
  --start-date 2024-01-01 \
  --end-date 2024-01-15
```

!!! warning "Production Gotcha"
    Backfills run in parallel by default. Backfilling 90 days at once can trigger 90 DAG runs simultaneously, overwhelming downstream systems. Use `--max-active-runs` to limit concurrency.

---

## Common Gotchas

### 1. DAGs That Are Too Fine-Grained

Breaking a Spark job into 50 Airflow tasks (one per transformation) adds no value and makes debugging harder. Airflow tasks should represent meaningful units of work — not individual SQL statements.

### 2. Using Airflow for Data Processing

```python
# ANTIPATTERN: Airflow worker processing 1 TB of data
def process_big_data(**context):
    df = pd.read_csv("s3://events/1TB.csv")  # Memory explosion
    ...
```

### 3. Unbounded Catchup

A new DAG with `catchup=True` (the default) and a start date 2 years ago triggers 730 DAG runs immediately. Set `catchup=False` unless you explicitly want historical backfill.

### 4. Sensor Poke Mode at Scale

`FileSensor`, `ExternalTaskSensor` and others in poke mode occupy a worker slot while waiting. With many sensors, you exhaust worker capacity. Use `reschedule` mode instead.

---

## How to Apply This at Work

When reviewing or designing a pipeline:

1. Is Airflow orchestrating compute engines (Spark, dbt, etc.) or executing data processing itself?
2. Are tasks idempotent — safe to retry?
3. Is `catchup` configured intentionally?
4. Are retries configured with appropriate backoff?
5. Are SLAs monitored?
6. Is there a sensible task granularity (not too fine, not monolithic)?
