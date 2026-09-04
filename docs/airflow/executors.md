# Airflow Executors

The executor determines how Airflow runs tasks. It is one of the most operationally significant configuration choices in a production Airflow deployment.

---

## The Executor's Job

The scheduler determines *which* tasks are ready to run and *when*. The executor determines *where* and *how* they run.

---

## SequentialExecutor

Runs one task at a time in the same process as the scheduler. No parallelism.

**Use case**: local development, debugging. Never use in production.

---

## LocalExecutor

Runs tasks as subprocesses on the same machine as the scheduler. Supports parallelism (multiple tasks running simultaneously).

**Use case**: small deployments with a single-machine Airflow instance. Not horizontally scalable — all tasks share the scheduler's resources.

---

## CeleryExecutor

The traditional production executor. Distributes tasks to a pool of Celery workers via a message broker (Redis or RabbitMQ).

```
Scheduler → Redis/RabbitMQ (message broker) → Celery Workers (run tasks)
```

**Scaling**: add more Celery workers to increase throughput.

**Operations**: you must operate and monitor the broker, the workers, and the Airflow web server separately. Non-trivial.

**Use case**: large Airflow deployments where you need many parallel workers and have the operational capacity to manage Celery.

---

## KubernetesExecutor

Spawns a new Kubernetes pod for each task. Tasks run in isolation and are cleaned up after completion.

```
Scheduler → Kubernetes API → Pod per task (runs and exits)
```

**Advantages**:
- No persistent workers to manage
- Each task gets its own pod with configurable CPU/memory
- Tasks can use different Docker images
- Native Kubernetes scaling

**Disadvantages**:
- Pod startup overhead (~10-30 seconds per task)
- Requires a Kubernetes cluster
- More complex debugging (pod logs, not Celery logs)

**Use case**: Kubernetes-native deployments, tasks with heterogeneous resource requirements, or where operational simplicity of no persistent workers is valued.

---

## CeleryKubernetesExecutor

Hybrid: routes some tasks to Celery workers (low startup cost) and others to Kubernetes pods (resource isolation). Routes based on queue.

```python
run_on_k8s = PythonOperator(
    task_id="heavy_task",
    queue="kubernetes",  # This task goes to K8s
    ...
)
```

**Use case**: mixed workloads where most tasks are lightweight (use Celery) but some need resource isolation or custom images (use Kubernetes).

---

## Comparing Executors

| Executor | Parallelism | Operations | Pod startup | Use case |
|----------|------------|------------|------------|---------|
| Sequential | None | Trivial | N/A | Dev only |
| Local | Yes (single machine) | Trivial | N/A | Small deployments |
| Celery | Yes (multi-machine) | High | N/A | Large traditional deployments |
| Kubernetes | Yes (multi-pod) | Medium | ~20s | K8s-native deployments |

---

## Worker Resource Configuration

For KubernetesExecutor, configure resources per task:

```python
from airflow.kubernetes.pod_override import PodOverride
from kubernetes.client import models as k8s

transform = SparkSubmitOperator(
    task_id="transform",
    executor_config={
        "pod_override": k8s.V1Pod(
            spec=k8s.V1PodSpec(
                containers=[
                    k8s.V1Container(
                        name="base",
                        resources=k8s.V1ResourceRequirements(
                            requests={"cpu": "2", "memory": "4Gi"},
                            limits={"cpu": "4", "memory": "8Gi"},
                        ),
                    )
                ]
            )
        )
    },
)
```

---

## Concurrency Settings

Airflow has multiple concurrency controls:

```
parallelism = 32                    # Max tasks running across entire Airflow
dag_concurrency = 16                # Max tasks running per DAG
max_active_runs_per_dag = 1         # Max concurrent DAG runs per DAG
worker_concurrency = 8              # (Celery) Tasks per worker
```

Tune these based on your cluster capacity and the resource requirements of your tasks.
