# Apache Ray

## The Problem

Python makes it easy to write one process. How do you run Python functions across 32 machines?

Python's GIL limits true parallelism within a process. `multiprocessing` works on one machine. For distributed Python across a cluster, you need Ray.

---

## The Mental Model

```python
# Normal Python
def process(data):
    return expensive_computation(data)

result = process(my_data)  # blocks, runs on this machine

# Ray: distributed Python
import ray
ray.init()

@ray.remote
def process(data):
    return expensive_computation(data)

future = process.remote(my_data)  # non-blocking, runs on any worker
result = ray.get(future)           # fetch when needed
```

The `@ray.remote` decorator tells Ray this function can run on any machine in the cluster. `process.remote()` submits it and returns an `ObjectRef` (future). `ray.get()` blocks until the result is ready.

---

## Core Concepts

### Tasks

Stateless remote functions. Can run in parallel across workers.

```python
@ray.remote
def compute_features(user_events):
    # Feature engineering on one user's data
    return extract_features(user_events)

# Launch 1000 tasks in parallel
futures = [compute_features.remote(events) for events in all_users_events]
results = ray.get(futures)  # collect all results
```

### Actors

Stateful distributed objects. Useful when tasks need shared, mutable state.

```python
@ray.remote
class ModelServer:
    def __init__(self, model_path):
        self.model = load_model(model_path)

    def predict(self, features):
        return self.model.predict(features)

# Create actor on some worker
server = ModelServer.remote("/models/fraud_model.pkl")

# Call methods on it
prediction = ray.get(server.predict.remote(features))
```

### ObjectRefs

`process.remote()` returns an `ObjectRef` — a reference to an object in the Ray object store. Objects are stored in a distributed shared-memory store across all workers.

```python
# Pass ObjectRef directly (no deserialization until needed)
data_ref = ray.put(large_dataset)
futures = [process_chunk.remote(data_ref, i) for i in range(100)]
```

---

## Ray Data

For distributed data processing:

```python
import ray.data

dataset = ray.data.read_parquet("s3://events/2024/")

# Distributed transformations
processed = (dataset
    .filter(lambda row: row["status_code"] >= 500)
    .map(lambda row: {**row, "features": extract_features(row)})
    .repartition(100))

# Streaming execution: data pipelines without loading all into memory
processed.write_parquet("s3://features/2024/")
```

---

## Ray vs Spark

| Dimension | Spark | Ray |
|-----------|-------|-----|
| **Model** | DataFrame-centric DAG | Python functions + actors |
| **SQL/ETL** | Excellent | Limited |
| **Arbitrary Python** | Limited (UDFs are slow) | First-class |
| **ML training** | MLlib | Ray Train, seamless |
| **Model serving** | Limited | Ray Serve, excellent |
| **State** | Structured (DataFrames) | Arbitrary (actors) |
| **Ecosystem maturity** | Very mature | Growing rapidly |

**Use Spark** for SQL-heavy ETL, large-scale batch transformations, Iceberg/Delta integration.

**Use Ray** for distributed Python, ML training/inference pipelines, simulation workloads, actor-based systems.

They are not competitors — many production systems use both.

---

## Use Case in the Running Examples

**SaaS Analytics Platform**: Ray for ML feature engineering at scale — distribute Python code across 50 nodes without wrestling with JVM serialization.

**IoT Platform**: Ray for distributed anomaly detection inference — run thousands of model predictions in parallel across device streams.
