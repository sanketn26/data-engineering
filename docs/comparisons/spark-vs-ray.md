# Spark vs Ray

Spark and Ray are both distributed Python frameworks. They solve different problems.

---

## The Core Difference

**Spark** is a data processing engine. It provides a high-level abstraction (DataFrame/RDD) over distributed data. The unit of work is a transformation on a partition.

**Ray** is a general-purpose distributed computing framework. It distributes arbitrary Python functions and objects. The unit of work is a task (function call) or an actor (stateful object).

Spark asks: *how do I transform this dataset?*
Ray asks: *how do I run this Python code across many machines?*

---

## What Spark Does Well

- SQL and DataFrame operations on large datasets
- Shuffle-based aggregations across distributed data
- Reading/writing from lakehouses (Iceberg, Delta, Hudi)
- Batch ETL pipelines
- ML training on tabular data (MLlib)
- Structured Streaming (micro-batch)

Spark is purpose-built for data transformation at scale. If your workload is "read this dataset, transform it, write it somewhere", Spark is the right abstraction.

---

## What Ray Does Well

- Distributed hyperparameter search (Ray Tune)
- Distributed reinforcement learning (RLlib)
- Serving ML models at low latency (Ray Serve)
- Parallelising embarrassingly parallel Python workloads
- Distributed deep learning (wrapping PyTorch/TensorFlow)
- Actor-based simulations and stateful distributed systems

Ray is purpose-built for distributed Python. If your workload is "run this Python function on many inputs in parallel" or "maintain state across many nodes", Ray is the right abstraction.

---

## API Comparison

**Spark**:
```python
df = spark.read.parquet("s3://bucket/data/")
result = df.groupBy("customer_id").agg(sum("revenue"))
result.write.parquet("s3://bucket/output/")
```

**Ray tasks**:
```python
@ray.remote
def process(batch):
    return model.predict(batch)

futures = [process.remote(b) for b in batches]
results = ray.get(futures)
```

**Ray actors**:
```python
@ray.remote
class ModelServer:
    def __init__(self):
        self.model = load_model()

    def predict(self, input):
        return self.model.predict(input)
```

---

## Performance Characteristics

| Characteristic | Spark | Ray |
|---------------|-------|-----|
| Large shuffle operations | Excellent | Poor (not designed for this) |
| Embarrassingly parallel tasks | Good | Excellent |
| Python overhead | Higher (JVM bridge) | Lower (pure Python) |
| Memory model | JVM heap + off-heap | Object store (plasma) |
| Task scheduling overhead | Higher | Lower |
| DataFrame operations | Native, optimised | Via Ray Data (improving) |

---

## ML Workloads

For training large models:
- **Spark MLlib**: good for traditional ML (logistic regression, gradient boosting) on tabular data. Struggles with deep learning (JVM + tensor operations don't mix well).
- **Ray**: designed for this. Ray Train wraps PyTorch/TensorFlow distributed training. Ray Tune does distributed hyperparameter search. Ray Serve deploys models with low-latency inference.

For ML pipelines end-to-end, Ray is increasingly the better choice. For feature engineering on large tabular datasets before ML, Spark is often still used in combination.

---

## Ray Data vs Spark DataFrames

Ray Data provides a streaming DataFrame-like API:

```python
ds = ray.data.read_parquet("s3://bucket/data/")
ds = ds.map_batches(transform_fn, batch_format="pandas")
ds.write_parquet("s3://bucket/output/")
```

Ray Data is designed for ML preprocessing pipelines (loading, transforming, feeding into training). It handles heterogeneous data types (images, text, tabular) better than Spark. For pure SQL/aggregation workloads on structured data, Spark DataFrames are still more mature and expressive.

---

## When to Use Spark

- Data pipeline: read → transform → write
- SQL analytics or aggregations
- Writing to a lakehouse
- Your team already knows PySpark
- Structured Streaming workloads

## When to Use Ray

- Distributed hyperparameter tuning
- Distributing PyTorch/TensorFlow training
- Serving ML models with low latency
- Parallelising Python code that doesn't fit the DataFrame model
- Actor-based stateful distributed systems

## Both Together

A common pattern in ML platforms:

```
Raw data → Spark (feature engineering) → Feature store
                                        → Ray Train (model training)
                                        → Ray Serve (model serving)
```

Spark handles the data transformation. Ray handles the ML lifecycle.
