---
description: Spark scales DataFrame transformations over partitions; Ray scales a Python task and actor graph — forcing one to do the other reinvents the worse of both.
---

# Spark vs Ray

A ML engineer asks in Slack: "we already have a 40-node Spark cluster for ETL — can't we just run our hyperparameter sweep as a Spark job over the search space instead of standing up Ray?" The sweep is 200 independent training runs, each holding a model in memory across several epochs, with early-stopping logic that needs to talk back to a scheduler. Predict before you read on: does Spark's DataFrame/partition model fit this, or does it fight it?

It fights it. Spark and Ray both spread Python over machines, but they answer different questions.

**Spark:** how do I transform this **dataset** (partitions, shuffle, SQL)?
**Ray:** how do I run this **Python call graph** (tasks, actors, object store)?

If you force Ray to do a 20 TB join, you will reinvent a worse Spark. If you force Spark to do distributed PyTorch and a hyperparam sweep, you will reinvent a worse Ray.

Related: [Ray](../distributed-python/ray.md), [Spark](../spark/index.md), [analytics platform](../architectures/analytics-platform.md).

---

## The core difference

Spark's abstraction is a DataFrame/RDD: **narrow vs wide** transformations, shuffle, Catalyst. The unit of scheduling is a **task on a partition**.

Ray's abstraction is a dynamic task/actor graph. The unit is a **function invocation** or a **stateful actor**. The object store holds futures/objects, not a query plan.

```python
# Spark: relational
df.groupBy("customer_id").agg(F.sum("revenue"))

# Ray: Python
@ray.remote
def fit(config):
    return train(config)

ray.get([fit.remote(c) for c in search_space])
```

---

## What each is for

| Spark is for | Ray is for |
|--------------|------------|
| SQL, joins, group-bys on lakes | Tune, Train, Serve, RLlib |
| Iceberg/Delta/Hudi IO | Embarrassingly parallel Python |
| ETL that must be replayable | Actors holding a model or sim |
| Tabular MLlib (limited) | PyTorch/TF distributed |

Ray Data exists and looks like a DataFrame. It is aimed at **ML preprocessing pipelines** (images, tensors, map_batches), not as your company warehouse.

---

## Performance character

| | Spark | Ray |
|--|-------|-----|
| Large shuffle | Excellent | Not the design centre |
| Million tiny Python calls | JVM + UDF tax | Excellent |
| SQL optimizer | Catalyst + AQE | Not Spark |
| Memory | JVM + Tungsten | Plasma/object store |
| Heterogeneous data | Awkward | Natural |

Python UDFs in Spark are the tell: if the job **is** the UDF, Ray (or plain multiprocessing) may be the engine. If the job is the **join**, stay in Spark SQL and avoid UDFs.

---

## Choose Spark when

- Read Parquet/Iceberg → join/agg → write.
- Analysts need SQL.
- Structured Streaming / batch ETL is the product.
- Feature **tables** for ML (wide group-bys) before training.

## Choose Ray when

- Hyperparameter search, distributed training, online model serve.
- Work is "map this Python function over 10k experiments."
- Stateful actors (simulations, model replicas).
- Deep learning — Spark MLlib will not save you.

## Choose neither when

- Single-box pandas/scikit fits in RAM — you do not need a cluster.
- The serving path is 5 ms in an existing JVM/Go service — don't insert Ray Serve for fashion.
- You need a **fraud score in 200 ms with Kafka state** — that is Flink/KV, not Ray, not Spark.
- You need a dashboard — ClickHouse.

!!! warning "Anti-pattern"
    "We're a Python shop, so Ray for ETL." PySpark is Python. The issue is the **dataflow**, not the logo on the intern's laptop.

---

## Running example: SaaS analytics + a model

From [analytics-platform](../architectures/analytics-platform.md) and a churn model:

```
Kafka → Spark/Flink → Iceberg feature tables
                    → Ray Train (weekly)
                    → Ray Serve or a boring sidecar
Customer UI still reads ClickHouse aggregates, not Ray.
```

Spark (or dbt) owns **features that are SQL**. Ray owns **fit()**. ClickHouse owns **the product**. Three tools because three workloads.

Fraud: do **not** call Ray Serve from the authorisation path unless you have measured p99. In-process model in the scorer is the V1 in [fraud](../architectures/fraud.md).

---

## APIs side by side

**Spark**

```python
df = spark.read.parquet("s3://bucket/data/")
result = df.groupBy("customer_id").agg(F.sum("revenue"))
result.write.mode("overwrite").parquet("s3://bucket/output/")
```

**Ray tasks**

```python
@ray.remote
def process(batch):
    return model.predict(batch)

results = ray.get([process.remote(b) for b in batches])
```

**Ray actors**

```python
@ray.remote
class ModelServer:
    def __init__(self):
        self.model = load_model()
    def predict(self, x):
        return self.model.predict(x)
```

**Ray Data** (ML ingest, not a warehouse)

```python
ds = ray.data.read_parquet("s3://bucket/data/")
ds = ds.map_batches(transform_fn, batch_format="pandas")
ds.write_parquet("s3://bucket/output/")
```

---

## ML split that holds up

| Step | Default engine |
|------|----------------|
| Warehouse / lake transforms | Spark |
| Small-data sklearn | pandas, one box |
| Distributed GBDT on huge tables | Spark ML or specialised (XGBoost on Spark); not Ray required |
| DL / Tune / RL | Ray |
| Feature store serving | KV / CH / dedicated FS |
| Product dashboard | ClickHouse |

---

## Both together

```
Raw → Spark (features) → Feature table
                       → Ray Train
                       → Ray Serve
```

Ops note: two clusters, two UIs, two failure modes. Worth it when each cluster is **at capacity doing its job**. Not worth it to "standardise on Ray" for SQL.

---

## Decision checklist

```
Is it SQL/joins/shuffle on a lake?     → Spark
Is it train/tune/serve Python models?  → Ray
Is it both?                            → Spark then Ray
Is it streaming state?                 → Flink, not this page
Does it fit on a laptop?               → Neither cluster
```

---

## Worked example: churn model on SaaS events

[Analytics](../architectures/analytics-platform.md) events in Iceberg, 1 TB/day.

| Step | Engine | Why not the other |
|------|--------|-------------------|
| Sessionize / 30 d aggregates | Spark SQL | Ray shuffle is not your friend at 30 TB |
| Join to billing Postgres dump | Spark or Trino | Ray Data will not federate PG well |
| Train LightGBM on 50 GB features | Spark ML **or** one big box | Ray optional |
| Train a small net + Tune 200 trials | **Ray Tune** | Spark cluster sitting idle per trial is waste |
| Serve 50 QPS batch scores nightly | Spark job | Ray Serve unnecessary |
| Serve 5k QPS online | Ray Serve or a boring Flask+ONNX sidecar | Spark |

Most companies need Spark for step 1 and a **single VM** for step 3. Ray arrives with Tune/DL. Buying Ray on day one of an ETL project is the anti-pattern.

---

## UDF as a smell

If 80% of Spark time is Python UDF (not vectorised pandas UDFs even), you are using Spark as a **task scheduler**. Ray (or Airflow + processes) may be cheaper. If 80% of time is `exchange hashpartitioning`, you are using Ray as a **worse warehouse** if you moved off Spark.

---

## Ray Data vs Spark, honestly

Ray Data shines when:

- Images/video/tokens, `map_batches` into GPU train.
- Streaming pipeline **into training**, not into a 5-year lake.

Spark shines when:

- Predicate pushdown, partition prune, AQE, Iceberg commits, SQL analysts.

A "lakehouse on Ray Data" in 2026 may exist in a blog. Your on-call still needs Iceberg stats and Spark/Trino. Do not strand the warehouse.

---

## Actors vs Spark mapPartitions

`mapPartitions` can hold a model per executor. That is a poor actor: recovery, routing, and serving QPS are worse than Ray Serve or a dedicated service. Use Spark to **score a table** offline. Use actors/services to **score a request**.

Fraud 200 ms: [fraud](../architectures/fraud.md) in-process model, not Ray remote per payment (unless colocated and measured).

---

## Review script

1. Is the unit a table or a Python call?
2. Is there a large shuffle?
3. GPU?
4. Online vs offline?
5. If "both," draw the handoff (feature table in object storage). No shared RAM fairy.

---

## Object store vs shuffle service

Spark's shuffle is a **designed** data exchange (with spill, encryption, AQE). Ray's object store is a **general** plasma/shared memory for futures. Dumping a 5 TB join through Ray objects is possible in theory and a support nightmare in practice.

Conversely, launching 5,000 Spark jobs for 5,000 hyperparam trials makes the YARN/k8s scheduler the bottleneck Ray Tune already solved.

---

## Serving

Ray Serve: replicas, autoscale, HTTP. Good at Python models. Spark: not a request server. Flink: not a request server (unless you stretch it).

Online fraud: in-process in the scorer ([fraud](../architectures/fraud.md)). Ray remote per request adds tail latency unless colocated and warmed. Measure p99.

---

## Team topology

Spark lives with data engineering. Ray lives with ML platform. The handoff is a **table** (features) plus a **training job**. Shared cluster "to save money" often means ML experiments evict ETL or vice versa. Separate queues at least.

---

## Choose-neither recap

Flink for keyed streaming. CH for tiles. pandas for 2 GB. Don't cluster-ify a Jupyter experiment that runs in 40 s.

---

## FAQ

**Ray Data for warehouse ETL?** Not as the company SoR. Use Spark/dbt/Iceberg.

**Spark for hyperparam?** Painful. Ray Tune or a job queue.

**Dask vs Ray vs Spark?** Dask is pandas-shaped single-ish team. Spark is SQL/lake. Ray is tasks/actors/ML. Dask is not covered in this academy; do not add it to V1 because a notebook imported it.

**GPU Spark?** Exists; still not the DL path. Ray Train / dedicated GPU schedulers.

**Can one k8s run both?** Yes, **separate** queues and node groups. Mixed autoscaling is how ETL OOMs training.

---

## Anti-patterns

- Rewriting `groupBy` in Ray tasks to "unify Python."
- Spark UDF calling `ray.get` inside a map (nested clusters).
- Ray Serve as the only way to run a 5 QPS sklearn model (a container is enough).

---

## Handoff contract

```
Iceberg feature table
  snapshot_id pinned for the run
  grain: customer_id, date
  produced by: Spark job gold.features
  consumed by: Ray Train job, versioned
```

If this contract is missing, ML and DE will fight over a moving Parquet path. [Metadata](../metadata/index.md). Pin the Iceberg snapshot in the training job's run config so "the data moved" is a reproducible ID, not a Slack argument.
