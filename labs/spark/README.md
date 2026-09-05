# Spark lab (local PySpark)

No Docker cluster. You will watch **shuffle**, **skew**, **join strategy**, and **partition count** on a laptop Spark, then break a join the same way [incident 2](../../docs/incidents/index.md) does.

Docs: [Spark labs (longer scripts)](../../docs/spark/labs.md), [shuffle](../../docs/spark/shuffle.md), [shuffle sim](../../docs/simulations/spark-shuffle.html).

## Prerequisites

```bash
pip install -r requirements.txt
# Java 17 or 11 on PATH (PySpark will fail fast if missing)
```

~8 GB RAM. Spark UI: http://localhost:4040 while a session is alive.

Committed quick path:

```bash
python run_lab.py --mode uniform --rows 1000000 --partitions 20 --hold
python run_lab.py --mode skew --rows 1000000 --partitions 20 --hold
```

## Predict

1. Does `groupBy("service")` cause a shuffle even when data is tiny and uniform?
2. If 80% of rows are `customer_1`, will 20 shuffle partitions split that key?
3. Will a broadcast join still shuffle the **large** fact table?
4. Is `spark.sql.shuffle.partitions=1000` always faster than `50` on 2M rows?

## Run 1 — observe shuffle

```python
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import random

spark = (
    SparkSession.builder.appName("lab-shuffle")
    .config("spark.ui.enabled", "true")
    .config("spark.sql.shuffle.partitions", "20")
    .getOrCreate()
)

data = [
    (f"customer_{i % 100}", f"service_{i % 10}", random.randint(10, 500))
    for i in range(1_000_000)
]
events = spark.createDataFrame(data, ["customer_id", "service", "latency_ms"])
events.cache().count()

result = events.groupBy("service").agg(
    F.count("*").alias("count"),
    F.avg("latency_ms").alias("avg_latency"),
)
result.write.mode("overwrite").parquet("/tmp/dea_lab1_output")
print("UI: http://localhost:4040 — Stages: Shuffle Read/Write")
spark.stop()
```

**Compare:** you should see a shuffle write even though this is "just a groupBy." Open the stage, look at task time distribution — should be **even** (uniform keys).

## Run 2 — skew

```python
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import random

spark = (
    SparkSession.builder.appName("lab-skew")
    .config("spark.sql.shuffle.partitions", "20")
    .config("spark.sql.adaptive.enabled", "false")
    .getOrCreate()
)

data = []
for i in range(1_000_000):
    customer = "customer_1" if random.random() < 0.8 else f"customer_{random.randint(2, 1000)}"
    data.append((customer, random.randint(10, 500)))

events = spark.createDataFrame(data, ["customer_id", "latency_ms"])
events.cache().count()

result = events.groupBy("customer_id").agg(F.count("*"), F.sum("latency_ms"))
result.write.mode("overwrite").parquet("/tmp/dea_lab2_skewed")
print("UI: one task's duration and shuffle read should dwarf the rest")
spark.stop()
```

**Compare to prediction 2.** Twenty partitions do not split `customer_1`. Enable AQE (`spark.sql.adaptive.enabled=true` and skew join) and re-run; it may **split** some skewed partitions — or not, if the aggregation is not a join. For **groupBy** skew, AQE coalesce helps small partitions more than the whale key. Salting is the honest fix for a single key.

## Run 3 — joins

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast
import time, random

spark = SparkSession.builder.appName("lab-joins").getOrCreate()

orders = spark.createDataFrame(
    [(i, f"customer_{i % 10000}", random.uniform(10, 1000)) for i in range(2_000_000)],
    ["order_id", "customer_id", "amount"],
)
customers = spark.createDataFrame(
    [(f"customer_{i}", f"Segment {i % 5}") for i in range(10000)],
    ["customer_id", "segment"],
)
orders.cache().count()
customers.cache().count()

spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
t0 = time.time()
orders.join(customers, "customer_id").count()
print(f"sort-merge: {time.time() - t0:.2f}s")

t0 = time.time()
orders.join(broadcast(customers), "customer_id").count()
print(f"broadcast: {time.time() - t0:.2f}s")
spark.stop()
```

**Compare to prediction 3.** Broadcast avoids shuffling **orders**. Sort-merge shuffles both (or at least the join key exchange).

## Run 4 — partition count

Sweep `spark.sql.shuffle.partitions` in `{10, 50, 200, 1000}` on a 2M-row `groupBy`. **Predict** a U-shape: too few → fat tasks; too many → scheduler overhead.

## Break — skewed join (incident shape)

Keep AQE **off**, broadcast **off**. Build `events` with 80% `cust_0042` and join to a **large** (not broadcastable) dimension you duplicate so Spark cannot broadcast — or join events to itself on `customer_id`.

```python
from pyspark.sql import SparkSession
import random

spark = (
    SparkSession.builder.appName("lab-skew-join")
    .config("spark.sql.adaptive.enabled", "false")
    .config("spark.sql.autoBroadcastJoinThreshold", "-1")
    .config("spark.sql.shuffle.partitions", "20")
    .getOrCreate()
)

left = [(("cust_0042" if random.random() < 0.8 else f"cust_{i%200}"), i) for i in range(500_000)]
right = [(f"cust_{i}", f"name_{i}") for i in range(200)] + [("cust_0042", "whale")] * 50
# 50 rows on the whale key on the right → output fan-out on that key
ldf = spark.createDataFrame(left, ["customer_id", "n"])
rdf = spark.createDataFrame(right, ["customer_id", "name"])
ldf.join(rdf, "customer_id").count()  # watch UI; one task crawls
spark.stop()
```

If the process OOMs, you have reproduced the incident on a laptop scale. Fix: `broadcast(rdf)` (small) or salt `cust_0042`.

## Check your work

```bash
python check_skew.py --rows 200000
```

Runs both the uniform and skew aggregations, reads the actual per-key row counts back from the Parquet output, and asserts the data-shape half of the prediction: no key dominates in uniform mode, and `cust_0042` dominates in skew mode. It prints the measured share for each and raises an `AssertionError` naming which mode failed to reproduce, instead of asking you to eyeball a bar chart. It does not replace watching the Spark UI — that's still how you see the *consequence* (one task's duration and shuffle read dwarfing the rest); this only confirms the *cause* (the skewed key distribution) actually happened.

## Notes

- `collect()` on 1M rows is a **driver** OOM drill; do not add it "to inspect."
- Match this lab to [e-commerce](../../docs/architectures/ecommerce.md) bot/wholesale keys.
