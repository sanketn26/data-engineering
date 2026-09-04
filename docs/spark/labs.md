# Spark Labs

These labs are designed to make abstract concepts concrete. Each lab demonstrates a specific mechanism — not just "Spark runs queries" but "here is exactly why this query is slow and how to fix it."

**Prerequisites**: Docker, Python 3.8+, ~8 GB free RAM

---

## Lab Setup

```bash
# Start local Spark (using PySpark)
pip install pyspark==3.5.0

# Or use Docker for a more realistic environment
docker run -it --rm \
  -p 4040:4040 \
  -v $(pwd)/labs:/opt/labs \
  bitnami/spark:3.5 \
  spark-submit /opt/labs/lab_shuffle.py
```

---

## Lab 1: Observing the Shuffle

**Goal**: see how GROUP BY creates a shuffle and how it shows in the Spark UI.

```python
# lab_shuffle.py
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import random

spark = SparkSession.builder \
    .appName("Lab1-Shuffle") \
    .config("spark.ui.enabled", "true") \
    .config("spark.sql.shuffle.partitions", "20") \
    .getOrCreate()

# Generate 1M events with uniform distribution
data = [(f"customer_{i % 100}", f"service_{i % 10}", random.randint(10, 500))
        for i in range(1_000_000)]

events = spark.createDataFrame(data, ["customer_id", "service", "latency_ms"])
events.cache().count()  # materialize

# This triggers a shuffle
result = events.groupBy("service").agg(
    F.count("*").alias("count"),
    F.avg("latency_ms").alias("avg_latency")
)
result.write.mode("overwrite").parquet("/tmp/lab1_output")

print("Open http://localhost:4040 and examine the shuffle bytes in the stages")
spark.stop()
```

**What to look for in the Spark UI**:
- Go to "Stages" tab
- Note the "Shuffle Write" and "Shuffle Read" columns
- Click on a stage to see task duration distribution

---

## Lab 2: Data Skew

**Goal**: observe how skewed data causes one task to dominate execution time.

```python
# lab_skew.py
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import random

spark = SparkSession.builder \
    .appName("Lab2-Skew") \
    .config("spark.sql.shuffle.partitions", "20") \
    .getOrCreate()

# SKEWED: customer_1 has 80% of events
data = []
for i in range(1_000_000):
    if random.random() < 0.8:
        customer = "customer_1"
    else:
        customer = f"customer_{random.randint(2, 1000)}"
    data.append((customer, random.randint(10, 500)))

events = spark.createDataFrame(data, ["customer_id", "latency_ms"])
events.cache().count()

# Run and observe: one task will be 5-10x slower
result = events.groupBy("customer_id").agg(F.count("*"), F.sum("latency_ms"))
result.write.mode("overwrite").parquet("/tmp/lab2_skewed")

# Now fix with AQE
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
result2 = events.groupBy("customer_id").agg(F.count("*"), F.sum("latency_ms"))
result2.write.mode("overwrite").parquet("/tmp/lab2_fixed")

print("Compare task durations: skewed vs AQE-fixed")
spark.stop()
```

**Benchmark**: record total job duration for both runs. AQE should significantly reduce the straggler effect.

---

## Lab 3: Join Strategies

**Goal**: compare sort-merge join vs broadcast join performance.

```python
# lab_joins.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast
import time, random

spark = SparkSession.builder.appName("Lab3-Joins").getOrCreate()

# Large fact table: 5M orders
orders_data = [(i, f"customer_{i % 10000}", random.uniform(10, 1000))
               for i in range(5_000_000)]
orders = spark.createDataFrame(orders_data, ["order_id", "customer_id", "amount"])

# Small dimension table: 10K customers
customers_data = [(f"customer_{i}", f"Segment {i % 5}")
                  for i in range(10000)]
customers = spark.createDataFrame(customers_data, ["customer_id", "segment"])

orders.cache().count()
customers.cache().count()

# Test 1: Sort-merge join (forces full shuffle of orders)
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")  # disable auto-broadcast
t0 = time.time()
result1 = orders.join(customers, "customer_id")
result1.count()
print(f"Sort-merge join: {time.time() - t0:.2f}s")

# Test 2: Broadcast join
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", str(50 * 1024 * 1024))
t0 = time.time()
result2 = orders.join(broadcast(customers), "customer_id")
result2.count()
print(f"Broadcast join: {time.time() - t0:.2f}s")

spark.stop()
```

**Expected result**: broadcast join should be 3–10× faster due to eliminated shuffle.

---

## Lab 4: Partition Count Impact

**Goal**: observe how wrong partition count affects performance.

```python
# lab_partitions.py
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import time

spark = SparkSession.builder.appName("Lab4-Partitions").getOrCreate()

data = [(f"customer_{i}", i % 50, i * 0.1) for i in range(2_000_000)]
events = spark.createDataFrame(data, ["customer_id", "service_id", "latency"])
events.cache().count()

for n_partitions in [10, 50, 200, 1000]:
    spark.conf.set("spark.sql.shuffle.partitions", str(n_partitions))
    t0 = time.time()
    result = events.groupBy("customer_id").agg(F.avg("latency"), F.count("*"))
    result.write.mode("overwrite").parquet(f"/tmp/lab4_{n_partitions}")
    print(f"Partitions={n_partitions}: {time.time() - t0:.2f}s")

spark.stop()
```

**What to observe**: very low partition count → tasks take too long; very high → scheduling overhead dominates.
