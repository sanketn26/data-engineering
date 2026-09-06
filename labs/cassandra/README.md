# Cassandra lab

One single-node Cassandra container. You will load the **same** SaaS events into two tables that differ only in partition key, then watch one design absorb an entire hot service into a single ever-growing partition while the other bounds it by day.

## Before you run

1. Read [Cassandra & ScyllaDB](../../docs/databases/cassandra.md) through **Build the mental picture**.
2. Add a node in the [consistent-hashing visualiser](../../docs/simulations/consistent-hashing-visualizer.html).
3. Predict why an evenly distributed token ring can still contain one enormous application partition.

The simulation shows node placement; this lab shows data-model skew. Keeping
those two layers separate is the lesson. See the [practice map](../../docs/practice-map.md)
for the full sequence.

## Prerequisites

- Docker Compose v2
- `pip install -r requirements.txt` (installs `cassandra-driver`). On very new Python versions the driver's source build can fail with a `tarfile`/`ez_setup` error — if that happens, force the prebuilt wheel instead: `pip install --only-binary=:all: cassandra-driver`.
- ~2 GB RAM for the JVM. Cassandra is slower to start than the other labs' containers -- **30-60s** before the healthcheck goes green, not the ~10s you're used to from ClickHouse or Kafka. Don't run `docker exec ... cqlsh` the instant `docker compose up -d` returns; wait for `docker compose ps` to show `healthy`.

```bash
cd labs/cassandra
docker compose up -d
docker compose ps   # wait for "healthy" -- this takes longer than the other labs
docker exec -i dea-cassandra cqlsh < schema.cql
python load_events.py --rows 200000
```

## Predict

Before you load anything, write down what you expect:

1. Both `events_by_service` (partitioned on `service`, 4 possible values) and `events_by_customer_day` (partitioned on `(customer_id, day_bucket)`) get the **same** rows, with 80% of traffic hitting `service='api-gateway'` and `customer_id='cust_0042'`. After loading, which table has the larger single partition?
2. `service` **is** the partition key of `events_by_service`. Is `SELECT count(*) FROM events_by_service WHERE service = 'api-gateway'` a fast single-partition read, or a slow scatter-gather scan?
3. As that one `api-gateway` partition grows into the tens of thousands of rows and beyond, what happens to read latency, compaction, and repair against it -- does it stay flat, or does it degrade with partition size?

## Run

```bash
python load_events.py --rows 200000 --hot-ratio 0.8
```

This inserts the same 200,000 synthetic events into both tables using `execute_concurrent_with_args` for reasonable throughput. 80% of rows use `service='api-gateway'` and `customer_id='cust_0042'` (the same whale key every other lab in this repo uses) so the hot key concentrates identically in both tables -- the only variable is which column(s) the partition key is built from.

Inspect partition sizes manually (the visual/manual path, before you trust the automated check):

```bash
docker exec -it dea-cassandra nodetool tablehistograms academy events_by_service
docker exec -it dea-cassandra nodetool tablehistograms academy events_by_customer_day
```

Look at the "Partition Size" column: `events_by_service` should show one dramatically larger max/99th-percentile partition size than `events_by_customer_day`, whose partitions are capped by `day_bucket`.

You can also query directly in `cqlsh`:

```bash
docker exec -it dea-cassandra cqlsh
```

```sql
-- Fast: equality on the full partition key. No ALLOW FILTERING needed
-- because `service` IS the partition key -- this is the query the table
-- was built for, and it is exactly what makes the partition dangerous.
SELECT count(*) FROM academy.events_by_service WHERE service = 'api-gateway';

-- Fast for the same reason: (customer_id, day_bucket) together are the
-- full partition key.
SELECT count(*) FROM academy.events_by_customer_day
WHERE customer_id = 'cust_0042' AND day_bucket = '2026-01-15';
```

**Compare to prediction 2.** Both queries above are fast single-partition reads -- Cassandra doesn't care that one of them is a bad idea at scale. It will happily let you build a partition that eventually cannot be compacted or repaired without pain, as long as you keep querying it with its own partition key.

## Check your work

```bash
python check_wide_partition.py
```

It queries `events_by_service` for the hot service's partition count and the table's total row count via Cassandra's own `count(*)`, then does the same for one day-bucket of the hot customer in `events_by_customer_day`, and asserts the property behind prediction 1: the `service`-partitioned table's single hot-service partition holds a large majority (by default at least 50%) of every row ever loaded, while the customer-day table's hot customer stays inside the one deterministic day bucket the loader used. It prints `PASS` with the measured counts, or raises an `AssertionError` naming exactly which expectation failed (usually: `--hot-ratio` wasn't high enough, or the schema/load steps weren't both run against a fresh keyspace).

## Break (optional, not required for the check to pass)

The automated check above already proves the structural point without needing to push Cassandra to its knees. If you want to see the real symptom, not just the row-count proxy for it:

```bash
python load_events.py --rows 1000000 --hot-ratio 0.98
docker exec -it dea-cassandra nodetool tablehistograms academy events_by_service
docker logs dea-cassandra 2>&1 | grep -i "large partition"
```

At a high enough row count and hot-ratio, `nodetool tablehistograms` will show a max partition size in `events_by_service` that dwarfs `events_by_customer_day`, and Cassandra's own logs may emit a "Compacting large partition" warning once that one partition's SSTable footprint crosses the configured threshold (`compaction_large_partition_warning_threshold_mb`, 100 MB by default). This is real Cassandra behavior worth reproducing once, but it is slow and resource-hungry on a laptop single-node container -- the committed check script proves the same mechanism deterministically and instantly.

## Clean up

```bash
docker compose down -v
```

## Notes

- The partition key **is** the query plan. There is no equivalent of a SQL `WHERE` on a non-key column without `ALLOW FILTERING`, and `ALLOW FILTERING` on a table this size means a full scatter-gather scan across every partition on every node -- forbidden on any hot path.
- Cassandra has no secondary index that makes an arbitrary predicate cheap the way a B-tree index would in Postgres. If you need to query by a different column, materialize a second table shaped for that query (as this lab does with two designs of the same data) -- don't reach for `CREATE INDEX`.
- Low-cardinality partition keys (`service`, `country`, `status`) are a standing invitation to build an unbounded partition. Bucketing time (or any other bounded dimension) into the partition key is not a schema nicety -- it is the mechanism that keeps compaction, repair, and read latency from degrading as data accumulates.
