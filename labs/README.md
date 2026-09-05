# Labs

Hands-on environments for the Data Engineering Academy. Documentation index: [`docs/labs/index.md`](../docs/labs/index.md).

| Directory | Stack | Failure you should see |
|-----------|--------|------------------------|
| [kafka/](kafka/) | Apache Kafka 3.7 KRaft, one broker | Hot partition lag |
| [spark/](spark/) | Local PySpark | Skewed shuffle / join |
| [flink/](flink/) | PyFlink + optional Compose UI | Stalled watermarks, no window output |
| [clickhouse/](clickhouse/) | Official ClickHouse | Wrong `ORDER BY`; too many parts |
| [cassandra/](cassandra/) | Official Cassandra 4.1, one node | Unbounded wide partition on a low-cardinality key |
| [time-series/](time-series/) | Synthetic exporter + official Prometheus | Series-count explosion from one high-cardinality label |

**Loop:** predict → run → break → explain. Do not skip the break step.

```bash
# Kafka
cd labs/kafka && docker compose up -d
pip install -r requirements.txt
python produce_events.py
python check_hot_partition.py    # objective pass/fail on the hot-key prediction

# ClickHouse
cd labs/clickhouse && docker compose up -d
docker exec -i dea-clickhouse clickhouse-client --user academy --password academy --multiquery < schema.sql
python load_events.py
python break_parts.py
python check_order_by.py         # objective pass/fail on the ORDER BY prediction

# Spark (no Compose)
cd labs/spark && pip install -r requirements.txt
python run_lab.py --mode uniform
python check_skew.py             # objective pass/fail on the skew prediction

cd labs/flink && python stalled_watermark.py   # self-asserting, prints PASS/FAIL
python check_event_time.py                     # objective pass/fail on event-time vs processing-time

# Cassandra
cd labs/cassandra && docker compose up -d      # slower boot than the others: 30-60s
pip install --only-binary=:all: cassandra-driver
docker exec -i dea-cassandra cqlsh < schema.cql
python load_events.py
python check_wide_partition.py   # objective pass/fail on the wide-partition prediction

# Time-series (Prometheus cardinality)
cd labs/time-series && docker compose up -d --build
python3 check_cardinality.py --expect-min 400 --expect-max 1200   # baseline
USER_IDS=10000 docker compose up -d --build exporter
python3 check_cardinality.py --expect-min 200000 --expect-max 700000   # after raising cardinality
```

Each `check_*.py` script is a **verification**, not a demo: it makes the same claim the README asks you to predict, runs it against the real system, and either prints `PASS` with the measured numbers or raises an `AssertionError` naming exactly what didn't hold. Don't treat "the command ran" as passing — read the assertion.

Shared event shape (SaaS analytics / System A): `timestamp`, `customer_id`, `user_id`, `service`, `endpoint`, `region`, `latency_ms`, `status_code`, `bytes`. The whale key is `cust_0042`.
