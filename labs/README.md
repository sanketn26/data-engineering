# Labs

Hands-on environments for the Data Engineering Academy. Documentation index: [`docs/labs/index.md`](../docs/labs/index.md).

| Directory | Stack | Failure you should see |
|-----------|--------|------------------------|
| [kafka/](kafka/) | Apache Kafka 3.7 KRaft, one broker | Hot partition lag |
| [spark/](spark/) | Local PySpark | Skewed shuffle / join |
| [flink/](flink/) | PyFlink + optional Compose UI | Stalled watermarks, no window output |
| [clickhouse/](clickhouse/) | Official ClickHouse | Wrong `ORDER BY`; too many parts |

**Loop:** predict → run → break → explain. Do not skip the break step.

```bash
# Kafka
cd labs/kafka && docker compose up -d
pip install -r requirements.txt
python produce_events.py

# ClickHouse
cd labs/clickhouse && docker compose up -d
docker exec -i dea-clickhouse clickhouse-client --user academy --password academy --multiquery < schema.sql
python load_events.py
python break_parts.py

# Spark (no Compose)
cd labs/spark && pip install -r requirements.txt
python run_lab.py --mode uniform

cd labs/flink && python stalled_watermark.py
```

Shared event shape (SaaS analytics / System A): `timestamp`, `customer_id`, `user_id`, `service`, `endpoint`, `region`, `latency_ms`, `status_code`, `bytes`. The whale key is `cust_0042`.
