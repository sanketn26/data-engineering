"""Load deterministic synthetic events into both Cassandra partition-key designs.

Every row is written to BOTH `events_by_service` (partitioned on the
low-cardinality `service` column) and `events_by_customer_day` (partitioned
on `(customer_id, day_bucket)`). Loading identical rows into both tables
means the only variable between them is the partition key design -- exactly
what check_wide_partition.py measures.
"""

import argparse
import random
import uuid
from datetime import datetime, timedelta, timezone

from cassandra.cluster import Cluster
from cassandra.concurrent import execute_concurrent_with_args

# Fixed synthetic day (not "now") so events_by_customer_day always buckets
# every row into exactly ONE deterministic partition per customer, no matter
# when this script happens to run. check_wide_partition.py uses this same
# constant to query that bucket directly.
DAY_BUCKET = "2026-01-15"
BASE_TIMESTAMP = datetime(2026, 1, 15, 0, 0, 0, tzinfo=timezone.utc)
SPREAD_SECONDS = 20 * 3600  # stay within the same calendar day as BASE_TIMESTAMP

SERVICES = ["api-gateway", "auth", "billing", "search"]
CUSTOMERS = [f"cust_{index:04d}" for index in range(100)]
HOT_SERVICE = "api-gateway"
HOT_CUSTOMER = "cust_0042"

INSERT_BY_SERVICE = """
INSERT INTO academy.events_by_service
    (service, event_id, timestamp, customer_id, user_id, endpoint, region, latency_ms, status_code, bytes)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
"""

INSERT_BY_CUSTOMER_DAY = """
INSERT INTO academy.events_by_customer_day
    (customer_id, day_bucket, event_id, timestamp, service, user_id, endpoint, region, latency_ms, status_code, bytes)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
"""


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--rows", type=int, default=200_000)
    parser.add_argument("--hot-ratio", type=float, default=0.8)
    parser.add_argument("--concurrency", type=int, default=100)
    return parser.parse_args()


def main():
    args = parse_args()
    if not 0 <= args.hot_ratio <= 1:
        raise SystemExit("--hot-ratio must be between 0 and 1")

    random.seed(42)
    cluster = Cluster(["127.0.0.1"], port=9042)
    session = cluster.connect()

    service_stmt = session.prepare(INSERT_BY_SERVICE)
    customer_day_stmt = session.prepare(INSERT_BY_CUSTOMER_DAY)

    service_params = []
    customer_day_params = []
    for index in range(args.rows):
        hot = random.random() < args.hot_ratio
        service = HOT_SERVICE if hot else random.choice(SERVICES)
        customer = HOT_CUSTOMER if hot else random.choice(CUSTOMERS)
        timestamp = BASE_TIMESTAMP + timedelta(seconds=random.randint(0, SPREAD_SECONDS))
        event_id = uuid.uuid4()
        user_id = f"user_{index}"
        endpoint = "/v2/events"
        region = "eu-west-1"
        latency_ms = random.randint(10, 400)
        status_code = 200
        num_bytes = 1024

        service_params.append((
            service, event_id, timestamp, customer, user_id, endpoint, region,
            latency_ms, status_code, num_bytes,
        ))
        customer_day_params.append((
            customer, DAY_BUCKET, event_id, timestamp, service, user_id, endpoint,
            region, latency_ms, status_code, num_bytes,
        ))

    # execute_concurrent_with_args raises on the first failure by default,
    # which is what we want for a lab loader: fail loudly, don't limp along
    # with a partially-loaded dataset that would make the check script lie.
    execute_concurrent_with_args(session, service_stmt, service_params, concurrency=args.concurrency)
    execute_concurrent_with_args(session, customer_day_stmt, customer_day_params, concurrency=args.concurrency)

    session.shutdown()
    cluster.shutdown()
    print(f"PASS: loaded {args.rows:,} rows into both tables")


if __name__ == "__main__":
    main()
