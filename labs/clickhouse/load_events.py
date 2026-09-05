"""Load deterministic synthetic events into both ClickHouse physical designs."""

import argparse
import base64
import json
import random
import urllib.request
import urllib.error
import urllib.parse
from datetime import datetime, timedelta, timezone


def insert(table, rows):
    columns = [
        "timestamp", "customer_id", "user_id", "service", "endpoint",
        "region", "latency_ms", "status_code", "bytes",
    ]
    body = "\n".join(json.dumps(dict(zip(columns, row))) for row in rows).encode()
    query = urllib.parse.quote(f"INSERT INTO {table} FORMAT JSONEachRow")
    request = urllib.request.Request(
        f"http://localhost:8123/?query={query}", data=body, method="POST"
    )
    credentials = b"academy:academy"
    request.add_header("Authorization", "Basic " + base64.b64encode(credentials).decode())
    try:
        urllib.request.urlopen(request, timeout=30).read()
    except urllib.error.HTTPError as error:
        detail = error.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"ClickHouse HTTP {error.code}: {detail}") from error


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--rows", type=int, default=200_000)
    args = parser.parse_args()
    random.seed(42)
    base = datetime.now(timezone.utc) - timedelta(hours=2)
    rows = []
    for index in range(args.rows):
        timestamp = base + timedelta(seconds=random.randint(0, 7200))
        rows.append(
            (
                timestamp.strftime("%Y-%m-%d %H:%M:%S"),
                f"cust_{random.randint(0, 99):04d}",
                f"user_{index}",
                random.choice(["api-gateway", "auth", "billing"]),
                "/v2/events",
                "eu-west-1",
                random.randint(10, 400),
                200,
                1024,
            )
        )
    for start in range(0, len(rows), 10_000):
        chunk = rows[start : start + 10_000]
        insert("events_by_tenant", chunk)
        insert("events_by_time", chunk)
    print(f"PASS: loaded {len(rows)} rows into both tables")


if __name__ == "__main__":
    main()
