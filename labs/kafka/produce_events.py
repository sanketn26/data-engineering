"""Produce the academy event shape with a configurable hot-key ratio."""

import argparse
import json
import random
import time

from kafka import KafkaProducer


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--topic", default="user-events")
    parser.add_argument("--count", type=int, default=10_000)
    parser.add_argument("--hot-ratio", type=float, default=0.0)
    return parser.parse_args()


def main():
    args = parse_args()
    if not 0 <= args.hot_ratio <= 1:
        raise SystemExit("--hot-ratio must be between 0 and 1")

    producer = KafkaProducer(
        bootstrap_servers=["localhost:9092"],
        key_serializer=lambda value: value.encode(),
        value_serializer=lambda value: json.dumps(value).encode(),
    )
    services = ["api-gateway", "auth", "billing", "search"]
    customers = [f"cust_{index:04d}" for index in range(100)]

    for index in range(args.count):
        customer = (
            "cust_0042"
            if random.random() < args.hot_ratio
            else random.choice(customers)
        )
        producer.send(
            args.topic,
            key=customer,
            value={
                "event_id": f"lab-{time.time_ns()}-{index}",
                "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                "customer_id": customer,
                "user_id": f"user_{random.randint(1, 10_000)}",
                "service": random.choice(services),
                "endpoint": "/v2/events",
                "region": random.choice(["eu-west-1", "us-east-1"]),
                "latency_ms": random.randint(10, 400),
                "status_code": random.choice([200, 200, 200, 200, 500]),
                "bytes": 1024,
            },
        )
    producer.flush()
    producer.close()
    print(f"produced={args.count} hot_ratio={args.hot_ratio:.2f}")


if __name__ == "__main__":
    main()
