"""Consume a bounded sample and report counts and offsets per partition."""

import argparse
import json
import time
from collections import Counter

from kafka import KafkaConsumer


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--topic", default="user-events")
    parser.add_argument("--group", default="lab-consumer")
    parser.add_argument("--max-records", type=int, default=10_000)
    parser.add_argument("--sleep-ms", type=float, default=0)
    return parser.parse_args()


def main():
    args = parse_args()
    consumer = KafkaConsumer(
        args.topic,
        group_id=args.group,
        bootstrap_servers=["localhost:9092"],
        value_deserializer=lambda value: json.loads(value.decode()),
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        consumer_timeout_ms=15_000,
    )
    counts = Counter()
    seen = 0
    for message in consumer:
        counts[message.partition] += 1
        seen += 1
        if args.sleep_ms:
            time.sleep(args.sleep_ms / 1000)
        if seen >= args.max_records:
            break
    consumer.commit()
    consumer.close()
    print(f"consumed={seen} partitions={dict(sorted(counts.items()))}")


if __name__ == "__main__":
    main()
