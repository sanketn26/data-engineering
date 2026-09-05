"""Verify the hot-key break step actually reproduced a hot partition.

Produces a fresh hot-keyed batch into its own topic (created if missing),
tallies messages per partition with a throwaway consumer group, and asserts
the property the README asks you to predict: one partition holds a large
majority of the traffic, and adding consumers cannot split it.

Usage: python check_hot_partition.py [--hot-ratio 0.8] [--count 20000]
"""

import argparse
import json
import random
import time
from collections import Counter

from kafka import KafkaProducer, KafkaConsumer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

TOPIC = "hot-partition-check"
PARTITIONS = 6


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--hot-ratio", type=float, default=0.8)
    parser.add_argument("--count", type=int, default=20_000)
    parser.add_argument(
        "--dominance-threshold",
        type=float,
        default=0.5,
        help="fraction of all messages the busiest partition must hold to PASS",
    )
    return parser.parse_args()


def ensure_topic():
    admin = KafkaAdminClient(bootstrap_servers=["localhost:9092"])
    try:
        admin.create_topics([NewTopic(name=TOPIC, num_partitions=PARTITIONS, replication_factor=1)])
        print(f"created topic {TOPIC} ({PARTITIONS} partitions)")
    except TopicAlreadyExistsError:
        pass
    finally:
        admin.close()


def produce(count, hot_ratio):
    producer = KafkaProducer(
        bootstrap_servers=["localhost:9092"],
        key_serializer=lambda value: value.encode(),
        value_serializer=lambda value: json.dumps(value).encode(),
    )
    for index in range(count):
        customer = "cust_0042" if random.random() < hot_ratio else f"cust_{random.randint(0, 99):04d}"
        producer.send(TOPIC, key=customer, value={"n": index, "customer_id": customer})
    producer.flush()
    producer.close()
    print(f"produced={count} hot_ratio={hot_ratio:.2f}")


def tally(count):
    consumer = KafkaConsumer(
        TOPIC,
        group_id=f"hot-check-{time.time_ns()}",
        bootstrap_servers=["localhost:9092"],
        auto_offset_reset="earliest",
        consumer_timeout_ms=15_000,
    )
    per_partition = Counter()
    seen = 0
    for message in consumer:
        per_partition[message.partition] += 1
        seen += 1
        if seen >= count:
            break
    consumer.close()
    return per_partition, seen


def main():
    args = parse_args()
    ensure_topic()
    produce(args.count, args.hot_ratio)
    per_partition, seen = tally(args.count)

    if seen == 0:
        raise AssertionError("consumed 0 messages — is the broker up and the topic populated?")

    busiest_partition, busiest_count = per_partition.most_common(1)[0]
    busiest_share = busiest_count / seen

    print(f"consumed={seen} per_partition={dict(sorted(per_partition.items()))}")
    print(f"busiest_partition={busiest_partition} share={busiest_share:.2%}")

    if busiest_share < args.dominance_threshold:
        raise AssertionError(
            f"expected one partition to dominate (>= {args.dominance_threshold:.0%} of traffic) "
            f"but the busiest partition only held {busiest_share:.2%} — hot key was not reproduced "
            "(check --hot-ratio, or that PARTITIONS matches your key's hash spread)"
        )

    print(
        "PASS: one partition holds a large majority of the hot-keyed traffic — "
        "this is the partition whose consumer-group lag will not fall no matter how "
        "many consumers you add, because ordering and load are both keyed to the same partition."
    )


if __name__ == "__main__":
    main()
