"""Verify the wide-partition prediction objectively via Cassandra's own counts.

events_by_service partitions on the low-cardinality `service` column, so
every row for `service='api-gateway'` lives in exactly one partition, on
exactly one set of replica nodes, forever. events_by_customer_day partitions
on `(customer_id, day_bucket)`, so the same hot customer's rows are bounded
to one partition per day instead of accumulating without limit.

Both queries below filter on a full partition key (an equality match on
every column in the partition key), so neither needs ALLOW FILTERING and
neither is a scatter-gather scan -- these are exactly the "know your
partition key" reads Cassandra is built to make fast. The point of this
check is not that the queries are slow; it is that one of the two
partitions they hit is unboundedly large and the other is not.

Usage: python check_wide_partition.py [--dominance-threshold 0.5]
"""

import argparse

from cassandra.cluster import Cluster

# Must match load_events.py exactly -- that script buckets every row into
# this single synthetic day so the query below is deterministic.
DAY_BUCKET = "2026-01-15"
HOT_SERVICE = "api-gateway"
HOT_CUSTOMER = "cust_0042"


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--dominance-threshold",
        type=float,
        default=0.5,
        help="fraction of all events_by_service rows the hot service's single "
        "partition must hold to PASS",
    )
    return parser.parse_args()


def main():
    args = parse_args()

    cluster = Cluster(["127.0.0.1"], port=9042)
    session = cluster.connect("academy")

    # Full partition-key equality: fast, single-partition read.
    hot_service_row = session.execute(
        "SELECT count(*) FROM events_by_service WHERE service = %s",
        (HOT_SERVICE,),
    ).one()
    hot_service_count = hot_service_row.count

    # Full-table scan. NOT how you'd query this in production (it visits
    # every partition on every node) -- it is only here so the check script
    # can independently learn the total row count without trusting a
    # command-line flag the loader was run with.
    total_row = session.execute("SELECT count(*) FROM events_by_service").one()
    total_count = total_row.count

    # Full partition-key equality on the bounded design: one customer, one
    # day. This is the same query shape as above, but the partition it hits
    # cannot grow past one day's worth of that customer's traffic.
    hot_bucket_row = session.execute(
        "SELECT count(*) FROM events_by_customer_day WHERE customer_id = %s AND day_bucket = %s",
        (HOT_CUSTOMER, DAY_BUCKET),
    ).one()
    hot_bucket_count = hot_bucket_row.count

    # How many distinct day_bucket partitions exist for the hot customer.
    # load_events.py only ever writes DAY_BUCKET, so this should be 1 -- the
    # bucketed design bounds this partition's growth to one day; loading
    # events that spanned N calendar days would automatically split this
    # customer's traffic across N partitions instead of piling into one.
    #
    # This filters on only HALF the partition key (customer_id, without
    # day_bucket), so Cassandra requires ALLOW FILTERING -- same category as
    # the full-table count(*) above: a diagnostic-only query this check
    # script needs, not a production access pattern. In production you
    # would always supply both partition-key components together.
    bucket_rows = session.execute(
        "SELECT DISTINCT customer_id, day_bucket FROM events_by_customer_day "
        "WHERE customer_id = %s ALLOW FILTERING",
        (HOT_CUSTOMER,),
    )
    distinct_buckets = {row.day_bucket for row in bucket_rows}

    session.shutdown()
    cluster.shutdown()

    print(f"events_by_service total rows (full table):        {total_count:,}")
    print(f"events_by_service WHERE service='{HOT_SERVICE}':   {hot_service_count:,}")
    print(f"events_by_customer_day WHERE customer_id='{HOT_CUSTOMER}' AND day_bucket='{DAY_BUCKET}': {hot_bucket_count:,}")
    print(f"distinct day_bucket partitions for {HOT_CUSTOMER}: {len(distinct_buckets)} ({sorted(distinct_buckets)})")

    if total_count == 0:
        raise AssertionError(
            "events_by_service is empty -- has schema.cql been applied and "
            "load_events.py been run?"
        )

    hot_share = hot_service_count / total_count
    print(f"the '{HOT_SERVICE}' partition holds {hot_share:.2%} of all rows in events_by_service")

    if hot_share < args.dominance_threshold:
        raise AssertionError(
            f"expected the hot service's single partition to hold a large majority "
            f"(>= {args.dominance_threshold:.0%}) of all rows, but it only held "
            f"{hot_share:.2%} -- check that load_events.py was run with a high enough "
            "--hot-ratio, or that HOT_SERVICE here matches the loader's hot service"
        )

    if len(distinct_buckets) != 1 or DAY_BUCKET not in distinct_buckets:
        raise AssertionError(
            f"expected the hot customer's rows to live in exactly one day_bucket "
            f"('{DAY_BUCKET}'), found {sorted(distinct_buckets)} -- did load_events.py "
            "change its DAY_BUCKET constant without this script being updated to match?"
        )

    print(
        "PASS: events_by_service has ONE partition key value ('service') absorbing "
        f"{hot_share:.2%} of every row ever written -- that partition only ever grows, "
        "and every read, repair, and compaction against it gets more expensive as it "
        "does. events_by_customer_day puts the same hot customer's traffic behind a "
        "compound key (customer_id, day_bucket): today's rows are bounded to today's "
        "partition, and a new day starts a fresh one. Same data, same query shape "
        "(equality on the full partition key), structurally different growth curve."
    )


if __name__ == "__main__":
    main()
