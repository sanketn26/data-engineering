"""Verify the ORDER BY prediction objectively via ClickHouse's own read_rows count.

Runs the same tenant-filtered dashboard query from the README against both
physical designs and reads the `read_rows` ClickHouse reports for each
(via the X-ClickHouse-Summary HTTP header, which is authoritative and does
not depend on waiting for system.query_log to flush). Asserts the property
you were asked to predict: the tenant-first ORDER BY reads far fewer rows
for a single-tenant filter than the time-first ORDER BY does.

Usage: python check_order_by.py
"""

import base64
import json
import urllib.error
import urllib.parse
import urllib.request

AUTHORIZATION = "Basic " + base64.b64encode(b"academy:academy").decode()

QUERY = """
SELECT count(), avg(latency_ms)
FROM {table}
WHERE customer_id = 'cust_0042'
  AND timestamp >= now() - INTERVAL 24 HOUR
"""
# A generous 24h window rather than the README's 15-minute dashboard window:
# load_events.py timestamps events relative to *load time*, not *check time*,
# so a tight window here would flake if you run this check any noticeable
# time after loading. The window is wide enough to safely contain everything
# load_events.py generates (it spreads rows over its own 2-hour span) while
# still validating the property that matters: does the tenant filter prune.


def run_query(table):
    query = QUERY.format(table=table)
    url = "http://localhost:8123/?" + urllib.parse.urlencode({"query": query})
    request = urllib.request.Request(url, data=b"", method="POST")
    request.add_header("Authorization", AUTHORIZATION)
    try:
        response = urllib.request.urlopen(request, timeout=30)
    except urllib.error.HTTPError as error:
        detail = error.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"query against {table} failed: {detail}") from error

    body = response.read().decode()
    summary_header = response.headers.get("X-ClickHouse-Summary")
    if not summary_header:
        raise RuntimeError(
            "no X-ClickHouse-Summary header returned — is the server version too old, "
            "or did the query fail silently? response body: " + body
        )
    summary = json.loads(summary_header)
    return int(summary["read_rows"]), int(summary["read_bytes"])


def main():
    tenant_read_rows, tenant_read_bytes = run_query("events_by_tenant")
    time_read_rows, time_read_bytes = run_query("events_by_time")

    print(f"events_by_tenant  (ORDER BY customer_id, timestamp): read_rows={tenant_read_rows:,} read_bytes={tenant_read_bytes:,}")
    print(f"events_by_time    (ORDER BY timestamp, customer_id): read_rows={time_read_rows:,} read_bytes={time_read_bytes:,}")

    if tenant_read_rows == 0 or time_read_rows == 0:
        raise AssertionError(
            "one of the queries read 0 rows — has schema.sql been applied and "
            "load_events.py been run against both tables?"
        )

    ratio = time_read_rows / tenant_read_rows
    print(f"events_by_time read {ratio:.1f}x more rows than events_by_tenant for the same tenant filter")

    if ratio < 2:
        raise AssertionError(
            f"expected the tenant-first table to read meaningfully fewer rows "
            f"(at least 2x less) than the time-first table for a single-tenant filter, "
            f"but the ratio was only {ratio:.1f}x — check that load_events.py used enough "
            "distinct customer_id values for the sparse index to actually skip granules"
        )

    print(
        "PASS: ORDER BY (customer_id, timestamp) puts this tenant's rows in a contiguous "
        "run the sparse primary index can seek to directly; ORDER BY (timestamp, customer_id) "
        "interleaves every tenant inside each time range, so a tenant filter cannot skip "
        "granules on that key alone — it degrades toward a full scan of the time window."
    )


if __name__ == "__main__":
    main()
