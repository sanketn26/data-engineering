"""Verify the cardinality prediction objectively via Prometheus's own head series count.

Queries Prometheus's HTTP API for `prometheus_tsdb_head_series` — the number
of distinct time series currently held in the TSDB head block. This is the
same metric the README asks you to watch by hand at http://localhost:9090/graph.

This script cannot change the exporter's cardinality itself (that requires
restarting the exporter container with a different USER_IDS environment
variable — see the README's "Break" section). It only reads the *current*
head series count and checks it against an expected range you supply,
because the "low-cardinality baseline" and "high-cardinality break" are two
different runs of the stack, not two branches of one script.

Usage:
    python check_cardinality.py --expect-min 400 --expect-max 1200
    python check_cardinality.py --expect-min 200000 --expect-max 700000
"""

import argparse
import json
import urllib.error
import urllib.parse
import urllib.request

PROMETHEUS_URL = "http://localhost:9090"
QUERY = "prometheus_tsdb_head_series"


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--expect-min",
        type=float,
        required=True,
        help="minimum acceptable prometheus_tsdb_head_series value",
    )
    parser.add_argument(
        "--expect-max",
        type=float,
        required=True,
        help="maximum acceptable prometheus_tsdb_head_series value",
    )
    return parser.parse_args()


def query_head_series():
    url = f"{PROMETHEUS_URL}/api/v1/query?" + urllib.parse.urlencode({"query": QUERY})
    try:
        response = urllib.request.urlopen(url, timeout=10)
    except urllib.error.URLError as error:
        raise RuntimeError(
            f"could not reach Prometheus at {PROMETHEUS_URL} — is `docker compose up -d` "
            f"running and has it had a few seconds to scrape? ({error})"
        ) from error

    payload = json.loads(response.read().decode())
    if payload.get("status") != "success":
        raise RuntimeError(f"Prometheus query did not succeed: {payload}")

    result = payload["data"]["result"]
    if not result:
        raise RuntimeError(
            "prometheus_tsdb_head_series returned no data — Prometheus may not have "
            "completed its first scrape yet; wait a few seconds and retry"
        )

    # Instant vector query: [{"metric": {...}, "value": [timestamp, "N"]}]
    _, raw_value = result[0]["value"]
    return float(raw_value)


def main():
    args = parse_args()
    if args.expect_min > args.expect_max:
        raise SystemExit("--expect-min must be <= --expect-max")

    head_series = query_head_series()
    print(f"prometheus_tsdb_head_series = {head_series:,.0f}")

    # Unconditional sanity check: Prometheus is actually scraping something,
    # independent of whichever range this run expects.
    if head_series <= 0:
        raise AssertionError(
            "prometheus_tsdb_head_series is 0 — Prometheus is not scraping the exporter "
            "at all (check `docker compose ps`, the exporter logs, and prometheus.yml)"
        )

    if not (args.expect_min <= head_series <= args.expect_max):
        raise AssertionError(
            f"prometheus_tsdb_head_series={head_series:,.0f} is outside the expected range "
            f"[{args.expect_min:,.0f}, {args.expect_max:,.0f}] — if you just changed USER_IDS, "
            "give Prometheus a few scrape intervals (5s each) to pick up the new series, or "
            "double check the --expect-min/--expect-max you passed match the scenario you ran"
        )

    print(
        f"PASS: head series count ({head_series:,.0f}) falls inside "
        f"[{args.expect_min:,.0f}, {args.expect_max:,.0f}] — this is the real, measured cost "
        "of the label combinations the exporter is currently producing, not a theoretical "
        "product-of-bounds estimate. If USER_IDS was raised, the jump you just saw is exactly "
        "the 'one unbounded label multiplies every other dimension' arithmetic from "
        "docs/time-series/cardinality.md and the cardinality calculator."
    )


if __name__ == "__main__":
    main()
