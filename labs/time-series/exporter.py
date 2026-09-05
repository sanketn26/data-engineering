"""Synthetic Prometheus exporter whose series cardinality is a dial, not a guess.

Exposes GET /metrics with a single counter, `http_requests_total`, in
Prometheus text exposition format. The exact number of distinct series is
the cross-product of four label domains, sized from environment variables:

    SERVICES      distinct `service` values      (default 4)
    REGIONS       distinct `region` values        (default 3)
    STATUS_CODES  distinct `status_code` values   (default 5)
    USER_IDS      distinct `user_id` values        (default 0 == label OFF)

With the defaults, every scrape emits 4 * 3 * 5 = 60 series. Setting
USER_IDS=10000 turns on a `user_id` label and multiplies the series count
by 10,000 — the same "one unbounded label multiplies every other dimension"
arithmetic as docs/time-series/cardinality.md and the cardinality calculator
sim. This is the knob the lab's README asks you to turn.

Values are generated fresh on every scrape (a cheap, deterministic-ish
counter derived from a per-series seed) rather than held in a giant dict,
so a 600,000-series scrape (4 x 3 x 5 x 10000) stays fast and memory-light.

Usage:
    python exporter.py
    SERVICES=4 REGIONS=3 STATUS_CODES=5 USER_IDS=10000 python exporter.py
"""

import itertools
import os
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

PORT = 9105

SERVICE_NAMES = ["api-gateway", "auth", "billing", "search", "checkout", "notify"]
REGION_NAMES = ["eu-west-1", "us-east-1", "ap-south-1", "us-west-2", "eu-central-1"]
STATUS_CODE_VALUES = [200, 201, 400, 404, 500, 503]

START = time.time()


def env_int(name, default):
    raw = os.environ.get(name)
    if raw is None or raw == "":
        return default
    return int(raw)


def label_domains():
    services = SERVICE_NAMES[: max(1, env_int("SERVICES", 4))]
    regions = REGION_NAMES[: max(1, env_int("REGIONS", 3))]
    status_codes = STATUS_CODE_VALUES[: max(1, env_int("STATUS_CODES", 5))]
    user_ids = env_int("USER_IDS", 0)
    return services, regions, status_codes, user_ids


def counter_value(seed, elapsed_seconds):
    # A cheap, deterministic-looking monotonic counter per series: it only
    # needs to look like real traffic, the cardinality is what this lab
    # measures, not the sample values.
    return int((seed % 997) * 1.0 + elapsed_seconds * ((seed % 7) + 1))


def render_metrics_lines():
    services, regions, status_codes, user_ids = label_domains()
    elapsed = time.time() - START

    yield "# HELP http_requests_total Synthetic request counter for the cardinality lab.\n"
    yield "# TYPE http_requests_total counter\n"

    if user_ids > 0:
        user_range = range(user_ids)
        for index, (service, region, status, user) in enumerate(
            itertools.product(services, regions, status_codes, user_range)
        ):
            value = counter_value(index, elapsed)
            yield (
                'http_requests_total{service="%s",region="%s",status_code="%s",user_id="user_%d"} %d\n'
                % (service, region, status, user, value)
            )
    else:
        for index, (service, region, status) in enumerate(
            itertools.product(services, regions, status_codes)
        ):
            value = counter_value(index, elapsed)
            yield (
                'http_requests_total{service="%s",region="%s",status_code="%s"} %d\n'
                % (service, region, status, value)
            )


class MetricsHandler(BaseHTTPRequestHandler):
    def log_message(self, format, *args):
        pass  # keep scrape logs quiet; the lab already prints cardinality math

    def do_GET(self):
        if self.path != "/metrics":
            self.send_response(404)
            self.end_headers()
            return

        self.send_response(200)
        self.send_header("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
        self.end_headers()

        chunk = []
        chunk_size = 2000
        for line in render_metrics_lines():
            chunk.append(line)
            if len(chunk) >= chunk_size:
                self.wfile.write("".join(chunk).encode("utf-8"))
                chunk = []
        if chunk:
            self.wfile.write("".join(chunk).encode("utf-8"))


def main():
    services, regions, status_codes, user_ids = label_domains()
    series = len(services) * len(regions) * len(status_codes) * max(user_ids, 1)
    print(
        f"exporter listening on :{PORT}/metrics "
        f"(services={len(services)} regions={len(regions)} status_codes={len(status_codes)} "
        f"user_ids={user_ids} -> {series:,} series per scrape)"
    )
    server = ThreadingHTTPServer(("0.0.0.0", PORT), MetricsHandler)
    server.serve_forever()


if __name__ == "__main__":
    main()
