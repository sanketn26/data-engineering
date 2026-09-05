"""Deterministically trigger ClickHouse's too-many-parts protection."""

import base64
import urllib.error
import urllib.parse
import urllib.request


AUTHORIZATION = "Basic " + base64.b64encode(b"academy:academy").decode()


def execute(query):
    url = "http://localhost:8123/?query=" + urllib.parse.quote(query)
    request = urllib.request.Request(url, data=b"", method="POST")
    request.add_header("Authorization", AUTHORIZATION)
    return urllib.request.urlopen(request, timeout=30).read().decode()


def main():
    execute("TRUNCATE TABLE tiny_inserts")
    execute("SYSTEM STOP MERGES tiny_inserts")
    rejected = None
    try:
        for index in range(100):
            try:
                execute(
                    "INSERT INTO tiny_inserts VALUES "
                    f"(now(), 'cust_0042', {index})"
                )
            except urllib.error.HTTPError as error:
                detail = error.read().decode("utf-8", errors="replace")
                if "Too many parts" not in detail:
                    raise RuntimeError(detail) from error
                rejected = index
                print(f"observed protection after {index} tiny inserts: Too many parts")
                break
    finally:
        execute("SYSTEM START MERGES tiny_inserts")

    if rejected is None:
        raise AssertionError("expected too-many-parts protection was not observed")
    print("PASS: batching is required; exact rejection count is intentionally not assumed")


if __name__ == "__main__":
    main()
