"""Fast CI checks for executable course assets that do not require services."""

import ast
import pathlib
import re
import subprocess
import sys


ROOT = pathlib.Path(__file__).resolve().parents[1]


def check_python():
    files = sorted((ROOT / "labs").glob("**/*.py"))
    if not files:
        raise AssertionError("no committed lab Python files")
    for path in files:
        ast.parse(path.read_text(), filename=str(path))
    return len(files)


def check_compose():
    files = sorted((ROOT / "labs").glob("*/docker-compose.yml"))
    if len(files) != 3:
        raise AssertionError(f"expected 3 Compose files, found {len(files)}")
    for path in files:
        subprocess.run(
            ["docker", "compose", "-f", str(path), "config", "--quiet"],
            check=True,
        )
    return len(files)


def check_javascript():
    files = sorted((ROOT / "docs" / "simulations").glob("*.html"))
    for path in files:
        html = path.read_text()
        scripts = re.findall(r"<script>(.*?)</script>", html, re.DOTALL)
        for script in scripts:
            subprocess.run(
                ["node", "--check", "-"],
                input=script,
                text=True,
                check=True,
            )
    javascript = sorted((ROOT / "docs" / "javascripts").glob("*.js"))
    for path in javascript:
        subprocess.run(["node", "--check", str(path)], check=True)
    return len(files) + len(javascript)


def main():
    checks = [
        ("lab Python files", check_python),
        ("Compose files", check_compose),
        ("JavaScript assets", check_javascript),
    ]
    for label, check in checks:
        try:
            count = check()
        except (AssertionError, OSError, subprocess.CalledProcessError) as error:
            print(f"FAIL {label}: {error}", file=sys.stderr)
            raise SystemExit(1) from error
        print(f"PASS {label}: {count}")


if __name__ == "__main__":
    main()
