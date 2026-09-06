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
    if len(files) != 5:
        raise AssertionError(f"expected 5 Compose files, found {len(files)}")
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


def check_practice_integration():
    docs = ROOT / "docs"
    practice_map = (docs / "practice-map.md").read_text()
    simulation_index = docs / "simulations" / "index.md"
    simulations = sorted((docs / "simulations").glob("*.html"))
    lesson_sources = sorted(docs.rglob("*.md"))
    lab_readmes = sorted((ROOT / "labs").glob("*/README.md"))

    for simulation in simulations:
        if simulation.name not in practice_map:
            raise AssertionError(f"simulation missing from practice map: {simulation.name}")

        contextual_sources = [
            source
            for source in lesson_sources + lab_readmes
            if source not in (simulation_index, docs / "practice-map.md")
            and simulation.name in source.read_text()
        ]
        if not contextual_sources:
            raise AssertionError(
                f"simulation has no concept or lab handoff: {simulation.name}"
            )

    for readme in lab_readmes:
        text = readme.read_text()
        required_markers = (
            "## Before you run",
            "../../docs/simulations/",
            "../../docs/practice-map.md",
            "## Predict",
            "## Check your work",
        )
        missing = [marker for marker in required_markers if marker not in text]
        if missing:
            raise AssertionError(
                f"lab is not connected to the learning loop: {readme} ({missing})"
            )

    practice_sections = sum(
        "## Practice the idea" in source.read_text() for source in lesson_sources
    )
    if practice_sections < len(simulations):
        raise AssertionError(
            f"expected at least {len(simulations)} contextual practice sections, "
            f"found {practice_sections}"
        )

    return len(simulations) + len(lab_readmes)


def main():
    checks = [
        ("lab Python files", check_python),
        ("Compose files", check_compose),
        ("JavaScript assets", check_javascript),
        ("practice integrations", check_practice_integration),
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
