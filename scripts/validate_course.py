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


def check_lesson_shape():
    """A lesson that closes its scene must do so before the exercise, and its
    reference material must sit after it. See CONTRIBUTING.md -> Page shape."""
    docs = ROOT / "docs"
    lessons = 0
    for path in sorted(docs.rglob("*.md")):
        source = path.read_text()
        headings = re.findall(r"(?m)^## [^\n]*$", source)
        has_reference = any(h.strip() == "## Reference" for h in headings)
        has_close = any("#what-happened-next" in h for h in headings)
        if not (has_reference or has_close):
            continue
        lessons += 1
        rel = path.relative_to(ROOT)

        exercise = [i for i, h in enumerate(headings) if "#exercise" in h]
        if has_reference and not exercise:
            raise AssertionError(f"{rel}: has a '## Reference' tail but no '{{ #exercise }}' heading")

        if has_close:
            if "## What happened next { #what-happened-next }" not in source:
                raise AssertionError(f"{rel}: close must be titled exactly '## What happened next {{ #what-happened-next }}'")
            close = [i for i, h in enumerate(headings) if "#what-happened-next" in h][0]
            # Case studies and comparisons have no exercise; the close simply ends the page.
            if exercise and close != exercise[0] - 1:
                raise AssertionError(
                    f"{rel}: 'What happened next' must be the section immediately "
                    f"before the exercise (found {close - exercise[0]} away)"
                )
            if not exercise and close != len(headings) - 1:
                raise AssertionError(
                    f"{rel}: with no exercise, 'What happened next' must be the final section"
                )

        exercise = exercise[0] if exercise else len(headings)

        if has_reference:
            reference = [i for i, h in enumerate(headings) if h.strip() == "## Reference"][0]
            if reference < exercise:
                raise AssertionError(f"{rel}: '## Reference' must come after the exercise")
            if not has_close:
                raise AssertionError(f"{rel}: has a '## Reference' tail but never closes its opening scene")
            stray = [
                h for h in headings[:exercise]
                if re.match(r"(?i)^## (scale\b|trade-?offs|alternatives|how to apply|apply\b)", h.strip())
                or "#debugging" in h
            ]
            if stray:
                raise AssertionError(f"{rel}: reference sections still before the exercise: {stray}")

    if lessons == 0:
        raise AssertionError("no lessons follow the page-shape contract")
    return lessons


def check_naming_locks():
    """One company, one whale tenant, one currency. See plans/storytelling.md."""
    docs = ROOT / "docs"
    banned = {
        "cust_1842": "the whale tenant is cust_0042",
        "BigCorp": "the enterprise tenant is Acme (cust_0042)",
        "bigcorp": "the enterprise tenant is Acme (cust_0042)",
        "\u20b9": "money in this academy is USD",
    }
    offenders = []
    for path in sorted(docs.rglob("*.md")):
        source = path.read_text()
        for token, why in banned.items():
            if token in source:
                offenders.append(f"{path.relative_to(ROOT)}: '{token}' ({why})")
    if offenders:
        raise AssertionError("; ".join(offenders[:5]) + (" ..." if len(offenders) > 5 else ""))
    return len(list(docs.rglob("*.md")))


def main():
    checks = [
        ("lab Python files", check_python),
        ("Compose files", check_compose),
        ("JavaScript assets", check_javascript),
        ("practice integrations", check_practice_integration),
        ("lesson page shape", check_lesson_shape),
        ("naming locks", check_naming_locks),
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
