# Contributing

Quality gate:

> A module is complete when it is technically reviewed, starts from a workload (not a dictionary definition), covers internals, names failure and production caveats, has an exercise or incident, and links related labs or simulations. If not, it is not done.

Never mark a stub complete. Do not leave “Coming soon” on a page that is linked from the nav.

## Teaching loop

Follow the five-act contract in [docs/start-here.md](docs/start-here.md):

PROBLEM → MODEL → MECHANISM → PRODUCTION → ASSESSMENT

Every substantial lesson also states prerequisites, 2–4 observable outcomes, estimated time, and a next step. Use only the production subsections that the topic needs; do not manufacture fifteen repetitive headings.

Assume the reader already knows Python, SQL, Docker, and Git.

## Local check

```bash
pip install -r requirements.txt
mkdocs build --strict
```

PRs run `.github/workflows/validate.yml`. Merges to `main` deploy Pages.
