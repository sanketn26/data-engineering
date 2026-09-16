# Contributing

Quality gate:

> A module is complete when it is technically reviewed, starts from a workload (not a dictionary definition), covers internals, names failure and production caveats, has an exercise or incident, and links related labs or simulations. If not, it is not done.

Never mark a stub complete. Do not leave “Coming soon” on a page that is linked from the nav.

## Teaching loop

Follow the five-act contract in [docs/start-here.md](docs/start-here.md):

PROBLEM → MODEL → MECHANISM → PRODUCTION → ASSESSMENT

Every substantial lesson also states prerequisites, 2–4 observable outcomes, estimated time, and a next step. Use only the production subsections that the topic needs; do not manufacture fifteen repetitive headings.

Assume the reader already knows Python, SQL, Docker, and Git.

## Page shape

A lesson opens on a situation and closes on what happened to it. The reference
material sits after the exercise, not between the reader and the ending.

```text
opening scene (a clock, a stake, a predict question)
  → Build the mental picture
  → Under the hood / Put it to work
  → What happened next
  → Exercise
  → ## Reference
```

Rules:

- **Close the scene.** A substantial lesson ends its opening incident in
  `## What happened next { #what-happened-next }`, placed immediately before
  the exercise. Answer the page's own predict question, and build it only from
  facts already on the page — do not invent a second incident.
- **Report, do not moralise.** State what happened and what changed. Leave out
  "the real lesson is…", "that is the whole point", and "everything above is
  the lesson". Readers draw their own conclusions from the facts.
- **Reference goes last.** `How to investigate`, `Scale`, `Trade-offs`,
  `Alternatives`, and `How to apply` belong under `## Reference`, after the
  exercise. Move them; never delete them.
- **Do not thin internals to make room.** Mechanisms, configs, failure modes
  and worked arithmetic stay. A page that got shorter in `Under the hood` is a
  regression.
- **One costume change.** A single "same physics, different system" aside is
  fine. A four-row observability / e-commerce / IoT / fraud table in the middle
  of a scene is not.
- **Keep the heading ids.** `#use-case`, `#intuition`, `#internals`,
  `#practice-the-idea`, `#exercise` and `#what-happened-next` drive
  `docs/javascripts/lesson-guide.js`, and `scripts/validate_course.py` asserts
  the literal heading `## Practice the idea`. Do not retitle them in passing.

Naming: SaaSCo is the company the course follows, `cust_0042` ("Acme") is the
enterprise tenant at **38%** of events, and volume steps are
**40 GB → 400 GB → 4 TB**/day. Money is USD. See
[plans/storytelling.md](plans/storytelling.md) for the full cast and number
bible.

## Local check

```bash
pip install -r requirements.txt
mkdocs build --strict
```

PRs run `.github/workflows/validate.yml`. Merges to `main` deploy Pages.
