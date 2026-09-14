# Implementation plan: bind attention with storytelling

Status: proposed
Branch: `plan/storytelling`
Audience: authors of this academy, not students
Do not add this file to `mkdocs.yml`. It is not curriculum.

## Problem

Reader feedback: the content is good; it does not hold attention.

The course already has incident *hooks* (timestamp, pager, design review) on
most lessons, a five-act teaching loop, five running systems, and
[SaaSCo: The Evolving Company](../docs/architectures/saasco-evolution.md) as a
spine. What it does not have is storytelling.

A typical lesson today:

1. A strong cold open with no named person.
2. A predict-before-you-read quiz.
3. Immediate switch to tables, mermaid, and `Under the hood`.
4. A mid-page roll call of observability / e-commerce / IoT / fraud.
5. No return to the opening scene. Nobody did anything. Nothing happened next.

The first screen is cinematic. The rest is a Staff-engineer briefing. Readers
bounce on screen two.

This plan does **not** thin internals, add jokes, or turn the academy into a
fable. It makes the scene the spine of a page that is already technically
complete.

## Goal

A reader who starts [Data at Scale](../docs/foundations/scale.md) can say, an
hour later, what happened to Maya, why Acme (`cust_0042`) caused it, and which
SaaSCo stage they are in — and still be able to do the arithmetic.

Non-goals:

- Do not reduce production depth, config, failure modes, or exercises.
- Do not invent a second curriculum plot. SaaSCo is the plot.
- Do not delete the five systems. They become *costumes* (same physics,
  different access pattern), not five parallel novels.
- Do not rewrite labs, simulations, or Compose environments except where a
  sentence of scene-setting helps. `cust_0042` stays the whale key.
- Do not put this plan, the cast bible, or author notes in the student nav
  as a separate "story" module. The story lives *inside* lessons.

## Key decisions

1. **One company.** SaaSCo is the only through-line. Start-here stops
   presenting five JSON schemas as "stories."
2. **A small named cast**, used everywhere the core curriculum needs a person.
   No "someone proposes," no "a VP pings," no "product wants."
3. **Acme (`cust_0042`) is the antagonist.** The whale tenant is a character,
   not a fixture in `load_events.py`.
4. **The opening scene must close.** Internals explain what the named person
   is looking at. Before the exercise, the page says what they did, what
   broke next, and which SaaSCo stage this is.
5. **At most one costume change per lesson.** "Fraud has the same physics" is
   a single aside, not a four-system coverage list in the middle of the scene.
6. **Voice stays dense.** Present tense, specific time, one stake, then the
   same engineering. Predict questions stay. Tables stay when they earn their
   keep.
7. **Specialised modules borrow the cast; they do not get a second novel.**
   Time series, graph, NoSQL, Ray are SaaSCo hitting a new access pattern
   (device agents, fraud graph, session store). The five architecture case
   studies keep their own stakes but still open and close on a named person.
8. **Do not hide SaaSCo until Phase 12.** The first hour should meet the
   company and the people. The evolution page remains the full timeline; it
   is no longer "read this after Phase 0."

## Cast bible

Use first names in lessons. Full names live only here and in a short
start-here section.

| Name | Role | Appears when |
|------|------|----------------|
| **Maya** (Maya Chen) | Data engineer, on-call for the events pipeline | Pager, Spark UI, Kafka lag, Flink watermarks, 3 AM pages |
| **Priya** (Priya Shah) | Product. Promised Acme a tenant dashboard by 07:00 | SLAs, "the chart is empty," design-review pressure |
| **Jordan** (Jordan Hale) | Platform lead | Design reviews, "do not add Kafka yet," key and `ORDER BY` decisions |
| **Elena** (Elena Voss) | VP Finance | Silent-wrong-number scenes: doubled GMV, ₹0 revenue, $40M vs $19M |
| **Acme** (`cust_0042`) | The enterprise tenant that 10×'d ingest | Skew, hot partitions, one reducer, noisy neighbour |

Event shape does not change:

```text
{timestamp, customer_id, user_id, service, endpoint, region, latency_ms, status_code, bytes}
```

Rules for using the cast:

- One scene, one primary person. Maya on a pager. Jordan in a review. Elena
  when the number is wrong. Priya when the SLA is political. Do not crowd
  four names into the opening paragraph.
- Acme can appear in any scene. They are the load, not the speaker.
- Do not give them backstories, hobbies, or dialogue longer than one line.
  They exist so the stake has a face.
- Other architecture pages (observability, IoT, e-commerce, fraud) may put
  Maya on loan ("Maya is sitting with fraud this quarter") rather than
  inventing a second on-call.

## Lesson contract

Keep the existing five-act loop from [start-here](../docs/start-here.md) and
[CONTRIBUTING.md](../CONTRIBUTING.md):

```text
PROBLEM → MODEL → MECHANISM → PRODUCTION → ASSESSMENT
```

Add two obligations that most pages currently skip:

```text
OPEN IN SCENE → stay in the scene through the mental picture
                → internals still earn their keep
                → CLOSE THE SCENE before the exercise
```

### Open in scene

Named person, specific clock time, one stake that can get worse if they are
wrong. Then the predict question.

Bad (today, compressed):

> 07:58 AM. The daily p95-by-customer job … just died at `read_parquet` …
> a single enterprise tenant signed, volume is 400 GB/day …

Good (same facts, a person left in the room):

> 07:58. Maya is still holding coffee. The p95 job that has finished by
> 07:00 every morning since January is dead at `read_parquet`. Priya is
> already in the channel: Acme’s CSMs were promised the tenant dashboard
> before standup. Acme signed six weeks ago. They are `cust_0042`. They
> are 38% of events. The script did not change. The company did.
>
> Before you open the cluster: is this RAM, disk format, or the deal itself?

### Stay in the scene

The mental-picture diagram is what Maya (or Jordan) is looking at. When a
mechanism is introduced, say what it does *in this incident* before giving
the general case.

Allowed: one short "same physics, different costume" aside.

Not allowed in the middle of the scene: a four-row table of observability /
e-commerce / IoT / fraud that exists only to prove coverage.

### Close the scene

A short section immediately before the exercise (or as the last prose before
it), titled consistently:

```markdown
## What happened next
```

It must answer three things:

1. What the named person did in *this* incident.
2. What breaks next week / at the next 10× (pointer, not a new lesson).
3. Which [SaaSCo stage](../docs/architectures/saasco-evolution.md) this is.

The exercise then asks the reader to be that person, not to recite a table.

### Do not

- Cut `Under the hood`, failure modes, or worked arithmetic to make room.
- Add humour, emojis, or cliffhanger marketing ("little did Maya know").
- Name people in reference pages that are genuinely dictionaries
  (`glossary.md`, `version-matrix.md`).
- Repeat the full Acme origin story on every page. One clause is enough
  after the first hour ("Acme, still 38% of the topic").

## Page definition of done

A substantial lesson is done with storytelling when:

- [ ] Opening names a person from the cast (or Acme) and a stake.
- [ ] The predict question is still there.
- [ ] Mental picture is framed as what that person is looking at.
- [ ] At most one costume-change aside. No five-system roll call mid-scene.
- [ ] Internals are not thinner than today (no deleted mechanisms, configs,
      or failure modes).
- [ ] `## What happened next` closes the scene before the exercise.
- [ ] SaaSCo stage is linked where the lesson sits on the timeline.
- [ ] `cust_0042` / Acme is used if the page already taught skew, hot keys,
      or noisy neighbours.

Index pages, labs, simulations, glossary, version matrix, and the capstone
rubric have a lighter bar: a named person in the opening is enough; they do
not all need a close-the-scene section.

## Voice

Keep the current engineering voice. Change only who is in the room.

| Keep | Drop |
|------|------|
| Present tense | Anonymous "someone" / "an engineer" |
| Predict-before-you-read | Coverage lists that break the scene |
| Numbers, bytes, SLAs | Character backstory |
| Tables that decide something | Tables that only prove we mentioned five systems |
| Second person for the *reader* ("before you read on") | Second person as the only character ("you own the pipeline") on pages that now have Maya |

"You" still addresses the reader. Maya does the job in the scene. The
exercise can switch to "you are Maya."

## Where the story lives in the product

| Surface | Change |
|---------|--------|
| [start-here.md](../docs/start-here.md) | Replace "Five stories you will keep revisiting" with "The company you work for": SaaSCo, the cast, Acme, the event shape. One paragraph that the other four systems are costumes you will meet later. |
| [how-to-study.md](../docs/how-to-study.md) | Pass 1 includes "leave able to say what happened in the scene." |
| [learning-paths.md](../docs/learning-paths.md) | Gentle first hour: meet the company on start-here, then scale → partition sim → partitions, still one session. |
| [saasco-evolution.md](../docs/architectures/saasco-evolution.md) | Same eight stages. Each stage opens on the cast (Maya's OOM, Priya's dashboard, Jordan refusing a box, Elena's ₹0 tile). Forcing functions stay measured. |
| [CONTRIBUTING.md](../CONTRIBUTING.md) | Storytelling contract next to the five-act loop, with a pointer at this plan. |
| Homepage [index.md](../docs/index.md) | One line under the existing "Five running systems" CTA: the through-line is one company, not five manuals. Do not turn the homepage into a story. |

SaaSCo stays in Phase 12 in the nav (it is still an architecture page). It
becomes *recommended early* from start-here and the first-hour path, not
moved in the sidebar.

## Phased pull requests

Each PR should be independently reviewable and mergeable. Do not batch the
whole academy. Prototype on the first hour, then roll forward in reading
order.

### PR 1 — Contract and front door

**Title:** Make SaaSCo and a named cast the front door of the academy

**Files:**

- `CONTRIBUTING.md`
- `docs/start-here.md`
- `docs/how-to-study.md`
- `docs/learning-paths.md`
- `docs/index.md` (one line, optional if the homepage already points at
  start-here)

**Work:**

- Add the storytelling contract (open in scene, stay, close, one costume
  change, do not thin internals) to CONTRIBUTING, next to the five-act loop.
- Rewrite start-here's "Five stories" into the company, the cast, Acme, and
  the event shape. Keep the JSON. Demote Systems B–E to a short "you will
  meet these later as the same physics in a different costume."
- Point the first-hour path at that section, then scale.
- Tell Pass 1 readers they should be able to retell the scene in one
  sentence.

**Done when:** a new reader meets Maya and Acme before they meet Spark.
`mkdocs build --strict` still passes.

### PR 2 — SaaSCo timeline gets people

**Title:** Put the cast on the SaaSCo stage timeline

**Depends on:** PR 1 (names are locked)

**Files:**

- `docs/architectures/saasco-evolution.md`
- `docs/architectures/index.md`

**Work:**

- Keep all eight stages, forcing functions, arithmetic, and the Stage 9
  deletion exercise.
- Open each stage on a person and a stake (Maya's laptop OOM, Priya's
  fraud-review queue, Jordan with three writers on one prefix, Elena's ₹0
  tile). The technical argument stays the body.
- Do not add a ninth stage. Do not add new components.

**Done when:** the evolution page reads as eight chapters of one company,
not eight status reports with a headcount column.

### PR 3 — Prototype the first hour

**Title:** Tell the first-hour lessons as one scene that continues

**Depends on:** PR 1

**Files (rewrite to the new contract, internals stay):**

- `docs/foundations/index.md`
- `docs/foundations/scale.md`
- `docs/foundations/partitions.md`
- `docs/kafka/log.md`
- `docs/spark/shuffle.md`

This is the proof PR. If these five pages still feel like manuals after the
rewrite, stop and fix the contract before rolling out.

**Work per page:** new opening, mental picture in-scene, strip mid-page
five-system roll calls to one aside, add `## What happened next`, keep every
mechanism and exercise.

Suggested continuity:

| Page | Scene | Close |
|------|-------|-------|
| Foundations index | Monday 09:02, Maya, 40 GB script vs 8 TB and Acme at 38% | This phase names the physics before any product |
| Scale | 07:58, Maya's OOM, Priya waiting on Acme | Bigger notebook vs new mechanism; Stage 1→2 |
| Partitions | 14:20, Jordan's review, partition by `region` | Key chosen, Acme still a hotspot; points at shuffle |
| Log | 10:04, Tuesday's 40-minute parser hole, "can we replay?" | Why a log, not a table; Stage 3 is waiting |
| Shuffle | 02:47, Maya, 199/200 tasks, Acme on one reducer | Salt / two-phase agg; this is the mechanism scale promised |

**Done when:** someone can read only these five openings + closes and retell
SaaSCo Stages 1–3 without opening the evolution page. Internals on each page
are intact.

### PR 4 — Rest of Phase 0–1

**Depends on:** PR 3 (voice is proven)

**Files:**

- `docs/foundations/data-movement.md`
- `docs/foundations/distributed-execution.md`
- `docs/foundations/backpressure.md`
- `docs/foundations/batch-vs-stream.md`
- `docs/foundations/parquet-internals.md` (almost no scene today — needs a
  hook, not just a file-anatomy tree)
- `docs/foundations/object-storage.md`
- `docs/foundations/data-modelling.md`
- `docs/foundations/data-contracts.md`
- `docs/foundations/transformation-engineering.md`
- `docs/foundations/cdc.md` (keep the 02:17 page; do not drop into
  "Contract" before the scene has a person)

### PR 5 — Kafka and Spark modules

**Files:** every page under `docs/kafka/` and `docs/spark/` except labs
(labs: opening sentence only).

Reuse the same incidents already on the page (billing lag, `NotEnoughReplicas`,
double charge, driver OOM). Name Maya / Jordan / Elena. Close the scene.
Keep gotchas as gotchas — do not novelise a checklist.

### PR 6 — Flink, Airflow, lakehouse, ClickHouse / Trino

**Files:** `docs/flink/` (except labs), `docs/airflow/`, `docs/lakehouse/`,
`docs/olap/`, `docs/query-engines/`.

These already have some of the best hooks in the course (silent Flink job,
₹0 Trino query, Grafana tile, `ORDER BY` that skips nothing). They need
people and a close, not new plots.

### PR 7 — Platform, quality, incidents, remaining architectures

**Files:**

- `docs/quality/index.md`
- `docs/metadata/index.md`
- `docs/security/index.md`
- `docs/notebooks/index.md`
- `docs/platform-delivery.md`
- `docs/incidents/index.md`
- `docs/architectures/analytics-platform.md`
- `docs/architectures/observability.md`
- `docs/architectures/ecommerce.md`
- `docs/architectures/iot.md`
- `docs/architectures/fraud.md`
- `docs/reference/selection-framework.md`
- `docs/reference/correctness-invariants.md`
- `docs/reference/cost-engineering.md`

Elena owns quality and correctness scenes. Jordan owns selection and
delivery. Incidents stay drills: name Maya in the alert line, keep
hypothesis-before-resolution.

### PR 8 — Specialised stores, comparisons, leftover indexes

**Files:** `docs/time-series/`, `docs/databases/`, `docs/graph/`,
`docs/distributed-python/`, `docs/comparisons/`, plus any module index still
on "someone proposes."

Comparisons: one named disagreement in a review ("Jordan: why not Structured
Streaming with a 1-second trigger?") then the same matrix. Do not pad.

Skip: `docs/reference/glossary.md`, `docs/reference/version-matrix.md`,
`docs/capstone.md` rubric mechanics, HTML simulations, lab Python.

## Suggested reading-order continuity (core path)

Use this so later PRs do not invent conflicting plots. Details can move; the
causality should not.

1. Acme signs. Volume 40 GB → 400 GB. Maya's pandas job OOMs. (scale)
2. Jordan's review wants `region` as the partition key. (partitions)
3. The job is distributed. Thirteen minutes are movement, not CPU.
   (data-movement, distributed-execution)
4. Five teams want the same events at five speeds. Replay after a parser
   bug. Kafka appears. (log, partitions, replication)
5. Acme hashes to one partition / one reducer. (kafka gotchas, shuffle)
6. Priya needs fraud flags while the card is still being used. Flink.
7. Nobody can say which of twelve jobs ran. Airflow.
8. Spark and Flink write the same prefix. Iceberg.
9. Acme's customer-facing chart must answer in milliseconds. ClickHouse.
10. DAG green, Elena's number wrong. Quality, contracts, lineage.

That is SaaSCo Stages 1–8 told as scenes. Specialised modules hang off it
(cardinality when someone adds `user_id` as a Prometheus label; graph when
fraud needs three hops in 200 ms).

## What we will measure

There is no analytics event for "attention." Use editorial checks:

- After PR 3, read the five prototype pages aloud. If you can skip from
  opening to close and still know what happened, the spine works.
- Spot-check that `Under the hood` word count did not drop.
- `mkdocs build --strict` on every PR.
- Grep for leftover anonymity that should have been replaced on touched
  files: `Someone proposes`, `an engineer asks`, `a VP pings`, `Product wants`
  at the start of a lesson (body text citing a generic stakeholder is fine).
- Grep that every substantial rewritten lesson has `## What happened next`.

Optional later: a short note on start-here asking readers who bounce to say
where. Do not block this work on instrumentation.

## Risks

| Risk | Mitigation |
|------|------------|
| Cast feels cute or patronising to Staff readers | First names only, no backstory, no dialogue scenes. One line of speech max. |
| Internals get cut "for pacing" | Definition of done forbids deleting mechanisms. Reviewers reject thinner pages. |
| Five systems feel demoted | Architecture pages stay. They are costumes, not deletions. Start-here says so. |
| Inconsistent names across PRs | This bible is the lock. Do not rename Maya mid-rollout. |
| SaaSCo-in-Phase-12 nav vs "read it first" | Link from start-here and first hour. Do not reshuffle the whole sidebar in PR 1. |
| Scope explosion | PR 3 is the gate. If the prototype is wrong, do not start PR 4. |

## Out of scope until the prototype lands

- New simulations or labs about the story.
- A fictional Slack export or multi-page "week in the life."
- Audio / video.
- Changing `cust_0042` in lab generators.
- Moving SaaSCo in `mkdocs.yml`.

## Implementation notes for whoever takes PR 3

Work page by page, not "add a character pass" across the repo.

For each prototype file:

1. Read the current opening and the current last section before the
   exercise.
2. Rewrite the opening in scene (see Scale example under Lesson contract).
3. Walk `Build the mental picture` and replace "consider a 10 TB dataset"
   with the same numbers attached to Maya / Jordan / Acme.
4. Find the five-system list. Collapse to one aside or move to the close.
5. Write `## What happened next` from facts already on the page. Do not
   invent a new incident.
6. Leave `Under the hood`, code, and the exercise items in place. Adjust
   exercise wording to "you are Maya" only where it helps.

Do not drive-by reformat, retitle sections, or "fix" adjacent pages in the
same PR.
