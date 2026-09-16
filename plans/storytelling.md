# Implementation plan: bind attention with storytelling

Status: partly landed — see *What actually shipped* below
Branch: `plan/storytelling`
Audience: authors of this academy, not students
Do not add this file to `mkdocs.yml`. It is not curriculum.

## What actually shipped (2026-09-16)

The first change landed ahead of the PR sequence below, because the sharper
diagnosis turned out to be structural rather than narrative: a core lesson ran
13–20 `##` sections with the last eight (`How to investigate`, `Scale`,
`Trade-offs`, `Alternatives`, `How to apply`) being interchangeable reference
material between the reader and the ending.

Landed across 47 lessons:

- Those reference sections moved **below the exercise**, under a plain
  `## Reference` heading. No text deleted — verified by token-set comparison
  against `HEAD` on all 47 files. Sections before the exercise drop to 8–10;
  first-pass length drops roughly 15–20%.
- The divider and the closes state what happened and stop there. They do not
  tell the reader what the lesson was ("the real lesson is…", "everything above
  is the lesson", "come back when you need it"). Readers draw their own.
- `## What happened next { #what-happened-next }` on **all 47** restructured
  lessons. Each answers its own page's predict question from facts already on
  that page.
- `lesson-guide.js` routes the first pass to the close, numbering computed at
  render time instead of hardcoded.

Landed since, covering PR 1 and most of PR 2:

- `CONTRIBUTING.md` carries the page-shape contract (close the scene, report
  don't moralise, reference last, don't thin internals, keep heading ids) plus
  the naming locks.
- `start-here.md`: "Five stories you will keep revisiting" is now "The company
  you work for" — SaaSCo, the four-person cast, Acme at 38%, the event shape.
  Systems B–E demoted to one costume table. The lesson shape now lists the
  close as step 5 of 6.
- The three first-hour surfaces agree: start-here's one-hour tip, learning-paths'
  gentle first hour, and how-to-study's Pass 1 all include the company section
  and `What happened next`. Pass 3 picks up `Reference`.
- `docs/index.md` and `README.md` each carry the one-line correction.
- `architectures/index.md` says eight stages plus the ninth as an exercise.
- All eight SaaSCo stages open on a named person. `₹` is gone repo-wide (USD).
- All 9 closes now name a person; the cast appears on 11 files, up from 3.

Also landed: the naming locks (`cust_1842`, `BigCorp`, `acme`, `₹` all gone;
`log.md` moved onto SaaSCo; `foundations/index.md` reconciled to the volume
ladder), named people on the PR 7 and PR 8 surfaces, and Elena's first lesson
scenes. The cast now appears on 40 files.

`scripts/validate_course.py` enforces the contract from here on: two new checks
fail CI if a lesson's close is missing or misplaced, if reference sections
drift back above the exercise, or if a banned name (`cust_1842`, `BigCorp`, `₹`)
reappears. `reading-time.js` reports the lesson estimate and the reference
estimate separately.

Still open: the seven PR 4 foundations lessons (backpressure, parquet-internals,
object-storage, data-modelling, data-contracts, transformation-engineering,
cdc), six module index pages, two reference pages, the five architecture case
studies, and the comparisons — none of which has a close yet. A close must be
built from facts already on its own page, so these stay hand-written, not
generated. Elena is on quality and IoT now but still the thinnest of the four
(4 files vs Maya's 21). The remaining editorial debt is listed above; the
per-page rosters in PRs 4–8 below are the source for it.

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

A reader who follows the first hour — [start-here](../docs/start-here.md), then
[Data at Scale](../docs/foundations/scale.md) through the mental picture *and*
`What happened next`, then the partition sim and
[Partitioning](../docs/foundations/partitions.md) — can say what happened to
Maya, why Acme (`cust_0042`) caused it, and which SaaSCo stage they are in, and
still be able to do the arithmetic.

Those three surfaces must agree: start-here's own "if you have only one hour"
tip, [learning-paths](../docs/learning-paths.md) "gentle first hour," and
[how-to-study](../docs/how-to-study.md) Pass 1. Do not send the one-hour reader
straight to scale while the company lives only on start-here.

Non-goals:

- Do not reduce production depth, config, failure modes, or exercises.
- Do not invent a second curriculum plot. SaaSCo is the plot.
- Do not delete the five systems. They become *costumes* (same physics,
  different access pattern), not five parallel novels.
- Do not rewrite labs, simulations, or Compose environments except where a
  sentence of scene-setting helps. `cust_0042` stays the whale key. Lab
  generators may keep the 80% hot-key exaggeration.
- Do not put this plan, the cast bible, or author notes in the student nav
  as a separate "story" module. The story lives *inside* lessons.
- Do not add a fifth named person. Security, notebooks, and Ray loan Jordan
  or Maya.

## Key decisions

1. **One company.** SaaSCo is the only through-line. Start-here stops
   presenting five JSON schemas as "stories."
2. **A small named cast**, used everywhere the core curriculum needs a person.
   No "someone proposes," no "a VP pings," no "product wants."
3. **Acme (`cust_0042`) is the antagonist.** The whale tenant is a character,
   not a fixture in `load_events.py`. Acme is not a second, smaller key.
4. **The opening scene must close, and first-pass readers must see the close.**
   Internals explain what the named person is looking at. Before the exercise,
   the page says what they did, what broke next, and which SaaSCo stage this
   is. Pass 1 is open → mental picture → skip internals → `What happened next`.
5. **At most one costume change per lesson.** "Fraud has the same physics" is
   a single aside, not a four-system coverage list in the middle of the scene.
6. **Voice stays dense.** Present tense, specific time, one stake, then the
   same engineering. Predict questions stay. Tables stay when they earn their
   keep.
7. **Specialised modules borrow the cast; they do not get a second novel.**
   Time series, graph, NoSQL, Ray are SaaSCo hitting a new access pattern
   (device agents, fraud graph, session store). The five architecture case
   studies keep their own stakes but still open and close on a named person.
   [analytics-platform.md](../docs/architectures/analytics-platform.md) is the
   multi-tenant *design drill* for the company the reader already works for,
   not a new origin story.
8. **Do not hide SaaSCo until Phase 12.** The first hour should meet the
   company and the people. The evolution page remains the full timeline; it
   is no longer "read this after Phase 0." Do not move it in `mkdocs.yml`.
9. **Lock numbers, clocks, and dashboards here.** Later PRs do not invent a
   second volume, a second currency, or a second Priya promise.

## Cast bible

Use first names in lessons. Full names live only here and in a short
start-here section.

| Name | Role | Appears when |
|------|------|----------------|
| **Maya** (Maya Chen) | Data engineer, on-call for the events pipeline | Pager, Spark UI, Kafka lag, Flink watermarks, 3 AM pages |
| **Priya** (Priya Shah) | Product. Two promises, not one (see below) | SLAs, "the chart is empty," design-review pressure |
| **Jordan** (Jordan Hale) | Platform lead | Design reviews, "do not add Kafka yet," keys, `ORDER BY`, security/notebooks/Ray on loan |
| **Elena** (Elena Voss) | VP Finance | Silent-wrong-number scenes: doubled GMV, $0 revenue, $40M vs $19M |
| **Acme** (`cust_0042`) | The enterprise tenant that 10×'d ingest | Skew, hot partitions, one reducer, noisy neighbour |

Event shape does not change:

```text
{timestamp, customer_id, user_id, service, endpoint, region, latency_ms, status_code, bytes}
```

### Priya's two dashboards

Do not collapse these. Spark and ClickHouse are not the same plot.

| Promise | When | What it is | Stage |
|---------|------|------------|-------|
| Acme CSM tile by **07:00** | Scale, Stage 1→2 | Internal tenant dashboard, nightly/batch | Spark earns its keep |
| In-app chart in **milliseconds** | Stage 7 | Customer-facing analytics while Acme is looking | ClickHouse earns its keep |

Same Priya, two promises, years apart on the timeline. Pinot, Trino, and
"product wants a live dashboard" must not steal the Stage 7 promise.

### Number bible

| Lock | Value | Do not |
|------|-------|--------|
| Acme share in lessons | **38%** of events | Do not keep start-here's "40%" after PR 1 |
| Volume steps | **40 GB → 400 GB → 4 TB**/day | Do not let `analytics-platform.md` restart at 1 TB/day as a second company |
| Morning SLA | p95 job done by **07:00** | |
| Currency | **USD** for Elena ($40M vs $19M, $0 tile) | Do not mix ₹0 on SaaSCo Stage 8 with dollars on quality |
| Lab hot-key | **80%** `cust_0042` in generators | Do not "fix" labs to 38%. One clause of author/lab copy may say the drill is exaggerated |

`docs/spark/shuffle.md` currently uses lowercase `acme` as a *different* small
key next to `cust_0042` (lines 29–30, in the executor-distribution block).
When Acme becomes the whale, that extra is renamed (keep `tiny` or similar).
Never write `acme` as a second tenant.

Do not leave `cust_1842` in a rewritten core lesson. The whale is `cust_0042`.
It is **not** only on `docs/kafka/log.md`: it is also the sample `customer_id`
in `docs/kafka/index.md`, `docs/kafka/partitions.md` (prose *and* JSON),
`docs/kafka/replication.md`, `docs/flink/index.md`, and `docs/flink/time.md`.
PR 3 clears it from `log.md`; PR 5 and PR 6 must clear the rest, or the grep
in *What we will measure* will keep firing on untouched files.

Leftover `40%` is likewise not only on start-here. Fix, in the PR that owns
each file: `docs/start-here.md` (PR 1), `docs/how-to-study.md` (PR 1, the
Pass-1 sample question), `docs/foundations/partitions.md` line 252 (PR 3),
`docs/architectures/analytics-platform.md` line 56 (PR 7).
`docs/architectures/observability.md` line 151 is **not** a tenant share — it
is `api-gateway` at 40% of traffic. Leave it.

`₹` is on two pages, not one: `docs/architectures/saasco-evolution.md` line
162 (Stage 8, PR 2) and `docs/lakehouse/index.md` line 10 (the 03:14 `₹0`
Trino cold open, PR 6). Both become USD.

`docs/foundations/index.md` contradicts the volume ladder today: it opens
"Friday this was 40 GB" and "this morning it's 8 TB" (200×, not 10×), while
already using `cust_0042` at 38%. PR 3 reconciles it to 40 GB → 400 GB. The
8 TB figure survives only where the page means the 100× step, and
`docs/foundations/parquet-internals.md`'s "8 TB scan that should have been
40 GB" is a *scan-amplification* number, not a daily volume — do not
"fix" it.

### Rules for using the cast

- One scene, one primary person. Maya on a pager. Jordan in a review. Elena
  when the number is wrong. Priya when the SLA is political. Do not crowd
  four names into the opening paragraph.
- Acme can appear in any scene. They are the load, not the speaker.
- Do not give them backstories, hobbies, or dialogue longer than one line.
  They exist so the stake has a face.
- Other architecture pages (observability, IoT, e-commerce, fraud) may put
  Maya on loan ("Maya is sitting with fraud this quarter") rather than
  inventing a second on-call.
- Security, notebooks, and Ray: Jordan (platform) or Maya. No fifth name.

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

First-pass route (how-to-study, start-here one-hour tip, lesson-guide):

```text
OPEN IN SCENE → mental picture → skip internals → What happened next
```

If the close sits after `Under the hood` and Pass 1 still stops at the mental
picture, first-pass readers — the ones who bounce on screen two — never see
the spine. The close is part of Pass 1, not a reward for finishing the page.

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

Some core pages today *are* a costume as the primary scene (the log page is
observability and `cust_1842`; data modelling is e-commerce GMV; CDC is
orders in Postgres). Rewriting those onto SaaSCo is a **system change**, not
a name swap. Do it on purpose, keep the mechanism, and say so in the PR.

### Close the scene

A short section immediately before the exercise (or as the last prose before
it), titled and anchored consistently:

```markdown
## What happened next { #what-happened-next }
```

It must answer three things:

1. What the named person did in *this* incident.
2. What breaks next week / at the next 10× (pointer, not a new lesson).
3. Which [SaaSCo stage](../docs/architectures/saasco-evolution.md) this is.

The exercise then asks the reader to be that person, not to recite a table.

`docs/javascripts/lesson-guide.js` today only links `#use-case`, `#intuition`,
`#practice-the-idea`, `#internals`, `#exercise`. Add `#what-happened-next` as
a first-pass step when the heading exists (show the link only if present, so
PR 1 can ship the JS before PR 3 writes the sections).

Two mechanics in that file the PR must actually handle:

- The step labels are **hardcoded strings with numbers in them**
  (`"1 · Situation"` … `"5 · Check the idea"`, already branching on whether
  `practice` exists). Adding a sixth step means the numbering has to be
  computed, not hand-patched, or pages without `Practice the idea` will show
  a gap.
- The guide's blurb still reads "understand the situation and mental picture.
  Return for mechanics and production detail when you need them." If Pass 1
  now ends at the close, that sentence is wrong and must change with the step.
- The early-out `if (!intuition || (!situation && !internals)) return;` means
  a page with only an opening and a close renders no guide at all. Fine for
  index/lighter-bar pages; do not "fix" it in PR 1.

Do not retitle existing headings. CI and the lesson guide depend on them:

| Keep | Why |
|------|-----|
| `## Start with the situation { #use-case }` | lesson-guide |
| `## Build the mental picture { #intuition }` | lesson-guide, Pass 1 |
| `## Under the hood { #internals }` (and `h2[id^='internals-']`) | lesson-guide |
| `## Practice the idea` / `{ #practice-the-idea }` | `scripts/validate_course.py` asserts this heading |
| `## Exercise` / `{ #exercise }` | lesson-guide |

### Do not

- Cut `Under the hood`, failure modes, or worked arithmetic to make room.
- Add humour, emojis, or cliffhanger marketing ("little did Maya know").
- Name people in reference pages that are genuinely dictionaries
  (`glossary.md`, `version-matrix.md`).
- Repeat the full Acme origin story on every page. One clause is enough
  after the first hour ("Acme, still 38% of the topic").
- Drive-by retitle `Practice the idea` or drop heading ids.

## Page definition of done

A substantial lesson is done with storytelling when:

- [ ] Opening names a person from the cast (or Acme) and a stake.
- [ ] The predict question is still there.
- [ ] Mental picture is framed as what that person is looking at.
- [ ] At most one costume-change aside. No five-system roll call mid-scene.
- [ ] Internals are not thinner than today (no deleted mechanisms, configs,
      or failure modes).
- [ ] `## What happened next { #what-happened-next }` closes the scene
      before the exercise.
- [ ] Existing heading ids above still resolve.
- [ ] SaaSCo stage is linked where the lesson sits on the timeline.
- [ ] `cust_0042` / Acme is used if the page already taught skew, hot keys,
      or noisy neighbours.
- [ ] Numbers match the number bible (38%, 40→400→4 TB, USD, 07:00).

Index pages, labs, simulations, glossary, version matrix, README, the
practice map, and the capstone rubric have a lighter bar: a named person in
the opening (or one SaaSCo sentence) is enough; they do not all need a
close-the-scene section.

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
| [start-here.md](../docs/start-here.md) | Replace "Five stories you will keep revisiting" with "The company you work for": SaaSCo, the cast, Acme, the event shape, 38%. One paragraph that the other four systems are costumes you will meet later. Rewrite the "if you have only one hour" tip so it includes that section, then scale through mental picture **and** `What happened next`, then the partition sim. |
| [how-to-study.md](../docs/how-to-study.md) | Pass 1 = open + mental picture + close. "Leave able to say what happened in the scene." |
| [learning-paths.md](../docs/learning-paths.md) | Gentle first hour: meet the company on start-here, then scale → partition sim → partitions, still one session. Must match start-here's one-hour tip. |
| [lesson-guide.js](../docs/javascripts/lesson-guide.js) | First-pass step for `#what-happened-next` when present. |
| [saasco-evolution.md](../docs/architectures/saasco-evolution.md) | Same eight stages plus Stage 9 as an exercise. Each stage opens on the assigned person below. Forcing functions stay measured. USD for Elena. |
| [architectures/index.md](../docs/architectures/index.md) | Today it says SaaSCo has **six** growth stages. That is wrong. Eight stages, Stage 9 remains the deletion exercise. |
| [CONTRIBUTING.md](../CONTRIBUTING.md) | Storytelling contract next to the five-act loop, with a pointer at this plan. Mention Pass 1 includes the close, and do not retitle `Practice the idea`. |
| Homepage [index.md](../docs/index.md) | One line under the existing "Five running systems" CTA: the through-line is one company, not five manuals. Do not turn the homepage into a story. |
| [README.md](../README.md) | Same one-line correction as the homepage. External front door still says five systems today. |

SaaSCo stays in Phase 12 in the nav (it is still an architecture page). It
becomes *recommended early* from start-here and the first-hour path, not
moved in the sidebar.

## Phased pull requests

Each PR should be independently reviewable and mergeable. Do not batch the
whole academy. Prototype on the first hour, then roll forward in reading
order. **Do not start PR 4 until PR 3 proves the contract** (including that
a first-pass reader hits the close).

CI on every PR: `mkdocs build --strict` **and**
`python scripts/validate_course.py` (that is what
`.github/workflows/validate.yml` runs).

Two things to know before trusting that pair as a safety net:

- `validate_course.py` shells out to `docker compose config` for the five lab
  Compose files, so it does not run locally without Docker. If you cannot run
  it, at minimum re-check its one prose assertion by hand: the count of files
  containing the literal `## Practice the idea` must stay `>=` the number of
  `docs/simulations/*.html`. That is the *only* heading this script guards —
  `#use-case`, `#intuition`, `#internals`, `#exercise` and
  `#what-happened-next` are enforced by nothing but review.
- `--strict` fails on broken *page* links, not on missing heading anchors.
  Every anchor promise in this plan needs a grep, not CI.

### PR 1 — Contract and front door

**Title:** Make SaaSCo and a named cast the front door of the academy

**Files:**

- `CONTRIBUTING.md`
- `docs/start-here.md`
- `docs/how-to-study.md`
- `docs/learning-paths.md`
- `docs/index.md` (one line)
- `docs/javascripts/lesson-guide.js`
- `README.md` (one line; same thought as the homepage)

**Work:**

- Add the storytelling contract (open in scene, stay, close, one costume
  change, do not thin internals, Pass 1 includes the close, keep heading
  ids) to CONTRIBUTING, next to the five-act loop.
- Rewrite start-here's "Five stories" into the company, the cast, Acme,
  38%, and the event shape. Keep the JSON. Demote Systems B–E to a short
  "you will meet these later as the same physics in a different costume."
- Align the one-hour tip, learning-paths first hour, and Pass 1.
- Point the first-hour path at the company section, then scale.
- Lesson-guide: add a first-pass link to `#what-happened-next` when that
  heading exists.

**Done when:** a new reader meets Maya and Acme before they meet Spark, and
the three first-hour surfaces agree. `mkdocs build --strict` still passes.

### PR 2 — SaaSCo timeline gets people

**Title:** Put the cast on the SaaSCo stage timeline

**Depends on:** PR 1 (names are locked)

**Files:**

- `docs/architectures/saasco-evolution.md`
- `docs/architectures/index.md`

**Work:**

- Keep all eight stages, forcing functions, arithmetic, and the Stage 9
  deletion exercise. Do not add a ninth stage. Do not add new components.
- **Do not retitle the `## Stage N — …` headings.** Only Stage 9 has an
  explicit `{ #stage-9 }`; Stages 1–8 rely on slugs generated from their
  titles, and ten pages deep-link them today:
  `foundations/scale.md` and `spark/index.md` → Stage 2,
  `kafka/index.md` → Stage 3, `flink/index.md` → Stage 4,
  `airflow/index.md` → Stage 5, `lakehouse/index.md` and
  `lakehouse/iceberg.md` → Stage 6, `olap/index.md` and
  `olap/clickhouse.md` → Stage 7, `metadata/index.md` → Stage 8.
  `mkdocs.yml` sets no `validation:` block, so a broken *anchor* does **not**
  fail `--strict` — these would rot silently. The person opens the stage in
  the body text under the existing heading. If a title must change, add an
  explicit `{ #stage-N }` to every stage first and repoint all ten links in
  the same PR.
- Fix architectures index: eight stages, not six.
- Elena's tile is USD ($0 / $40M vs $19M), not ₹.
- Open each stage on the person below. The technical argument stays the body.

| Stage | Person | Opening stake |
|------:|--------|----------------|
| 1 | Maya | Laptop OOM at `read_parquet` after Acme signs (40 GB) |
| 2 | Maya + Priya | 400 GB, 07:00 CSM tile still promised; Spark is earned by the workload, not the bytes |
| 3 | Jordan | Five consumers, replay, per-entity order; refuse "just write S3" |
| 4 | Priya | Fraud-review queue must flag the card *while it is still being used* |
| 5 | Maya | Nobody can say which of twelve jobs ran this morning |
| 6 | Jordan | Three engines writing one prefix; rename is not atomic |
| 7 | Priya | Customer-facing in-app chart in milliseconds (not the 07:00 tile) |
| 8 | Elena | Dashboard green, the number is wrong; contracts and lineage |
| 9 (exercise) | Jordan + Elena | Which boxes disappear after cost-cutting |

**Done when:** the evolution page reads as eight chapters of one company,
not eight status reports with a headcount column, and the index no longer
says six stages.

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
five-system roll calls to one aside, add `## What happened next { #what-happened-next }`,
keep every mechanism and exercise, keep heading ids.

**Clocks must not run backwards.** Nav order is index → scale. Index is a
*map*, not a second timed incident of the same OOM.

| Page | Scene | Close |
|------|-------|-------|
| Foundations index | No competing clock. Maya, Acme at 38%, this phase names the physics behind the OOM on the next page | This phase names the physics before any product |
| Scale | **07:58**, Maya's OOM, Priya waiting on the 07:00 CSM tile | Bigger notebook vs new mechanism; Stage 1→2 |
| Partitions | **Same day 14:20**, Jordan's review, partition by `region` | Key chosen, Acme still a hotspot; points at shuffle |
| Log | **Tuesday 10:04**, 40-minute parser hole, "can we replay?" | Why a log, not a table; Stage 3 is waiting |
| Shuffle | **Wednesday 02:47**, Maya, 199/200 tasks, Acme on one reducer | Salt / two-phase agg; this is the mechanism scale promised |

`docs/kafka/log.md` today is an observability cold open with `cust_1842`.
This PR **moves the page onto SaaSCo**. Keep the parser-hole / replay
mechanism. Do not leave `cust_1842`.

`docs/spark/shuffle.md` today uses `acme` as a small extra key. Rename it.

**Done when:** someone can read only these five openings + closes and retell
SaaSCo Stages 1–3 without opening the evolution page. A first-pass reader
using the lesson guide hits `What happened next`. Internals on each page are
intact. `validate_course.py` still passes.

### PR 4 — Foundations remainder (Phase 0–1, plus CDC)

**Depends on:** PR 3 (voice is proven)

CDC lives in Phase 2 in the nav. It is in this PR because it is a
foundations file and it already has a clock. Title the PR for the files,
not "Phase 0–1" alone.

Use this roster. Do not invent a second plot.

| File | Person | Scene to keep / write | Stage |
|------|--------|------------------------|------:|
| `data-movement.md` | Maya | The minutes that are movement, not CPU, on the same p95 job | 2 |
| `distributed-execution.md` | Maya | Job → stage → task of that job | 2 |
| `backpressure.md` | Maya | Queue growth / lag; Acme is the slow partition if the page already has a whale | 3 |
| `batch-vs-stream.md` | Jordan | Design review: "put everything on Flink." Keep today's hook, name him | 3→4 |
| `parquet-internals.md` | Maya | Hook already on the page: 8 TB scan that should have been 40 GB. Not a file-anatomy tree with no person | 2 |
| `object-storage.md` | Jordan | Prefix hotspot / rename is not atomic. This is the Stage 6 forcing function taught as physics | 6 (physics early) |
| `data-modelling.md` | Elena | GMV 2.3× from a grain mismatch. Move the example onto SaaSCo facts (billing / plan), not a second e-commerce company | 8 (physics early) |
| `data-contracts.md` | Jordan + Elena | Schema change that will become Stage 8 | 8 (physics early) |
| `transformation-engineering.md` | Maya | Incremental vs full rebuild of the p95 job | 2–5 |
| `cdc.md` | Maya | Keep **02:17**, 40 missing rows, COPY vs slot. SaaSCo's app Postgres, not a second company. Do not drop into "Contract" before she is in the room | 3 |

### PR 5 — Kafka and Spark modules

**Files:** every page under `docs/kafka/` and `docs/spark/` **except**

- `docs/kafka/log.md` and `docs/spark/shuffle.md` (already PR 3)
- lab Python / Compose

Labs (`docs/kafka/labs.md`, `docs/spark/labs.md`): opening sentence only.

Reuse the same incidents already on the page (billing lag, `NotEnoughReplicas`,
double charge, driver OOM). Name Maya / Jordan / Elena. Close the scene.
Keep gotchas as gotchas — do not novelise a checklist.

### PR 6 — Flink, Airflow, lakehouse, ClickHouse / Trino

**Files:** `docs/flink/` (except `labs.md`, opening sentence only),
`docs/airflow/`, `docs/lakehouse/`, `docs/olap/`, `docs/query-engines/`.

These already have some of the best hooks in the course (silent Flink job,
Grafana tile, `ORDER BY` that skips nothing, Trino prune miss). They need
people and a close, not new plots.

Do not collide beats:

| Page | Person | Must not become |
|------|--------|-----------------|
| Flink (time / windows / index) | Priya's Stage 4 fraud-review queue; Maya on the pager | A second fraud company |
| Airflow | Maya, which of twelve jobs (Stage 5) | |
| Iceberg / why table formats | Jordan, three writers (Stage 6) | |
| ClickHouse | Maya on the 03:14 Grafana tile; Priya's Stage 7 in-app chart in the close / architecture link | The 07:00 CSM tile |
| Pinot | Jordan in review: high-QPS in-app vs internal CH | Priya's Stage 7 promise |
| Trino 10:41 prune miss | Maya (or "the analyst" named once, then Maya) | Elena's silent-wrong-number morning — that is quality, a different day |
| Cloud warehouses | Jordan | |

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

Elena owns quality and correctness scenes ($40M vs $19M, keep the 14:02
clock, USD). Jordan owns selection, delivery, security, and notebooks
(11:15 emails on screen; Friday 4:50 Drive export). Incidents stay drills:
name Maya in the alert line, keep hypothesis-before-resolution.

`analytics-platform.md` is not a second SaaSCo origin. Same company, same
38% / volume steps. Today's "customer A showed customer B's spike" is Acme
as noisy neighbour. Do not keep a parallel 1 TB/day company.

Observability / e-commerce / IoT / fraud: Maya on loan, one costume, own
stakes, still open and close.

### PR 8 — Specialised stores, comparisons, leftover indexes

**Files:** `docs/time-series/`, `docs/databases/`, `docs/graph/`,
`docs/distributed-python/`, `docs/comparisons/`, plus leftover indexes and
front-door stragglers:

- `docs/practice-map.md` (lighter bar: one SaaSCo / Maya sentence)
- `docs/labs/index.md` (already Friday 4:55 PM; name Maya)
- `docs/simulations/index.md` (lighter bar)
- `docs/reference/index.md` (lighter bar)
- `docs/flink/labs.md` opening sentence
- `docs/capstone.md` **one line only**: default the architecture deliverable
  at SaaSCo unless the reader picks a costume. Do not novelise the rubric.

Comparisons: one named disagreement in a review ("Jordan: why not Structured
Streaming with a 1-second trigger?") then the same matrix. Do not pad.
`docs/flink/comparison.md` (PR 6) and `docs/comparisons/spark-vs-flink.md`
must not be two different Jordan fights; reuse the beat.

Skip: `docs/reference/glossary.md`, `docs/reference/version-matrix.md`,
`docs/capstone.md` rubric mechanics, HTML simulations, lab Python.

## Suggested reading-order continuity (core path)

Use this so later PRs do not invent conflicting plots. Details can move; the
causality should not. The PR 4–7 rosters hang off these beats.

1. Acme signs. Volume 40 GB → 400 GB. Maya's pandas job OOMs. Priya's 07:00
   CSM tile. (scale)
2. Jordan's review wants `region` as the partition key. (partitions)
3. The job is distributed. Thirteen minutes are movement, not CPU.
   (data-movement, distributed-execution, parquet hook)
4. Five teams want the same events at five speeds. Replay after a parser
   bug. Kafka appears. (log, kafka partitions, replication, CDC, backpressure)
5. Acme hashes to one partition / one reducer. (kafka gotchas, shuffle)
6. Priya needs fraud flags while the card is still being used. Flink.
7. Nobody can say which of twelve jobs ran. Airflow.
8. Spark and Flink write the same prefix. Iceberg. (object-storage physics)
9. Acme's **customer-facing** chart must answer in milliseconds. ClickHouse.
   (not the 07:00 tile)
10. DAG green, Elena's number wrong in USD. Quality, contracts, lineage,
    modelling grain.

That is SaaSCo Stages 1–8 told as scenes. Specialised modules hang off it
(cardinality when someone adds `user_id` as a Prometheus label; graph when
fraud needs three hops in 200 ms; NoSQL when checkout sessions leave
Postgres; Ray when the simulator will not fit Spark).

## What we will measure

There is no analytics event for "attention." Use editorial checks:

- After PR 3, read the five prototype pages aloud. If you can skip from
  opening to close and still know what happened, the spine works.
- After PR 3, click the lesson-guide first-pass route on scale.md and
  confirm `What happened next` is a stop, not a section you only reach by
  scrolling through internals.
- Spot-check that `Under the hood` word count did not drop.
- `mkdocs build --strict` **and** `python scripts/validate_course.py` on
  every PR.
- Grep for leftover anonymity on *touched* files, at the start of a lesson
  (body text citing a generic stakeholder is fine):

  ```text
  Someone proposes
  an engineer asks
  a VP pings
  Product wants
  a PM wants
  a teammate
  a colleague says
  Finance asks
  Someone asks
  an analyst
  cust_1842
  ```

  Also grep rewritten shuffle / scale copy for a second tenant called `acme`.
- Repo-wide (not just touched files) at the end of PR 6: `cust_1842` and `₹`
  should both be gone. Both currently span more files than their owning PR.
- Grep that every substantial rewritten lesson has
  `## What happened next` and `{ #what-happened-next }`.
- Spot-check 38% vs leftover 40%, and ₹ vs $ on Elena pages.

Optional later: a short note on start-here asking readers who bounce to say
where. Do not block this work on instrumentation.

## Verified against the repo (2026-09-16, `plan/storytelling`)

Checked before adopting this plan. Confirmed true:

- `lesson-guide.js` links exactly the five ids listed and nothing else.
- `validate_course.py` asserts `## Practice the idea` and nothing else in prose.
- `.github/workflows/validate.yml` runs `mkdocs build --strict` then
  `python scripts/validate_course.py`.
- `architectures/index.md` line 11 really does say **six** growth stages while
  `saasco-evolution.md` has eight plus the Stage 9 exercise.
- The clocks the plan reuses all exist as written: 07:58 (scale), 14:20
  (partitions), 02:47 (shuffle), 10:04 (log), 02:17 (cdc), 10:41 (trino),
  03:14 (clickhouse), 09:02 Monday (foundations index).
- `partitions.md` line 7 literally opens "Someone proposes" — the anonymity
  grep is not hypothetical.
- **No file in `docs/` contains Maya, Priya, Jordan, or Elena today.** The
  cast is greenfield; there is no existing character to collide with or
  rename, which is why the bible must be locked in PR 1.

Corrected by this revision: the scope of `cust_1842`, of leftover `40%`, and
of `₹`; the stage-anchor hazard in PR 2; the hardcoded step numbering in
`lesson-guide.js`; the 40 GB → 8 TB contradiction on `foundations/index.md`;
and the Docker dependency in `validate_course.py`.

## Risks

| Risk | Mitigation |
|------|------------|
| Cast feels cute or patronising to Staff readers | First names only, no backstory, no dialogue scenes. One line of speech max. |
| Internals get cut "for pacing" | Definition of done forbids deleting mechanisms. Reviewers reject thinner pages. |
| Five systems feel demoted | Architecture pages stay. They are costumes, not deletions. Start-here says so. |
| Inconsistent names across PRs | This bible is the lock. Do not rename Maya mid-rollout. |
| SaaSCo-in-Phase-12 nav vs "read it first" | Link from start-here and first hour. Do not reshuffle the whole sidebar in PR 1. |
| Scope explosion | PR 3 is the gate. If the prototype is wrong, do not start PR 4. |
| First-pass readers never see the close | Pass 1 + lesson-guide include `#what-happened-next`. PR 3 done-when tests it. |
| Plot drift after PR 3 | Number bible, two Priya dashboards, and per-PR scene rosters. No file glob without a person. |
| `analytics-platform.md` becomes a second novel | Same company, same numbers. Design drill, not origin. |
| CI / lesson-guide break | Do not retitle `Practice the idea` or drop `#use-case` / `#intuition`. |
| Acme naming collision | Rename shuffle's extra `acme` key in PR 3. |
| SaaSCo stage anchors rot silently | Ten pages deep-link generated stage slugs and `--strict` does not check anchors. Do not retitle Stage 1–8 headings. |
| `cust_1842` / `₹` survive in untouched modules | They span Kafka, Flink and lakehouse pages, not just `log.md`. Repo-wide grep at PR 6. |

## Out of scope until the prototype lands

- New simulations or labs about the story.
- A fictional Slack export or multi-page "week in the life."
- Audio / video.
- Changing `cust_0042` in lab generators, or changing lab skew from 80% to
  38%.
- Moving SaaSCo in `mkdocs.yml`.
- A fifth named character.

## Implementation notes for whoever takes PR 3

Work page by page, not "add a character pass" across the repo.

For each prototype file:

1. Read the current opening and the current last section before the
   exercise.
2. Rewrite the opening in scene (see Scale example under Lesson contract).
   Index is a map; do not give it a 09:02 clock that runs later than scale's
   07:58.
3. Walk `Build the mental picture` and replace "consider a 10 TB dataset"
   with the same numbers attached to Maya / Jordan / Acme.
4. Find the five-system list. Collapse to one aside or move to the close.
5. Write `## What happened next { #what-happened-next }` from facts already
   on the page. Do not invent a new incident.
6. Leave `Under the hood`, code, `Practice the idea`, heading ids, and the
   exercise items in place. Adjust exercise wording to "you are Maya" only
   where it helps.
7. On `log.md`, move the primary scene onto SaaSCo; drop `cust_1842`.
8. On `shuffle.md`, stop using `acme` as a non-whale key.

Do not drive-by reformat, retitle sections, or "fix" adjacent pages in the
same PR.
