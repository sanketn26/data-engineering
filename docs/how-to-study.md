---
title: How to Study
description: How to use this academy — predict, run, compare, explain. Not passive reading.
---

# How to Study

The pages contain both a friendly mental model and production depth. Trying to
absorb both at once makes a useful course feel like a reference manual. Use
three passes instead.

## The three-pass rhythm

### Pass 1 — get the picture (10–15 minutes)

Read the opening situation and **Build the mental picture**. Ignore unfamiliar
configuration names. At the end, say the central idea in one sentence. If you
can predict the broad failure, the pass worked.

### Pass 2 — make it concrete (15–30 minutes)

Read **Under the hood** and **Put it to work**. Trace one event, file, or query
through the diagram and example. Write down only the terms needed to narrate
that journey.

### Pass 3 — operate it (when relevant)

Read **Where teams get caught**, **How it fails**, and **How to investigate**.
Run the exercise, simulation, or lab. This pass matters before production work;
it does not need to block your first understanding of the concept.

!!! tip "One session, one outcome"
    A good session can be just Pass 1 plus the check at the bottom of the page.
    Stop there if the idea is clear. Depth is easier after the mental model has
    had time to settle.

## Pause for predictions

Every page contains questions in the text. Stop when you see them and make a
quick prediction before reading the answer. They are invitations to make the
next paragraph useful, not tests you must get right.

Example:

> What happens if one customer generates 40% of all events in a partitioned Kafka topic?

Before reading further: think. Where does that imbalance show up? Which consumer gets overloaded? What metric would alert you?

*Then* read the explanation.

!!! tip "The loop"
    **Predict → read or run → compare → explain.** A wrong prediction is useful:
    it reveals exactly which part of the model changed.

### Turn a symptom into a measurement { #what-would-you-measure }

A production symptom (rising lag, a slow dashboard, a failing job) is usually consistent with several causes at once. "What would you measure?" is the question that converts a list of plausible stories into one you can actually confirm: for each hypothesis, name the specific metric that would be different if that hypothesis, and only that one, were true. If two hypotheses would move the same metric the same way, you have not found a distinguishing measurement yet — keep going until you have one per hypothesis. You will see this pattern applied directly in [Kafka gotchas — lag rising](kafka/gotchas.md#1-consumer-lag-growing-silently) and in the [cross-system incidents](incidents/index.md).

## Follow one event, not every feature

When studying ClickHouse, follow one row from insert to immutable part to query.
You do not need to memorise the list of table engines. Once the journey is
clear, the operational consequences are much easier to derive.

The question to ask about any concept: **"What problem forced someone to design it this way?"**

If you cannot answer that yet, return to the opening situation and diagram
before adding more details.

## Three-level explanations

| Level | Use it when | You should be able to |
|-------|-------------|------------------------|
| Intuition | Whiteboard with a product manager | Explain the idea without jargon |
| Engineering | Design review | Name the data structure, the network hop, the file layout |
| Production | Incident or capacity planning | Name the metric, the failure, the cost, the 10× cliff |

In a design conversation, start at intuition. Depth-first only on what the other person probes. Jumping to config knobs without a mental model is how you get a zoo of technologies and no architecture.

## Run a lab after the picture is clear

Reading about shuffle is not the same as watching one slow task dominate a
Spark stage. Labs turn the picture into something you can observe, but they do
not have to be part of the first reading session.

Every lab is designed to run on a laptop with Docker Compose and synthetic data. You do not need a cloud account.

**Predict the outcome before each command.** Then run it. Then explain the gap.

When the result surprises you, write down what your prediction missed. That gap
is often the most valuable part of the exercise.

See [Labs](labs/index.md).

The [practice map](practice-map.md) keeps the sequence explicit: concept first,
then simulation, runnable lab, and incident. Use only as many stages as your
current goal needs.

## Use the simulations the same way

1. Read the concept page first
2. Predict what the default parameters will show
3. Change one variable (partition count, skew, `ORDER BY`, cardinality)
4. Explain the picture in one sentence

Simulations live under [Phase 12: Production → Simulations](simulations/index.md). They verify a mental model; they do not replace one.

## Use incidents as practice, not judgement

These are incident scenarios with real-shaped telemetry. Form a hypothesis
**before** expanding the resolution, then compare your reasoning with the
worked diagnosis. The goal is to improve the next hypothesis, not to guess the
answer immediately.

[Incidents](incidents/index.md) collects them. Module gotchas pages have more.

## Recommended progression

If you are new to data-system internals, use this sequence. The numbered phases
in the sidebar remain useful as a map, but you do not need to finish every page
before moving forward:

1. Phase 0-1 [Foundations](foundations/index.md) — mental models and data representation before product names
2. Phase 2 [Kafka](kafka/index.md) + CDC — the log and the change stream
3. Phase 3 [Spark](spark/index.md) — distributed compute
4. Phase 4 [Flink](flink/index.md) — stream processing and time
5. Phase 5 [Airflow](airflow/index.md) — orchestration
6. Phase 6 [Lakehouse](lakehouse/index.md) — what a table is on object storage
7. Phase 7-8 [Trino](query-engines/trino.md) + [ClickHouse](olap/clickhouse.md) — query engines and real-time OLAP
8. Phase 9 Specialised: [Time series](time-series/index.md), [Graph](graph/index.md), [NoSQL](databases/index.md), [Ray](distributed-python/ray.md)
9. Phase 10 Platform: [Metadata](metadata/index.md), [Quality](quality/index.md), [Security](security/index.md)
10. Phase 11 Architecture & Economics: [selection framework](reference/selection-framework.md), [cost engineering](reference/cost-engineering.md)
11. Phase 12 Production: [Architectures](architectures/index.md), [Incidents](incidents/index.md), [Capstone](capstone.md)

If you are experienced and targeting a gap, use [Learning paths](learning-paths.md) and jump.

## When an exit check does not pass

Do not remain stuck on the same prose. Use this recovery loop:

1. Revisit the linked prerequisite and write the missing term in your own words.
2. Change one variable in the matching simulation or run the smallest lab case.
3. Read the worked answer, close it, and solve a changed version from memory.
4. Explain the result aloud using one diagram and one production metric.
5. Retry the exit check the next day. If it still fails, continue on the foundations route and return after the adjacent lesson.

Difficulty is diagnostic information. Use it to choose the next smaller
example, not as a reason to stop.

## How to use architecture pages

For each architecture:

1. Cover the finished diagram
2. Write down volume, latency, access pattern, retention, and failure budget
3. Derive a V1 with the fewest moving parts
4. Name the first bottleneck
5. Add the next component only when you can justify it
6. Compare your V2 to the page

Starting from the constraints makes the finished boxes easier to understand.

## The standard

Understanding grows in layers. First, explain **why someone had to invent the
idea**. Next, predict how it behaves. Finally, when you need production depth,
name how it fails at 10× and the metric you would inspect first.

Record completion in the checklist on [Learning paths](learning-paths.md), then finish the [capstone](capstone.md). A path is complete when its exit checks and deliverable pass—not when browser history says every page was visited.
