---
title: How to Study
description: How to use this academy — predict, run, compare, explain. Not passive reading.
---

# How to Study

This material is dense on purpose. It is closer to a staff-engineer briefing than a blog series.

## Do not read passively

Every page contains questions in the text. Stop when you see them. Think before reading the answer. They are not rhetorical — they are the reasoning you will need in production.

Example:

> What happens if one customer generates 40% of all events in a partitioned Kafka topic?

Before reading further: think. Where does that imbalance show up? Which consumer gets overloaded? What metric would alert you?

*Then* read the explanation.

!!! tip "The loop"
    **Predict → read / run → compare → explain.** If you skip predict, you are collecting facts, not building judgement.

## Follow the reasoning, not the tool

When studying ClickHouse, do not memorise the list of table engines. Understand *why* ClickHouse writes immutable parts and merges them in the background. Once that is solid, most operational consequences follow.

The question to ask about any concept: **"What problem forced someone to design it this way?"**

If you cannot answer that, you cannot choose the tool under a new constraint.

## Three-level explanations

| Level | Use it when | You should be able to |
|-------|-------------|------------------------|
| Intuition | Whiteboard with a product manager | Explain the idea without jargon |
| Engineering | Design review | Name the data structure, the network hop, the file layout |
| Production | Incident or capacity planning | Name the metric, the failure, the cost, the 10× cliff |

In a design conversation, start at intuition. Depth-first only on what the other person probes. Jumping to config knobs without a mental model is how you get a zoo of technologies and no architecture.

## Run the labs

Reading about shuffle is not the same as watching one slow task dominate a Spark stage. Labs are not optional colour — they are where the intuition becomes visceral.

Every lab is designed to run on a laptop with Docker Compose and synthetic data. You do not need a cloud account.

**Predict the outcome before each command.** Then run it. Then explain the gap.

Do not rewrite the exercise after the fact to match the output. The surprise *is* the learning.

See [Labs](labs/index.md).

## Use the simulations the same way

1. Read the concept page first
2. Predict what the default parameters will show
3. Change one variable (partition count, skew, `ORDER BY`, cardinality)
4. Explain the picture in one sentence

Simulations live under [Practice → Simulations](simulations/index.md). They verify a mental model; they do not replace one.

## Treat every “Production is on fire” section as an exam

These are incident scenarios with real-shaped telemetry. Form a hypothesis **before** expanding the resolution. That is the skill that separates people who can operate systems from people who can only assemble demos.

[Incidents](incidents/index.md) collects them. Module gotchas pages have more.

## Recommended progression

If you are new to data-system internals, follow the phases in order:

1. [Foundations](foundations/index.md) — mental models before product names
2. [Kafka](kafka/index.md) + [Spark](spark/index.md) — the core data plane
3. [Flink](flink/index.md) — stream processing and time
4. [Lakehouse](lakehouse/index.md) — what a table is on object storage
5. [Trino](query-engines/trino.md) + [ClickHouse](olap/clickhouse.md) — query engines
6. Specialised: [Time series](time-series/index.md), [Graph](graph/index.md), [NoSQL](databases/index.md)
7. [Ray](distributed-python/ray.md) — distributed Python
8. Platform: [Airflow](airflow/index.md), [Metadata](metadata/index.md), [Quality](quality/index.md), [Security](security/index.md)
9. [Architectures](architectures/index.md) — end-to-end design

If you are experienced and targeting a gap, use [Learning paths](learning-paths.md) and jump.

## When an exit check does not pass

Do not remain stuck on the same prose. Use this recovery loop:

1. Revisit the linked prerequisite and write the missing term in your own words.
2. Change one variable in the matching simulation or run the smallest lab case.
3. Read the worked answer, close it, and solve a changed version from memory.
4. Explain the result aloud using one diagram and one production metric.
5. Retry the exit check the next day. If it still fails, continue on the foundations route and return after the adjacent lesson.

Difficulty is diagnostic information, not a command to stop progressing.

## How to use architecture pages

For each architecture:

1. Cover the finished diagram
2. Write down volume, latency, access pattern, retention, and failure budget
3. Derive a V1 with the fewest moving parts
4. Name the first bottleneck
5. Add the next component only when you can justify it
6. Compare your V2 to the page

Never start from the finished boxes.

## The standard

You have understood a concept when you can explain **why someone had to invent it**, predict how it fails at 10×, and name the metric you would look at first.

If you can only recite APIs, keep going.

Record completion in the checklist on [Learning paths](learning-paths.md), then finish the [capstone](capstone.md). A path is complete when its exit checks and deliverable pass—not when browser history says every page was visited.
