# Production Incidents

Real incident scenarios. Your job is to form a hypothesis before looking at the resolution.

---

## How to Use These

Each incident follows this structure:

1. **The alert fires** — what the monitoring shows
2. **The symptoms** — logs, metrics, unusual behaviour
3. **Your job** — diagnose before reading the resolution
4. **The resolution** — what actually happened and how it was fixed
5. **The lesson** — what systemic change prevents recurrence

Read the symptoms. Stop. Think about what could cause them. Form a hypothesis. Only then expand the resolution.

This is the kind of reasoning that separates engineers who can operate systems from engineers who can only build demos.

---

## Incidents by System

### Kafka
- [Consumer lag growing on one partition](../kafka/gotchas.md#production-is-on-fire) — classic hot partition scenario

### Spark
Coming soon — executor OOM during a join on a skewed dataset

### Flink
Coming soon — watermark stalled, output stopped entirely

### ClickHouse
Coming soon — query latency spiked 10x, dashboards timing out

### Iceberg
Coming soon — snapshot accumulation, table scan slowed over weeks

### Trino
Coming soon — coordinator OOM on a large cross-system join

---

## Incident Template

When contributing new incident scenarios, use this structure:

```markdown
## [System]: [Short description]

### Alert
What the monitoring system showed.

### Symptoms
- Metric A: value
- Log line B: text
- Behaviour C: description

> Before expanding: what is your hypothesis?

<details>
<summary>Resolution</summary>

### Root Cause
...

### Fix
...

### Prevention
...

</details>
```
