# Interactive Simulations

Browser-based interactive tools that make abstract data engineering concepts tangible.

---

## Available Simulations

### [Kafka Partition Simulator](kafka-partitions.html)

Configure partitions, consumers, producer rate, and hot key percentage. Watch lag build up on hot partitions. Observe idle consumers when consumers outnumber partitions.

**What to try**: Set hot key ratio to 70% and watch one partition fall behind while others stay at zero lag.

---

### [Spark Shuffle Visualiser](spark-shuffle.html)

See how data moves between map and reduce partitions. Switch between uniform, skewed, and salted distributions. Observe skew ratio and which reduce partition becomes the bottleneck.

**What to try**: Set distribution to "Very skewed" and observe that one reduce partition receives 90% of rows. Then switch to "Salted" to see the fix.

---

### [ClickHouse ORDER BY Explorer](clickhouse-order-by.html)

Three scenarios showing how ORDER BY affects query performance. Visualise which granules are read vs skipped depending on whether the WHERE clause matches the sort key.

**What to try**: Compare Scenario 1 (wrong ORDER BY) vs Scenario 2 (right ORDER BY) and observe 100% scan vs 0.3% scan for the same query.

---

### [Cardinality Calculator](cardinality-calculator.html)

Calculate the total time series cardinality for a metric with multiple label dimensions. Add a user_id dimension and watch cardinality explode past the TSDB threshold.

**What to try**: Start with 4 label dimensions (manageable). Add the user_id dimension with 100K users and observe the jump from thousands to millions of series.

---

## How to Use

These are standalone HTML files. Open them directly in your browser — no server required. They work offline.

In the MkDocs site, they are embedded as linked pages. You can also open them directly from the repository.

---

## Contributing Simulations

Simulations should be:
- Self-contained (single HTML file, no external dependencies)
- Interactive (sliders, buttons, real-time updates)
- Demonstrating one concept clearly
- Visually consistent with the dark theme

The goal is to make the learner *see* the failure mode, not just read about it.
