# Prompt: Build an Intuition-First Data Engineering Academy

You are an expert Data Engineer, Distributed Systems Engineer, Database Engineer, Streaming Systems Architect, Data Platform Engineer, and technical educator.

Create a complete GitHub repository for an advanced, hands-on **Data Engineering learning academy**, delivered as Markdown using **Material for MkDocs** and deployable through **GitHub Pages**.

The site should have a similar teaching philosophy, clarity, visual style, depth, and engineering tone to:

`https://sanketn26.github.io/interview-prep/`

However, this is NOT an interview-preparation clone.

It is a **Data Engineering Academy for experienced software/data/platform engineers who want to understand how modern data systems actually work**.

The objective is not:

> "Learn Spark, Kafka, ClickHouse and Airflow."

The objective is:

> "Given a data workload, its scale, latency requirements, access patterns, reliability requirements and cost constraints, I can derive an appropriate data architecture, choose sensible technologies, explain their trade-offs, predict how they will fail, debug them in production, and evolve the architecture as scale increases."

---

# 1. Core Teaching Philosophy

Do NOT create a traditional documentation-style course.

Do NOT start pages with dictionary definitions such as:

> Apache Kafka is a distributed event streaming platform...

Do NOT create pages that resemble product documentation.

Do NOT teach tools as isolated technologies.

Teach every topic starting from an engineering problem.

The learner should repeatedly encounter this sequence:

# USE CASE → WHY → INTUITION → WHAT → INTERNALS → HOW → GOTCHAS → APPLY

Every major lesson should answer:

1. **USE CASE — What problem are we trying to solve?**
2. **WHY — Why is this problem difficult at scale?**
3. **INTUITION — What mental model makes the solution obvious?**
4. **WHAT — What concept or technology solves it?**
5. **INTERNALS — What actually happens inside the system?**
6. **ARCHITECTURE — Where does it sit in a real architecture?**
7. **HOW — How do I build or use it?**
8. **GOTCHAS — Where do engineers get this wrong?**
9. **FAILURE MODES — How does it break in production?**
10. **DEBUGGING — How would I investigate the failure?**
11. **SCALE — What changes at 10×, 100× and 1000× scale?**
12. **TRADE-OFFS — What am I giving up by choosing this approach?**
13. **ALTERNATIVES — What competing solution might be better?**
14. **HOW TO APPLY — How do I recognise this problem at work?**
15. **EXERCISE — Can I prove that I understood it?**

The reader should finish a lesson being able to reason about the technology rather than merely describe it.

---

# 2. Intended Audience

Target:

* Experienced Data Engineers
* Senior Software Engineers
* Backend Engineers
* Platform Engineers
* Distributed Systems Engineers
* ML Engineers working with data infrastructure
* SREs supporting data platforms
* Engineers preparing for Staff-level Data Engineering responsibilities

Assume the learner already knows:

* Python
* SQL
* Linux basics
* APIs
* Docker
* basic databases
* basic cloud concepts
* basic software engineering
* Git

Do NOT spend time explaining:

* what an API is
* what JSON is
* what SQL SELECT means
* what Docker is
* elementary Python

But DO explain distributed-data concepts from first principles.

---

# 3. Three Levels of Understanding

Every substantial topic must teach at three levels.

## Level 1 — Intuition

Use:

* simple language
* analogies
* animated/interactive diagrams where useful
* progressively revealed diagrams
* concrete workloads
* relatable examples

The learner should be able to explain the concept without jargon.

---

## Level 2 — Engineering

Cover:

* algorithms
* data structures
* execution model
* architecture
* storage layout
* partitioning
* networking
* memory
* concurrency
* scheduling
* query execution
* state management
* consistency
* reliability

The learner should understand how the system actually works.

---

## Level 3 — Production

Cover:

* scale limits
* bottlenecks
* skew
* hot partitions
* backpressure
* observability
* failures
* recovery
* cost
* security
* operational complexity
* debugging
* upgrade/migration problems
* real-world trade-offs

The learner should be able to operate the system rather than merely build a demo.

---

# 4. Global Running Use Cases

Do not teach concepts in isolation.

Create several recurring fictional production systems that evolve throughout the course.

Use at least these:

## A. SaaS Analytics Platform

Millions of users generate product events.

Need:

* event collection
* streaming
* transformations
* warehouse/lakehouse
* dashboards
* ad-hoc querying
* retention
* governance

Potential technologies:

Kafka → Flink/Spark → Iceberg → Trino → ClickHouse

---

## B. Security / Observability Platform

Millions to billions of:

* logs
* metrics
* traces
* security events

Requirements:

* very high ingestion rate
* high-cardinality data
* late events
* enrichment
* real-time detection
* historical investigation
* retention tiers

Use this extensively for:

* Kafka
* Flink
* ClickHouse
* Pinot
* time-series systems
* partitioning
* indexing
* cardinality
* stream processing

---

## C. E-Commerce Platform

Data includes:

* orders
* payments
* users
* inventory
* clickstream
* recommendations

Use to demonstrate:

* CDC
* batch + streaming
* lakehouse
* data modelling
* orchestration
* Graph DBs
* real-time analytics

---

## D. IoT Platform

Millions of devices send:

timestamp + device + sensor + value

Use for:

* time series
* Kafka
* windows
* event time
* downsampling
* retention
* time-series databases
* anomaly detection

---

## E. Recommendation / Fraud Graph

Relationships:

User → Device → IP → Transaction → Merchant

Use to demonstrate:

* graph modelling
* graph traversal
* connected components
* graph DBs
* fraud-ring detection
* recommendations

---

# 5. Curriculum Structure

Organise the site around **engineering problems**, while still providing technology-specific deep dives.

---

# PHASE 0 — Data Systems Mental Models

Before teaching products, build the mental foundations.

Teach:

## Data at Scale

Start with:

> You have 1 GB.

Then:

> 100 GB.

Then:

> 10 TB.

Then:

> 1 PB/day.

Ask repeatedly:

**What breaks next?**

Teach:

* compute vs storage
* local vs distributed processing
* throughput vs latency
* batch vs streaming
* vertical vs horizontal scaling

---

## Data Movement

Teach:

* network cost
* serialization
* compression
* data locality
* shuffle
* partition movement

Interactive idea:

Let the learner change:

* number of nodes
* data size
* partition count
* network bandwidth

Visualise where execution time goes.

---

## Partitioning

Teach this as one of the most important concepts in the entire academy.

Explain:

* partition keys
* hash partitioning
* range partitioning
* time partitioning
* skew
* hot partitions
* partition pruning
* repartitioning

Show:

UserID partitioning vs Country vs Timestamp.

Demonstrate why apparently reasonable partition choices fail.

---

## Distributed Execution

Teach:

Job

→ stages

→ tasks

→ partitions

→ workers

Use this later to connect:

* Spark
* Flink
* Trino
* Ray

---

# PHASE 1 — Apache Spark

Start with the problem.

> One machine cannot efficiently process a 5 TB dataset.

Then derive distributed processing.

Teach:

## Intuition

Dataset divided into partitions.

Workers process partitions.

Some operations can happen independently.

Others require data to move between workers.

That movement is the **shuffle**.

Build Spark concepts from this mental model.

---

## Concepts

Cover deeply:

* Driver
* Executors
* Cluster Manager
* DAG
* Jobs
* Stages
* Tasks
* Partitions
* Transformations
* Actions
* Lazy evaluation

Then:

* Catalyst
* Tungsten
* whole-stage code generation
* predicate pushdown
* column pruning
* adaptive query execution
* cost-based optimization

---

## The Shuffle

Make this a Gold Standard lesson.

Explain visually:

before shuffle:

Worker A → customer 1,5,8
Worker B → customer 2,5,7
Worker C → customer 1,3,5

GROUP BY customer

requires redistribution.

Animate the records moving.

Teach:

* shuffle write
* shuffle read
* exchange
* spill
* serialization
* partition count
* skew

---

## Gotchas

Include:

* too few partitions
* too many partitions
* small files
* collect()
* driver OOM
* executor OOM
* skewed joins
* exploding joins
* caching everything
* unnecessary repartition()
* Python UDF performance

---

## Hands-on

Create runnable examples using generated datasets.

Examples:

* 10M event dataset locally
* joins
* aggregations
* partitioning
* intentional skew

Learner should benchmark before/after optimization.

---

# PHASE 2 — Apache Kafka

Do NOT begin with brokers.

Start with:

> Hundreds of services generate events faster than downstream consumers can process them.

Ask:

How do producers and consumers evolve independently?

Derive the append-only log.

---

## Mental Model

Kafka topic:

Partition 0:

0 → 1 → 2 → 3 → 4 → 5

Partition 1:

0 → 1 → 2 → 3 → 4

Consumers maintain positions.

Use visual simulations.

---

## Teach

* brokers
* topics
* partitions
* offsets
* producer
* consumer
* consumer groups
* replication
* ISR
* leader/follower
* acknowledgements
* retention
* compaction

Then:

* batching
* compression
* throughput
* ordering
* idempotent producers
* transactions
* exactly-once semantics

---

## Critical Questions

Why doesn't Kafka simply use one partition?

Why does increasing partitions increase concurrency?

Why can excessive partitions hurt?

Why is ordering only guaranteed within a partition?

What happens during consumer rebalance?

---

## Production Gotchas

* hot partitions
* consumer lag
* rebalance storms
* poison messages
* schema evolution
* oversized messages
* incorrect partition keys
* retention surprises
* disk saturation
* broker imbalance

---

# PHASE 3 — Stream Processing

Cover:

* Kafka Streams
* Apache Flink
* Spark Structured Streaming

Begin with:

> Processing events individually is easy.

Then:

> Calculate "failed logins per user during the last five minutes."

Now time becomes part of computation.

---

# Time Semantics

Teach deeply:

* processing time
* event time
* ingestion time
* out-of-order events
* late events
* watermarks

Create an animation showing:

Event timestamp:

10:01
10:03
10:02
10:07
10:04

while processing order differs.

Ask:

"When does the 10:00–10:05 window close?"

---

# Stateful Stream Processing

Explain:

stream + state

State examples:

* count per user
* previous value
* rolling average
* session state

Teach:

* checkpointing
* recovery
* state backend
* exactly-once processing
* backpressure

---

# PHASE 4 — Apache Flink

Deep dive into:

* JobManager
* TaskManagers
* operators
* operator chains
* task slots
* state
* checkpoints
* savepoints

Compare:

Flink vs Kafka Streams vs Spark Structured Streaming.

Not with a feature matrix alone.

Compare using actual workloads.

---

# PHASE 5 — Apache Airflow

Start with:

> My data pipeline contains 25 dependent jobs.

Ask:

How do we coordinate them reliably?

Derive orchestration.

Teach:

* DAGs
* Tasks
* Operators
* Scheduler
* Executor
* Workers
* metadata DB

Then:

* retries
* idempotency
* dependencies
* backfills
* catchup
* sensors
* dynamic DAGs
* task mapping
* SLAs

---

## Very Important

Clearly distinguish:

**Orchestration**

from:

**Data processing**

Airflow should schedule Spark.

Airflow should generally not process a 1 TB dataset itself.

Explain why.

---

# PHASE 6 — Lakehouse and Table Formats

First show the problem with raw Parquet files.

Imagine:

```
s3://events/
    part-001.parquet
    part-002.parquet
    part-003.parquet
```

Then concurrently:

* readers query
* writers append
* records are updated
* schema changes
* jobs fail halfway

Ask:

**Where is the table?**

Derive why table metadata is necessary.

---

# Apache Iceberg

Teach:

* metadata files
* manifest lists
* manifests
* data files
* snapshots

Visual hierarchy:

Table

→ Metadata

→ Snapshot

→ Manifest List

→ Manifest

→ Data Files

Teach:

* schema evolution
* partition evolution
* hidden partitioning
* time travel
* snapshot isolation
* optimistic concurrency
* compaction

---

# Hudi

Teach:

* Copy-on-Write
* Merge-on-Read
* timeline
* file groups
* record keys
* incremental processing
* upserts
* CDC

---

# Delta Lake

Teach:

* transaction log
* JSON commits
* checkpoints
* optimistic transactions
* file actions
* VACUUM
* OPTIMIZE

---

# Compare Table Formats

Do not ask:

> Which is best?

Ask:

* Are writes mostly append?
* Frequent updates?
* CDC?
* Streaming ingestion?
* Engine interoperability?
* Cloud object storage?
* Need time travel?
* Operational expertise?

Let workload determine the choice.

---

# PHASE 7 — Trino

Start with:

> My data exists in Iceberg, PostgreSQL, MySQL and object storage.

How can analysts query everything without copying all the data first?

Derive federated query engines.

Teach:

* Coordinator
* Workers
* Connectors
* Stages
* Tasks
* Splits
* Exchanges

Show distributed query execution visually.

---

## Important Concepts

* predicate pushdown
* column pruning
* partition pruning
* dynamic filtering
* join distribution
* broadcast joins
* partitioned joins
* CBO
* statistics

---

# PHASE 8 — ClickHouse

This must be a major module.

Start with:

> We receive hundreds of millions of events every day and need dashboard queries to return in hundreds of milliseconds.

A traditional OLTP database becomes increasingly expensive or slow for this access pattern.

Ask why.

Derive column-oriented analytical storage.

---

## Why Columnar?

Show a table:

timestamp | user | country | endpoint | latency | status

Query:

```sql
SELECT country, avg(latency)
FROM requests
GROUP BY country;
```

Row-store reads most/all columns.

Column store primarily needs:

country + latency.

Visualize the difference.

---

# ClickHouse Architecture

Teach:

* databases
* tables
* parts
* columns
* granules
* marks
* sparse primary indexes

Then:

## MergeTree

Explain MergeTree carefully.

Visual:

INSERT

→ immutable part

INSERT

→ immutable part

INSERT

→ immutable part

background merge

→ larger part

Explain why ClickHouse does not behave like PostgreSQL UPDATE-heavy OLTP.

---

## ORDER BY

Treat ClickHouse `ORDER BY` as a critical physical-design decision.

Explain:

Why:

```sql
ORDER BY (customer_id, timestamp)
```

behaves very differently from:

```sql
ORDER BY (timestamp, customer_id)
```

for different query patterns.

---

## Teach

* MergeTree
* primary index
* granules
* marks
* partitions
* ORDER BY
* compression
* LowCardinality
* codecs
* data skipping indexes
* projections
* materialized views

Then:

* ReplicatedMergeTree
* Distributed tables
* sharding
* replication

---

## Ingestion

Show:

Kafka

→ ClickHouse Kafka Engine / connector

→ Materialized View

→ MergeTree

Discuss production alternatives rather than presenting one pattern as universally correct.

---

## Gotchas

Cover:

* too many small inserts
* too many parts
* poor ORDER BY choice
* excessive partitions
* mutations
* UPDATE expectations
* high-cardinality GROUP BY
* FINAL
* distributed query amplification
* badly designed materialized views

---

## Compare

ClickHouse vs:

* PostgreSQL
* Elasticsearch/OpenSearch
* Pinot
* Druid
* BigQuery
* Snowflake
* Trino over Iceberg

Explain based on workload.

---

# PHASE 9 — Apache Pinot

Teach Pinot as a real-time OLAP system.

Cover:

* Controller
* Broker
* Server
* Minion
* segments

Teach:

* real-time segments
* offline segments
* inverted index
* range index
* text index
* star-tree index

Use cases:

* user-facing analytics
* clickstreams
* anomaly dashboards
* fraud analytics

Compare carefully with ClickHouse.

---

# PHASE 10 — Time Series Data

Do NOT make this merely a "TimescaleDB tutorial."

Start with the nature of time-series data.

Example:

```text
10:00 CPU 40
10:01 CPU 43
10:02 CPU 95
10:03 CPU 72
```

Ask:

What makes timestamped data different?

---

# Core Time-Series Concepts

Teach:

* timestamp
* observation
* metric
* label/tag
* dimensions
* samples
* irregular sampling
* missing values

---

## Time Windows

Explain:

* tumbling windows
* sliding windows
* hopping windows
* session windows

Use interactive timelines.

---

## Aggregation

Teach:

raw samples

→ 1-minute aggregates

→ hourly aggregates

→ daily aggregates

Explain why systems downsample data.

---

## Functions

Teach intuitively:

* rate
* derivative
* moving average
* EWMA
* rolling statistics
* percentile
* quantile

Explain where each is useful.

---

## Time-Series Storage

Explain:

* append-heavy workloads
* time partitioning
* compression
* delta encoding
* retention
* TTL
* downsampling
* rollups

---

## Cardinality

Make cardinality a Gold Standard lesson.

Start:

```
http_requests{
  service="api",
  region="india"
}
```

Then add:

```
user_id="<millions of users>"
```

Explain how series count explodes.

Interactive:

labels × possible values → resulting number of series.

---

## TSDB Systems

Compare:

* Prometheus
* VictoriaMetrics
* TimescaleDB
* InfluxDB
* ClickHouse

Do NOT simply provide feature tables.

Use workloads.

---

# PHASE 11 — Ray

Start with:

> Python makes it easy to write one process.

Then:

> How do I execute Python functions across 32 machines?

Introduce distributed Python.

---

## Core Ray Mental Model

Start with:

```python
@ray.remote
def process(data):
    ...
```

Explain how a normal function becomes a distributed task.

---

## Teach

* Ray Core
* tasks
* actors
* ObjectRefs
* object store
* scheduler
* resources
* task dependencies
* ownership

---

## Ray Actors

Explain why distributed applications sometimes need persistent mutable state.

Example:

```python
@ray.remote
class Counter:
    ...
```

Connect actors to:

* model servers
* coordinators
* workers
* simulation state

---

## Ray Data

Teach:

* distributed datasets
* blocks
* transformations
* streaming execution
* pipelining

Use:

ETL → preprocessing → inference

as a running example.

---

## Scheduling

Teach:

* CPU resources
* GPU resources
* custom resources
* locality
* placement groups

---

## Fault Tolerance

Explain:

* task retry
* actor failure
* object loss
* lineage reconstruction

---

# Ray vs Spark

This is mandatory.

Do not say one is newer or better.

Compare mental models.

Spark:

DataFrame/Dataset

→ DAG

→ stages/tasks

Ray:

Python tasks + actors

→ arbitrary distributed application graph

Workloads:

### Distributed SQL/ETL

Usually Spark.

### Distributed Python simulation

Ray may fit better.

### ML preprocessing + training + serving

Ray may fit well.

### Large SQL transformations

Spark likely has a stronger ecosystem.

Teach the learner to select based on computation shape.

---

# PHASE 12 — NoSQL Data Modelling

Do NOT teach NoSQL as:

"Here are five databases."

Begin from workloads.

Teach:

## Key-Value

Redis / DynamoDB

Problem:

> I know the key and need the value immediately.

---

## Document

MongoDB

Problem:

> My application data naturally belongs together and evolves structurally.

---

## Wide Column

Cassandra / ScyllaDB / HBase

Problem:

> I need extremely high distributed writes and known query patterns.

---

# CAP and PACELC

Explain with production scenarios rather than abstract diagrams alone.

Cover:

* consistency
* availability
* partitions
* latency trade-offs

---

# PHASE 13 — Graph Databases

Make this a major module.

Start with relational modelling.

Tables:

Users

Devices

Transactions

Merchants

IPs

Then ask:

> Find all accounts connected within three hops to an IP address used by a fraudulent transaction.

Show the SQL complexity.

Then introduce graph modelling.

---

# Graph Mental Model

```
(User)
   |
USES
   |
(Device)
   |
CONNECTED_FROM
   |
(IP)
```

Teach:

* nodes
* relationships/edges
* properties
* labels
* directions

---

## Graph Modelling

Emphasize:

**The hardest part of graph databases is often the model, not Cypher syntax.**

Show multiple possible models for the same domain and discuss their trade-offs.

---

## Cypher

Teach gradually:

```cypher
MATCH (u:User)-[:USES]->(d:Device)
RETURN u, d
```

Then:

* filtering
* paths
* variable-length traversal
* aggregation
* shortest path

---

## Graph Algorithms

Teach intuitively:

* BFS
* DFS
* shortest path
* connected components
* PageRank
* centrality
* community detection

Connect each algorithm to a real business problem.

---

## Use Cases

### Fraud

Account → Device → IP → Transaction

Detect fraud rings.

### Recommendations

User → Product → User

### IAM / Security

User → Role → Permission → Resource

Ask:

> What resources can this compromised identity eventually reach?

### Knowledge Graphs

Concept → Relationship → Entity

### Lineage

Dataset → Job → Table → Dashboard

---

## Neo4j

Use Neo4j for practical exercises.

Teach:

* labels
* relationships
* indexes
* constraints
* Cypher
* EXPLAIN
* PROFILE

---

## Gotchas

* supernodes
* high-degree nodes
* unlimited traversal depth
* poor graph models
* treating graph DB as a universal replacement for relational databases
* distributed graph complexity
* expensive traversals

---

# Graph DB vs Relational DB

Give examples where PostgreSQL is preferable.

This distinction is mandatory.

The learner should never finish the module thinking:

> "Graph databases are better for anything involving relationships."

The correct intuition should be:

> "Graph databases become interesting when relationships and multi-hop traversal are first-class parts of the access pattern."

---

# PHASE 14 — Metadata, Catalogues and Lineage

Teach:

* technical metadata
* operational metadata
* business metadata

Then:

* ownership
* discovery
* schemas
* tags
* lineage
* governance

Cover:

* DataHub
* Apache Atlas
* Amundsen
* OpenLineage

---

## Lineage

Visualise:

Kafka

↓

raw_events

↓

Spark job

↓

clean_events

↓

dbt model

↓

revenue_dashboard

Then ask:

> The `country` column is wrong. What downstream assets are impacted?

---

# PHASE 15 — Data Quality

Teach why data pipelines can be operationally healthy while producing incorrect data.

Cover:

* schema validation
* null checks
* uniqueness
* referential integrity
* distributions
* freshness
* completeness
* anomaly detection

Tools:

* Great Expectations
* Deequ
* dbt tests

---

# PHASE 16 — Data Security

Cover:

* encryption at rest
* encryption in transit
* authentication
* authorization
* RBAC
* ABAC
* column-level access
* row-level access
* masking
* tokenisation
* KMS
* secrets

Then:

* PII
* GDPR
* data residency
* deletion
* retention

Use real data-platform architectures.

---

# PHASE 17 — JupyterHub and Shared Compute

Teach:

* JupyterHub architecture
* authentication
* kernels
* resource allocation
* Kubernetes
* multi-tenancy

Explain where notebook environments belong in production data platforms.

---

# 6. Architecture-First Learning

Every technology page should show where it belongs in an architecture.

For example:

```text
Applications
     │
     ▼
   Kafka
     │
 ┌───┴─────────┐
 ▼             ▼
Flink        Spark
 │             │
 ▼             ▼
ClickHouse   Iceberg
 │             │
 ▼             ▼
Dashboard    Trino
```

Then ask:

Why two paths?

Explain:

Hot path:

Kafka → Flink → ClickHouse

Cold/historical path:

Kafka → Object Storage → Iceberg → Trino

Discuss:

* latency
* cost
* retention
* durability
* query patterns

---

# 7. Cross-Technology Decision Guides

Create dedicated decision pages.

Examples:

# Spark vs Flink

Compare:

* batch
* streaming
* latency
* state
* event time
* ecosystem

---

# ClickHouse vs Pinot

Use workloads such as:

* observability
* customer-facing dashboards
* ad-hoc analytics
* streaming analytics

---

# ClickHouse vs Trino

Teach the fundamental distinction:

ClickHouse owns/manages analytical storage.

Trino is primarily a distributed query engine over external data sources.

Then discuss when they overlap.

---

# Spark vs Ray

Discuss:

* DataFrame-centric computation
* distributed arbitrary Python
* ML
* ETL
* actor-style workloads

---

# Kafka vs Database

Explain why Kafka is not simply:

> "a faster database."

---

# Graph vs Relational

Base the decision on traversal complexity and access patterns.

---

# Time-Series DB vs General OLAP

Compare using:

* retention
* cardinality
* aggregation
* labels
* query shape
* compression

---

# 8. Visual Learning Requirements

Visual learning is mandatory.

Do not decorate pages with meaningless diagrams.

A diagram must explain a mechanism.

Use Mermaid where appropriate.

Prefer:

* packet/data movement
* partitions
* worker execution
* tree structures
* distributed topology
* timelines
* query plans
* state machines

---

# Progressive Diagrams

Do not immediately show a complex production architecture.

Build it gradually.

Example Kafka:

### Step 1

Producer → Broker

### Step 2

Producer → Topic

### Step 3

Topic → Partitions

### Step 4

Partition → Replicas

### Step 5

Consumer Group

This progressive approach should appear throughout the site.

---

# 9. Interactive Explainers

Create lightweight HTML/JavaScript simulations where they materially improve understanding.

Examples:

## Kafka Partition Simulator

Controls:

* messages/sec
* partition count
* consumer count
* processing rate

Visualize:

* partition assignment
* lag
* throughput

---

## Spark Shuffle Simulator

Controls:

* workers
* partitions
* key distribution
* skew

Show data moving between executors.

---

## ClickHouse ORDER BY Explorer

Dataset:

timestamp
customer
endpoint
latency

Allow learner to choose ORDER BY.

Visualize which granules need scanning for different queries.

---

## Time-Series Cardinality Calculator

Inputs:

services = 100
regions = 10
endpoints = 500
users = 1,000,000

Show series explosion depending on selected labels.

---

## Graph Traversal Explorer

Display:

User

→ Device

→ IP

→ Account

Allow traversal depth:

1
2
3
4

Highlight number of visited nodes.

Demonstrate graph-explosion problems.

---

## Trino Query Execution Explorer

Show:

SQL

→ coordinator

→ stages

→ tasks

→ splits

→ workers

→ exchange

---

# 10. Hands-On Labs

The site must contain executable labs.

Prefer laptop-runnable workloads.

Do NOT require the learner to actually own a 1 PB cluster.

Scale the workload while preserving the behaviour.

Use:

* Docker Compose
* Python
* SQL
* Jupyter
* synthetic datasets

Where appropriate use:

* Kafka
* Spark
* Flink
* ClickHouse
* Trino
* Neo4j

locally.

---

# Lab Philosophy

A lab must demonstrate a concept.

Bad lab:

> Install ClickHouse and run SELECT.

Good lab:

> Insert the same dataset into two ClickHouse tables with different ORDER BY keys and measure query behaviour.

Bad lab:

> Run Spark word count.

Good lab:

> Create a skewed join, observe one slow task, apply salting or repartitioning, and compare execution.

---

# 11. Failure Labs

Some exercises should deliberately break systems.

Examples:

### Kafka

Kill a broker.

Observe leadership changes.

---

### Spark

Create data skew.

Observe one task dominating runtime.

---

### Flink

Kill a worker.

Observe checkpoint recovery.

---

### ClickHouse

Generate many tiny inserts.

Observe parts increasing and merge pressure.

---

### Graph

Create a supernode.

Run traversal.

Observe fan-out.

---

The learner should become comfortable investigating failure.

---

# 12. Production Debugging Sections

Every major system should include:

# "Production Is On Fire"

Give an incident.

Example:

> Kafka consumer lag went from 30 seconds to 45 minutes.

Provide telemetry.

Ask the learner to investigate:

* producer throughput?
* consumer throughput?
* rebalance?
* hot partition?
* downstream dependency?
* GC?
* network?
* storage?

Reveal clues progressively.

Do NOT immediately reveal the answer.

---

# 13. Observability

Every major system should show:

## Metrics

For example Kafka:

* records/sec
* bytes/sec
* consumer lag
* ISR
* under-replicated partitions
* request latency

Spark:

* task duration
* shuffle bytes
* spill
* executor memory
* GC
* skew

ClickHouse:

* query duration
* rows read
* bytes read
* parts
* merge queue
* memory
* disk IO

---

## Logs

Show representative log snippets where helpful.

Teach the learner what they mean.

---

## Tracing / Query Plans

For engines supporting query plans:

Show:

```text
EXPLAIN
```

and teach how to interpret it.

---

# 14. Cost Engineering

Cost must be part of architecture.

Example:

You process:

1 PB/day.

Ask:

Should all of this remain in ClickHouse for one year?

Derive tiered storage.

Example:

Hot:

ClickHouse — 7 days

Warm:

Iceberg — 90 days

Cold:

Object storage archive — 1 year

Explain why.

Teach:

* storage cost
* compute cost
* data transfer
* replicas
* query frequency
* retention

---

# 15. Scale Ladder

For major architecture exercises, use:

## Stage 1

1 GB/day

## Stage 2

100 GB/day

## Stage 3

10 TB/day

## Stage 4

1 PB/day

At each stage ask:

**What breaks next?**

The architecture should evolve.

Do not present a 1 PB architecture to solve a 1 GB problem.

Teach the learner to avoid premature distribution.

---

# 16. Architecture Challenges

Create guided exercises.

Examples:

## Build an Observability Platform

Requirements:

5 million events/sec

Queries:

* last 15 minutes
* last 24 hours
* historical investigation

Retention:

365 days

Derive:

Kafka

→ Flink

→ ClickHouse

*

Object Storage

→ Iceberg

→ Trino

Explain every decision.

---

## Build an IoT Platform

Requirements:

10M devices

one sample every 30 seconds

Need:

* latest state
* one-hour charts
* one-year trend
* anomaly detection

Learner should reason about:

* Kafka
* time-series storage
* downsampling
* object storage
* streaming

---

## Build Fraud Detection

Model:

users

cards

devices

IPs

merchants

transactions

Use:

Kafka

→ Flink

→ Graph store

*

ClickHouse

Discuss why one database may not solve every access pattern.

---

# 17. "How To Apply This Monday" Section

Every major concept page must end with:

# How to Apply This at Work

Example for partitioning:

When reviewing a pipeline this week, ask:

1. What is the partition key?
2. What is its cardinality?
3. Is distribution uniform?
4. Can one key dominate traffic?
5. Does the query filter align with partitioning?
6. What happens when the number of workers doubles?

For ClickHouse:

1. What are the dominant WHERE predicates?
2. Does ORDER BY align with them?
3. Are inserts sufficiently batched?
4. Are parts accumulating?
5. Are queries scanning too many rows?

Make the knowledge operational.

---

# 18. "Gotchas" Must Be Prominent

Do not hide caveats at the bottom of pages.

Create visually distinct:

> ⚠️ Production Gotcha

blocks.

Example:

> ⚠️ More Kafka partitions do not automatically mean more throughput forever.

Then explain why.

---

# 19. Avoid Cargo-Cult Architecture

Explicitly teach:

Do NOT use:

* Kafka because "microservices use Kafka"
* Spark because data is "big"
* Kubernetes because systems should be "cloud native"
* Graph DB because the domain contains relationships
* Ray because the workload uses Python
* ClickHouse because queries should be fast
* Flink because streaming sounds modern

Start with the workload.

Then derive the technology.

---

# 20. Build a Technology Selection Framework

For every major decision, consider:

## Workload

* batch?
* streaming?
* transactional?
* analytical?
* graph traversal?
* time-series?

## Volume

* GB?
* TB?
* PB?

## Velocity

events/sec?

## Latency

* milliseconds?
* seconds?
* minutes?
* hours?

## Access Pattern

* point lookup?
* scan?
* aggregation?
* join?
* traversal?
* window?

## Mutation Pattern

* append?
* update?
* delete?
* upsert?

## Consistency

How fresh/correct must results be?

## Retention

How long?

## Operational Complexity

Who will run the platform?

## Cost

What is affordable?

---

# 21. Repository Structure

Use something similar to:

```text
data-engineering-academy/
│
├── mkdocs.yml
├── requirements.txt
├── README.md
│
├── docs/
│   ├── index.md
│   ├── start-here.md
│   ├── how-to-study.md
│   │
│   ├── foundations/
│   │   ├── index.md
│   │   ├── scale.md
│   │   ├── partitions.md
│   │   ├── data-movement.md
│   │   ├── distributed-execution.md
│   │   └── batch-vs-stream.md
│   │
│   ├── spark/
│   ├── kafka/
│   ├── flink/
│   ├── airflow/
│   │
│   ├── lakehouse/
│   │   ├── why-table-formats.md
│   │   ├── iceberg.md
│   │   ├── hudi.md
│   │   └── delta.md
│   │
│   ├── query-engines/
│   │   └── trino.md
│   │
│   ├── olap/
│   │   ├── columnar-storage.md
│   │   ├── clickhouse.md
│   │   └── pinot.md
│   │
│   ├── time-series/
│   │   ├── index.md
│   │   ├── time-semantics.md
│   │   ├── windows.md
│   │   ├── cardinality.md
│   │   ├── downsampling.md
│   │   └── tsdbs.md
│   │
│   ├── distributed-python/
│   │   └── ray.md
│   │
│   ├── databases/
│   │   ├── nosql.md
│   │   ├── cassandra.md
│   │   └── dynamodb.md
│   │
│   ├── graph/
│   │   ├── graph-thinking.md
│   │   ├── graph-modelling.md
│   │   ├── neo4j.md
│   │   ├── graph-algorithms.md
│   │   └── graph-vs-relational.md
│   │
│   ├── quality/
│   ├── metadata/
│   ├── security/
│   ├── notebooks/
│   │
│   ├── architectures/
│   │   ├── observability.md
│   │   ├── ecommerce.md
│   │   ├── iot.md
│   │   ├── fraud.md
│   │   └── analytics-platform.md
│   │
│   ├── comparisons/
│   │   ├── spark-vs-flink.md
│   │   ├── spark-vs-ray.md
│   │   ├── clickhouse-vs-pinot.md
│   │   ├── clickhouse-vs-trino.md
│   │   ├── graph-vs-relational.md
│   │   └── tsdb-vs-olap.md
│   │
│   ├── labs/
│   ├── incidents/
│   └── reference/
│
└── simulations/
```

---

# 22. Required Page Template

EVERY major concept page should roughly follow:

```markdown
# Topic

## The Problem

## Use Case

## Why This Gets Hard

## Intuition

## Mental Model

## What Is It?

## Build It From First Principles

## Architecture

## How It Works Internally

## Walk Through One Request / Event / Query

## Hands-On

## What Happens at Scale?

## Production Gotchas

## Failure Modes

## Production Is On Fire

## How to Debug It

## Performance

## Cost

## Security

## Alternatives

## Trade-Offs

## When to Use It

## When NOT to Use It

## How to Apply This at Work

## Reasoning Exercise

## Key Takeaways

## Further Reading
```

The exact headings may change naturally, but the reasoning progression must remain.

---

# 23. Writing Style

Match the tone of a senior engineer explaining a difficult system to another capable engineer.

Tone:

* direct
* technical
* conversational
* opinionated where justified
* practical
* concise where concepts are simple
* deep where concepts are subtle

Avoid:

* academic filler
* marketing language
* generic AI prose
* excessive bullet spam
* repeated definitions
* meaningless "best practices"
* pretending trade-offs don't exist

Prefer statements like:

> Increasing the partition count increases the maximum available consumer parallelism. It does not mean your workload will automatically become faster.

Instead of:

> Kafka partitions are an important feature that provide many benefits including scalability and performance.

---

# 24. Questions Throughout the Content

Frequently stop the learner and ask:

> What do you think happens if one customer generates 40% of all events?

> Why can't ClickHouse simply update one row in-place like PostgreSQL?

> If the consumer crashes after writing to the database but before committing its Kafka offset, what happens?

> Why would a graph database make this traversal easier?

> What happens to Prometheus if `user_id` becomes a label?

Questions should cause reasoning before explanation.

---

# 25. Code Requirements

Include practical code where code improves comprehension.

Preferred languages:

* Python
* SQL

Use Java/Scala only where a technology requires it or where it substantially improves explanation.

All code should:

* run
* be concise
* demonstrate the concept
* include expected output where useful

Do not dump giant production applications into lessons.

---

# 26. Data Sets

Generate realistic synthetic datasets so everything can be run locally.

Examples:

## events

```text
timestamp
customer_id
user_id
service
endpoint
region
latency_ms
status_code
bytes
```

## transactions

```text
transaction_id
user_id
device_id
merchant_id
amount
timestamp
country
```

## metrics

```text
timestamp
host
service
metric
value
region
```

Reuse datasets across technologies where useful.

This lets the learner see the same workload represented differently in:

* Spark
* Kafka
* ClickHouse
* Iceberg
* Trino
* Time-Series DB
* Neo4j

---

# 27. Avoid Fake Scale

Do not claim:

> Process a 1 TB dataset locally.

Instead teach a scaled experiment.

Example:

Generate 5–20 GB or even smaller if appropriate.

Then explain mathematically:

If:

10 GB causes X shuffle bytes,

estimate what happens at:

100 GB

1 TB

10 TB

Separate measured results from extrapolation.

---

# 28. Learning Roadmap

Create a roadmap rather than a rigid "Day 1, Day 2" schedule.

Recommended progression:

### Phase 1 — Foundations

Partitioning
Distributed execution
Data movement
Storage
Query patterns

↓

### Phase 2 — Core Data Plane

Kafka
Spark
Flink

↓

### Phase 3 — Storage

Parquet
Iceberg/Hudi/Delta
NoSQL

↓

### Phase 4 — Query & Analytics

Trino
ClickHouse
Pinot

↓

### Phase 5 — Specialised Data Models

Time Series
Graph

↓

### Phase 6 — Distributed Applications

Ray

↓

### Phase 7 — Platform Engineering

Airflow
Metadata
Quality
Security
JupyterHub

↓

### Phase 8 — Architecture

End-to-end systems
Failures
Scaling
Cost optimization

---

# 29. Capstone

Create one substantial final project.

# Build a Mini Data Platform

Architecture:

```text
                    ┌──────────────┐
                    │ Event Source │
                    └──────┬───────┘
                           │
                           ▼
                         Kafka
                           │
                  ┌────────┴─────────┐
                  ▼                  ▼
                Flink              Spark
                  │                  │
                  ▼                  ▼
             ClickHouse           Iceberg
                  │                  │
                  ▼                  ▼
             Live Analytics        Trino
                  │                  │
                  └────────┬─────────┘
                           ▼
                       Analytics
```

Optional branches:

Time-Series storage

Graph DB for entity relationships

Ray for distributed ML/enrichment

Airflow for orchestration

DataHub/OpenLineage for metadata

Great Expectations/dbt for quality

---

# Capstone Tasks

Learner must:

1. ingest events
2. partition them
3. process streaming data
4. write analytical data
5. maintain historical lakehouse data
6. query both
7. model relationships
8. model time-series metrics
9. introduce data quality checks
10. instrument the pipeline
11. deliberately trigger a failure
12. diagnose it
13. document scaling strategy
14. document cost strategy
15. justify every major technology choice

---

# 30. Exit Criteria

A learner has completed the academy only when they can answer questions such as:

* Why is data partitioned?
* What creates a Spark shuffle?
* Why does skew cause stragglers?
* How does Kafka preserve ordering?
* What does consumer lag actually tell us?
* Why is event time different from processing time?
* How does Flink recover state?
* Why do table formats exist on top of Parquet?
* How does an Iceberg snapshot work?
* Why can Trino query data it does not own?
* Why is ClickHouse fast for analytical workloads?
* Why does ClickHouse ORDER BY matter?
* Why do too many ClickHouse parts cause problems?
* Why does time-series cardinality explode?
* Why is downsampling often necessary?
* What is the difference between Ray tasks and actors?
* When would Ray make more sense than Spark?
* When does graph modelling outperform relational modelling?
* Why can graph traversals explode?
* When should a Graph DB NOT be used?
* When should Pinot be preferred over ClickHouse?
* What should live in the hot path versus the historical path?
* How would you debug a pipeline whose latency suddenly increased 10×?
* How would the architecture change from 10 GB/day to 1 PB/day?

More importantly:

> Give the learner an unfamiliar data problem and they should be able to reason toward a sensible architecture without memorising a predefined stack.

---

# 31. Quality Bar

Each major module should be good enough that an experienced engineer can say:

> "I understood not just what this technology does, but why somebody had to invent it."

Every important architecture should make the learner ask:

> "What problem made this component necessary?"

Every technology choice should be explainable through:

**workload → constraints → access pattern → trade-offs → architecture**

rather than:

**technology → list of features**

---

# 32. Final Instruction

Generate the complete repository, not merely an outline.

Start by establishing:

1. information architecture
2. MkDocs configuration
3. common styling/components
4. reusable page structure
5. curriculum navigation
6. learning roadmap

Then implement the content module by module.

For important concepts, prioritize:

**intuition and visual explanation before implementation details.**

For each section repeatedly use the teaching loop:

> **Use Case → Why → Intuition → What → Internals → How → Gotchas → Apply**

Do not optimize for the number of pages.

Optimize for the moment when a learner says:

> **"Ah — now I understand why this system works this way."**

That is the standard for the entire academy.
