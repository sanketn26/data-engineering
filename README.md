# Data Engineering Academy

[![Deploy to GitHub Pages](https://github.com/sanketn26/data-engineering/actions/workflows/deploy.yml/badge.svg)](https://github.com/sanketn26/data-engineering/actions/workflows/deploy.yml)
[![Validate PR](https://github.com/sanketn26/data-engineering/actions/workflows/validate.yml/badge.svg)](https://github.com/sanketn26/data-engineering/actions/workflows/validate.yml)
[![Buy Me A Coffee](https://img.shields.io/badge/☕-Buy%20me%20a%20coffee-FFDD00?style=flat-square)](https://buymeacoffee.com/sanketn)

An intuition-first, production-focused academy for engineers who want to
understand **how modern data systems actually work** without reading the course
like a product manual.

This is not a tool tutorial. The objective is:

> Given a data workload — its scale, latency, access patterns, reliability, and cost — derive an architecture, choose technologies, explain trade-offs, predict failure, debug it in production, and evolve it as scale increases.

**[Open the Academy →](https://sanketn26.github.io/data-engineering/)**

Sister academies: [Learn ML](https://sanketn26.github.io/learn-ml/) · [AI Engineering](https://sanketn26.github.io/AIEngineering/) · [Senior Engineer Academy](https://sanketn26.github.io/interview-prep/)

---

## Who it is for

Data engineers, backend / platform engineers, ML engineers working with data
infrastructure, SREs supporting data platforms, and Staff-track engineers. The
production sections go deep, but every core lesson has a lighter first pass.

**Helpful background:** basic Python and SQL, a command line, JSON, and a
database. Docker is only required for hands-on labs.

**Does not assume:** Spark internals, Kafka ISR, Flink watermarks, Iceberg snapshots, ClickHouse `ORDER BY`, or on-call data-platform debugging — that is what this teaches.

This does not teach Python or SQL from scratch, but unfamiliar data-platform
terms are introduced through the foundations route and linked glossary.

---

## Curriculum

| Phase | What you reason about |
|-------|------------------------|
| 0 Foundations | Scale, partitioning, modelling, CDC, transformations, batch vs stream |
| 1 Spark | DAG, shuffle, Catalyst/Tungsten, skew |
| 2 Kafka | Log, partitions, replication, EOS |
| 3 Stream processing | Event time, watermarks, Flink state |
| 4 Orchestration | Airflow DAGs, idempotency, backfills |
| 5 Lakehouse | Iceberg / Hudi / Delta as table metadata |
| 6 Query engines | Trino federation, distributed SQL, managed cloud warehouses |
| 7 OLAP | Columnar storage, ClickHouse, Pinot |
| 8 Time series | Cardinality, downsampling, TSDBs |
| 9 Distributed Python | Ray |
| 10 NoSQL | Cassandra, DynamoDB access-pattern design |
| 11 Graph | Modelling, Neo4j, algorithms vs relational |
| 12 Platform | Metadata, quality, security, notebooks, delivery/IaC |

Plus end-to-end **architectures**, **comparisons**, **labs**, **incident drills**, **cost engineering**, **simulations**, and a scored **capstone**.

Five running production systems (SaaS analytics, observability, e-commerce, IoT, fraud graph) reappear throughout so the same workload is seen from each engine.

---

## Local development

```bash
git clone https://github.com/sanketn26/data-engineering.git
cd data-engineering
python3 -m venv .venv
source .venv/bin/activate          # Windows: .venv\Scripts\activate
pip install -r requirements.txt
mkdocs serve
```

Open [http://127.0.0.1:8000](http://127.0.0.1:8000).

```bash
make setup                         # venv + deps
make serve                         # live preview
make build                         # mkdocs build --strict (what CI runs)
make validate                      # strict build + lab/Compose/simulation checks
```

Pushes to `main` run [`.github/workflows/deploy.yml`](.github/workflows/deploy.yml): `mkdocs build --strict` → GitHub Pages.

PRs run [`.github/workflows/validate.yml`](.github/workflows/validate.yml).

Pages source must be **GitHub Actions** (not a branch): **Settings → Pages → Build and deployment → Source: GitHub Actions.**

---

## Repo layout

| Path | Role |
|------|------|
| `docs/` | Curriculum (MkDocs source — this is the course) |
| `docs/stylesheets/extra.css` | Shared academy theme (hero, cards, admonitions) |
| `overrides/` | Material header with hub Home icon |
| `labs/` | Docker Compose environments for hands-on work |
| `mkdocs.yml` | Site config and navigation |
| `.github/workflows/` | Strict build + GitHub Pages deploy |

---

## How to study

Read [How to Study](docs/how-to-study.md) and [Learning paths](docs/learning-paths.md). Short version:

1. First pass: read the situation and build the mental picture.
2. Second pass: trace one event through the internals and worked example.
3. Production pass: study failures and run the lab when it is relevant.
4. Predict before each explanation, then compare without treating a wrong guess as failure.

---

## License

MIT — see [LICENSE](LICENSE).
