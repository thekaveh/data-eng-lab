# data-eng-lab

An Iceberg-lakehouse data-engineering lab built on the [Atlas](https://github.com/thekaveh/atlas) platform.

**📖 Documentation → https://thekaveh.github.io/data-eng-lab/**

---

## What it is

`data-eng-lab` is a curated collection of data-engineering scenarios and production-grade Spark applications
running over a full Iceberg lakehouse (REST catalog + MinIO). It is built on the Atlas platform — a
Docker-compose-based stack providing Spark, Zeppelin, Jupyter, Airflow, Jenkins, Trino, and Redpanda — consumed
as a pinned submodule via `infra/` (never edited directly). Every scenario is implemented twice for parity:
Scala in Zeppelin and PySpark in Jupyter. Maven Scala Spark apps are built by Jenkins, published as shaded
JARs to MinIO, and invoked by Airflow DAGs — closing the build-to-orchestration loop on a true medallion
bronze→silver→gold pipeline.

## By the numbers

| | |
|---|---|
| Scenarios | 19 (core 10 + roadmap) |
| CI-verified Maven apps | 2 (`nyc-taxi-etl`, `nyc-taxi-medallion`) |
| Datasets | 5 (NYC Taxi, GH Archive, MovieLens, Online Retail, TPC-H) |
| Atlas contract coverage | A1–A9 full (A7 Trino + A9 Redpanda in-progress) |

## Architecture

```
MinIO  ←→  Iceberg REST catalog  ←→  Spark / Trino / Redpanda (via Atlas)
              │
        bronze → silver → gold   (medallion tiers, Iceberg tables)
              │
        Airflow DAGs  ←  Jenkins-built shaded JARs  ←  spark-apps/
```

The `infra/` submodule pins Atlas. `compose/` holds the single overlay merged into the Atlas Compose stack.
All Iceberg / Spark / streaming expectations are tracked in [`docs/atlas-expectations.md`](docs/atlas-expectations.md).

## Quick start

```bash
make setup      # initialize the Atlas submodule (infra/)
make up         # launch Atlas data-eng track + bootstrap buckets (runs scripts/start-all.sh)
make datasets   # download datasets into MinIO landing bucket (default: SCALE=small)
make preflight  # prove the stack is up, initialized, and integrated
make down       # tear down (add COLD=1 to wipe volumes)
```

See [`docs/go-live.md`](docs/go-live.md) for a full end-to-end go-live walkthrough and
[`docs/datasets.md`](docs/datasets.md) for dataset options and `SCALE` tiers (tiny / small / medium).

## Repository layout

```
data-eng-lab/
├── scenarios/          # 19 scenario folders — Zeppelin Scala + Jupyter PySpark (+ DAG)
├── spark-apps/         # Maven Scala Spark projects + Jenkinsfile + Airflow DAG
├── datasets/           # dataset registry (registry.yaml) + downloader script
├── lakehouse/          # Iceberg catalog/table configuration
├── tests/              # tiered tests: unit, static, network, infra preflight
├── docs/               # MkDocs Material site source (published to GitHub Pages)
├── infra/              # Atlas platform submodule (pinned; never edited)
├── compose/            # single Docker-compose overlay for Atlas
├── scripts/            # bootstrap, start-all.sh, stop-all.sh, dataset downloader, verifier
├── Makefile            # canonical entry points (setup / up / down / datasets / test / lint)
└── pyproject.toml      # uv project — all dev deps in [dependency-groups].dev
```

## Documentation

The full site is at **https://thekaveh.github.io/data-eng-lab/**.

Key pages:

| Topic | File |
|---|---|
| Scenario catalog | [`docs/scenarios.md`](docs/scenarios.md) |
| Spark apps | [`docs/spark-apps.md`](docs/spark-apps.md) |
| Datasets | [`docs/datasets.md`](docs/datasets.md) |
| Lakehouse architecture | [`docs/lakehouse.md`](docs/lakehouse.md) |
| Atlas contract & expectations | [`docs/atlas-expectations.md`](docs/atlas-expectations.md) |
| Go-live walkthrough | [`docs/go-live.md`](docs/go-live.md) |
| Getting started | [`docs/getting-started.md`](docs/getting-started.md) |

## License

Copyright (c) 2026 Kaveh Razavi. All rights reserved.
Private and proprietary — see [`LICENSE`](LICENSE). No license is granted to use, copy, modify, or
distribute this software without explicit written permission.
