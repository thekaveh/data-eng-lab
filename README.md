# data-eng-lab

An Iceberg-lakehouse **data-engineering lab** built on the [Atlas](https://github.com/thekaveh/atlas)
platform. Curated Spark scenarios in **Scala (Zeppelin)** and **PySpark (Jupyter)**, orchestrated with
**Airflow**, plus **Maven Scala Spark** apps built by **Jenkins** and published to **MinIO** — all over
an **Apache Iceberg** lakehouse (REST catalog) on MinIO.

## 1. Overview
See the design spec: [`docs/superpowers/specs/2026-07-02-data-eng-lab-design.md`](docs/superpowers/specs/2026-07-02-data-eng-lab-design.md).

## 2. Repository layout
- `infra/` — Atlas platform (git submodule; never edited)
- `compose/` — the single compose overlay merged into Atlas
- `scenarios/` — flat scenario folders (Zeppelin Scala + Jupyter PySpark [+ DAG]); see [`docs/scenarios.md`](docs/scenarios.md)
- `spark-apps/` — Maven Scala Spark projects (+ Jenkinsfile + DAG)
- `datasets/` — dataset registry + downloader
- `scripts/` — bootstrap & tooling
- `tests/` — comprehensive tiered tests (incl. `tests/infra/` preflight)
- `docs/` — design, Atlas contract, per-topic docs

## 3. Quick start
```bash
make setup      # init the Atlas submodule
make up         # launch Atlas data-eng track + bootstrap buckets
make preflight  # prove the stack is up, initialized, and integrated
make down       # tear down
```
See [`docs/datasets.md`](docs/datasets.md) for dataset landing & SCALE configuration.

## 4. Lakehouse + Atlas contract
The lakehouse architecture is documented in [`docs/lakehouse.md`](docs/lakehouse.md).
This repo consumes an enhanced Atlas; required upstream enhancements are tracked in
[`docs/atlas-enablement.md`](docs/atlas-enablement.md).

## 5. License
Private / proprietary — see [`LICENSE`](LICENSE).
