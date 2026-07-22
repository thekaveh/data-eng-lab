# Getting Started

Get the full `data-eng-lab` stack running in five steps.

---

## 2.1 Prerequisites

| Requirement | Notes |
|-------------|-------|
| Docker + Docker Compose | Required for the Atlas platform (all services run in containers) |
| Git with submodule support | The Atlas platform is a git submodule under `infra/` |
| Python >= 3.11 + [uv](https://docs.astral.sh/uv/) | Used for tooling and local scripts |
| Java 11+ (optional) | Only needed if you build Maven apps locally with `make build-apps` |
| ~10 GB free disk | For Docker images, dataset downloads, and Iceberg data |

---

## 2.2 Architecture

The `data-eng-lab` platform runs on the Atlas Docker Compose cluster, consisting of ~30 containers across six layers:

- **Notebook clients**: JupyterHub (PySpark/PyIceberg) and Zeppelin (Scala Spark)
- **Compute**: Spark 4.1.2 (Connect server, standalone master, workers, history)
- **Catalog**: Iceberg REST catalog with Postgres backend
- **Storage**: MinIO object storage with four buckets (landing, lakehouse, jars, checkpoints)
- **Query & streaming**: Trino SQL engine and Redpanda (Kafka API)
- **Orchestration**: Airflow for DAG-based workflows

The medallion data flow runs at the bottom: bronze (raw) → silver (clean) → gold (aggregated).

![Architecture](architectures/overview.svg)

---

## 2.3 Step 1 — Clone and initialise the submodule

```bash
git clone https://github.com/thekaveh/data-eng-lab.git
cd data-eng-lab
make setup         # git submodule update --init --recursive infra
```

---

## 2.4 Step 2 — Download datasets

> **Prerequisite:** `make datasets` uploads into the live MinIO landing bucket, so the
> stack must be up first — run **Step 3 (`make up`)** below before this step on a fresh
> clone, then return here. (The steps are numbered in medallion order; on first setup the
> launch precedes the dataset load.)

The lab ships five curated datasets (NYC Taxi, TPC-H, Online Retail, GitHub Archive, MovieLens). Download the `small` tier (default) into the MinIO landing bucket:

```bash
make datasets                     # default: --scale small
make datasets SCALE=tiny          # faster; less data
make datasets SCALE=medium        # more data; heavier queries
```

See [Datasets](datasets.md) for the full dataset registry and scale options.

---

## 2.5 Step 3 — Launch the stack

```bash
make up
```

This runs `./scripts/start-all.sh`, a thin wrapper over Atlas's own headless commands:

1. Removes any stale legacy overlay symlink (pre-manifest layout).
2. Backfills new upstream `.env` keys (additive; `env backfill`).
3. Runs the consumer `doctor` (manifest + compose + env lints) against `atlas.consumer.yml`.
4. Starts the full `data-eng` track detached (`start.sh --consumer … --track data-eng --no-tui --detach`) — Atlas health-gates the whole track before returning, provisions the MinIO buckets (including the `lakehouse-test` bucket declared under the manifest's `storage:` key), and materializes `infra/.env` from the manifest.
5. Registers Iceberg namespaces (`bronze`, `silver`, `gold`) via `scripts/register_iceberg.py`.
6. Runs preflight (Layer 1 + Layer 2).

All nine `data-eng` services (Spark, Zeppelin, Airflow, MinIO, JupyterHub, Iceberg REST, Jenkins, Trino, Redpanda) are containerized by default — set in `atlas.consumer.yml`'s `env.values` (`*_SOURCE: container`), not passed as CLI flags. To disable one, edit the manifest and re-run `make up`.

Ports are auto-allocated (`BASE_PORT: auto`) — read them from `infra/.env` or `(cd infra && ./start.sh endpoints export --format env)`.

---

## 2.6 Step 4 — Verify the stack

```bash
make preflight     # Layer 1 (service health) + Layer 2 (integration round-trips)
make verify        # repo-level structural checks
make test          # offline unit/integration tests (no live stack required)
```

A passing `make preflight` confirms end-to-end connectivity: Spark ↔ MinIO ↔ Iceberg, Jupyter ↔ PyIceberg, Airflow ↔ MinIO/Spark, Zeppelin ↔ Spark.

---

## 2.7 Step 5 — Run notebooks

=== "Zeppelin (Scala Spark)"

     1. Navigate to `http://localhost:${ZEPPELIN_PORT}` (the slot-allocated port from `infra/.env`).
     2. Open any scenario notebook under `scenarios/<name>/zeppelin/`.
     3. Use the `%spark` interpreter for Scala Spark cells, `%trino` for SQL via Trino.

=== "JupyterHub (PySpark)"

     1. Navigate to `http://localhost:${JUPYTERHUB_PORT}` (the slot-allocated port from `infra/.env`).
     2. Open any `scenarios/<name>/jupyter/` notebook.
     3. The kernel ships PySpark + PyIceberg pre-installed.

---

## 2.8 Tear down

```bash
make down               # stop containers, preserve volumes
make down COLD=1        # stop and wipe all volumes (full reset)
```

---

## 2.9 What next?

- Browse the [Scenario Catalog](scenarios/index.md) — 19 end-to-end scenarios across bronze, silver, and gold.
- Check [Lakehouse Architecture](lakehouse.md) for the medallion design, Iceberg features, and integration matrix.
- See [Atlas Expectations](atlas-expectations.md) for the delivered platform contract and known deviations.
- Read [Go-Live Results](go-live-results.md) for the actual validation run with row counts and bug fixes.
- Review [Go-Live Findings](atlas-feedback-go-live.md) for infrastructure issues surfaced during the live run.
