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

This runs `./scripts/start-all.sh`, which:

1. Pulls and starts all Atlas containers (Spark, Trino, MinIO, Iceberg REST catalog, Airflow, Jenkins, Zeppelin, JupyterHub, Redpanda).
2. Bootstraps MinIO buckets (`landing`, `lakehouse`, `jars`, `checkpoints`).
3. Registers Iceberg namespaces (`bronze`, `silver`, `gold`) via `scripts/register_iceberg.py`.

!!! tip "Full Atlas stack (Trino + Redpanda)"
    To enable Trino and Redpanda as well:
    ```bash
    ./scripts/start-all.sh \
      --iceberg-rest-source container \
      --jenkins-source container \
      --trino-source container \
      --redpanda-source container
    ```

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
