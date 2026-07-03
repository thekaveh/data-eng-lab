# Datasets

`data-eng-lab` lands a curated set of standard open datasets into MinIO's `landing` bucket, driven by
a declarative registry.

## Registry
`datasets/registry.yaml` declares each dataset: `format`, `license`, `landing_prefix`, a `fetch.kind`
(`http` for direct downloads with optional `unzip`, or `tpch` for DuckDB-generated TPC-H), and per-SCALE
parameters (`tiny` / `small` / `medium`). It is schema-validated by the repo verifier.

## Usage
```bash
make up                 # boot the Atlas data-eng track (MinIO must be running)
make datasets            # land the 'small' tier
make datasets SCALE=tiny # CI-sized subset
uv run python scripts/download_datasets.py --scale medium --only nyc_taxi
uv run python scripts/download_datasets.py --dry-run   # show what would be landed
```
The downloader reads MinIO credentials + the published S3 port from `infra/.env` and is idempotent
(existing objects are skipped unless `--force`).

## Current datasets
| Dataset | Shape | Fetch |
|---|---|---|
| `nyc_taxi` | Columnar analytical (Parquet) | http |
| `gh_archive` | Semi-structured JSON events / streaming source | http |
| `movielens` | Ratings + joins | http (unzip) |
| `tpch` | Benchmark star-schema | tpch (DuckDB) |

More datasets (e.g. `online_retail`, `noaa`) are added as pure registry entries (`http` kind) when
their Phase 2 scenarios need them — no code change required.
