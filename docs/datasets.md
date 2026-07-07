# Datasets

`data-eng-lab` lands a curated set of five standard open datasets into MinIO's `landing` bucket, driven by a declarative registry. Each dataset comes in multiple scale levels (`tiny`, `small`, `medium`) to support CI, local development, and heavier analytical workloads.

## 4.1 Registry

`datasets/registry.yaml` declares each dataset with the following properties:

| Property | Description |
|---|---|
| `format` | File format: `parquet`, `csv`, or `json.gz` |
| `license` | Dataset license |
| `landing_prefix` | MinIO object key prefix (e.g. `nyc_taxi/`) |
| `fetch.kind` | `http` for direct downloads with optional `unzip`, `tpch` for DuckDB-generated TPC-H |
| `fetch.scale_params` | Per-scale parameters (`tiny` / `small` / `medium`) |

The registry is schema-validated by the repo verifier. Adding a dataset requires only a new entry in this file and a corresponding fetch function — no code change to the core pipeline is needed.

## 4.2 Current Datasets

| Dataset | Shape | Format | Fetch | Scenarios |
|---|---|---|---|---|
| `nyc_taxi` | Columnar analytical | Parquet | http | batch_ingest, data_quality, medallion, federated_query, table_maintenance, time_travel, join_optimization, star_schema, bi_query, incremental_upsert, scd2 |
| `gh_archive` | Semi-structured JSON events | JSON.gz | http | json_flatten, schema_evolution, sessionization, streaming_ingest |
| `movielens` | Rating and join data | CSV | http (unzip) | feature_engineering |
| `online_retail` | Transactional retail invoices | CSV | http (unzip) | incremental_upsert, scd2, cdc_streaming |
| `tpch` | Benchmark star-schema | Parquet | tpch (DuckDB) | star_schema, join_optimization, bi_query |

## 4.3 Adding a Dataset

1. Add an entry to `datasets/registry.yaml` with `format`, `license`, `landing_prefix`, `fetch.kind`, and `fetch.scale_params`.
2. Add a fetch function in `scripts/download_datasets.py` if `fetch.kind` is not `http` or `tpch`.
3. Run `make datasets SCALE=tiny` to land the dataset in MinIO.
4. Verify with `uv run python scripts/download_datasets.py --dry-run`.
5. Create a scenario folder and write the notebooks.

## 4.4 Usage

```bash
make up                  # boot the Atlas data-eng track (MinIO must be running)
make datasets            # land the 'small' tier
make datasets SCALE=tiny # CI-sized subset
make datasets SCALE=medium # more data; heavier queries
uv run python scripts/download_datasets.py --scale medium --only nyc_taxi
uv run python scripts/download_datasets.py --dry-run    # show what would be landed
```

The downloader reads MinIO credentials and the published S3 port from `infra/.env` and is idempotent — existing objects are skipped unless `--force` is specified.

## 4.5 Related Scenarios by Dataset

### NYC Taxi (`nyc_taxi`)
- [batch_ingest-nyc_taxi-spark-iceberg](scenarios/batch_ingest-nyc_taxi-spark-iceberg.md)
- [medallion-nyc_taxi-spark-iceberg](scenarios/medallion-nyc_taxi-spark-iceberg.md)
- [data_quality-nyc_taxi-spark-iceberg](scenarios/data_quality-nyc_taxi-spark-iceberg.md)
- [time_travel-nyc_taxi-spark-iceberg](scenarios/time_travel-nyc_taxi-spark-iceberg.md)
- [table_maintenance-nyc_taxi-spark-iceberg](scenarios/table_maintenance-nyc_taxi-spark-iceberg.md)
- [federated_query-nyc_taxi-trino-iceberg](scenarios/federated_query-nyc_taxi-trino-iceberg.md)

### TPC-H (`tpch`)
- [star_schema-tpch-spark-iceberg](scenarios/star_schema-tpch-spark-iceberg.md)
- [join_optimization-tpch-spark-iceberg](scenarios/join_optimization-tpch-spark-iceberg.md)
- [bi_query-tpch-trino-iceberg](scenarios/bi_query-tpch-trino-iceberg.md)

### MovieLens (`movielens`)
- [feature_engineering-movielens-spark-iceberg](scenarios/feature_engineering-movielens-spark-iceberg.md)

### Online Retail (`online_retail`)
- [incremental_upsert-online_retail-spark-iceberg](scenarios/incremental_upsert-online_retail-spark-iceberg.md)
- [scd2-online_retail-spark-iceberg](scenarios/scd2-online_retail-spark-iceberg.md)
- [cdc_streaming-online_retail-spark-iceberg](scenarios/cdc_streaming-online_retail-spark-iceberg.md)

### GitHub Archive (`gh_archive`)
- [streaming_ingest-events-spark-iceberg](scenarios/streaming_ingest-events-spark-iceberg.md)
- [streaming_ingest-gh_archive-spark-iceberg](scenarios/streaming_ingest-gh_archive-spark-iceberg.md)
- [streaming_windows-events-spark-iceberg](scenarios/streaming_windows-events-spark-iceberg.md)
- [json_flatten-gh_archive-spark-iceberg](scenarios/json_flatten-gh_archive-spark-iceberg.md)
- [sessionization-gh_archive-spark-iceberg](scenarios/sessionization-gh_archive-spark-iceberg.md)

## 4.6 See Also

- [Scenario Catalog](scenarios/index.md)
- [Lakehouse Architecture](lakehouse.md)
