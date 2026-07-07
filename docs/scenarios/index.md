# Scenario Catalog

This page catalogs all 19 scenarios in the `data-eng-lab` lakehouse, organized by functional category. Each scenario is a self-contained folder with Scala Spark (Zeppelin), PySpark (Jupyter), and optional Airflow DAG implementations. Architecture diagrams are linked from each scenario's README.

## Categories

The scenarios demonstrate the end-to-end data engineering lifecycle: raw data ingestion through medallion transformation, quality validation, streaming, BI, and advanced Iceberg features.

### Batch Ingestion

Batch processing of raw Parquet data into Iceberg bronze. Demonstrates the fundamental lakehouse path from MinIO landing zone through Spark to cataloged Iceberg tables.

- [batch_ingest-nyc_taxi-spark-iceberg](batch_ingest-nyc_taxi-spark-iceberg.md)

### Medallion Pipeline

Multi-layer transformation: bronze → silver (deduplication) → gold (daily aggregation). Demonstrates the medallion architecture with a single dataset flowing through all three tiers.

- [medallion-nyc_taxi-spark-iceberg](medallion-nyc_taxi-spark-iceberg.md)

### Data Quality

Validation, quarantine, and quality gates. Demonstrates filtering invalid data into a quarantine table while preserving it rather than silently dropping it.

- [data_quality-nyc_taxi-spark-iceberg](data_quality-nyc_taxi-spark-iceberg.md)

### Schema & Maintenance

Schema evolution (ADD/RENAME columns with backward-compatible NULL filling), time travel (VERSION AS OF, rollback, WAP branches), and table maintenance (compaction, expire snapshots, remove orphan files). These are pure Iceberg capabilities that require only the delivered Spark/Iceberg runtime.

- [schema_evolution-gh_archive-spark-iceberg](schema_evolution-gh_archive-spark-iceberg.md)
- [time_travel-nyc_taxi-spark-iceberg](time_travel-nyc_taxi-spark-iceberg.md)
- [table_maintenance-nyc_taxi-spark-iceberg](table_maintenance-nyc_taxi-spark-iceberg.md)

### Streaming

Four streaming scenarios covering file-source and Kafka-source structured streaming, windowed aggregation with watermarks, and CDC (Change Data Capture) with MERGE INTO. These require the enhanced-Atlas stack with Redpanda enabled.

- [streaming_ingest-events-spark-iceberg](streaming_ingest-events-spark-iceberg.md)
- [streaming_ingest-gh_archive-spark-iceberg](streaming_ingest-gh_archive-spark-iceberg.md)
- [streaming_windows-events-spark-iceberg](streaming_windows-events-spark-iceberg.md)
- [cdc_streaming-online_retail-spark-iceberg](cdc_streaming-online_retail-spark-iceberg.md)

### BI & Queries

Trino SQL queries over Iceberg tables via the REST connector. Demonstrates multi-engine interoperability: Spark writes, Trino reads (and writes via CTAS), with both engines accessing the same Iceberg tables on shared storage.

- [federated_query-nyc_taxi-trino-iceberg](federated_query-nyc_taxi-trino-iceberg.md)
- [bi_query-tpch-trino-iceberg](bi_query-tpch-trino-iceberg.md)

### Join Optimization

Broadcast join hints and Adaptive Query Execution (AQE) on TPCH data. Demonstrates query performance optimization: broadcasting a dimension table to avoid shuffle, plus AQE repartitioning.

- [join_optimization-tpch-spark-iceberg](join_optimization-tpch-spark-iceberg.md)

### Dimensional Modeling

Star schema construction from TPCH source tables. Fact and dimension gold marts are written as Iceberg tables for BI consumption, demonstrating traditional data warehouse patterns on the lakehouse.

- [star_schema-tpch-spark-iceberg](star_schema-tpch-spark-iceberg.md)

### Feature Engineering

ML feature marts from MovieLens ratings. Per-user and per-movie aggregated features stored as Iceberg tables, bridging to Spark MLlib pipelines for collaborative filtering.

- [feature_engineering-movielens-spark-iceberg](feature_engineering-movielens-spark-iceberg.md)

### SCD Type 2

Slowly Changing Dimension Type 2 with effective_from/to tracking and is_current flags. Demonstrates full history preservation through UPDATE + INSERT patterns on Iceberg.

- [scd2-online_retail-spark-iceberg](scd2-online_retail-spark-iceberg.md)

### JSON Processing

Nested JSON semi-structured data from the GitHub Archive flattened into typed relational columns using Spark's `from_json` and `explode` functions. Demonstrates schema enforcement on semi-structured data.

- [json_flatten-gh_archive-spark-iceberg](json_flatten-gh_archive-spark-iceberg.md)

### Session Analysis

Window-based sessionization on timestamped events. Gap-based session boundary detection (>30s gap = new session) with cumsum-assigned session IDs.

- [sessionization-gh_archive-spark-iceberg](sessionization-gh_archive-spark-iceberg.md)

## Running Scenarios

Each scenario follows a dual-notebook pattern: a Scala Spark notebook for Zeppelin and a PySpark notebook for JupyterHub. Both must produce equivalent output, validated by the parity harness (`tests/scenarios/parity.py`). Airflow DAGs provide an orchestration alternative for batch scenarios.

Streaming scenarios require Redpanda to be running and the appropriate Kafka topic to be created (via `REDPANDA_DEMO_TOPICS` in `.env` or `producer.py`). Checkpoint state is stored in MinIO's `checkpoints` bucket; re-running a streaming notebook without a checkpoint cleanup will resume from the last offset.

## See Also

- [Spark Applications](../spark-apps/index.md)
- [Datasets](../datasets.md)
- [Lakehouse](../lakehouse.md)
