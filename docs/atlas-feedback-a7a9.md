# Atlas Feedback — A7/A9

This document captures observations and feedback from building and testing A7 (Trino federated queries) and A9 (Redpanda + Structured Streaming).

## A7: Trino Federated Query

### What was built

Trino 482 configured with the Iceberg REST connector to query Iceberg tables in the `lakehouse` catalog. Two scenarios validate this capability: `federated_query-nyc_taxi` (Bronze table queries) and `bi_query-tpch` (multi-table joins + CTAS into Gold).

### Findings

- Trino's Iceberg connector resolves table locations through the REST catalog without any additional configuration beyond the catalog properties file.
- CTAS into Gold tables works without issues — Trino writes Parquet files with Iceberg metadata.
- Multi-table joins (TPC-H fact/dimension) perform well on the dataset sizes used.
- The `%trino` Zeppelin interpreter provides a convenient UI for ad-hoc queries against the same tables queried by Trino.

### Gotchas

- Trino and Spark must use **compatible** Iceberg connector versions. We use `trino-iceberg:1.11.0` matching the Spark Iceberg runtime.
- Trino has its own SQL dialect — some Spark SQL syntax (e.g., certain window function expressions) may need adjustment.

## A9: Redpanda + Structured Streaming

### What was built

Redpanda v26 provides Kafka-compatible streaming. Three scenarios validate this: `streaming_ingest-events` (file-source + Redpanda), `streaming_ingest-gh_archive` (file-source polling), and `streaming_windows-events` (windowed aggregation with watermarks). The `cdc_streaming-online_retail` scenario uses `foreachBatch` with `MERGE INTO` for real-time upserts.

### Findings

- Redpanda's Kafka wire protocol is fully compatible with Spark Structured Streaming's Kafka source. No code changes between Kafka and Redpanda consumers.
- Checkpoint directories (`checkpoints/` bucket) are essential for streaming offset management.
- `trigger(availableNow=True)` is the correct way to test streaming in a notebook context (avoids infinite running triggers).
- `foreachBatch` + `MERGE INTO` provides a clean CDC upsert pattern.

### Gotchas

- The streaming producer must be running **before** the Spark consumer starts, otherwise the stream has no data to process.
- Streaming scenarios cannot be validated with parity tests (they are infinite streams), so they are gated on Atlas delivery rather than Scala/PySpark parity.
- Watch checkpoint directory growth — unbounded in production without retention policies.

## See Also

- [Atlas Expectations](atlas-expectations.md) — Full delivery log
- [Lakehouse Architecture](lakehouse.md) — Platform architecture
- [Go-Live Results](go-live-results.md) — Go-live validation results
