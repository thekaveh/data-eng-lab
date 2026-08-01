# 6.1. Index

Each scenario ships paired Zeppelin and Jupyter notebooks with equivalent intent. Seventeen Spark scenarios pair Scala with PySpark; the two Trino scenarios pair `%trino` SQL with the Python Trino client.
The manifest-owned walkthroughs below are canonical. Spark pages show side-by-side language parity, while Trino pages document query/result equivalence without calling SQL “Scala” or the Python client “PySpark.”

Update a walkthrough alongside its source notebooks, then run `make docs-check`. The aggregate gate verifies that every paired scenario has exactly one manifest-owned walkthrough and projects it to the site and wiki.

## 1. Batch

- [batch_ingest-nyc_taxi-spark-iceberg](batch_ingest-nyc_taxi-spark-iceberg.md)
- [medallion-nyc_taxi-spark-iceberg](medallion-nyc_taxi-spark-iceberg.md)

## 2. Streaming

- [streaming_ingest-events-spark-iceberg](streaming_ingest-events-spark-iceberg.md)
- [streaming_ingest-gh_archive-spark-iceberg](streaming_ingest-gh_archive-spark-iceberg.md)
- [streaming_windows-events-spark-iceberg](streaming_windows-events-spark-iceberg.md)
- [cdc_streaming-online_retail-spark-iceberg](cdc_streaming-online_retail-spark-iceberg.md)

## 3. Quality / Modeling

- [data_quality-nyc_taxi-spark-iceberg](data_quality-nyc_taxi-spark-iceberg.md)
- [schema_evolution-gh_archive-spark-iceberg](schema_evolution-gh_archive-spark-iceberg.md)
- [star_schema-tpch-spark-iceberg](star_schema-tpch-spark-iceberg.md)
- [feature_engineering-movielens-spark-iceberg](feature_engineering-movielens-spark-iceberg.md)
- [scd2-online_retail-spark-iceberg](scd2-online_retail-spark-iceberg.md)

## 4. Ops

- [time_travel-nyc_taxi-spark-iceberg](time_travel-nyc_taxi-spark-iceberg.md)
- [table_maintenance-nyc_taxi-spark-iceberg](table_maintenance-nyc_taxi-spark-iceberg.md)
- [incremental_upsert-online_retail-spark-iceberg](incremental_upsert-online_retail-spark-iceberg.md)

## 5. SQL / Analytics

- [bi_query-tpch-trino-iceberg](bi_query-tpch-trino-iceberg.md)
- [federated_query-nyc_taxi-trino-iceberg](federated_query-nyc_taxi-trino-iceberg.md)
- [join_optimization-tpch-spark-iceberg](join_optimization-tpch-spark-iceberg.md)

## 6. Semi-structured

- [json_flatten-gh_archive-spark-iceberg](json_flatten-gh_archive-spark-iceberg.md)
- [sessionization-gh_archive-spark-iceberg](sessionization-gh_archive-spark-iceberg.md)

## 7. See Also

- [Scenario Catalog](../scenarios/index.md)
- [Spark Apps](../spark-apps/index.md)
