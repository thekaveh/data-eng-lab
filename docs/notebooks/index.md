# 6. Notebooks

Each scenario ships paired Zeppelin (Scala) and Jupyter (PySpark) notebooks with identical logic.
A comprehensive, auto-extracted notebook doc per scenario lives below — also reachable from each scenario's §4.

The docs below are generated from `scenarios/<name>/jupyter/notebook.ipynb` and `scenarios/<name>/zeppelin/notebook.zpln` by `scripts/docslib/notebooks.py`. Each doc shows the section map, the side-by-side Scala/PySpark walkthrough for every numbered section, and the Scala/PySpark parity statement.

## Batch

- [batch_ingest-nyc_taxi-spark-iceberg](batch_ingest-nyc_taxi-spark-iceberg.md)
- [medallion-nyc_taxi-spark-iceberg](medallion-nyc_taxi-spark-iceberg.md)

## Streaming

- [streaming_ingest-events-spark-iceberg](streaming_ingest-events-spark-iceberg.md)
- [streaming_ingest-gh_archive-spark-iceberg](streaming_ingest-gh_archive-spark-iceberg.md)
- [streaming_windows-events-spark-iceberg](streaming_windows-events-spark-iceberg.md)
- [cdc_streaming-online_retail-spark-iceberg](cdc_streaming-online_retail-spark-iceberg.md)

## Quality / Modeling

- [data_quality-nyc_taxi-spark-iceberg](data_quality-nyc_taxi-spark-iceberg.md)
- [schema_evolution-gh_archive-spark-iceberg](schema_evolution-gh_archive-spark-iceberg.md)
- [star_schema-tpch-spark-iceberg](star_schema-tpch-spark-iceberg.md)
- [feature_engineering-movielens-spark-iceberg](feature_engineering-movielens-spark-iceberg.md)
- [scd2-online_retail-spark-iceberg](scd2-online_retail-spark-iceberg.md)

## Ops

- [time_travel-nyc_taxi-spark-iceberg](time_travel-nyc_taxi-spark-iceberg.md)
- [table_maintenance-nyc_taxi-spark-iceberg](table_maintenance-nyc_taxi-spark-iceberg.md)
- [incremental_upsert-online_retail-spark-iceberg](incremental_upsert-online_retail-spark-iceberg.md)

## SQL / Analytics

- [bi_query-tpch-trino-iceberg](bi_query-tpch-trino-iceberg.md)
- [federated_query-nyc_taxi-trino-iceberg](federated_query-nyc_taxi-trino-iceberg.md)
- [join_optimization-tpch-spark-iceberg](join_optimization-tpch-spark-iceberg.md)

## Semi-structured

- [json_flatten-gh_archive-spark-iceberg](json_flatten-gh_archive-spark-iceberg.md)
- [sessionization-gh_archive-spark-iceberg](sessionization-gh_archive-spark-iceberg.md)

## See Also

- [Scenario Catalog](../scenarios/index.md)
- [Spark Apps](../spark-apps/index.md)
