<!-- AUTO-GENERATED — do not edit; run scripts/build_docs.py -->
# data-eng-lab

**An Iceberg-lakehouse data-engineering lab built on the [Atlas](https://github.com/thekaveh/atlas) platform.**

Curated Spark scenarios in Scala (Zeppelin) and PySpark (Jupyter), orchestrated with Airflow, plus Maven Scala Spark apps built by Jenkins — all over Apache Iceberg on MinIO, cataloged by the Atlas Iceberg REST catalog.

---

## 1.1 Architecture

![Full-stack Lakehouse Architecture](architectures/overview.svg)

The lab implements a **medallion lakehouse** with four layers:

```
s3a://landing/   →   bronze   →   silver   →   gold
  (raw Parquet)       (clean)     (enriched)   (aggregated/modelled)
```

Every table is an **Apache Iceberg** table, accessed through the Atlas **Iceberg REST catalog** (`lakehouse`). Compute is provided by a Spark cluster; Trino handles ad-hoc SQL and federated queries. Redpanda (Kafka-compatible) drives all streaming scenarios. Airflow schedules production DAGs; Jenkins builds and publishes Maven Spark apps to MinIO artifacts.

---

## 1.2 Quick navigation

<div class="grid cards" markdown>

-   :material-database-search:{ .lg .middle } **Scenario catalog**

    ---

    19 end-to-end Spark and Trino scenarios across bronze, silver, and gold layers.

     [:octicons-arrow-right-24: Browse scenarios](scenarios/index/README.md)

-   :material-rocket-launch:{ .lg .middle } **Spark apps**

    ---

    2 CI-verified Maven Scala Spark apps built by Jenkins and run by Airflow.

     [:octicons-arrow-right-24: Browse apps](spark-apps/index/README.md)

-   :material-table-large:{ .lg .middle } **Datasets**

    ---

    5 curated datasets (NYC Taxi, TPC-H, Online Retail, GH Archive, Events) with `make datasets`.

    [:octicons-arrow-right-24: Dataset guide](README.md#datasets)

-   :material-layers-triple:{ .lg .middle } **Lakehouse design**

    ---

    Medallion layout, Iceberg namespaces, MinIO buckets, and the bronze smoke test.

    [:octicons-arrow-right-24: Lakehouse guide](README.md#lakehouse-architecture)

-   :material-check-decagram:{ .lg .middle } **Atlas platform**

    ---

    A1–A9 Atlas enablement checklist, expectations, and go-live runbook.

    :octicons-arrow-right-24: Atlas enablement

-   :material-play-box-multiple:{ .lg .middle } **Getting started**

    ---

    Prerequisites, `make datasets`, starting the stack, and running notebooks.

    [:octicons-arrow-right-24: Quick start](README.md#getting-started)

</div>

---

## 1.3 By the numbers

| What | Count |
|------|-------|
| Scenario notebooks (Scala + PySpark pairs) | 19 (14 batch, 4 streaming, 1 hybrid) |
| CI-verified Maven Spark apps | 2 |
| Curated datasets | 5 |
| Atlas enablement items (A1–A9) | 9 |
| Iceberg medallion layers | 3 (bronze / silver / gold) |

---

## 1.4 Scenario catalog

| Scenario | Engine | Layer | Dataset |
|---|---|---|---|
| [batch_ingest-nyc_taxi-spark-iceberg](scenarios/batch_ingest-nyc_taxi-spark-iceberg/README.md) | Spark | Bronze | NYC Taxi |
| [medallion-nyc_taxi-spark-iceberg](scenarios/medallion-nyc_taxi-spark-iceberg/README.md) | Spark | Bronze→Silver→Gold | NYC Taxi |
| [data_quality-nyc_taxi-spark-iceberg](scenarios/data_quality-nyc_taxi-spark-iceberg/README.md) | Spark | Silver | NYC Taxi |
| [schema_evolution-gh_archive-spark-iceberg](scenarios/schema_evolution-gh_archive-spark-iceberg/README.md) | Spark | Silver | GH Archive |
| [time_travel-nyc_taxi-spark-iceberg](scenarios/time_travel-nyc_taxi-spark-iceberg/README.md) | Spark | Silver | NYC Taxi |
| [table_maintenance-nyc_taxi-spark-iceberg](scenarios/table_maintenance-nyc_taxi-spark-iceberg/README.md) | Spark | Silver | NYC Taxi |
| [streaming_ingest-events-spark-iceberg](scenarios/streaming_ingest-events-spark-iceberg/README.md) | Spark (stream) | Bronze | Events |
| [streaming_ingest-gh_archive-spark-iceberg](scenarios/streaming_ingest-gh_archive-spark-iceberg/README.md) | Spark (stream) | Bronze | GH Archive |
| [streaming_windows-events-spark-iceberg](scenarios/streaming_windows-events-spark-iceberg/README.md) | Spark (stream) | Silver | Events |
| [cdc_streaming-online_retail-spark-iceberg](scenarios/cdc_streaming-online_retail-spark-iceberg/README.md) | Spark (stream) | Silver | Online Retail |
| [federated_query-nyc_taxi-trino-iceberg](scenarios/federated_query-nyc_taxi-trino-iceberg/README.md) | Trino | Gold | NYC Taxi |
| [bi_query-tpch-trino-iceberg](scenarios/bi_query-tpch-trino-iceberg/README.md) | Trino | Gold | TPC-H |
| [join_optimization-tpch-spark-iceberg](scenarios/join_optimization-tpch-spark-iceberg/README.md) | Spark | Gold | TPC-H |
| [star_schema-tpch-spark-iceberg](scenarios/star_schema-tpch-spark-iceberg/README.md) | Spark | Gold | TPC-H |
| [feature_engineering-movielens-spark-iceberg](scenarios/feature_engineering-movielens-spark-iceberg/README.md) | Spark | Gold | MovieLens |
| [scd2-online_retail-spark-iceberg](scenarios/scd2-online_retail-spark-iceberg/README.md) | Spark | Silver | Online Retail |
| [json_flatten-gh_archive-spark-iceberg](scenarios/json_flatten-gh_archive-spark-iceberg/README.md) | Spark | Silver | GH Archive |
| [sessionization-gh_archive-spark-iceberg](scenarios/sessionization-gh_archive-spark-iceberg/README.md) | Spark | Silver | GH Archive |
| [incremental_upsert-online_retail-spark-iceberg](scenarios/incremental_upsert-online_retail-spark-iceberg/README.md) | Spark | Silver | Online Retail |

---

## 1.5 Scenarios by Category

**Batch Ingestion** — `batch_ingest`

**Medallion Pipeline** — `medallion`

**Data Quality** — `data_quality`

**Schema & Maintenance** — `schema_evolution`, `time_travel`, `table_maintenance`

**Streaming** — `streaming_ingest` (events + gh_archive), `streaming_windows`, `cdc_streaming`

**BI & Queries** — `federated_query`, `bi_query`

**Join Optimization** — `join_optimization`

**Dimensional Modeling** — `star_schema`

**Feature Engineering** — `feature_engineering`

**SCD** — `scd2`

**JSON Processing** — `json_flatten`

**Session Analysis** — `sessionization`

---

!!! tip "New here?"
    Start with [Getting started](README.md#getting-started) to get the stack running, then pick a scenario from the [catalog](scenarios/index/README.md) or dive into the [lakehouse design](README.md#lakehouse-architecture).

!!! info "Atlas platform"
    The Atlas platform underpins this lab. See Atlas enablement for the full A1–A9 checklist and Go-live runbook for production readiness steps.

## Scenario catalog

| # | Scenario | Notebook doc |
|---|---|---|
| 1 | [batch_ingest-nyc_taxi-spark-iceberg](scenarios/batch_ingest-nyc_taxi-spark-iceberg/README.md) | [notebooks](scenarios/batch_ingest-nyc_taxi-spark-iceberg/notebooks.md) |
| 2 | [bi_query-tpch-trino-iceberg](scenarios/bi_query-tpch-trino-iceberg/README.md) | [notebooks](scenarios/bi_query-tpch-trino-iceberg/notebooks.md) |
| 3 | [cdc_streaming-online_retail-spark-iceberg](scenarios/cdc_streaming-online_retail-spark-iceberg/README.md) | [notebooks](scenarios/cdc_streaming-online_retail-spark-iceberg/notebooks.md) |
| 4 | [data_quality-nyc_taxi-spark-iceberg](scenarios/data_quality-nyc_taxi-spark-iceberg/README.md) | [notebooks](scenarios/data_quality-nyc_taxi-spark-iceberg/notebooks.md) |
| 5 | [feature_engineering-movielens-spark-iceberg](scenarios/feature_engineering-movielens-spark-iceberg/README.md) | [notebooks](scenarios/feature_engineering-movielens-spark-iceberg/notebooks.md) |
| 6 | [federated_query-nyc_taxi-trino-iceberg](scenarios/federated_query-nyc_taxi-trino-iceberg/README.md) | [notebooks](scenarios/federated_query-nyc_taxi-trino-iceberg/notebooks.md) |
| 7 | [incremental_upsert-online_retail-spark-iceberg](scenarios/incremental_upsert-online_retail-spark-iceberg/README.md) | [notebooks](scenarios/incremental_upsert-online_retail-spark-iceberg/notebooks.md) |
| 8 | [join_optimization-tpch-spark-iceberg](scenarios/join_optimization-tpch-spark-iceberg/README.md) | [notebooks](scenarios/join_optimization-tpch-spark-iceberg/notebooks.md) |
| 9 | [json_flatten-gh_archive-spark-iceberg](scenarios/json_flatten-gh_archive-spark-iceberg/README.md) | [notebooks](scenarios/json_flatten-gh_archive-spark-iceberg/notebooks.md) |
| 10 | [medallion-nyc_taxi-spark-iceberg](scenarios/medallion-nyc_taxi-spark-iceberg/README.md) | [notebooks](scenarios/medallion-nyc_taxi-spark-iceberg/notebooks.md) |
| 11 | [scd2-online_retail-spark-iceberg](scenarios/scd2-online_retail-spark-iceberg/README.md) | [notebooks](scenarios/scd2-online_retail-spark-iceberg/notebooks.md) |
| 12 | [schema_evolution-gh_archive-spark-iceberg](scenarios/schema_evolution-gh_archive-spark-iceberg/README.md) | [notebooks](scenarios/schema_evolution-gh_archive-spark-iceberg/notebooks.md) |
| 13 | [sessionization-gh_archive-spark-iceberg](scenarios/sessionization-gh_archive-spark-iceberg/README.md) | [notebooks](scenarios/sessionization-gh_archive-spark-iceberg/notebooks.md) |
| 14 | [star_schema-tpch-spark-iceberg](scenarios/star_schema-tpch-spark-iceberg/README.md) | [notebooks](scenarios/star_schema-tpch-spark-iceberg/notebooks.md) |
| 15 | [streaming_ingest-events-spark-iceberg](scenarios/streaming_ingest-events-spark-iceberg/README.md) | [notebooks](scenarios/streaming_ingest-events-spark-iceberg/notebooks.md) |
| 16 | [streaming_ingest-gh_archive-spark-iceberg](scenarios/streaming_ingest-gh_archive-spark-iceberg/README.md) | [notebooks](scenarios/streaming_ingest-gh_archive-spark-iceberg/notebooks.md) |
| 17 | [streaming_windows-events-spark-iceberg](scenarios/streaming_windows-events-spark-iceberg/README.md) | [notebooks](scenarios/streaming_windows-events-spark-iceberg/notebooks.md) |
| 18 | [table_maintenance-nyc_taxi-spark-iceberg](scenarios/table_maintenance-nyc_taxi-spark-iceberg/README.md) | [notebooks](scenarios/table_maintenance-nyc_taxi-spark-iceberg/notebooks.md) |
| 19 | [time_travel-nyc_taxi-spark-iceberg](scenarios/time_travel-nyc_taxi-spark-iceberg/README.md) | [notebooks](scenarios/time_travel-nyc_taxi-spark-iceberg/notebooks.md) |

## Spark Apps

- [NYC Taxi ETL — Raw to Bronze](spark-apps/nyc-taxi-etl/README.md)
- [NYC Taxi Medallion Pipeline](spark-apps/nyc-taxi-medallion/README.md)
