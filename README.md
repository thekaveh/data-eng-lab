# data-eng-lab

**An Iceberg-lakehouse data-engineering lab built on the [Atlas](https://github.com/thekaveh/atlas) platform.**

Curated Spark scenarios in Scala (Zeppelin) and PySpark (Jupyter), orchestrated with Airflow, plus Maven Scala Spark apps built by Jenkins — all over Apache Iceberg on MinIO, cataloged by the Atlas Iceberg REST catalog.

---

## Architecture

The lab implements a **medallion lakehouse** with four layers:

```
s3a://landing/   →   bronze   →   silver   →   gold
  (raw Parquet)       (clean)     (enriched)   (aggregated/modelled)
```

Every table is an **Apache Iceberg** table, accessed through the Atlas **Iceberg REST catalog** (`lakehouse`). Compute is provided by a Spark cluster; Trino handles ad-hoc SQL and federated queries. Redpanda (Kafka-compatible) drives all streaming scenarios. Airflow schedules production DAGs; Jenkins builds and publishes Maven Spark apps to MinIO artifacts.

---

## Quick navigation

-   ### Scenario catalog
    19 end-to-end Spark and Trino scenarios across bronze, silver, and gold layers.
     - [Browse scenarios](docs/scenarios/index.md)
-   ### Spark apps
    2 CI-verified Maven Scala Spark apps built by Jenkins and run by Airflow.
     - [Browse apps](docs/spark-apps/index.md)
-   ### Datasets
    5 curated datasets (NYC Taxi, TPC-H, Online Retail, GH Archive, Events) with `make datasets`.
    - [Dataset guide](docs/datasets.md)
-   ### Lakehouse design
    Medallion layout, Iceberg namespaces, MinIO buckets, and the bronze smoke test.
    - [Lakehouse guide](docs/lakehouse.md)
-   ### Atlas platform
    A1–A9 Atlas enablement checklist, expectations, and go-live runbook.
    - [Atlas enablement](docs/atlas-enablement.md)
-   ### Getting started
    Prerequisites, `make datasets`, starting the stack, and running notebooks.
    - [Quick start](docs/getting-started.md)

---

## By the numbers

| What | Count |
|------|-------|
| Scenario notebooks (Scala + PySpark pairs) | 19 |
| CI-verified Maven Spark apps | 2 |
| Curated datasets | 5 |
| Atlas enablement items (A1–A9) | 9 |
| Iceberg medallion layers | 3 (bronze / silver / gold) |

---

## Scenario catalog

| Scenario | Engine | Layer | Dataset |
|---|---|---|---|
| [batch_ingest-nyc_taxi-spark-iceberg](scenarios/batch_ingest-nyc_taxi-spark-iceberg.md) | Spark | Bronze | NYC Taxi |
| [medallion-nyc_taxi-spark-iceberg](scenarios/medallion-nyc_taxi-spark-iceberg.md) | Spark | Bronze→Silver→Gold | NYC Taxi |
| [data_quality-nyc_taxi-spark-iceberg](scenarios/data_quality-nyc_taxi-spark-iceberg.md) | Spark | Silver | NYC Taxi |
| [schema_evolution-gh_archive-spark-iceberg](scenarios/schema_evolution-gh_archive-spark-iceberg.md) | Spark | Silver | GH Archive |
| [time_travel-nyc_taxi-spark-iceberg](scenarios/time_travel-nyc_taxi-spark-iceberg.md) | Spark | Silver | NYC Taxi |
| [table_maintenance-nyc_taxi-spark-iceberg](scenarios/table_maintenance-nyc_taxi-spark-iceberg.md) | Spark | Silver | NYC Taxi |
| [streaming_ingest-events-spark-iceberg](scenarios/streaming_ingest-events-spark-iceberg.md) | Spark (stream) | Bronze | Events |
| [streaming_ingest-gh_archive-spark-iceberg](scenarios/streaming_ingest-gh_archive-spark-iceberg.md) | Spark (stream) | Bronze | GH Archive |
| [streaming_windows-events-spark-iceberg](scenarios/streaming_windows-events-spark-iceberg.md) | Spark (stream) | Silver | Events |
| [cdc_streaming-online_retail-spark-iceberg](scenarios/cdc_streaming-online_retail-spark-iceberg.md) | Spark (stream) | Silver | Online Retail |
| [federated_query-nyc_taxi-trino-iceberg](scenarios/federated_query-nyc_taxi-trino-iceberg.md) | Trino | Gold | NYC Taxi |
| [bi_query-tpch-trino-iceberg](scenarios/bi_query-tpch-trino-iceberg.md) | Trino | Gold | TPC-H |
| [join_optimization-tpch-spark-iceberg](scenarios/join_optimization-tpch-spark-iceberg.md) | Spark | Gold | TPC-H |
| [star_schema-tpch-spark-iceberg](scenarios/star_schema-tpch-spark-iceberg.md) | Spark | Gold | TPC-H |
| [feature_engineering-movielens-spark-iceberg](scenarios/feature_engineering-movielens-spark-iceberg.md) | Spark | Gold | MovieLens |
| [scd2-online_retail-spark-iceberg](scenarios/scd2-online_retail-spark-iceberg.md) | Spark | Silver | Online Retail |
| [json_flatten-gh_archive-spark-iceberg](scenarios/json_flatten-gh_archive-spark-iceberg.md) | Spark | Silver | GH Archive |
| [sessionization-gh_archive-spark-iceberg](scenarios/sessionization-gh_archive-spark-iceberg.md) | Spark | Silver | GH Archive |
| [incremental_upsert-online_retail-spark-iceberg](scenarios/incremental_upsert-online_retail-spark-iceberg.md) | Spark | Silver | Online Retail |

---

> **New here?**
Start with [Getting started](docs/getting-started.md) to get the stack running, then pick a scenario from the [catalog](docs/scenarios/index.md) or dive into the [lakehouse design](docs/lakehouse.md).

> **Atlas platform**
The Atlas platform underpins this lab. See [Atlas enablement](docs/atlas-enablement.md) for the full A1–A9 checklist and [Go-live runbook](docs/go-live.md) for production readiness steps.

---

## Quick Start

```bash
git clone https://github.com/thekaveh/data-eng-lab.git
cd data-eng-lab
make setup          # initialize Atlas submodule
make datasets       # download datasets
make up             # launch all services
make preflight      # verify connectivity
```

See [Getting Started](docs/getting-started.md) for the full guide.

## Spark Applications

| Application | Description |
|---|---|
| [nyc-taxi-etl](spark-apps/nyc-taxi-etl/) | Raw Parquet → cleaned Bronze Iceberg |
| [nyc-taxi-medallion](spark-apps/nyc-taxi-medallion/) | Bronze → Silver → Gold medallion pipeline |

Built by Jenkins CI, submitted via Airflow DAG. See [Spark Apps](docs/spark-apps/index.md) for details.

## License

This project uses a proprietary license — see [`LICENSE`](LICENSE) for terms.

---

*Full documentation at [thekaveh.github.io/data-eng-lab](https://thekaveh.github.io/data-eng-lab/). Maintained by `data-eng-lab`. Questions or issues → open a GitHub issue.*
