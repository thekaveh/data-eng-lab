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

<div class="grid cards" markdown>

-   :material-database-search:{ .lg .middle } **Scenario catalog**

    ---

    19 end-to-end Spark and Trino scenarios across bronze, silver, and gold layers.

    [:octicons-arrow-right-24: Browse scenarios](scenarios.md)

-   :material-rocket-launch:{ .lg .middle } **Spark apps**

    ---

    2 CI-verified Maven Scala Spark apps built by Jenkins and run by Airflow.

    [:octicons-arrow-right-24: Browse apps](spark-apps.md)

-   :material-table-large:{ .lg .middle } **Datasets**

    ---

    5 curated datasets (NYC Taxi, TPC-H, Online Retail, GH Archive, Events) with `make datasets`.

    [:octicons-arrow-right-24: Dataset guide](datasets.md)

-   :material-layers-triple:{ .lg .middle } **Lakehouse design**

    ---

    Medallion layout, Iceberg namespaces, MinIO buckets, and the bronze smoke test.

    [:octicons-arrow-right-24: Lakehouse guide](lakehouse.md)

-   :material-check-decagram:{ .lg .middle } **Atlas platform**

    ---

    A1–A9 Atlas enablement checklist, expectations, and go-live runbook.

    [:octicons-arrow-right-24: Atlas enablement](atlas-enablement.md)

-   :material-play-box-multiple:{ .lg .middle } **Getting started**

    ---

    Prerequisites, `make datasets`, starting the stack, and running notebooks.

    [:octicons-arrow-right-24: Quick start](getting-started.md)

</div>

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

!!! tip "New here?"
    Start with [Getting started](getting-started.md) to get the stack running, then pick a scenario from the [catalog](scenarios.md) or dive into the [lakehouse design](lakehouse.md).

!!! info "Atlas platform"
    The Atlas platform underpins this lab. See [Atlas enablement](atlas-enablement.md) for the full A1–A9 checklist and [Go-live runbook](go-live.md) for production readiness steps.
