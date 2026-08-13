# cdc_streaming-online_retail-spark-iceberg

Streaming CDC (Change Data Capture) upserts from the Redpanda `online_retail_cdc` topic, applied to an Iceberg table via `foreachBatch` + `MERGE INTO` for idempotent real-time updates.

## 1. Purpose

This scenario demonstrates streaming CDC upserts using Kafka + Spark Structured Streaming combined with Iceberg's `MERGE INTO` syntax. The `foreachBatch` pattern allows full DML control per micro-batch — each incoming batch of changes is merged into the target Iceberg table, updating existing rows and inserting new ones. This is the streaming counterpart of the batch `incremental_upsert-online_retail` scenario.

## 2. Data Model

### 2.1 Input Source

Source: `redpanda:9092` → `online_retail_cdc` Kafka topic (JSON messages).

| Column | Type | Notes |
|---|---|---|
| `invoice` | string | Invoice number (part of composite key) |
| `stock_code` | string | Product code (part of composite key) |
| `quantity` | int | Quantity ordered |
| `price` | double | Unit price |
| `CustomerID` | double (nullable) | Customer identifier |
| `Country` | string | Customer country |

Checkpoint: `s3a://checkpoints/online_retail_cdc`

### Checkpoint policy (#85)

Checkpoint ID `streaming-online-retail-cdc-v1` is owned by **Streaming Data Engineering** and classified as a **durable stream**. While active or uncertain it is never age-deleted. Eligibility requires reviewed retirement, a stopped or retired terminal lease, approved recovery, and a 30-day quarantine. CDC recovery is not assumed safe: source retention, event ordering, and sink corrections must be reviewed. Manual exact-leaf retention is available through issue #86's reviewed plan/prepare/apply protocol. Automated and scheduled deletion remain disabled pending stronger MinIO cross-process CAS and conditional delete proof.

### 2.2 Output Tables

| Table | Layer | Key Columns |
|---|---|---|
| `lakehouse.silver.online_retail_cdc` | Silver | Same as input schema; updated and inserted rows reflect latest values |

## 3. Architecture

![Architecture](../../docs/diagrams/img/cdc_streaming-online_retail-spark-iceberg.png)

CDC events flow from the Redpanda `online_retail_cdc` topic through Spark Structured Streaming (`readStream` + `from_json`) into an Iceberg table. Each micro-batch triggers a `foreachBatch` callback that executes `MERGE INTO` — the same MERGE SQL as the batch `incremental_upsert-online_retail` scenario. The upsert key is the composite `(invoice, stock_code)`.

## 4. Notebooks

- **Zeppelin (Scala):** `zeppelin/notebook.zpln` — Sections: Overview, Setup, Read (`CREATE TABLE` + `readStream` + `from_json`), Transform (pass-through), Write (`foreachBatch` + `MERGE INTO`), Verify; 6 sections; Scala uses an anonymous function for the foreachBatch callback
- **Jupyter (PySpark):** `jupyter/notebook.ipynb` — Same 6 sections; PySpark uses `upsert_batch` function; the `MERGE INTO` SQL string is identical across both languages

## 5. Orchestration

Classification: **intentionally unscheduled long-running streaming**. No Airflow DAG or batch schedule exists. An operator starts, monitors, and stops the continuous notebook query and owns its `online_retail_cdc` checkpoint.

## 6. Usage

1. Start Atlas with Redpanda: `make up`
2. Produce CDC events to the `online_retail_cdc` topic (JSON: `invoice`, `stock_code`, `quantity`, `price`)
3. Open either notebook on the Atlas stack and run all sections
4. The `writeStream.foreachBatch` call upserts each micro-batch; verify:
    ```bash
    spark-sql -e "SELECT * FROM lakehouse.silver.online_retail_cdc ORDER BY invoice LIMIT 10"
    ```

## 7. Dependencies

- **Dataset:** Synthetic CDC events (producer must emit JSON with schema `{invoice, stock_code, quantity, price}`)
- **Atlas services:** A1-A4 (Spark, Iceberg, S3 catalog, lakehouse catalog), A9 (Redpanda)
- **Other:** None

## 8. Known Issues & Caveats

The `online_retail_cdc` topic is auto-created on first produce. Notebook execution and Scala/PySpark parity are live-gated on Atlas A9 (Redpanda). Produce CDC events to the topic before running. Checkpoints at `s3a://checkpoints/online_retail_cdc`. The `MERGE INTO` SQL is identical to the batch `incremental_upsert-online_retail` scenario — this is its streaming form.

## See Also

- [Related: incremental_upsert-online_retail-spark-iceberg](../incremental_upsert-online_retail-spark-iceberg/README.md) — Batch form of the same CDC upsert pattern
- [Related: scd2-online_retail-spark-iceberg](../scd2-online_retail-spark-iceberg/README.md) — Another online_retail dimension scenario
- [Datasets](../../docs/datasets.md)
- [Lakehouse Architecture](../../docs/lakehouse.md)
