# cdc_streaming-online_retail-spark-iceberg

Streaming CDC upserts: Kafka change-stream from the Redpanda `online_retail_cdc` topic applied to an Iceberg table via `foreachBatch` + `MERGE INTO`.

## 1. Scenario summary
Structured Streaming CDC: Redpanda `online_retail_cdc` topic -> Iceberg `lakehouse.silver.online_retail_cdc`. Schema: `invoice STRING, stock_code STRING, quantity INT, price DOUBLE`. Each micro-batch is merged into the target table via `MERGE INTO` (upsert on `invoice + stock_code`). Checkpoint at `s3a://checkpoints/online_retail_cdc`.

## 2. Why this exists
Demonstrates streaming CDC upserts using Kafka + Spark Structured Streaming + Iceberg MERGE INTO. The `foreachBatch` pattern allows full DML control per micro-batch — the streaming form of the `incremental_upsert-online_retail` batch scenario. The MERGE SQL is identical between both scenarios.

## 3. What's in the notebooks
`zeppelin/notebook.zpln` (Scala `%spark`) and `jupyter/notebook.ipynb` (PySpark), each with 6 sections: Overview -> Setup -> Read (CREATE TABLE + readStream + from_json) -> Transform (pass-through) -> Write (foreachBatch MERGE) -> Verify. Scala uses an anonymous function; PySpark uses `upsert_batch`; the MERGE SQL string is identical across both surfaces.

## 4. How to run
1. Start Atlas with Redpanda: `make up` (requires Atlas A9 / issue #269).
2. Produce CDC events to the `online_retail_cdc` topic (JSON: `invoice, stock_code, quantity, price`).
3. Open either notebook on the Atlas stack and run all sections.
4. The `writeStream.foreachBatch` call upserts each micro-batch; verify with `spark.table("lakehouse.silver.online_retail_cdc").orderBy("invoice").show()`.

## 5. Data & dependencies
Requires Redpanda broker at `redpanda:9092` (Atlas A9 / issue #269); Spark with Kafka + Iceberg connectors; `lakehouse` Iceberg REST catalog (Atlas A1-A4); `silver` namespace. Producer must emit JSON with schema `{invoice, stock_code, quantity, price}` to the `online_retail_cdc` topic.

## 6. Known issues & caveats
Atlas seeds only the `atlas_stream_events` demo topic; this scenario's topic (`online_retail_cdc`) is auto-created on first produce — run `producer.py` first, or add the topic to `REDPANDA_DEMO_TOPICS` in `infra/.env`.
Live-gated on Atlas A9 (Redpanda) / issue #269 — notebooks cannot be executed until the Redpanda service is wired into the stack. Produce CDC events to the `online_retail_cdc` topic before running. Checkpoints at `s3a://checkpoints/online_retail_cdc`. The MERGE SQL is identical to the batch `incremental_upsert-online_retail` scenario — this is its streaming form. The DAG (`cdc_streaming_online_retail`) is an EmptyOperator placeholder — Structured Streaming is long-running and not scheduled as a batch DAG.
