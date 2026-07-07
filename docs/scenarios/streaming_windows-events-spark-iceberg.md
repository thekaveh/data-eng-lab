# streaming_windows-events-spark-iceberg

Windowed aggregation with watermark on the Redpanda `events` Kafka topic, writing closed window counts to `lakehouse.gold.event_windows` (Iceberg).

## 1. Purpose

This scenario demonstrates windowed aggregation with watermark on a Kafka stream — the aggregated streaming counterpart to the `streaming_ingest-events` scenario. It teaches how to define watermarks to handle late data and emit only closed windows to Iceberg in append mode, a critical pattern for real-time analytics.

## 2. Data Model

### 2.1 Input Source

Source: `redpanda:9092` → `events` Kafka topic (same data source as `streaming_ingest-events`; produced by `producer.py`).

| Column | Type | Notes |
|---|---|---|
| `user_id` | string | User identifier |
| `event` | string | Event type |
| `ts` | timestamp | Event timestamp |

Checkpoint: `s3a://checkpoints/event_windows`

### 2.2 Output Tables

| Table | Layer | Key Columns |
|---|---|---|
| `lakehouse.gold.event_windows` | Gold | `event`, `window_start`, `window_end`, `count` |

## 3. Architecture

![Architecture](architectures/streaming_windows-events-spark-iceberg.html)

Data flows from the Redpanda `events` topic through Spark Structured Streaming with `withWatermark` and `groupBy` over tumbling windows (5-minute windows, 10-minute watermark). Aggregation: counts events per event type per window. Results are written to Iceberg in append mode — only closed windows emit.

## 4. Notebooks

- **Zeppelin (Scala):** `zeppelin/notebook.zpln` — Sections: Overview, Setup, Read (`readStream` + schema + `from_json`), Transform (`withWatermark` + `groupBy` window + `count`), Write (`writeStream` Iceberg append), Verify; 6 sections
- **Jupyter (PySpark):** `jupyter/notebook.ipynb` — Same 6 sections, same windowed streaming logic

Both languages implement identical windowed streaming logic with watermark definition, tumbling window aggregation, and verification.

## 5. Orchestration

Streaming queries are long-running and not scheduled as batch DAGs. The Airflow DAG (`streaming_windows_events`) is an `EmptyOperator` placeholder.

## 6. Usage

1. Start Atlas with Redpanda: `make up` (requires Atlas A9 / issue #269)
2. Produce events: `python scenarios/streaming_ingest-events-spark-iceberg/producer.py [count]`
3. Open either notebook on the Atlas stack and run all sections
4. Closed windows appear in `lakehouse.gold.event_windows`
5. Verify:
    ```bash
    spark-sql -e "SELECT * FROM lakehouse.gold.event_windows LIMIT 10"
    ```

## 7. Dependencies

- **Dataset:** Synthetic events from `streaming_ingest-events-spark-iceberg/producer.py`
- **Atlas services:** A1-A4 (Spark, Iceberg, S3 catalog, lakehouse catalog), A9 (Redpanda)
- **Other:** None

## 8. Known Issues & Caveats

Atlas seeds only the `atlas_stream_events` demo topic; this scenario's topic (`events`) is auto-created on first produce. Notebook execution and Scala/PySpark parity are live-gated on Atlas A9 (Redpanda). Produce events first via `streaming_ingest-events-spark-iceberg/producer.py`. Checkpoints at `s3a://checkpoints/event_windows`. Append mode emits only closed windows (after watermark passes); call `query.awaitTermination()` to block in both Scala and PySpark notebooks. The DAG (`streaming_windows_events`) is an `EmptyOperator` — Structured Streaming is long-running, not scheduled as a batch DAG.

## See Also

- [Upstream: streaming_ingest-events-spark-iceberg](./streaming_ingest-events-spark-iceberg.md) — Produces the events topic this scenario consumes
- [Related: cdc_streaming-online_retail-spark-iceberg](./cdc_streaming-online_retail-spark-iceberg.md) — Another CDC/streaming scenario
- [Datasets](../datasets.md)
- [Lakehouse Architecture](../lakehouse.md)
