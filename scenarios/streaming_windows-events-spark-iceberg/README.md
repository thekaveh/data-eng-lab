# streaming_windows-events-spark-iceberg

Windowed aggregation with watermark on the Redpanda `events` Kafka topic, writing closed window counts to `lakehouse.gold.event_windows` (Iceberg).

## 1. Scenario summary
Structured Streaming windowed aggregation: Redpanda `events` topic -> Iceberg `lakehouse.gold.event_windows`. Schema: `user_id STRING, event STRING, ts TIMESTAMP`. Groups events into 5-minute tumbling windows (10-minute watermark), counts per event type per window. Checkpoint at `s3a://checkpoints/event_windows`.

## 2. Why this exists
Demonstrates windowed aggregation with watermark on a Kafka stream — the aggregated streaming counterpart to `streaming_ingest-events`. Teaches how to define watermarks to handle late data and emit only closed windows to Iceberg in append mode.

## 3. What's in the notebooks
`zeppelin/notebook.zpln` (Scala `%spark`) and `jupyter/notebook.ipynb` (PySpark), each with 6 sections: Overview -> Setup -> Read (readStream + schema + from_json) -> Transform (withWatermark + groupBy window + count) -> Write (writeStream Iceberg append) -> Verify. Both languages implement the same windowed streaming logic.

## 4. How to run
1. Start Atlas with Redpanda: `make up` (requires Atlas A9 / issue #269).
2. Produce events: `python scenarios/streaming_ingest-events-spark-iceberg/producer.py [count]`.
3. Open either notebook on the Atlas stack and run all sections.
4. The `writeStream` call starts the streaming query; closed windows appear in `lakehouse.gold.event_windows`.

## 5. Data & dependencies
Requires Redpanda broker at `redpanda:9092` (Atlas A9 / issue #269); Spark with Kafka + Iceberg connectors; `lakehouse` Iceberg REST catalog (Atlas A1-A4); `gold` namespace. Producer from `streaming_ingest-events-spark-iceberg/producer.py` generates synthetic events to the `events` topic.

## 6. Known issues & caveats
Live-gated on Atlas A9 (Redpanda) / issue #269 — notebooks cannot be executed until the Redpanda service is wired into the stack. Produce events first via `streaming_ingest-events-spark-iceberg/producer.py`. Checkpoints at `s3a://checkpoints/event_windows`. Append mode emits only closed windows (after watermark passes); call `query.awaitTermination()` to block. The DAG (`streaming_windows_events`) is an EmptyOperator placeholder — Structured Streaming is long-running and not scheduled as a batch DAG.
