# streaming_ingest-events-spark-iceberg

Ingest synthetic click events from the Redpanda `events` Kafka topic into
`lakehouse.bronze.events` (Iceberg) via Spark Structured Streaming.
Scala (Zeppelin `%spark`) and PySpark (Jupyter) notebooks implement the same
streaming logic; `producer.py` generates synthetic events for local testing.

## 1. Scenario summary
Structured Streaming ingest: Redpanda `events` topic -> Iceberg `lakehouse.bronze.events`.
Schema: `user_id STRING, event STRING, ts TIMESTAMP`. Checkpoint at `s3a://checkpoints/events`.

## 2. Why this exists
Demonstrates real-time lakehouse ingest using Kafka (Redpanda) as the source and
Iceberg as the sink — the streaming counterpart to the batch-ingest scenario.

## 3. What's in the notebooks
`zeppelin/notebook.zpln` (Scala `%spark`) and `jupyter/notebook.ipynb` (PySpark),
each with 6 sections: Overview -> Setup -> Read (readStream) -> Transform (from_json) ->
Write (writeStream Iceberg) -> Verify. Both languages implement identical streaming logic.
`producer.py` produces synthetic events to the `events` topic for end-to-end testing.

## 4. How to run
1. Start Atlas with Redpanda: `make up` (requires Atlas A9 / issue #269).
2. Produce events: `python producer.py [count]` (defaults to 100 events).
3. Open either notebook on the Atlas stack and run all sections.
4. The `writeStream` call starts the streaming query; verify with `spark.table("lakehouse.bronze.events").count()`.

## 5. Data & dependencies
Requires Redpanda broker at `redpanda:9092` (Atlas A9 / issue #269); Spark with Kafka +
Iceberg connectors; `lakehouse` Iceberg REST catalog (Atlas A1-A4); `bronze` namespace
created by `scripts/register_iceberg.py`. `producer.py` requires `kafka-python`.

## 6. Known issues & caveats
Live-gated on Atlas A9 (Redpanda) / issue #269 — notebooks cannot be executed until the
Redpanda service is wired into the stack. The streaming query runs indefinitely;
call `query.awaitTermination()` (Scala) or `query.awaitTermination()` (PySpark) to block.
The DAG (`streaming_ingest_events`) is an EmptyOperator placeholder — Structured Streaming
is long-running and not scheduled as a batch DAG.
