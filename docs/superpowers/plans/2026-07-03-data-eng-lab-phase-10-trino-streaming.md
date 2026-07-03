# data-eng-lab — Phase 10: Trino BI + Advanced Streaming Scenarios (3) — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Author the remaining roadmap scenarios that exercise Atlas A7 (Trino) and A9 (Redpanda) — assumed fulfilled: `bi_query-tpch-trino` (multi-engine read, #15), `streaming_windows-events` (windowed aggregation + watermark), `cdc_streaming-online_retail` (streaming CDC via `foreachBatch` MERGE). Structural CI-green; execution live-gated on A7/A9.

**Architecture:** Same framework (`build_notebooks.write_scenario`). `bi_query` reuses the Zeppelin `%jdbc(trino)` interpreter override + a Jupyter `trino` client (same SQL over the Spark-written `gold` TPC-H marts — true multi-engine). The two streaming scenarios use `%spark` Kafka Structured Streaming against `redpanda:9092`. `EmptyOperator` DAGs. All live-gated.

**Tech Stack:** Python 3.11 + nbformat; Trino SQL; Spark 4.1.2 Structured Streaming + Kafka + Iceberg `MERGE` (authored, live-gated on A7/A9).

## Global Constraints

- **Never edit `infra/` or `pyproject.toml`.**
- **Scenario layout:** `scenarios/<name>/{README.md (6 sections), zeppelin/notebook.zpln, jupyter/notebook.ipynb, dag.py}`; 6 sections; 4-part naming.
- **Authoring:** THROWAWAY script per scenario → `bn.write_scenario(...)`. `bi_query` passes `zeppelin_interpreter="%jdbc(trino)"` (SQL cells) + a Jupyter `trino`-client `PY` (same SQL). The streaming scenarios use default `%spark`. Scala Setup imports `spark.implicits._` + `functions._` (+ `types._` for streaming schemas); PySpark Setup `from pyspark.sql import SparkSession, functions as F` (+ `from pyspark.sql.types import StructType, StringType, IntegerType, DoubleType, TimestampType` for streaming). Import only what's used.
- **Parity:** `bi_query` — identical SQL across `%jdbc` + client. Streaming — Scala/PySpark same pipeline (topic, schema, window/checkpoint/target); `cdc_streaming` shares the identical `MERGE` SQL inside `foreachBatch` (Scala anonymous fn vs Python def is the idiomatic delta).
- **Assumed A7/A9 contract:** Trino `trino:8080` catalog `lakehouse`; Redpanda `redpanda:9092`; `spark-sql-kafka-0-10_2.13:4.1.2` baked; checkpoints `s3a://checkpoints/`.
- **DAGs `EmptyOperator` placeholders** (Trino interactive; streaming long-running) — no `SparkSubmitOperator` (guard stays green). READMEs §6 note live-gated on A7 (bi_query) / A9 (streaming) + `register_iceberg` + (bi_query) that the `gold` TPC-H marts come from the `star_schema` scenario.
- Python 3.11, ruff 120. Branch/PR: both required checks green; **push before merge**.

---

### Task 1: `bi_query-tpch-trino-iceberg`

**Files:** 4 files under `scenarios/bi_query-tpch-trino-iceberg/`. Uses `zeppelin_interpreter="%jdbc(trino)"`.
Teaches: multi-engine read via Trino over the Spark-written TPC-H `gold` marts (`gold.fct_orders`, `gold.dim_customer` from the `star_schema` scenario). Same SQL in Zeppelin `%jdbc(trino)` (raw SQL) and Jupyter `trino` client. Sections:
- `2. Setup` (Zeppelin `CODE`): `-- %jdbc(trino) is pre-bound to the Atlas Trino coordinator (catalog: lakehouse)`. (Jupyter `PY`: `from trino.dbapi import connect` + `cur = connect(host='trino', port=8080, user='data-eng', catalog='lakehouse').cursor()` + a `def q(sql):\n    cur.execute(sql)\n    return cur.fetchall()`.)
- `3. Read` SQL: `SELECT * FROM lakehouse.gold.fct_orders LIMIT 10`.
- `4. Transform` SQL: `SELECT c.c_mktsegment, sum(f.revenue) AS revenue, sum(f.line_count) AS lines FROM lakehouse.gold.fct_orders f JOIN lakehouse.gold.dim_customer c ON f.o_custkey = c.c_custkey GROUP BY c.c_mktsegment ORDER BY revenue DESC`.
- `5. Write` SQL: `CREATE TABLE IF NOT EXISTS lakehouse.gold.bi_segment_revenue AS SELECT c.c_mktsegment, sum(f.revenue) AS revenue FROM lakehouse.gold.fct_orders f JOIN lakehouse.gold.dim_customer c ON f.o_custkey = c.c_custkey GROUP BY c.c_mktsegment`.
- `6. Verify` SQL: `SELECT count(*) FROM lakehouse.gold.bi_segment_revenue`.
Zeppelin cells are the raw SQL; Jupyter cells wrap each in `q('…')`.

- [ ] Author via throwaway (`zeppelin_interpreter="%jdbc(trino)"`); 6-section README (multi-engine Trino read of the star_schema gold marts; live-gated on A7 / atlas#268; run `star_schema` first). DAG `EmptyOperator` dag_id `bi_query_tpch`.
- [ ] `uv run python scripts/verify_repo.py --root .` exit 0; `uv run ruff check .` clean; `uv run pytest -m "not infra" -q` green; `uv run pytest tests/test_dag_catalog_conf.py -q` 1 passed. Sanity: `.zpln` `%jdbc(trino)` (not %spark), `.ipynb` trino client, SQL matches, 6 sections, EmptyOperator DAG.
- [ ] `git add scenarios/bi_query-tpch-trino-iceberg && git commit -m "feat(scenarios): bi_query-tpch-trino (multi-engine Trino read of gold marts)"`

---

### Task 2: `streaming_windows-events-spark-iceberg`

**Files:** 4 files under `scenarios/streaming_windows-events-spark-iceberg/`. Default `%spark`.
Teaches: windowed aggregation + watermark on a Kafka stream (Redpanda `events` topic; same topic as `streaming_ingest-events`). Scala Setup adds `import org.apache.spark.sql.types._`; PySpark adds the types import. Sections:
- `3. Read`:
  Scala: `val schema = new StructType().add("user_id", StringType).add("event", StringType).add("ts", TimestampType)`; `val raw = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "redpanda:9092").option("subscribe", "events").option("startingOffsets", "earliest").load()`; `val events = raw.select(from_json($"value".cast("string"), schema).as("e")).select("e.*")`.
  PySpark: `schema = StructType().add("user_id", StringType()).add("event", StringType()).add("ts", TimestampType())`; `raw = spark.readStream.format("kafka").option("kafka.bootstrap.servers","redpanda:9092").option("subscribe","events").option("startingOffsets","earliest").load()`; `events = raw.select(F.from_json(F.col("value").cast("string"), schema).alias("e")).select("e.*")`.
- `4. Transform`:
  Scala: `val windows = events.withWatermark("ts", "10 minutes").groupBy(window($"ts", "5 minutes"), $"event").count()`.
  PySpark: `windows = events.withWatermark("ts", "10 minutes").groupBy(F.window("ts", "5 minutes"), F.col("event")).count()`.
- `5. Write`:
  `val query = windows.writeStream.format("iceberg").outputMode("append").option("checkpointLocation", "s3a://checkpoints/event_windows").toTable("lakehouse.gold.event_windows")` (+ comment `query.awaitTermination()`). PySpark identical.
- `6. Verify`: `spark.table("lakehouse.gold.event_windows").orderBy("window").show(false)` (PySpark `.orderBy("window").show(truncate=False)`).

- [ ] Author; README (windowed streaming agg + watermark; append mode emits closed windows; Redpanda `events` topic — run the `streaming_ingest-events` `producer.py`; live-gated on A9 / atlas#269; checkpoints `s3a://checkpoints/event_windows`). DAG `EmptyOperator` dag_id `streaming_windows_events`.
- [ ] verify gates (as Task 1). commit `feat(scenarios): streaming_windows-events (windowed aggregation + watermark)`.

---

### Task 3: `cdc_streaming-online_retail-spark-iceberg`

**Files:** 4 files under `scenarios/cdc_streaming-online_retail-spark-iceberg/`. Default `%spark`.
Teaches: streaming CDC upserts — a Kafka change-stream applied to an Iceberg table via `foreachBatch` + `MERGE INTO`. Scala Setup adds `import org.apache.spark.sql.types._` + `import org.apache.spark.sql.DataFrame`; PySpark adds the types import. Sections:
- `3. Read` (create target + read stream):
  Scala: `spark.sql("CREATE TABLE IF NOT EXISTS lakehouse.silver.online_retail_cdc (invoice string, stock_code string, quantity int, price double) USING iceberg")`; `val schema = new StructType().add("invoice", StringType).add("stock_code", StringType).add("quantity", IntegerType).add("price", DoubleType)`; `val raw = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "redpanda:9092").option("subscribe", "online_retail_cdc").option("startingOffsets", "earliest").load()`; `val cdc = raw.select(from_json($"value".cast("string"), schema).as("c")).select("c.*")`.
  PySpark: same with `spark.sql(...)`, `StructType().add(...)`, `spark.readStream…`, `F.from_json`.
- `4. Transform`: (no-op / pass-through; the CDC merge is the write). Scala: `val parsed = cdc`. PySpark: `parsed = cdc`. (Include a `// CDC rows are upserted per micro-batch in the Write step` comment.)
- `5. Write` (foreachBatch MERGE):
  Scala:
  ```
  val query = parsed.writeStream.foreachBatch { (batchDF: DataFrame, batchId: Long) =>
    batchDF.createOrReplaceTempView("cdc_batch")
    batchDF.sparkSession.sql("MERGE INTO lakehouse.silver.online_retail_cdc t USING cdc_batch s ON t.invoice = s.invoice AND t.stock_code = s.stock_code WHEN MATCHED THEN UPDATE SET t.quantity = s.quantity, t.price = s.price WHEN NOT MATCHED THEN INSERT *")
  }.option("checkpointLocation", "s3a://checkpoints/online_retail_cdc").start()
  ```
  PySpark:
  ```
  def upsert_batch(batch_df, batch_id):
      batch_df.createOrReplaceTempView("cdc_batch")
      batch_df.sparkSession.sql("MERGE INTO lakehouse.silver.online_retail_cdc t USING cdc_batch s ON t.invoice = s.invoice AND t.stock_code = s.stock_code WHEN MATCHED THEN UPDATE SET t.quantity = s.quantity, t.price = s.price WHEN NOT MATCHED THEN INSERT *")

  query = parsed.writeStream.foreachBatch(upsert_batch).option("checkpointLocation", "s3a://checkpoints/online_retail_cdc").start()
  ```
- `6. Verify`: `spark.table("lakehouse.silver.online_retail_cdc").orderBy("invoice").show(false)` (PySpark truncate=False).

- [ ] Author; README (streaming CDC via foreachBatch MERGE; Redpanda `online_retail_cdc` topic; live-gated on A9 / atlas#269; the MERGE SQL is identical to the batch `incremental_upsert` scenario — this is its streaming form; checkpoints `s3a://checkpoints/online_retail_cdc`). DAG `EmptyOperator` dag_id `cdc_streaming_online_retail`.
- [ ] verify gates. commit `feat(scenarios): cdc_streaming-online_retail (streaming CDC via foreachBatch MERGE)`.

---

### Task 4: Docs

**Files:** Modify `docs/scenarios.md`.
- [ ] Add an "Advanced (Trino / streaming; A7/A9-gated)" group listing the 3 (each: name + teaches + gate). Note bi_query needs A7 (#268) + the `star_schema` marts; the streaming ones need A9 (#269) + the producers. This exhausts the design's roadmap scenario list.
- [ ] `uv run python scripts/verify_repo.py --root .` exit 0; `uv run ruff check .` clean; `uv run pytest -m "not infra" -q` green.
- [ ] `git add docs/scenarios.md && git commit -m "docs(scenarios): list the Trino BI + advanced streaming scenarios"`

---

## Phase 10 exit criteria

- [ ] `uv run pytest -m "not infra" -q` all pass; `uv run python scripts/verify_repo.py --root .` exit 0 (3 new scenarios validate); `uv run ruff check .` clean; `test_dag_catalog_conf` guard passes.
- [ ] `bi_query` uses `%jdbc(trino)` + trino client (same SQL); the two streaming scenarios use `%spark` Kafka; all `EmptyOperator` DAGs; 6-section READMEs.
- [ ] PR into `main`: both required checks green; **push the branch, confirm CI, then squash-merge.**

## Self-review

**Coverage:** roadmap #15 bi_query-trino (Task 1) + the advanced-streaming roadmap entries streaming_windows (Task 2) + cdc_streaming (Task 3). With Phases 2b/5/7/8/9, this exhausts the design's core-10 + roadmap scenario catalog.

**Parity:** bi_query SQL identical across `%jdbc`/client; streaming_windows Scala/PySpark same pipeline; cdc_streaming shares the identical `MERGE` SQL inside `foreachBatch` (the fn-shape delta is idiomatic).

**Placeholder scan:** `EmptyOperator` DAGs (Trino interactive; streaming long-running) are the established pattern; Transform being a pass-through in cdc_streaming is deliberate (the merge is the write) with an explanatory comment. Live execution gated on A7/A9; capabilities map to `docs/atlas-expectations.md` §2 (Trino, Kafka+Spark) + §3.

**Consistency:** new tables `gold.bi_segment_revenue`, `gold.event_windows`, `silver.online_retail_cdc` are distinct from all existing scenarios; checkpoints `s3a://checkpoints/{event_windows,online_retail_cdc}` distinct.
