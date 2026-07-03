# data-eng-lab — Phase 7: Iceberg-Lever & Transform Scenarios (6) — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Author 6 of the design's core-10 scenarios that need only the **delivered** Atlas (Spark 4.1.2 + Iceberg 1.11.0) and already-registered datasets — demonstrating Iceberg's signature levers. Structural checks pass in CI now; execution is live-gated.

**Architecture:** Each scenario uses the existing framework (`build_notebooks.write_scenario`): README (6 sections) + `zeppelin/notebook.zpln` (Scala `%spark`) + `jupyter/notebook.ipynb` (PySpark) + an `EmptyOperator` `dag.py`. Most cells are `spark.sql("…")` (Iceberg DDL / `system.*` procedures / metadata tables), so **Scala and PySpark are nearly identical** — parity is trivial and exact. All 6 scenarios read datasets already in the registry (`nyc_taxi`, `tpch`, `gh_archive`) and write into `lakehouse.{silver,gold}` (namespaces created by `register_iceberg`).

**Tech Stack:** Python 3.11 + `nbformat`; Spark 4.1.2 SQL + Iceberg 1.11.0 (authored, live-gated).

## Global Constraints

- **Never edit `infra/`.**
- **Scenario layout:** `scenarios/<name>/{README.md (6 sections), zeppelin/notebook.zpln, jupyter/notebook.ipynb, dag.py}`; the 6 notebook sections `1. Overview`/`2. Setup`/`3. Read`/`4. Transform`/`5. Write`/`6. Verify`; naming `[pattern]-[dataset]-[engine]-[format]` (4 hyphen parts).
- **Authoring:** build each scenario with a THROWAWAY script calling `bn.write_scenario(ROOT, name, CODE, PY, DAG, README)` (Scala `CODE`, PySpark `PY`; default `%spark` interpreter). Do NOT commit the throwaway script. Scala Setup imports `import spark.implicits._` + `import org.apache.spark.sql.functions._`; PySpark Setup `from pyspark.sql import SparkSession, functions as F` + `spark = SparkSession.builder.remote("sc://spark-connect:15002").getOrCreate()`.
- **DAGs are `EmptyOperator` placeholders** (mirror `scenarios/medallion-nyc_taxi-spark-iceberg/dag.py`); no `SparkSubmitOperator` (keeps the Phase-4 `test_dag_catalog_conf` guard green). Each README §6 notes: live-gated on the enhanced stack; requires `scripts/register_iceberg.py` (namespaces) + the dataset in `landing` (`make datasets`).
- **CI-green now** structurally; execution live-gated (no new tests here beyond the verifier). Python 3.11, ruff 120.
- **Branch/PR:** land via feature branch → PR; `static-and-unit` + `maven-spark-apps` green.

## Per-scenario authoring note

For SQL-heavy scenarios, the Scala `CODE[sec]` and PySpark `PY[sec]` are the SAME `spark.sql("…")` string (Scala and PySpark both call `spark.sql`). Where a DataFrame API is used, translate idiomatically (`$"c"`/`col("c")` → `F.col("c")`, `.as(x)` → `.alias(x)`). Keep the SQL identical across surfaces.

---

### Task 1: `time_travel-nyc_taxi-spark-iceberg`

**Files:** Create the 4 scenario files under `scenarios/time_travel-nyc_taxi-spark-iceberg/`.

Teaches: snapshots, `VERSION AS OF`, rollback, branch/tag (WAP). Sections (Scala == PySpark, all `spark.sql`):
- `2. Setup`: imports (Scala) / SparkSession.remote (PySpark).
- `3. Read`: `spark.sql("CREATE TABLE IF NOT EXISTS lakehouse.silver.nyc_taxi_tt AS SELECT * FROM lakehouse.bronze.nyc_taxi_trips").show()` — snapshot 1.
- `4. Transform`: `spark.sql("INSERT INTO lakehouse.silver.nyc_taxi_tt SELECT * FROM lakehouse.bronze.nyc_taxi_trips WHERE passenger_count > 3")` — snapshot 2.
- `5. Write`: `spark.sql("ALTER TABLE lakehouse.silver.nyc_taxi_tt CREATE BRANCH audit")` — WAP branch.
- `6. Verify`: `spark.sql("SELECT committed_at, snapshot_id FROM lakehouse.silver.nyc_taxi_tt.history ORDER BY committed_at").show(false)` then a comment showing `SELECT count(*) FROM lakehouse.silver.nyc_taxi_tt VERSION AS OF <first-snapshot-id>` and `CALL lakehouse.system.rollback_to_snapshot('lakehouse.silver.nyc_taxi_tt', <id>)`.

(For PySpark, `.show(false)` → `.show(truncate=False)`.)

- [ ] **Step 1:** author via throwaway script; 6-section README describing time-travel on nyc_taxi.
- [ ] **Step 2:** `uv run python scripts/verify_repo.py --root .` exit 0; `uv run ruff check .` clean; `uv run pytest -m "not infra" -q` green; confirm `.zpln` Scala `%spark`, `.ipynb` PySpark, DAG EmptyOperator.
- [ ] **Step 3:** `git add scenarios/time_travel-nyc_taxi-spark-iceberg && git commit -m "feat(scenarios): time_travel-nyc_taxi (snapshots, VERSION AS OF, branch/WAP)"`

---

### Task 2: `table_maintenance-nyc_taxi-spark-iceberg`

**Files:** the 4 files under `scenarios/table_maintenance-nyc_taxi-spark-iceberg/`.

Teaches: compaction, `expire_snapshots`, orphan cleanup. Sections (Scala == PySpark):
- `3. Read`: `spark.sql("SELECT count(*) AS files FROM lakehouse.bronze.nyc_taxi_trips.files").show()` — data-file count before.
- `4. Transform`: `spark.sql("CALL lakehouse.system.rewrite_data_files(table => 'lakehouse.bronze.nyc_taxi_trips', options => map('target-file-size-bytes','134217728'))").show()` — compaction.
- `5. Write`: two procedures —
  `spark.sql("CALL lakehouse.system.expire_snapshots(table => 'lakehouse.bronze.nyc_taxi_trips', older_than => TIMESTAMP '2000-01-01 00:00:00', retain_last => 1)").show()`
  and `spark.sql("CALL lakehouse.system.remove_orphan_files(table => 'lakehouse.bronze.nyc_taxi_trips')").show()`.
- `6. Verify`: `spark.sql("SELECT count(*) AS files FROM lakehouse.bronze.nyc_taxi_trips.files").show()` — data-file count after.

- [ ] **Step 1–3:** as Task 1 (README: maintenance on nyc_taxi; commit `feat(scenarios): table_maintenance-nyc_taxi (rewrite_data_files, expire_snapshots, remove_orphan_files)`).

---

### Task 3: `star_schema-tpch-spark-iceberg`

**Files:** the 4 files under `scenarios/star_schema-tpch-spark-iceberg/`.

Teaches: dimensional modeling; fact/dim gold marts from TPC-H (landed at `s3a://landing/tpch/<table>/` per the `tpch` dataset). Sections:
- `3. Read` (Scala): `val orders = spark.read.parquet("s3a://landing/tpch/orders")` + `val customer = spark.read.parquet("s3a://landing/tpch/customer")` + `val lineitem = spark.read.parquet("s3a://landing/tpch/lineitem")`. (PySpark: `orders = spark.read.parquet("s3a://landing/tpch/orders")`, etc.)
- `4. Transform`:
  Scala: `val dimCustomer = customer.select($"c_custkey", $"c_name", $"c_nationkey", $"c_mktsegment")`;
  `val fctOrders = orders.join(lineitem, orders("o_orderkey") === lineitem("l_orderkey")).groupBy($"o_orderkey", $"o_custkey", $"o_orderdate").agg(sum($"l_extendedprice").as("revenue"), count("*").as("line_count"))`.
  PySpark: same with `F.col`, `.alias`, `F.sum`, `F.count`.
- `5. Write`: `dimCustomer.writeTo("lakehouse.gold.dim_customer").using("iceberg").createOrReplace()` and `fctOrders.writeTo("lakehouse.gold.fct_orders").using("iceberg").createOrReplace()`.
- `6. Verify`: `spark.table("lakehouse.gold.fct_orders").join(spark.table("lakehouse.gold.dim_customer"), "c_custkey" -> ...)` — keep simple: `spark.sql("SELECT c.c_mktsegment, sum(f.revenue) AS revenue FROM lakehouse.gold.fct_orders f JOIN lakehouse.gold.dim_customer c ON f.o_custkey = c.c_custkey GROUP BY c.c_mktsegment").show()`.

- [ ] **Step 1–3:** README: star schema on tpch (needs `make datasets` for tpch); commit `feat(scenarios): star_schema-tpch (fact/dim gold marts)`.

---

### Task 4: `json_flatten-gh_archive-spark-iceberg`

**Files:** the 4 files under `scenarios/json_flatten-gh_archive-spark-iceberg/`.

Teaches: nested JSON → typed columns, explode arrays. GH Archive events land at `s3a://landing/gh_archive/*.json.gz`. Sections:
- `3. Read` (Scala): `val raw = spark.read.json("s3a://landing/gh_archive")` + `raw.printSchema()`. (PySpark same.)
- `4. Transform`:
  Scala: `val flat = raw.select($"id", $"type", $"actor.login".as("actor_login"), $"repo.name".as("repo_name"), $"created_at".cast("timestamp").as("created_at"))`.
  PySpark: `flat = raw.select(F.col("id"), F.col("type"), F.col("actor.login").alias("actor_login"), F.col("repo.name").alias("repo_name"), F.col("created_at").cast("timestamp").alias("created_at"))`.
- `5. Write`: `flat.writeTo("lakehouse.silver.gh_events").using("iceberg").createOrReplace()`.
- `6. Verify`: `spark.sql("SELECT type, count(*) AS n FROM lakehouse.silver.gh_events GROUP BY type ORDER BY n DESC").show()`.

- [ ] **Step 1–3:** README: json flatten on gh_archive (needs `make datasets`); commit `feat(scenarios): json_flatten-gh_archive (nested JSON -> typed columns)`.

---

### Task 5: `schema_evolution-gh_archive-spark-iceberg`

**Files:** the 4 files under `scenarios/schema_evolution-gh_archive-spark-iceberg/`.

Teaches: add/rename/reorder columns; read old + new. Sections (Scala == PySpark, `spark.sql`):
- `3. Read`: `spark.sql("CREATE TABLE IF NOT EXISTS lakehouse.silver.gh_events_se AS SELECT id, type, actor.login AS actor_login FROM (SELECT * FROM json.`s3a://landing/gh_archive`)")` — NOTE: simpler to base on the Task-4 table if present; keep self-contained: `spark.sql("CREATE TABLE IF NOT EXISTS lakehouse.silver.gh_events_se (id string, type string, actor_login string) USING iceberg")` then `spark.sql("INSERT INTO lakehouse.silver.gh_events_se VALUES ('1','PushEvent','octocat')")`.
- `4. Transform`: `spark.sql("ALTER TABLE lakehouse.silver.gh_events_se ADD COLUMN repo_name string")` then `spark.sql("ALTER TABLE lakehouse.silver.gh_events_se RENAME COLUMN type TO event_type")`.
- `5. Write`: `spark.sql("INSERT INTO lakehouse.silver.gh_events_se VALUES ('2','WatchEvent','torvalds','linux')")` — new row with the evolved schema.
- `6. Verify`: `spark.sql("SELECT id, event_type, actor_login, repo_name FROM lakehouse.silver.gh_events_se ORDER BY id").show()` — old row shows `repo_name` null, both read under the new schema.

- [ ] **Step 1–3:** README: schema evolution (add/rename, read old+new); commit `feat(scenarios): schema_evolution-gh_archive (add/rename column, read old+new)`.

---

### Task 6: `streaming_ingest-gh_archive-spark-iceberg`

**Files:** the 4 files under `scenarios/streaming_ingest-gh_archive-spark-iceberg/`.

Teaches: Structured Streaming **file source** → Iceberg + checkpoints, exactly-once (NO Kafka — reads landing files; needs no Atlas A9). Sections:
- `2. Setup` (also declare a schema): Scala `import org.apache.spark.sql.types._`; PySpark `from pyspark.sql.types import StructType, StringType`.
- `3. Read`:
  Scala: `val schema = new StructType().add("id", StringType).add("type", StringType).add("created_at", StringType)`; `val stream = spark.readStream.schema(schema).json("s3a://landing/gh_archive")`.
  PySpark: `schema = StructType().add("id", StringType()).add("type", StringType()).add("created_at", StringType())`; `stream = spark.readStream.schema(schema).json("s3a://landing/gh_archive")`.
- `4. Transform`: Scala `val events = stream.withColumn("created_at", $"created_at".cast("timestamp"))`; PySpark `events = stream.withColumn("created_at", F.col("created_at").cast("timestamp"))`.
- `5. Write`: `val query = events.writeStream.format("iceberg").outputMode("append").option("checkpointLocation", "s3a://checkpoints/gh_events_file").toTable("lakehouse.bronze.gh_events_stream")` (+ comment: `query.awaitTermination()` to keep running). PySpark identical.
- `6. Verify`: `spark.table("lakehouse.bronze.gh_events_stream").count()`.

- [ ] **Step 1–3:** README: file-source streaming (needs `make datasets`; checkpoints in `s3a://checkpoints/`; needs NO Kafka/A9); commit `feat(scenarios): streaming_ingest-gh_archive (file-source Structured Streaming -> Iceberg)`.

---

### Task 7: Docs

**Files:** Modify `docs/scenarios.md`.

- [ ] **Step 1:** Add the 6 new scenarios to the "Authored scenarios" list, grouped as "Iceberg-lever & transform scenarios (Spark, delivered-Atlas only)", each one line (what it teaches + dataset). Note all 6 need only the delivered Spark/Iceberg (no A7/A9) + `register_iceberg` + `make datasets`.
- [ ] **Step 2:** `uv run python scripts/verify_repo.py --root .` exit 0; `uv run ruff check .` clean; `uv run pytest -m "not infra" -q` green.
- [ ] **Step 3:** `git add docs/scenarios.md && git commit -m "docs(scenarios): list the 6 Iceberg-lever/transform scenarios"`

---

## Phase 7 exit criteria

- [ ] `uv run python scripts/verify_repo.py --root .` → exit 0 (all 6 new scenarios validate: naming, files, 6 sections, JSON).
- [ ] `uv run ruff check .` clean; `uv run pytest -m "not infra" -q` green; the Phase-4 `test_dag_catalog_conf` guard still passes (all new DAGs `EmptyOperator`).
- [ ] Each scenario: Scala `.zpln` + PySpark `.ipynb` with matching logic; `EmptyOperator` DAG; 6-section README naming `register_iceberg`/`make datasets` dependency.
- [ ] PR into `main`: `static-and-unit` + `maven-spark-apps` green; squash-merge.

## Self-review

**Coverage:** core-10 #6 time_travel (Task 1), #7 table_maintenance (Task 2), #10 star_schema (Task 3), #8 json_flatten (Task 4), #5 schema_evolution (Task 5), #9 streaming_ingest-gh_archive file-source (Task 6). All use delivered Atlas + registered datasets — no A7/A9, no new dataset. `online_retail` scenarios (#3 incremental_upsert, #4 scd2) are Phase 8 (need the online_retail dataset).

**Parity:** SQL-heavy scenarios (time_travel, maintenance, schema_evolution) have identical `spark.sql` strings across Scala/PySpark; DataFrame scenarios (star_schema, json_flatten, streaming) translate idiomatically (`$"c"`/`.as` ↔ `F.col`/`.alias`) with identical logic.

**Placeholder scan:** DAGs are intentional `EmptyOperator` placeholders (the established pattern); every section has runnable content. Live execution is gated (Iceberg procedures/branch/WAP verified on first live run — capability expectations are in `docs/atlas-expectations.md` §3).

**Consistency:** table names (`lakehouse.silver.nyc_taxi_tt`, `lakehouse.gold.fct_orders`/`dim_customer`, `lakehouse.silver.gh_events`/`gh_events_se`, `lakehouse.bronze.gh_events_stream`) and landing paths (`s3a://landing/{tpch,gh_archive}`) are used consistently within each scenario.
