# data-eng-lab — Phase 9: Analytics Roadmap Scenarios (4) — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Author 4 of the design's roadmap scenarios (#11–14) — all Spark-only (delivered Atlas; no A7/A9) on already-registered datasets. Structural CI-green; execution live-gated.

**Architecture:** Same framework as the core-10 (`build_notebooks.write_scenario`): README (6 sections) + Scala `%spark` `.zpln` + PySpark `.ipynb` + `EmptyOperator` `dag.py`. DataFrame-API scenarios — Scala↔PySpark idiomatic parity. Reads `nyc_taxi`/`gh_archive`/`tpch`/`movielens` (registered); writes distinct `lakehouse.{silver,gold}` tables.

**Tech Stack:** Python 3.11 + nbformat; Spark 4.1.2 DataFrame/SQL + Iceberg (authored, live-gated).

## Global Constraints

- **Never edit `infra/` or `pyproject.toml`.**
- **Scenario layout:** `scenarios/<name>/{README.md (6 sections), zeppelin/notebook.zpln (Scala %spark), jupyter/notebook.ipynb (PySpark), dag.py}`; 6 notebook sections; 4-part naming.
- **Authoring:** THROWAWAY script per scenario → `bn.write_scenario(ROOT, name, CODE, PY, DAG, README)` (default `%spark`). Do NOT commit throwaway scripts. Scala Setup: `import spark.implicits._` + `import org.apache.spark.sql.functions._` (+ `import org.apache.spark.sql.expressions.Window` for sessionization). PySpark Setup: `from pyspark.sql import SparkSession, functions as F` (+ `from pyspark.sql import Window` for sessionization) + `spark = SparkSession.builder.remote("sc://spark-connect:15002").getOrCreate()`. Only import what's used (ruff lints `.ipynb`).
- **Parity:** DataFrame ops translate idiomatically (`$"c"`↔`F.col("c")`, `.as`↔`.alias`, `when/lag/avg/count/sum/broadcast`↔`F.when/F.lag/…`, `.show(false)`↔`.show(truncate=False)`). Same logic/columns/tables.
- **DAGs `EmptyOperator` placeholders** (no `SparkSubmitOperator`; guard `test_dag_catalog_conf` stays green). READMEs §6 note live-gated + `register_iceberg` (namespaces) + `make datasets`.
- Python 3.11, ruff 120 (notebooks: F/W/I linted, E501 ignored). Branch/PR: `static-and-unit` + `maven-spark-apps` green.

---

### Task 1: `data_quality-nyc_taxi-spark-iceberg`

**Files:** 4 files under `scenarios/data_quality-nyc_taxi-spark-iceberg/`.
Teaches: validation, quarantine table, metrics. Sections:
- `3. Read` (Scala): `val df = spark.table("lakehouse.bronze.nyc_taxi_trips")`. (PySpark: `df = spark.table("lakehouse.bronze.nyc_taxi_trips")`.)
- `4. Transform`: a rule string + split.
  Scala: `val rule = "fare_amount > 0 AND passenger_count BETWEEN 1 AND 6"`; `val valid = df.where(rule)`; `val quarantine = df.where(s"NOT ($rule) OR fare_amount IS NULL")`.
  PySpark: `rule = "fare_amount > 0 AND passenger_count BETWEEN 1 AND 6"`; `valid = df.where(rule)`; `quarantine = df.where(f"NOT ({rule}) OR fare_amount IS NULL")`.
- `5. Write`: `valid.writeTo("lakehouse.silver.nyc_taxi_clean").using("iceberg").createOrReplace()` and `quarantine.writeTo("lakehouse.silver.nyc_taxi_quarantine").using("iceberg").createOrReplace()`.
- `6. Verify`: `spark.sql("SELECT (SELECT count(*) FROM lakehouse.silver.nyc_taxi_clean) AS clean, (SELECT count(*) FROM lakehouse.silver.nyc_taxi_quarantine) AS quarantined").show(false)` (PySpark `.show(truncate=False)`).

- [ ] Author via throwaway; 6-section README (DQ split + quarantine + metrics on nyc_taxi). DAG `EmptyOperator` dag_id `data_quality_nyc_taxi`.
- [ ] `uv run python scripts/verify_repo.py --root .` exit 0; `uv run ruff check .` clean; `uv run pytest -m "not infra" -q` green; `uv run pytest tests/test_dag_catalog_conf.py -q` 1 passed.
- [ ] `git add scenarios/data_quality-nyc_taxi-spark-iceberg && git commit -m "feat(scenarios): data_quality-nyc_taxi (validation, quarantine table, metrics)"`

---

### Task 2: `sessionization-gh_archive-spark-iceberg`

**Files:** 4 files under `scenarios/sessionization-gh_archive-spark-iceberg/`.
Teaches: window functions, gap-based sessions (30-min inactivity). Scala Setup adds `import org.apache.spark.sql.expressions.Window`; PySpark adds `from pyspark.sql import Window`. Sections:
- `3. Read`:
  Scala: `val raw = spark.read.json("s3a://landing/gh_archive")`; `val events = raw.select($"actor.login".as("actor_login"), $"created_at".cast("timestamp").as("ts"))`.
  PySpark: `raw = spark.read.json("s3a://landing/gh_archive")`; `events = raw.select(F.col("actor.login").alias("actor_login"), F.col("created_at").cast("timestamp").alias("ts"))`.
- `4. Transform`:
  Scala: `val w = Window.partitionBy($"actor_login").orderBy($"ts")`; `val gaps = events.withColumn("prev_ts", lag($"ts", 1).over(w)).withColumn("new_session", when($"prev_ts".isNull || (unix_timestamp($"ts") - unix_timestamp($"prev_ts")) > 1800, 1).otherwise(0))`; `val sessions = gaps.withColumn("session_id", sum($"new_session").over(w))`.
  PySpark: `w = Window.partitionBy("actor_login").orderBy("ts")`; `gaps = events.withColumn("prev_ts", F.lag("ts", 1).over(w)).withColumn("new_session", F.when(F.col("prev_ts").isNull() | ((F.unix_timestamp("ts") - F.unix_timestamp("prev_ts")) > 1800), 1).otherwise(0))`; `sessions = gaps.withColumn("session_id", F.sum("new_session").over(w))`.
- `5. Write`: `sessions.writeTo("lakehouse.silver.gh_sessions").using("iceberg").createOrReplace()`.
- `6. Verify`: `spark.sql("SELECT actor_login, max(session_id) + 1 AS sessions FROM lakehouse.silver.gh_sessions GROUP BY actor_login ORDER BY sessions DESC").show(false)` (PySpark truncate=False).

- [ ] Author; README (gap-based sessionization on gh_archive). DAG `EmptyOperator` dag_id `sessionization_gh_archive`.
- [ ] verify gates (as Task 1); commit `feat(scenarios): sessionization-gh_archive (window functions, gap-based sessions)`.

---

### Task 3: `join_optimization-tpch-spark-iceberg`

**Files:** 4 files under `scenarios/join_optimization-tpch-spark-iceberg/`.
Teaches: broadcast vs sort-merge join, AQE (Spark 4 on by default). Sections:
- `3. Read`:
  Scala: `val orders = spark.read.parquet("s3a://landing/tpch/orders")`; `val customer = spark.read.parquet("s3a://landing/tpch/customer")`.
  PySpark: same with `spark.read.parquet(...)`.
- `4. Transform` (broadcast the small dim, then aggregate):
  Scala: `val joined = orders.join(broadcast(customer), $"o_custkey" === $"c_custkey")`; `joined.explain()`; `val mart = joined.groupBy($"c_mktsegment").agg(sum($"o_totalprice").as("revenue"), count("*").as("orders"))`.
  PySpark: `joined = orders.join(F.broadcast(customer), F.col("o_custkey") == F.col("c_custkey"))`; `joined.explain()`; `mart = joined.groupBy("c_mktsegment").agg(F.sum("o_totalprice").alias("revenue"), F.count("*").alias("orders"))`.
- `5. Write`: `mart.writeTo("lakehouse.gold.tpch_segment_revenue").using("iceberg").createOrReplace()`.
- `6. Verify`: `println("AQE: " + spark.conf.get("spark.sql.adaptive.enabled"))` (PySpark: `print("AQE:", spark.conf.get("spark.sql.adaptive.enabled"))`) then `spark.table("lakehouse.gold.tpch_segment_revenue").orderBy($"revenue".desc).show(false)` (PySpark `F.desc("revenue")`, `truncate=False`).

- [ ] Author; README (broadcast vs SMJ + AQE + SCALE-knob perf note; the `explain()` shows BroadcastHashJoin). DAG `EmptyOperator` dag_id `join_optimization_tpch`.
- [ ] verify gates; commit `feat(scenarios): join_optimization-tpch (broadcast vs SMJ, AQE)`.

---

### Task 4: `feature_engineering-movielens-spark-iceberg`

**Files:** 4 files under `scenarios/feature_engineering-movielens-spark-iceberg/`.
Teaches: ML feature marts (bridges to ml-lab). MovieLens lands `ratings.csv` (userId,movieId,rating,timestamp) + `movies.csv` at `s3a://landing/movielens/`. Sections:
- `3. Read`:
  Scala: `val ratings = spark.read.option("header", true).option("inferSchema", true).csv("s3a://landing/movielens/ratings.csv")`.
  PySpark: `ratings = spark.read.option("header", True).option("inferSchema", True).csv("s3a://landing/movielens/ratings.csv")`.
- `4. Transform`:
  Scala: `val userFeatures = ratings.groupBy($"userId").agg(avg($"rating").as("avg_rating"), count("*").as("num_ratings"))`; `val movieFeatures = ratings.groupBy($"movieId").agg(avg($"rating").as("movie_avg"), count("*").as("popularity"))`.
  PySpark: `userFeatures = ratings.groupBy("userId").agg(F.avg("rating").alias("avg_rating"), F.count("*").alias("num_ratings"))`; `movieFeatures = ratings.groupBy("movieId").agg(F.avg("rating").alias("movie_avg"), F.count("*").alias("popularity"))`.
- `5. Write`: `userFeatures.writeTo("lakehouse.gold.ml_user_features").using("iceberg").createOrReplace()` and `movieFeatures.writeTo("lakehouse.gold.ml_movie_features").using("iceberg").createOrReplace()`.
- `6. Verify`: `spark.table("lakehouse.gold.ml_movie_features").orderBy($"popularity".desc).show(10, false)` (PySpark `.orderBy(F.desc("popularity")).show(10, truncate=False)`).

- [ ] Author; README (feature marts from movielens; bridges to ml-lab; needs `make datasets`). DAG `EmptyOperator` dag_id `feature_engineering_movielens`.
- [ ] verify gates; commit `feat(scenarios): feature_engineering-movielens (ML feature marts)`.

---

### Task 5: Docs

**Files:** Modify `docs/scenarios.md`.
- [ ] Add a "Roadmap analytics scenarios (Spark; delivered-Atlas only)" group listing the 4 (each: name + teaches + dataset); note they need only delivered Spark/Iceberg + `register_iceberg` + `make datasets`.
- [ ] `uv run python scripts/verify_repo.py --root .` exit 0; `uv run ruff check .` clean; `uv run pytest -m "not infra" -q` green.
- [ ] `git add docs/scenarios.md && git commit -m "docs(scenarios): list the 4 roadmap analytics scenarios"`

---

## Phase 9 exit criteria

- [ ] `uv run pytest -m "not infra" -q` all pass; `uv run python scripts/verify_repo.py --root .` exit 0 (4 new scenarios validate); `uv run ruff check .` clean; `test_dag_catalog_conf` guard passes.
- [ ] Each scenario: Scala `.zpln` + PySpark `.ipynb` matching logic; 6-section README; `EmptyOperator` DAG; 4-part naming.
- [ ] PR into `main`: both required checks green; squash-merge (**push the branch before merging**).

## Self-review

**Coverage:** roadmap #11 data_quality, #12 sessionization, #13 join_optimization, #14 feature_engineering — all Spark-only on registered datasets (nyc_taxi/gh_archive/tpch/movielens). #15 bi_query (Trino) + advanced streaming are Phase 10.

**Parity:** DataFrame ops translate idiomatically (Window/lag/broadcast/agg); same tables/columns/logic; `.show(false)`↔`.show(truncate=False)`.

**Placeholder scan:** `EmptyOperator` DAGs are the established pattern; every step has runnable content. Live execution gated; capabilities (Window functions, broadcast/AQE, CSV read, aggregations) are core Spark — delivered by A2.

**Consistency:** distinct tables — `silver.nyc_taxi_clean`/`nyc_taxi_quarantine`, `silver.gh_sessions`, `gold.tpch_segment_revenue`, `gold.ml_user_features`/`ml_movie_features` — none collide with existing scenarios.
