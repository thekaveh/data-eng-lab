# data-eng-lab — Phase 8: online_retail dataset + upsert/SCD2 scenarios — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Complete the design's **core-10** by registering the `online_retail` dataset and authoring the two transactional scenarios — `incremental_upsert` (#3, `MERGE INTO` / CDC) and `scd2` (#4, Slowly Changing Dimension Type 2). Structural CI-green; execution live-gated on the delivered Atlas (Spark 4.1.2 + Iceberg 1.11.0).

**Architecture:** Register `online_retail` (UCI Online Retail II) in the dataset registry for completeness. The two scenarios demonstrate `MERGE INTO` and SCD2 on **small controlled seed data** authored inline (`spark.sql … VALUES`) — the standard way to teach these patterns clearly (you need deterministic before/after batches), with the README noting the registered `online_retail` dataset can be substituted at scale. Both scenarios are `spark.sql`-driven, so Scala `.zpln` and PySpark `.ipynb` share identical SQL — exact parity. `EmptyOperator` DAGs.

**Tech Stack:** Python 3.11 + PyYAML (registry); Spark 4.1.2 SQL + Iceberg `MERGE INTO` / `UPDATE` (authored, live-gated).

## Global Constraints

- **Never edit `infra/` or `pyproject.toml`.**
- **Scenario layout:** `scenarios/<name>/{README.md (6 sections), zeppelin/notebook.zpln (Scala %spark), jupyter/notebook.ipynb (PySpark), dag.py}`; 6 notebook sections; naming `[pattern]-[dataset]-[engine]-[format]` (4 hyphen parts).
- **Authoring:** build each scenario with a THROWAWAY script calling `bn.write_scenario(ROOT, name, CODE, PY, DAG, README)` (default `%spark`). Do NOT commit throwaway scripts. Scala Setup imports `import spark.implicits._` + `import org.apache.spark.sql.functions._`; PySpark Setup `from pyspark.sql import SparkSession` + `spark = SparkSession.builder.remote("sc://spark-connect:15002").getOrCreate()` (do NOT import `functions as F` if unused — ruff lints notebooks; these scenarios are pure `spark.sql`).
- **SQL parity:** the two scenarios are `spark.sql("…")`-driven, so `CODE[sec]` (Scala) and `PY[sec]` (PySpark) are the SAME strings (only `.show(false)`↔`.show(truncate=False)`).
- **DAGs are `EmptyOperator` placeholders** (mirror `scenarios/medallion-nyc_taxi-spark-iceberg/dag.py`); NO `SparkSubmitOperator` (keeps the `test_dag_catalog_conf` guard green). READMEs §6 note: live-gated; needs `scripts/register_iceberg.py` (namespaces).
- **CI-green now** structurally. Python 3.11, ruff 120 (notebooks: F/W/I linted, E501 per-file-ignored).
- **Branch/PR:** land via feature branch → PR; `static-and-unit` + `maven-spark-apps` green.

## File Structure

- `datasets/registry.yaml` + `tests/datasets/test_registry.py` — register `online_retail`.
- `scenarios/incremental_upsert-online_retail-spark-iceberg/{…4 files}`.
- `scenarios/scd2-online_retail-spark-iceberg/{…4 files}`.
- `docs/scenarios.md` — list the two.

---

### Task 1: Register the `online_retail` dataset

**Files:**
- Modify: `datasets/registry.yaml`, `tests/datasets/test_registry.py`

- [ ] **Step 1: Add the failing test assertion**

In `tests/datasets/test_registry.py::test_load_real_registry_has_core_datasets`, extend the core-set assertion to include `online_retail`, and add its kind/unzip checks:

```python
    assert {"nyc_taxi", "gh_archive", "movielens", "tpch", "online_retail"} <= set(ds)
    assert ds["online_retail"].kind == "http"
    assert ds["online_retail"].unzip is True
```

Run: `uv run pytest tests/datasets/test_registry.py -q` → FAIL (online_retail not in registry).

- [ ] **Step 2: Register the dataset**

Add to `datasets/registry.yaml` under `datasets:` (mirror the `movielens` `unzip` shape):

```yaml
  online_retail:
    description: "UCI Online Retail II — transactional retail invoices (upserts, SCD2, data quality)"
    format: csv
    license: "UCI ML Repository — CC BY 4.0"
    landing_prefix: online_retail
    fetch: { kind: http, unzip: true }
    scales:
      tiny:
        urls: ["https://archive.ics.uci.edu/static/public/502/online+retail+ii.zip"]
      small:
        urls: ["https://archive.ics.uci.edu/static/public/502/online+retail+ii.zip"]
      medium:
        urls: ["https://archive.ics.uci.edu/static/public/502/online+retail+ii.zip"]
```

- [ ] **Step 3: Verify**

Run: `uv run pytest tests/datasets/test_registry.py -q` → PASS.
Run: `uv run python scripts/verify_repo.py --root .` → exit 0 (registry validates via `datasets/schema.py`).
Run: `uv run pytest -m "not infra" -q` and `uv run ruff check .` → green.

- [ ] **Step 4: Commit**

```bash
git add datasets/registry.yaml tests/datasets/test_registry.py
git commit -m "feat(datasets): register online_retail (UCI Online Retail II)"
```

---

### Task 2: `incremental_upsert-online_retail-spark-iceberg`

**Files:** the 4 scenario files under `scenarios/incremental_upsert-online_retail-spark-iceberg/`.

Teaches: `MERGE INTO` upserts (CDC), idempotency. All `spark.sql` (Scala `code` == PySpark `py`); default `%spark`. Sections:
- `3. Read`: create + seed the target (batch 1):
  `spark.sql("CREATE TABLE IF NOT EXISTS lakehouse.silver.online_retail (invoice string, stock_code string, quantity int, price double) USING iceberg").show()`
  then `spark.sql("INSERT INTO lakehouse.silver.online_retail VALUES ('A1','SKU1',5,2.0), ('A2','SKU2',3,4.0)").show()`
  then `spark.sql("SELECT * FROM lakehouse.silver.online_retail ORDER BY invoice").show()`.
- `4. Transform`: a change-set (batch 2 = 1 update + 1 insert):
  `spark.sql("CREATE OR REPLACE TEMP VIEW online_retail_updates AS SELECT * FROM VALUES ('A1','SKU1',10,2.0), ('A3','SKU3',1,9.0) AS t(invoice, stock_code, quantity, price)").show()`.
- `5. Write`: the MERGE:
  `spark.sql("MERGE INTO lakehouse.silver.online_retail t USING online_retail_updates s ON t.invoice = s.invoice AND t.stock_code = s.stock_code WHEN MATCHED THEN UPDATE SET t.quantity = s.quantity WHEN NOT MATCHED THEN INSERT *").show()`.
- `6. Verify`: `spark.sql("SELECT * FROM lakehouse.silver.online_retail ORDER BY invoice").show()` (A1 quantity now 10, A3 inserted) + a comment: re-running the MERGE is idempotent (same result).

- [ ] **Step 1:** author via throwaway script; 6-section README (MERGE upsert on online_retail; the inline seed can be replaced by the registered `online_retail` dataset via `make datasets`; needs `register_iceberg`). DAG `EmptyOperator` dag_id `incremental_upsert_online_retail`.
- [ ] **Step 2:** `uv run python scripts/verify_repo.py --root .` exit 0; `uv run ruff check .` clean; `uv run pytest -m "not infra" -q` green; `uv run pytest tests/test_dag_catalog_conf.py -q` 1 passed. Sanity: `.zpln` Scala, `.ipynb` PySpark, identical SQL, 6 sections, EmptyOperator DAG.
- [ ] **Step 3:** `git add scenarios/incremental_upsert-online_retail-spark-iceberg && git commit -m "feat(scenarios): incremental_upsert-online_retail (MERGE INTO upserts, idempotency)"`

---

### Task 3: `scd2-online_retail-spark-iceberg`

**Files:** the 4 files under `scenarios/scd2-online_retail-spark-iceberg/`.

Teaches: Slowly Changing Dimension Type 2 (effective_from/to + is_current). All `spark.sql`. Sections:
- `3. Read`: create + seed a current dimension row:
  `spark.sql("CREATE TABLE IF NOT EXISTS lakehouse.gold.dim_customer_scd2 (customer_id string, segment string, effective_from timestamp, effective_to timestamp, is_current boolean) USING iceberg").show()`
  then `spark.sql("INSERT INTO lakehouse.gold.dim_customer_scd2 VALUES ('C1','standard', TIMESTAMP '2023-01-01 00:00:00', NULL, true)").show()`
  then `spark.sql("SELECT customer_id, segment, is_current FROM lakehouse.gold.dim_customer_scd2 ORDER BY effective_from").show()`.
- `4. Transform` (a segment change for C1 arrives — close the current row):
  `spark.sql("UPDATE lakehouse.gold.dim_customer_scd2 SET effective_to = current_timestamp(), is_current = false WHERE customer_id = 'C1' AND is_current = true").show()`.
- `5. Write` (open the new current version):
  `spark.sql("INSERT INTO lakehouse.gold.dim_customer_scd2 VALUES ('C1','premium', current_timestamp(), NULL, true)").show()`.
- `6. Verify`: `spark.sql("SELECT customer_id, segment, effective_to IS NOT NULL AS closed, is_current FROM lakehouse.gold.dim_customer_scd2 ORDER BY effective_from").show()` — the old `standard` row is closed (`is_current=false`), the new `premium` row is current.

- [ ] **Step 1:** author via throwaway script; 6-section README (SCD2 on an online_retail customer dimension; needs `register_iceberg`; Iceberg row-level `UPDATE` via the SQL extensions). DAG `EmptyOperator` dag_id `scd2_online_retail`.
- [ ] **Step 2:** same verify gates as Task 2.
- [ ] **Step 3:** `git add scenarios/scd2-online_retail-spark-iceberg && git commit -m "feat(scenarios): scd2-online_retail (Slowly Changing Dimension Type 2)"`

---

### Task 4: Docs

**Files:** Modify `docs/scenarios.md`.

- [ ] **Step 1:** Add the two new scenarios to "Authored scenarios" (a "Transactional (upsert / SCD2)" group): `incremental_upsert-online_retail` (MERGE INTO upserts) and `scd2-online_retail` (SCD Type 2), both online_retail, delivered-Atlas only, live-gated + `register_iceberg`. Note this **completes the design's core-10**.
- [ ] **Step 2:** `uv run python scripts/verify_repo.py --root .` exit 0; `uv run ruff check .` clean; `uv run pytest -m "not infra" -q` green.
- [ ] **Step 3:** `git add docs/scenarios.md && git commit -m "docs(scenarios): list upsert/SCD2 scenarios — core-10 complete"`

---

## Phase 8 exit criteria

- [ ] `uv run pytest -m "not infra" -q` → all pass (registry test incl. online_retail).
- [ ] `uv run python scripts/verify_repo.py --root .` → exit 0 (2 new scenarios + registry validate).
- [ ] `uv run ruff check .` clean; `test_dag_catalog_conf` guard passes (both new DAGs EmptyOperator).
- [ ] Both scenarios: Scala `.zpln` + PySpark `.ipynb` with identical `spark.sql`; 6-section READMEs; EmptyOperator DAGs.
- [ ] PR into `main`: `static-and-unit` + `maven-spark-apps` green; squash-merge. **Core-10 complete.**

## Self-review

**Coverage:** core-10 #3 `incremental_upsert` (Task 2), #4 `scd2` (Task 3), + the `online_retail` dataset (Task 1). With Phase 7's six + Phase 2b's two, this closes the design's core-10 (plus the bonus Trino/Redpanda scenarios).

**Parity:** both scenarios are pure `spark.sql`, so Scala/PySpark strings are identical (only `.show` truncate + comment prefix differ). Capabilities (`MERGE INTO`, row-level `UPDATE`) map to `docs/atlas-expectations.md` §3 row 1 (delivered A2).

**Placeholder scan:** seed-data via inline `VALUES` is deliberate (controlled before/after for teaching MERGE/SCD2), not a stub; every step has runnable content. `online_retail` dataset registered for completeness; the scenarios note it can be substituted at scale.

**Consistency:** `lakehouse.silver.online_retail` (upsert) and `lakehouse.gold.dim_customer_scd2` (scd2) are distinct tables, distinct from all existing scenarios; namespaces `silver`/`gold` created by `register_iceberg`.
