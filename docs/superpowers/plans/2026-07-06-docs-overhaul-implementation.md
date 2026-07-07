# Full Documentation Overhaul Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Create 19 enriched scenario docs, 2 enriched spark-app docs, 3 redirect README stubs, 3 doc upgrades (index, lakehouse, datasets), update build scripts (build_wiki.py, render-readme.py), and CI workflows — so all three surfaces (MkDocs site, Wiki, README) are auto-generated from canonical sources.

**Architecture:** Source documents in `docs/` (19 scenario docs + 2 spark-app docs + 3 guide docs) are canonical. A wiki sync script (`scripts/build_wiki.py`) renders them to `wiki/` for push to the wiki repo. A README render script (`scripts/render-readme.py`) extracts content from `docs/index.md` into root `README.md`. Scenario/scenario READMEs become thin redirect stubs.

**Tech Stack:** Python 3.11+, MkDocs Material, PyYAML, Markdown, HTML/SVG architecture diagrams.

---

## File Map

### Files to create (19 scenario docs):
- `docs/scenarios/batch_ingest-nyc_taxi-spark-iceberg.md`
- `docs/scenarios/medallion-nyc_taxi-spark-iceberg.md`
- `docs/scenarios/streaming_ingest-events-spark-iceberg.md`
- `docs/scenarios/streaming_ingest-gh_archive-spark-iceberg.md`
- `docs/scenarios/streaming_windows-events-spark-iceberg.md`
- `docs/scenarios/cdc_streaming-online_retail-spark-iceberg.md`
- `docs/scenarios/data_quality-nyc_taxi-spark-iceberg.md`
- `docs/scenarios/schema_evolution-gh_archive-spark-iceberg.md`
- `docs/scenarios/time_travel-nyc_taxi-spark-iceberg.md`
- `docs/scenarios/table_maintenance-nyc_taxi-spark-iceberg.md`
- `docs/scenarios/star_schema-tpch-spark-iceberg.md`
- `docs/scenarios/bi_query-tpch-trino-iceberg.md`
- `docs/scenarios/federated_query-nyc_taxi-trino-iceberg.md`
- `docs/scenarios/join_optimization-tpch-spark-iceberg.md`
- `docs/scenarios/feature_engineering-movielens-spark-iceberg.md`
- `docs/scenarios/scd2-online_retail-spark-iceberg.md`
- `docs/scenarios/incremental_upsert-online_retail-spark-iceberg.md`
- `docs/scenarios/json_flatten-gh_archive-spark-iceberg.md`
- `docs/scenarios/sessionization-gh_archive-spark-iceberg.md`

### Files to create (2 spark-app docs):
- `docs/spark-apps/nyc-taxi-etl.md` (enhanced over existing)
- `docs/spark-apps/nyc-taxi-medallion.md` (enhanced over existing)

### Files to create (redirect stubs — 3 per scenario with cross-category links):
- `scenarios/batch_ingest-nyc_taxi-spark-iceberg/README.md`
- `scenarios/medallion-nyc_taxi-spark-iceberg/README.md`
- `scenarios/streaming_ingest-events-spark-iceberg/README.md`
- `scenarios/streaming_ingest-gh_archive-spark-iceberg/README.md`
- `scenarios/streaming_windows-events-spark-iceberg/README.md`
- `scenarios/cdc_streaming-online_retail-spark-iceberg/README.md`
- `scenarios/data_quality-nyc_taxi-spark-iceberg/README.md`
- `scenarios/schema_evolution-gh_archive-spark-iceberg/README.md`
- `scenarios/time_travel-nyc_taxi-spark-iceberg/README.md`
- `scenarios/table_maintenance-nyc_taxi-spark-iceberg/README.md`
- `scenarios/star_schema-tpch-spark-iceberg/README.md`
- `scenarios/bi_query-tpch-trino-iceberg/README.md`
- `scenarios/federated_query-nyc_taxi-trino-iceberg/README.md`
- `scenarios/join_optimization-tpch-spark-iceberg/README.md`
- `scenarios/feature_engineering-movielens-spark-iceberg/README.md`
- `scenarios/scd2-online_retail-spark-iceberg/README.md`
- `scenarios/incremental_upsert-online_retail-spark-iceberg/README.md`
- `scenarios/json_flatten-gh_archive-spark-iceberg/README.md`
- `scenarios/sessionization-gh_archive-spark-iceberg/README.md`

### Files to modify:
- `mkdocs.yml` — Ensure nav structure matches final doc layout with categories
- `docs/index.md` — Enhance with additional cross-links, ensure scenario catalog is accurate
- `docs/lakehouse.md` — Add cross-links to scenarios that use lakehouse
- `docs/datasets.md` — Add cross-links from each dataset to scenarios that use it
- `docs/superpowers/plans/2026-07-06-docs-overhaul-design.md` — Already written, no changes needed
- `scripts/build_wiki.py` — Update scenario pages to pull from `docs/scenarios/*.md` instead of `scenarios/*/README.md`
- `scripts/gen_doc_pages.py` — Optionally remove (nav now explicit in mkdocs.yml) or keep for future auto-discovery
- `scripts/render-readme.py` — NEW: extract content from `docs/index.md` to root `README.md`
- `README.md` — Keep as-is for now (will be overwritten by render-readme.py in CI)
- `.github/workflows/docs-deploy.yml` — Add `scripts/render-readme.py` to trigger paths
- `.github/workflows/wiki-sync.yml` (if it exists) — Ensure it uses updated build_wiki.py

### Files to review (no changes needed):
- `docs/scenarios/index.md` — Already exists and is comprehensive; review for cross-links
- `docs/getting-started.md` — Review for cross-links
- `docs/spark-apps.md` — Review for cross-links
- `.github/workflows/wiki.yml` — Wiki sync; review trigger paths

---

### Task 1: Verify MkDocs nav structure and confirm scenario doc targets

**Files:**
- Check: `mkdocs.yml` (already has all 19 scenarios + 2 spark apps in nav — verify completeness)
- Check: `docs/scenarios/index.md` (overview page — already exists)

- [ ] **Step 1: Confirm mkdocs.yml nav includes all 19 scenarios**

The current `mkdocs.yml` already has:
- 19 scenario entries under `Scenarios:` group (lines 84-104)
- 3 spark-app entries under `Spark Apps:` group (lines 106-109)
- Lakehouse & Atlas group with 7 entries (lines 111-118)
- Datasets, Changelog entries

Verify the nav entries match exactly this list:
1. batch_ingest-nyc_taxi-spark-iceberg
2. medallion-nyc_taxi-spark-iceberg
3. streaming_ingest-events-spark-iceberg
4. streaming_ingest-gh_archive-spark-iceberg
5. streaming_windows-events-spark-iceberg
6. cdc_streaming-online_retail-spark-iceberg
7. data_quality-nyc_taxi-spark-iceberg
8. schema_evolution-gh_archive-spark-iceberg
9. time_travel-nyc_taxi-spark-iceberg
10. table_maintenance-nyc_taxi-spark-iceberg
11. star_schema-tpch-spark-iceberg
12. bi_query-tpch-trino-iceberg
13. federated_query-nyc_taxi-trino-iceberg
14. join_optimization-tpch-spark-iceberg
15. feature_engineering-movielens-spark-iceberg
16. scd2-online_retail-spark-iceberg
17. incremental_upsert-online_retail-spark-iceberg
18. json_flatten-gh_archive-spark-iceberg
19. sessionization-gh_archive-spark-iceberg

And spark apps:
1. nyc-taxi-etl
2. nyc-taxi-medallion

Run:
```bash
python3 -c "
import yaml
from pathlib import Path
y = yaml.safe_load(Path('mkdocs.yml').read_text())
nav = y['nav']
scenarios = [e for e in nav if 'Scenarios' in str(e) and isinstance(e, dict) for s in list(e.values())[0][1:]]
apps = [e for e in nav if 'Spark Apps' in str(e) and isinstance(e, dict) for s in list(e.values())[0][1:]]
print(f'Scenarios: {len(scenarios)}')
print(f'Apps: {len(apps)}')
for s in scenarios:
    print(f'  - {s}')
for a in apps:
    print(f'  - {a}')
"
```

Expected output: `Scenarios: 19`, `Apps: 2`, with all listed names above.

---

### Task 2: Write redirect stubs for all 19 scenario READMEs

**Files:**
- Create (overwrite): All 19 `scenarios/<name>/README.md` files
- These are thin redirect stubs — NOT the full docs

- [ ] **Step 1: Write the redirect stub template**

Every scenario README becomes this template (same content for all 19):

```markdown
# <scenario-name>

Full documentation for this scenario lives in the project documentation site:

[🔗 View full documentation](https://thekaveh.github.io/data-eng-lab/scenarios/<scenario-name>/)

The rendered docs include the complete architecture diagram, data model, notebook walkthroughs, and cross-links — all auto-generated from this README's canonical content.
```

Replace `<scenario-name>` and the URL slug with the actual scenario name for each file. For example, `scenarios/batch_ingest-nyc_taxi-spark-iceberg/README.md` gets the URL `https://thekaveh.github.io/data-eng-lab/scenarios/batch_ingest-nyc_taxi-spark-iceberg/`.

- [ ] **Step 2: Apply the template to all 19 scenario READMEs**

Use a Python script to write all 19 at once:

```python
"""Write redirect stub READMEs for all scenarios.

Run from repo root:
    python3 scripts/write_readme_stubs.py (or inline)
"""
from pathlib import Path

SCENARIO_NAMES = [
    "batch_ingest-nyc_taxi-spark-iceberg",
    "medallion-nyc_taxi-spark-iceberg",
    "streaming_ingest-events-spark-iceberg",
    "streaming_ingest-gh_archive-spark-iceberg",
    "streaming_windows-events-spark-iceberg",
    "cdc_streaming-online_retail-spark-iceberg",
    "data_quality-nyc_taxi-spark-iceberg",
    "schema_evolution-gh_archive-spark-iceberg",
    "time_travel-nyc_taxi-spark-iceberg",
    "table_maintenance-nyc_taxi-spark-iceberg",
    "star_schema-tpch-spark-iceberg",
    "bi_query-tpch-trino-iceberg",
    "federated_query-nyc_taxi-trino-iceberg",
    "join_optimization-tpch-spark-iceberg",
    "feature_engineering-movielens-spark-iceberg",
    "scd2-online_retail-spark-iceberg",
    "incremental_upsert-online_retail-spark-iceberg",
    "json_flatten-gh_archive-spark-iceberg",
    "sessionization-gh_archive-spark-iceberg",
]

DOCS_BASE = "https://thekaveh.github.io/data-eng-lab"

for name in SCENARIO_NAMES:
    readme = Path("scenarios") / name / "README.md"
    content = f"""# {name}

Full documentation for this scenario lives in the project documentation site:

[{"\\U0001f517 View full documentation"}]({DOCS_BASE}/scenarios/{name}/)

The rendered docs include the complete architecture diagram, data model, notebook walkthroughs, and cross-links \u2014 all auto-generated from this README's canonical content.
"""
    readme.write_text(content, encoding="utf-8")
    print(f"Wrote {readme}")
```

This script creates all 19 stubs in one invocation. Expected output: 19 `Wrote scenarios/.../README.md` lines.

- [ ] **Step 3: Verify all 19 stubs exist and have correct content**

```bash
for f in scenarios/*/README.md; do
  echo "$f: $(wc -l < "$f") lines"
  head -1 "$f"
done
```

Expected: all 19 files, each ~7 lines, with `# <scenario-name>` as the first line.

---

### Task 3: Write all 19 enriched scenario MkDocs docs

Each scenario doc follows this structure with explicit hierarchical numbering:

```markdown
# <Scenario Name>

<One-sentence summary: what this scenario demonstrates and its high-level data flow.>

## 1. Purpose

<Two to three sentences explaining what the scenario demonstrates, its role within
the lakehouse architecture, and why it exists.>

## 2. Data Model

### 2.1 Input Source

<Source path/format, dataset name, schema description.>

### 2.2 Output Tables

<table>
<tr><th>Table</th><th>Layer</th><th>Key Columns</th></tr>
<tr><td>lakehouse.<namespace>.<table></td><td><layer></td><td><columns></td></tr>
</table>

## 3. Architecture

![Architecture](architectures/<scenario-name>.html)

<Explanation of the diagram: data flow source → processing → sink, services involved, checkpoint locations for streaming scenarios.>

## 4. Notebooks

<Paragraph describing both Zeppelin Scala and Jupyter PySpark notebooks — identical logic,
different languages, same sections.>

## 5. Orchestration

<Airflow DAG ID, schedule, or note that it's long-running streaming (EmptyOperator placeholder).>

## 6. Usage

Step-by-step:
1. <Ensure Iceberg namespaces exist (run register_iceberg.py if needed).>
2. <Populate data (make datasets or prerequisite scenario).>
3. <Open notebook / trigger DAG.>
4. <Verification command.>

## 7. Dependencies

- **Dataset:** <dataset name and path>
- **Atlas services:** <A1-A9 items>
- **Other:** <kafka-python, trino client, etc.>

## 8. Known Issues & Caveats

<!-- List from original README -->

## See Also

- [Upstream: <related scenario>](./<upstream-scenario>.md) — <one-line reason>
- [Datasets](../datasets.md)
- [Lakehouse Architecture](../lakehouse.md)
```

The "See Also" section must include at minimum:
- Any upstream scenarios (scenarios that produce data consumed by this one)
- Any downstream scenarios (scenarios that consume data from this one)
- Related scenarios in the same category (cross-links to same-dataset scenarios)
- Datasets overview and Lakehouse Architecture (always)

**Cross-linking rules:**
- `batch_ingest` → downstream: `medallion`, `data_quality`, `federated_query`, `table_maintenance`, `time_travel`
- `streaming_ingest-events` → downstream: `streaming_windows`, `cdc_streaming`
- `streaming_ingest-gh_archive` → downstream: `json_flatten`, `sessionization`
- `json_flatten` → downstream: `sessionization`
- `star_schema` → downstream: `bi_query`, `join_optimization`
- `incremental_upsert` → related: `scd2`, `cdc_streaming`
- `scd2` → related: `incremental_upsert`, `cdc_streaming`
- `cdc_streaming` → related: `incremental_upsert`, `scd2`
- All others → datasets + lakehouse always

I will write each scenario doc based on the README content I have. Each doc is 30-50 lines.

- [ ] **Step 1: Write the first 5 scenario docs (batch ingestion and medallion group)**

Create `docs/scenarios/batch_ingest-nyc_taxi-spark-iceberg.md`:

```markdown
# batch_ingest-nyc_taxi-spark-iceberg

Land raw NYC-taxi Parquet from `s3a://landing/nyc_taxi/` into the Iceberg `lakehouse.bronze.nyc_taxi_trips` table, cleaned (drop null pickups + non-positive passenger counts, add `trip_date`). Scala (Zeppelin) and PySpark (Jupyter) notebooks implement the same logic; a Phase-3a JAR productionizes it for Airflow.

## 1. Purpose

This scenario demonstrates the fundamental lakehouse write path: raw data arrives from a source system and lands in an Iceberg bronze table with minimal transformation. It is the simplest end-to-end lakehouse pipeline and serves as the entry point of the medallion architecture — every downstream scenario that consumes NYC-taxi data depends on this scenario running first.

## 2. Data Model

### 2.1 Input Source

Source: `s3a://landing/nyc_taxi/` (Parquet files downloaded via `make datasets`).

| Column | Type | Notes |
|---|---|---|
| `tpep_pickup_datetime` | timestamp | Pickup time |
| `tpep_dropoff_datetime` | timestamp | Dropoff time |
| `passenger_count` | int | Number of passengers |
| `fare_amount` | double | Trip fare |
| Other columns | varied | Passed through as-is |

### 2.2 Output Tables

| Table | Layer | Key Columns |
|---|---|---|
| `lakehouse.bronze.nyc_taxi_trips` | Bronze | `tpep_pickup_datetime`, `tpep_dropoff_datetime`, `passenger_count`, `fare_amount`, `trip_date` (computed) |

The `trip_date` column is derived from `tpep_pickup_datetime` and partitions the table by day.

## 3. Architecture

![Architecture](architectures/batch_ingest-nyc_taxi-spark-iceberg.html)

Data flows from `s3a://landing/nyc_taxi/` (Parquet) into an Iceberg bronze table via PySpark or Scala Spark batch processing. Cleaning operations include dropping rows with null pickup times, filtering out non-positive passenger counts, and computing a `trip_date` column for daily partitioning. The table is partitioned by `trip_date`.

## 4. Notebooks

- **Zeppelin (Scala):** `zeppelin/notebook.zpln` — Sections: Overview, Setup, Read, Transform (clean), Write (Iceberg), Verify
- **Jupyter (PySpark):** `jupyter/notebook.ipynb` — Sections: Overview, Setup, Read, Transform (clean), Write (Iceberg), Verify

Both notebooks implement identical batch ingest logic: read Parquet from S3, apply filtering and column transforms, write to an Iceberg partitioned table, and verify row counts.

## 5. Orchestration

Airflow DAG: `batch_ingest_nyc_taxi` — a scheduled batch DAG.

## 6. Usage

1. Ensure the `bronze` Iceberg namespace exists: `scripts/register_iceberg.py`
2. Download the dataset: `make datasets`
3. Open either notebook on the Atlas stack, or trigger the Airflow DAG:
   ```bash
   airflow dags trigger batch_ingest_nyc_taxi
   ```
4. Verify output:
   ```bash
   spark-sql -e "SELECT COUNT(*) FROM lakehouse.bronze.nyc_taxi_trips"
   ```

## 7. Dependencies

- **Dataset:** NYC Taxi Parquet (via `make datasets`)
- **Atlas services:** A1-A4 (Spark, Iceberg, S3 catalog, lakehouse catalog)
- **Other:** None

## 8. Known Issues & Caveats

Notebook execution and Scala/PySpark parity are live-gated on Atlas A1-A4.
```

Create `docs/scenarios/medallion-nyc_taxi-spark-iceberg.md`:

```markdown
# medallion-nyc_taxi-spark-iceberg

Promote NYC-taxi data through the medallion architecture: `lakehouse.bronze.nyc_taxi_trips` → `lakehouse.silver.nyc_taxi_trips` (deduplicated) → `lakehouse.gold.nyc_taxi_daily` (daily aggregate). Scala (Zeppelin) and PySpark (Jupyter) notebooks implement the same logic; a Phase-3a JAR productionizes it for Airflow.

## 1. Purpose

This scenario demonstrates the full three-tier medallion lakehouse pipeline: bronze (raw landing) → silver (cleaned, deduplicated) → gold (business-level aggregations). It is the canonical example of how the lakehouse operates end-to-end, and its logic is productionized as the `nyc-taxi-medallion` Maven Spark app.

## 2. Data Model

### 2.1 Input Source

Source: `lakehouse.bronze.nyc_taxi_trips` (populated by the upstream `batch_ingest-nyc_taxi-spark-iceberg` scenario).

| Column | Type | Notes |
|---|---|---|
| `tpep_pickup_datetime` | timestamp | Pickup time |
| `tpep_dropoff_datetime` | timestamp | Dropoff time |
| `passenger_count` | int | Number of passengers |
| `fare_amount` | double | Trip fare |
| `trip_date` | date | Derived from pickup datetime |

### 2.2 Output Tables

| Table | Layer | Key Columns |
|---|---|---|
| `lakehouse.silver.nyc_taxi_trips` | Silver | Same as bronze, deduplicated on pickup/dropoff datetime |
| `lakehouse.gold.nyc_taxi_daily` | Gold | `trip_date`, `trip_count`, `avg_fare` |

## 3. Architecture

![Architecture](architectures/medallion-nyc_taxi-spark-iceberg.html)

Data flows from the bronze table through a deduplication step (removing duplicates on pickup/dropoff datetime) into silver, then a daily aggregation (trip count and average fare) into gold. The pipeline validates data quality at each tier and enforces schema consistency.

## 4. Notebooks

- **Zeppelin (Scala):** `zeppelin/notebook.zpln` — Sections: Overview, Bronze Read, Silver Dedup, Gold Aggregation, Verify
- **Jupyter (PySpark):** `jupyter/notebook.ipynb` — Sections: Overview, Bronze Read, Silver Dedup, Gold Aggregation, Verify

Both notebooks implement identical medallion logic: read bronze data, deduplicate into silver, aggregate into gold, and verify row counts and aggregations.

## 5. Orchestration

Airflow DAG: `medallion_nyc_taxi` — a scheduled batch DAG. Depends on `batch_ingest_nyc_taxi` completing first (DAG dependency).

## 6. Usage

1. Ensure the `silver` and `gold` Iceberg namespaces exist: `scripts/register_iceberg.py`
2. Run the upstream ingest scenario first (or ensure `lakehouse.bronze.nyc_taxi_trips` exists)
3. Open either notebook on the Atlas stack, or trigger the Airflow DAG:
   ```bash
   airflow dags trigger medallion_nyc_taxi
   ```
4. Verify output:
   ```bash
   spark-sql -e "SELECT COUNT(*) FROM lakehouse.silver.nyc_taxi_trips"
   spark-sql -e "SELECT * FROM lakehouse.gold.nyc_taxi_daily LIMIT 10"
   ```

## 7. Dependencies

- **Dataset:** NYC Taxi Parquet (populated via `make datasets`)
- **Atlas services:** A1-A4 (Spark, Iceberg, S3 catalog, lakehouse catalog)
- **Other:** None; requires the `trip_date` column to exist on the bronze table (added during ingest)

The `silver` and `gold` namespaces must exist in the Iceberg REST catalog before running standalone. Run `scripts/register_iceberg.py` beforehand.

## 8. Known Issues & Caveats

Notebook execution and Scala/PySpark parity are live-gated on Atlas A1-A4.
This scenario writes to `lakehouse.silver.*` and `lakehouse.gold.*`, both of which require their respective namespaces to exist in the Iceberg REST catalog. Run `scripts/register_iceberg.py` (creates `bronze`, `silver`, and `gold`) before executing this scenario standalone.
```

Create `docs/scenarios/streaming_ingest-events-spark-iceberg.md`:

```markdown
# streaming_ingest-events-spark-iceberg

Ingest synthetic click events from the Redpanda `events` Kafka topic into `lakehouse.bronze.events` (Iceberg) via Spark Structured Streaming. Scala (Zeppelin) and PySpark (Jupyter) notebooks implement the same streaming logic; `producer.py` generates synthetic events for local testing.

## 1. Purpose

This scenario demonstrates real-time lakehouse ingestion using Kafka (Redpanda) as the source and Iceberg as the sink. It is the streaming counterpart to the batch-ingest scenario, showing how to write continuous streaming queries directly to Iceberg tables with checkpoint-based fault tolerance.

## 2. Data Model

### 2.1 Input Source

Source: `redpanda:9092` → `events` Kafka topic (JSON messages produced by `producer.py`).

| Column | Type | Notes |
|---|---|---|
| `user_id` | string | User identifier |
| `event` | string | Event type (e.g., click, view) |
| `ts` | timestamp | Event timestamp |

Checkpoint: `s3a://checkpoints/events`

### 2.2 Output Tables

| Table | Layer | Key Columns |
|---|---|---|
| `lakehouse.bronze.events` | Bronze | `user_id`, `event`, `ts` |

## 3. Architecture

![Architecture](architectures/streaming_ingest-events-spark-iceberg.html)

Data flows from the Redpanda `events` topic through Spark Structured Streaming (`readStream` + `from_json` + `writeStream`) into the Iceberg bronze table. Checkpointing ensures exactly-once semantics for streaming offsets.

## 4. Notebooks

- **Zeppelin (Scala):** `zeppelin/notebook.zpln` — Sections: Overview, Setup, Read (`readStream`), Transform (`from_json`), Write (`writeStream`), Verify; 6 sections total
- **Jupyter (PySpark):** `jupyter/notebook.ipynb` — Sections: Overview, Setup, Read (`readStream`), Transform (`from_json`), Write (`writeStream`), Verify; 6 sections total

`producer.py` generates synthetic events to the `events` topic for end-to-end testing. Both languages implement identical streaming logic.

## 5. Orchestration

Streaming queries are long-running and not scheduled as batch DAGs. The Airflow DAG (`streaming_ingest_events`) is an `EmptyOperator` placeholder.

## 6. Usage

1. Start the Atlas stack with Redpanda: `make up` (requires Atlas A9 / issue #269)
2. Produce events: `python scenarios/streaming_ingest-events-spark-iceberg/producer.py [count]` (defaults to 100)
3. Open either notebook on the Atlas stack and run all sections
4. Verify output:
   ```bash
   spark-sql -e "SELECT COUNT(*) FROM lakehouse.bronze.events"
   ```

## 7. Dependencies

- **Dataset:** None (synthetic events from `producer.py`)
- **Atlas services:** A1-A4 (Spark, Iceberg, S3 catalog, lakehouse catalog), A9 (Redpanda/Kafka)
- **Other:** `kafka-python` (required by `producer.py`)

## 8. Known Issues & Caveats

Atlas seeds only the `atlas_stream_events` demo topic; this scenario uses its own topic (`events`), which is auto-created on first produce by `producer.py`. Alternatively, add `events` to `REDPANDA_DEMO_TOPICS` in `infra/.env`. Notebook execution and Scala/PySpark parity are live-gated on Atlas A9 (Redpanda). The streaming query runs indefinitely; call `query.awaitTermination()` to block in both Scala and PySpark notebooks.
```

Create `docs/scenarios/streaming_ingest-gh_archive-spark-iceberg.md`:

```markdown
# streaming_ingest-gh_archive-spark-iceberg

Demonstrate Iceberg ingestion via Structured Streaming with a file source: read JSON files incrementally from S3 landing, parse with schema, cast the timestamp column, and write to Iceberg with checkpoints for exactly-once semantics. No Kafka or external messaging queue required.

## 1. Purpose

This scenario demonstrates Structured Streaming to Iceberg using a simple file source (not Kafka), which enables exactly-once ingestion semantics and checkpointing for fault tolerance. It is useful when the data source is a directory of files rather than a message queue, and it does not require Atlas A9 (Redpanda).

## 2. Data Model

### 2.1 Input Source

Source: `s3a://landing/gh_archive/` (compressed JSON files downloaded via `make datasets`).

| Column | Type | Notes |
|---|---|---|
| `id` | string | Event ID |
| `type` | string | Event type (e.g., PushEvent, CreateEvent) |
| `created_at` | timestamp | Event creation time (casted from string) |
| Other nested fields | varied | Extracted via dot notation (`actor.login`, `repo.name`) |

Checkpoint: `s3a://checkpoints/gh_events_file`

### 2.2 Output Tables

| Table | Layer | Key Columns |
|---|---|---|
| `lakehouse.bronze.gh_events_stream` | Bronze | `id`, `type`, `created_at`, `actor_login`, `repo_name` |

## 3. Architecture

![Architecture](architectures/streaming_ingest-gh_archive-spark-iceberg.html)

Data flows from `s3a://landing/gh_archive/*.json.gz` through Spark Structured Streaming with a file source. The stream reads JSON files incrementally, defines a schema to extract nested fields (`actor.login` → `actor_login`, `repo.name` → `repo_name`), casts `created_at` to timestamp, and writes to Iceberg with checkpointing for exactly-once semantics.

## 4. Notebooks

- **Zeppelin (Scala):** `zeppelin/notebook.zpln` — Sections: Overview, Setup, Read (file source), Transform (schema + cast), Write (Iceberg), Verify
- **Jupyter (PySpark):** `jupyter/notebook.ipynb` — Sections: Overview, Setup, Read (file source), Transform (schema + cast), Write (Iceberg), Verify

Both notebooks implement identical streaming ingest logic with file source, schema definition, field extraction, and sink write.

## 5. Orchestration

Airflow DAG: `streaming_ingest_gh_archive` — a scheduled batch DAG (not streaming, since the file source is incremental).

## 6. Usage

1. Ensure the `bronze` Iceberg namespace exists: `scripts/register_iceberg.py`
2. Populate the landing zone: `make datasets`
3. Open either notebook on the Atlas stack, or trigger the Airflow DAG:
   ```bash
   airflow dags trigger streaming_ingest_gh_archive
   ```
4. Verify output:
   ```bash
   spark-sql -e "SELECT COUNT(*) FROM lakehouse.bronze.gh_events_stream"
   ```

## 7. Dependencies

- **Dataset:** GitHub Archive compressed JSON (via `make datasets`)
- **Atlas services:** A1-A4 (Spark, Iceberg, S3 catalog, lakehouse catalog)
- **Other:** None; uses file source, does not require Atlas A9 (Redpanda)

Requires `lakehouse.bronze` namespace to exist before running.

## 8. Known Issues & Caveats

Notebook execution and Scala/PySpark parity are live-gated on Atlas A1-A4. This scenario uses a file source, not Kafka, so it does not require Atlas A9. Run `scripts/register_iceberg.py` and `make datasets` before executing standalone.
```

Create `docs/scenarios/data_quality-nyc_taxi-spark-iceberg.md`:

```markdown
# data_quality-nyc_taxi-spark-iceberg

Validates NYC taxi trip records against business rules, splitting them into clean and quarantine tables based on fare and passenger count constraints.

## 1. Purpose

Data quality enforcement is a fundamental requirement for reliable data pipelines. This scenario demonstrates how to implement a quarantine pattern in a lakehouse: records that pass validation rules flow to a clean table, while records that violate business rules land in a quarantine table. This ensures downstream consumers (analytics, ML models) use only validated data while maintaining visibility into data quality issues.

## 2. Data Model

### 2.1 Input Source

Source: `lakehouse.bronze.nyc_taxi_trips` (populated by the upstream `batch_ingest-nyc_taxi-spark-iceberg` scenario).

| Column | Type | Notes |
|---|---|---|
| `fare_amount` | double | Must be > 0 for valid records |
| `passenger_count` | int | Must be BETWEEN 1 AND 6 for valid records |
| `tpep_pickup_datetime` | timestamp | Pickup time |
| `tpep_dropoff_datetime` | timestamp | Dropoff time |
| Other columns | varied | Passed through to both clean and quarantine tables |

### 2.2 Output Tables

| Table | Layer | Key Columns |
|---|---|---|
| `lakehouse.silver.nyc_taxi_clean` | Silver | All bronze columns for records passing `fare_amount > 0 AND passenger_count BETWEEN 1 AND 6` |
| `lakehouse.silver.nyc_taxi_quarantine` | Silver | All bronze columns plus a `reason` column indicating which rule was violated (NULL fare, invalid passenger count) |

## 3. Architecture

![Architecture](architectures/data_quality-nyc_taxi-spark-iceberg.html)

Data flows from the bronze table through Spark batch processing. Business rules are applied using conditional filters (`filter` in Scala, `when/case` in PySpark), which split records into two output paths: clean (valid records) and quarantine (violations with reason). The sink is the Iceberg silver namespace.

## 4. Notebooks

- **Zeppelin (Scala):** Sections: Overview → Verify; reads bronze table, applies business rules via `filter`/`when`, splits into clean and quarantine DataFrames, writes both to Silver, and verifies row counts
- **Jupyter (PySpark):** Sections: Overview → Verify; same data quality logic using PySpark DataFrame filtering with `when()`, `otherwise()` conditional logic, and dual sink writes

Both languages implement identical quality validation: rule definition, conditional splitting, dual sink writes, and verification sections that confirm row counts.

## 5. Orchestration

Airflow DAG: `data_quality_nyc_taxi` — a scheduled batch DAG.

## 6. Usage

1. Ensure the `silver` and `bronze` Iceberg namespaces exist: `scripts/register_iceberg.py`
2. Populate the NYC taxi bronze table by running the upstream scenario first (or ensure `lakehouse.bronze.nyc_taxi_trips` exists)
3. Open either notebook on the Atlas stack, or trigger the Airflow DAG:
   ```bash
   airflow dags trigger data_quality_nyc_taxi
   ```
4. Verify output:
   ```bash
   spark-sql -e "SELECT COUNT(*) AS clean_count FROM lakehouse.silver.nyc_taxi_clean"
   spark-sql -e "SELECT COUNT(*) AS quarantine_count FROM lakehouse.silver.nyc_taxi_quarantine"
   ```

## 7. Dependencies

- **Dataset:** NYC Taxi trips (via `lakehouse.bronze.nyc_taxi_trips`)
- **Atlas services:** A1-A4 (Spark, Iceberg, S3 catalog, lakehouse catalog)
- **Other:** None

## 8. Known Issues & Caveats

Notebook execution and Scala/PySpark parity are live-gated on Atlas A1-A4. The `silver` namespace must exist in the Iceberg REST catalog; run `scripts/register_iceberg.py` before executing standalone. Requires the upstream `batch_ingest-nyc_taxi-spark-iceberg` scenario to run first to populate the bronze table.
```

- [ ] **Step 2: Write the next 5 scenario docs (medallion, streaming ingest, data quality group + schema evolution group)**

Create `docs/scenarios/streaming_windows-events-spark-iceberg.md`:

```markdown
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
```


Create `docs/scenarios/cdc_streaming-online_retail-spark-iceberg.md`:

```markdown
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

### 2.2 Output Tables

| Table | Layer | Key Columns |
|---|---|---|
| `lakehouse.silver.online_retail_cdc` | Silver | Same as input schema; updated and inserted rows reflect latest values |

## 3. Architecture

![Architecture](architectures/cdc_streaming-online_retail-spark-iceberg.html)

CDC events flow from the Redpanda `online_retail_cdc` topic through Spark Structured Streaming (`readStream` + `from_json`) into an Iceberg table. Each micro-batch triggers a `foreachBatch` callback that executes `MERGE INTO` — the same MERGE SQL as the batch `incremental_upsert-online_retail` scenario. The upsert key is the composite `(invoice, stock_code)`.

## 4. Notebooks

- **Zeppelin (Scala):** `zeppelin/notebook.zpln` — Sections: Overview, Setup, Read (`CREATE TABLE` + `readStream` + `from_json`), Transform (pass-through), Write (`foreachBatch` + `MERGE INTO`), Verify; 6 sections; Scala uses an anonymous function for the foreachBatch callback
- **Jupyter (PySpark):** `jupyter/notebook.ipynb` — Same 6 sections; PySpark uses `upsert_batch` function; the `MERGE INTO` SQL string is **identical** across both languages

## 5. Orchestration

Streaming queries are long-running and not scheduled as batch DAGs. The Airflow DAG (`cdc_streaming_online_retail`) is an `EmptyOperator` placeholder.

## 6. Usage

1. Start Atlas with Redpanda: `make up` (requires Atlas A9 / issue #269)
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

The `online_retail_cdc` topic is auto-created on first produce. Notebook execution and Scala/PySpark parity are live-gated on Atlas A9 (Redpanda). Produce CDC events to the topic before running. Checkpoints at `s3a://checkpoints/online_retail_cdc`. The `MERGE INTO` SQL is **identical** to the batch `incremental_upsert-online_retail` scenario — this is its streaming form. The DAG (`cdc_streaming_online_retail`) is an `EmptyOperator`.
```

Create `docs/scenarios/schema_evolution-gh_archive-spark-iceberg.md`:

```markdown
# schema_evolution-gh_archive-spark-iceberg

Demonstrates Apache Iceberg's schema evolution capabilities: adding columns with NULL propagation for existing rows (metadata-only, no data rewrite), and renaming columns without rewriting data.

## 1. Purpose

Schema evolution without data rewriting is one of Iceberg's core differentiators. This scenario shows that adding or renaming columns is a metadata-only operation in Iceberg — a critical capability for evolving data models over time without costly full-table rewrites. It simulates a realistic scenario where new data fields become available over time.

## 2. Data Model

### 2.1 Input Source

Source: Inline sample data embedded in the notebooks (no external dataset required).

| Column | Type | Notes |
|---|---|---|
| `id` | long | Event identifier |
| `type` | string | Original column name (later renamed) |
| `actor_login` | string | GitHub actor username |

### 2.2 Output Tables

| Table | Layer | Key Columns |
|---|---|---|
| `lakehouse.silver.gh_events_evolved` | Silver | `id`, `event_type` (renamed from `type`), `actor_login`, `repo_name` (new, NULL for all old rows) |

## 3. Architecture

![Architecture](architectures/schema_evolution-gh_archive-spark-iceberg.html)

Data flows from inline sample data through Spark batch processing into an Iceberg table. The notebook performs schema evolution: creates an initial table, inserts a row, adds a new column `repo_name` (where existing rows show NULL), renames the column `type` to `event_type`, inserts a new row with the new schema, and verifies that old and new data are consistent.

## 4. Notebooks

- **Zeppelin (Scala):** Sections: Overview → Verify; creates table with initial schema, inserts a row, evolves schema (add column via Iceberg's `ALTER TABLE ADD COLUMN` / `updateTableSchema`, rename column via `ALTER TABLE RENAME TO`), inserts a new row, and queries to demonstrate NULL propagation
- **Jupyter (PySpark):** Same sections; same evolution logic using PySpark DataFrame operations with Iceberg's `updateTableSchema` and `alterTable.rename` APIs

Both languages implement identical schema evolution: table creation, schema evolution, data insertions, and verification that old rows show NULL for the newly added column.

## 5. Orchestration

Airflow DAG: `schema_evolution_gh_archive` — a scheduled batch DAG.

## 6. Usage

1. Ensure the `silver` Iceberg namespace exists: `scripts/register_iceberg.py`
2. Open either notebook on the Atlas stack, or trigger the Airflow DAG:
   ```bash
   airflow dags trigger schema_evolution_gh_archive
   ```
3. Verify output:
   ```bash
   spark-sql -e "DESCRIBE lakehouse.silver.gh_events_evolved"
   ```
   Check that old rows show NULL for the newly added `repo_name` column.

## 7. Dependencies

- **Dataset:** None (inline sample data only)
- **Atlas services:** A1-A4 (Spark, Iceberg, S3 catalog, lakehouse catalog)
- **Other:** None

## 8. Known Issues & Caveats

Notebook execution and Scala/PySpark parity are live-gated on Atlas A1-A4. The `silver` namespace must exist in the Iceberg REST catalog; run `scripts/register_iceberg.py` before executing standalone. Iceberg schema evolution is a metadata-only operation — no data rewriting occurs, which is the core insight of this scenario.
```

Create `docs/scenarios/time_travel-nyc_taxi-spark-iceberg.md`:

```markdown
# time_travel-nyc_taxi-spark-iceberg

Demonstrates Iceberg time-travel capabilities on NYC-taxi data: create snapshots via inserts, query historical versions with `VERSION AS OF`, explore branching (Write-Audit-Publish pattern), and demonstrate rollback to earlier snapshots.

## 1. Purpose

Timetravel and versioning are essential for data governance, audit trails, and safe data mutations. This scenario demonstrates Iceberg's core time-travel and versioning features on a familiar dataset: inserting data to create snapshots, querying historical versions, creating WAP branches for safe mutations, and rolling back. These are critical capabilities for production lakehouse operations.

## 2. Data Model

### 2.1 Input Source

Source: `lakehouse.bronze.nyc_taxi_trips` (populated by upstream `batch_ingest-nyc_taxi-spark-iceberg` scenario).

### 2.2 Output Tables

| Table | Layer | Key Columns |
|---|---|---|
| `lakehouse.silver.nyc_taxi_tt` | Silver | Subset of bronze columns; created as a time-travel sandbox, dropped at end of notebook |

## 3. Architecture

![Architecture](architectures/time_travel-nyc_taxi-spark-iceberg.html)

Data flows from the bronze table into a dedicated sandbox table (`nyc_taxi_tt`). The notebook inserts data to create snapshots, then demonstrates time-travel queries (`VERSION AS OF`), WAP branching (write-audit-publish), and rollback to earlier snapshots. The sandbox table is dropped at the end of verification.

## 4. Notebooks

- **Zeppelin (Scala):** `zeppelin/notebook.zpln` — Sections: Overview, Setup, Create Snapshot, Time Travel (`VERSION AS OF`), WAP Branch, Verify (`DROP TABLE`)
- **Jupyter (PySpark):** `jupyter/notebook.ipynb` — Same sections; same time-travel operations via PySpark DataFrame and SQL APIs

Both notebooks implement identical time-travel logic: snapshot creation, historical version queries, WAP branching, rollback, and cleanup.

## 5. Orchestration

Airflow DAG: `time_travel_nyc_taxi` — a scheduled batch DAG.

## 6. Usage

1. Ensure the `silver` Iceberg namespace exists: `scripts/register_iceberg.py`
2. Ensure `lakehouse.bronze.nyc_taxi_trips` exists (populate via upstream scenario)
3. Open either notebook on the Atlas stack, or trigger the Airflow DAG:
   ```bash
   airflow dags trigger time_travel_nyc_taxi
   ```
4. Verify: check that historical queries return expected results from each snapshot.

## 7. Dependencies

- **Dataset:** NYC Taxi trips (via `lakehouse.bronze.nyc_taxi_trips`)
- **Atlas services:** A1-A4 (Spark, Iceberg, S3 catalog, lakehouse catalog)
- **Other:** None

## 8. Known Issues & Caveats

Notebook execution and Scala/PySpark parity are live-gated on Atlas A1-A4. The `silver` namespace must exist in the Iceberg REST catalog. Run `scripts/register_iceberg.py` (creates `bronze`, `silver`, `gold`) before executing standalone. `lakehouse.silver.nyc_taxi_tt` is dropped at the end of the Verify section (or manually via `DROP TABLE IF EXISTS lakehouse.silver.nyc_taxi_tt`). The `VERSION AS OF` and `rollback_to_snapshot` lines in the Verify cell are commented examples; replace `<snapshot_id>` with a concrete ID from the history query before uncommenting.
```

Create `docs/scenarios/table_maintenance-nyc_taxi-spark-iceberg.md`:

```markdown
# table_maintenance-nyc_taxi-spark-iceberg

Demonstrates Iceberg table maintenance operations on NYC-taxi data: compact data files via `rewrite_data_files`, expire old snapshots with `expire_snapshots`, and clean orphaned files via `remove_orphan_files`.

## 1. Purpose

Table maintenance and optimization are critical for production lakehouse hygiene and cost control. This scenario demonstrates Iceberg's maintenance and optimization capabilities: data file compaction (rewriting many small files into fewer large ones), snapshot expiry (cleaning up historical snapshots while retaining the latest), and orphan file removal (deleting data files no longer referenced by any snapshot). These operations prevent storage bloat and query slowdowns that accumulate over time in active tables.

## 2. Data Model

### 2.1 Input Source

Source: `lakehouse.bronze.nyc_taxi_trips` (populated by upstream `batch_ingest-nyc_taxi-spark-iceberg` scenario). The scenario creates a scenario-owned copy `lakehouse.silver.nyc_taxi_tm` (seeded from bronze, augmented with a second snapshot), then operates on this copy — the shared bronze table is never modified.

### 2.2 Output Tables

| Table | Layer | Key Columns |
|---|---|---|
| `lakehouse.silver.nyc_taxi_tm` | Silver | Same as bronze; used as the maintenance sandbox; not modified in bronze |

## 3. Architecture

![Architecture](architectures/table_maintenance-nyc_taxi-spark-iceberg.html)

A copy of the bronze table is created. Multiple snapshots are created to simulate data over time. Then the maintenance operations are applied: `rewrite_data_files` compacts files to a target size, `expire_snapshots` removes old snapshots (retaining the last one), and `remove_orphan_files` deletes data files no longer referenced. The final snapshot and file counts are verified.

## 4. Notebooks

- **Zeppelin (Scala):** `zeppelin/notebook.zpln` — Sections: Overview, Setup, Create Snapshot 1, Create Snapshot 2, Compact Files (`rewrite_data_files`), Expire Snapshots, Remove Orphan Files, Verify
- **Jupyter (PySpark):** `jupyter/notebook.ipynb` — Same sections; same maintenance operations via PySpark DataFrame and SQL APIs

Both notebooks implement identical table maintenance logic: seed copy → create snapshots → compact → expire → cleanup → verify.

## 5. Orchestration

Airflow DAG: `table_maintenance_nyc_taxi` — a scheduled batch DAG.

## 6. Usage

1. Ensure the `silver` and `bronze` Iceberg namespaces exist: `scripts/register_iceberg.py`
2. Populate `lakehouse.bronze.nyc_taxi_trips`: `make datasets`
3. Open either notebook on the Atlas stack, or trigger the Airflow DAG:
   ```bash
   airflow dags trigger table_maintenance_nyc_taxi
   ```
4. Verify: check final snapshot count is 1, and data file count is reduced from initial state.

## 7. Dependencies

- **Dataset:** NYC Taxi trips (via `lakehouse.bronze.nyc_taxi_trips`)
- **Atlas services:** A1-A4 (Spark, Iceberg, S3 catalog, lakehouse catalog) with system procedures enabled
- **Other:** None

## 8. Known Issues & Caveats

Notebook execution and Scala/PySpark parity are live-gated on Atlas A1-A4. The Iceberg catalog must support `system.rewrite_data_files`, `system.expire_snapshots`, and `system.remove_orphan_files`. Run `scripts/register_iceberg.py` (creates `bronze`, `silver`, `gold`) before executing standalone. The scenario creates and operates on `lakehouse.silver.nyc_taxi_tm` — the shared `lakehouse.bronze.nyc_taxi_trips` table is never modified.
```

- [ ] **Step 3: Write the next 5 scenario docs (streaming windows, CDC, schema evolution, time travel, table maintenance)** — See Step 2 code above.

- [ ] **Step 4: Write the remaining 9 scenario docs (star schema, BI query, federated query, join optimization, feature engineering, SCD2, incremental upsert, JSON flatten, sessionization)**

Create `docs/scenarios/star_schema-tpch-spark-iceberg.md`:

```markdown
# star_schema-tpch-spark-iceberg

Builds fact and dimension tables from the TPC-H dataset using star schema dimensional modeling, creating `dim_customer` and `fct_orders` in the gold layer.

## 1. Purpose

Star schema design is the foundation of dimensional data warehousing. This scenario demonstrates how to implement a star schema in Spark over a lakehouse: joining source tables (orders, customer, lineitem) into a structured dimensional model optimized for analytical queries and BI tool consumption. The dimension table (`dim_customer`) and fact table (`fct_orders`) serve as the canonical data model for downstream queries.

## 2. Data Model

### 2.1 Input Source

Source: TPC-H Parquet datasets in S3 (`s3a://landing/tpch/`), downloaded via `make datasets`.

**orders table** (`s3a://landing/tpch/orders`):

| Column | Type | Notes |
|---|---|---|
| `o_orderkey` | long | Order key (FK in fact) |
| `o_custkey` | long | Customer key (FK to dimension) |
| `o_totalprice` | double | Order total |
| `o_orderstatus` | string | Order status |

**customer table** (`s3a://landing/tpch/customer`):

| Column | Type | Notes |
|---|---|---|
| `c_custkey` | long | Customer key (PK) |
| `c_name` | string | Customer name |
| `c_mktsegment` | string | Market segment |

**lineitem table** (`s3a://landing/tpch/lineitem`):

| Column | Type | Notes |
|---|---|---|
| `l_orderkey` | long | Order key (FK) |
| `l_quantity` | double | Line item quantity |
| `l_extendedprice` | double | Line item extended price |

### 2.2 Output Tables

| Table | Layer | Key Columns |
|---|---|---|
| `lakehouse.gold.dim_customer` | Gold (dimension) | `c_custkey` (PK), `c_name`, `c_mktsegment` |
| `lakehouse.gold.fct_orders` | Gold (fact) | `o_orderkey` (PK), `o_custkey` (FK), `o_orderstatus`, `o_totalprice`, `l_quantity`, `l_extendedprice` |

## 3. Architecture

![Architecture](architectures/star_schema-tpch-spark-iceberg.html)

Data flows from three Parquet tables in S3 (`orders`, `customer`, `lineitem`) through Spark batch processing. Orders are joined with lineitems on order key, then joined with customers on customer key. The result produces two gold-layer Iceberg tables: a dimension table (`dim_customer`) and a fact table (`fct_orders`), forming a star schema.

## 4. Notebooks

- **Zeppelin (Scala):** `zeppelin/notebook.zpln` — Sections: Overview, Read Sources (3 Parquet tables), Join Orders+Lineitems, Join + Customer, Create Dimensions, Create Fact Table, Write to Gold, Verify
- **Jupyter (PySpark):** `jupyter/notebook.ipynb` — Same 8 sections; same dimensional modeling logic using PySpark DataFrame joins, dimension construction, fact table aggregation

Both languages implement identical star schema logic: source ingestion, multi-table joins, dimension/fact table creation, and verification of schema and row counts.

## 5. Orchestration

Airflow DAG: `star_schema_tpch` — a scheduled batch DAG.

## 6. Usage

1. Ensure the `gold` Iceberg namespace exists: `scripts/register_iceberg.py`
2. Populate the TPC-H dataset: `make datasets` to download Parquet files to S3
3. Open either notebook on the Atlas stack, or trigger the Airflow DAG:
   ```bash
   airflow dags trigger star_schema_tpch
   ```
4. Verify output:
   ```bash
   spark-sql -e "SELECT COUNT(*) FROM lakehouse.gold.dim_customer"
   spark-sql -e "SELECT COUNT(*) FROM lakehouse.gold.fct_orders"
   ```

## 7. Dependencies

- **Dataset:** TPC-H Parquet (`orders`, `customer`, `lineitem`) from `s3a://landing/tpch/`
- **Atlas services:** A1-A4 (Spark, Iceberg, S3 catalog, lakehouse catalog)
- **Other:** None
- **Note:** Reads directly from S3 landing — no medallion intermediate layers

## 8. Known Issues & Caveats

Notebook execution and Scala/PySpark parity are live-gated on Atlas A1-A4. The `gold` namespace must exist in the Iceberg REST catalog; run `scripts/register_iceberg.py` before executing standalone. `make datasets` is required to populate the TPC-H landing zone before the notebook can read data.
```

Create `docs/scenarios/bi_query-tpch-trino-iceberg.md`:

```markdown
# bi_query-tpch-trino-iceberg

Queries gold-layer marts via Trino SQL, demonstrating Trino as a lightweight SQL-only analytics engine over Iceberg tables produced by Spark.

## 1. Purpose

Trino provides a lightweight, SQL-only query path over lakehouse data that complements Spark's programmatic ETL. This scenario shows how different engines can share the same underlying Iceberg tables: analysts can query data without writing Spark code. It reads `fct_orders` and `dim_customer` from the `star_schema` scenario, joins them, aggregates revenue by market segment, and writes a summary table — demonstrating true multi-engine lakehouse architecture.

## 2. Data Model

### 2.1 Input Source

Source: `lakehouse.gold` tables written by the upstream `star_schema-tpch-spark-iceberg` scenario.

From `lakehouse.gold.fct_orders`:

| Column | Type | Notes |
|---|---|---|
| `o_orderkey` | long | Order key |
| `o_custkey` | long | Customer FK |
| `o_totalprice` | double | Order total |

From `lakehouse.gold.dim_customer`:

| Column | Type | Notes |
|---|---|---|
| `c_custkey` | long | Customer PK |
| `c_name` | string | Customer name |
| `c_mktsegment` | string | Market segment |

### 2.2 Output Tables

| Table | Layer | Key Columns |
|---|---|---|
| `lakehouse.gold.bi_segment_revenue` | Gold | `market_segment`, `total_revenue`, `order_count` |

## 3. Architecture

![Architecture](architectures/bi_query-tpch-trino-iceberg.html)

Data flows from gold-layer Iceberg tables (`fct_orders`, `dim_customer`) through Trino SQL queries. The Trino coordinator connects to the Iceberg catalog, reads the gold tables, joins them, aggregates revenue by market segment, and writes the summary back to the gold layer — all via standard ANSI SQL with no Spark involvement.

## 4. Notebooks

- **Zeppelin (Scala, `%trino`):** Sections: Overview, Read Gold Tables, Join + Aggregate, Write Summary, Verify; identical SQL to PySpark
- **Jupyter (Py, `trino` client):** Sections: Overview, Read Gold Tables, Join + Aggregate, Write Summary, Verify; identical SQL executed via the Trino Python client connecting to `trino:8080`

Both notebooks run the same SQL queries to demonstrate cross-engine parity for analytical queries.

## 5. Orchestration

Airflow DAG: Placeholder (`EmptyOperator`). Trigger via notebooks only until TrinoOperator integration is added (Atlas #268).

## 6. Usage

1. Run the prerequisite scenario: `star_schema-tpch-spark-iceberg` (creates `fct_orders` and `dim_customer`)
2. Ensure the `gold` Iceberg namespace exists: `scripts/register_iceberg.py`
3. Open either notebook on the Atlas stack (Trino coordinator must be reachable at `trino:8080`) and run all sections
4. Verify:
   ```bash
   spark-sql -e "SELECT * FROM lakehouse.gold.bi_segment_revenue ORDER BY total_revenue DESC"
   ```

## 7. Dependencies

- **Dataset:** TPC-H gold tables (`fct_orders`, `dim_customer`) from `lakehouse.gold`
- **Atlas services:** A5-A7 (Trino, Trino coordinator, Iceberg REST catalog)
- **Other:** `trino` Python client (Jupyter notebook)

## 8. Known Issues & Caveats

Live execution is gated on Atlas #268 (Trino coordinator integration). The `%trino` Zeppelin interpreter is seeded by Atlas pointing to the Trino coordinator. The `lakehouse.gold` namespace must exist before the Write query runs. Requires the upstream `star_schema-tpch-spark-iceberg` to run first.
```

Create `docs/scenarios/federated_query-nyc_taxi-trino-iceberg.md`:

```markdown
# federated_query-nyc_taxi-trino-iceberg

Query the NYC-taxi Iceberg lakehouse via Trino SQL — `lakehouse.bronze.nyc_taxi_trips` → `lakehouse.gold.nyc_taxi_daily_trino` — from both a Zeppelin `%trino` notebook and a Jupyter notebook using the `trino` Python client. Both surfaces run identical SQL.

## 1. Purpose

This scenario demonstrates Trino as a query engine over the same Iceberg lakehouse used by Spark scenarios. Trino reads Iceberg tables directly via the `lakehouse` catalog, aggregates NYC-taxi trips into a daily summary, and writes the result back to the gold layer — all via standard ANSI SQL (no Spark required). It provides a lightweight SQL-only alternative to PySpark/Scala workloads, complementing Spark's programmatic ETL with ad-hoc querying.

## 2. Data Model

### 2.1 Input Source

Source: `lakehouse.bronze.nyc_taxi_trips` (populated by `batch_ingest-nyc_taxi-spark-iceberg`).

### 2.2 Output Tables

| Table | Layer | Key Columns |
|---|---|---|
| `lakehouse.gold.nyc_taxi_daily_trino` | Gold | `trip_date`, `trip_count`, `avg_fare` (aggregated daily summary) |

## 3. Architecture

![Architecture](architectures/federated_query-nyc_taxi-trino-iceberg.html)

Data flows from the bronze Iceberg table through Trino SQL aggregation into the gold layer. Trino reads directly from the Iceberg REST catalog (same catalog as Spark), executes ANSI SQL queries for daily aggregation, and writes results back — no Spark cluster involved.

## 4. Notebooks

- **Zeppelin (Scala, `%trino`):** Sections: Overview → Read Bronze Table, Aggregate by Day, Write Gold Summary → Verify; identical SQL to PySpark
- **Jupyter (Py, `trino`):** Same sections; identical SQL via the Trino Python client

## 5. Orchestration

Airflow DAG: EmptyOperator placeholder (trigger via notebooks; TrinoOperator integration adds scheduling, Atlas #268).

## 6. Usage

1. Populate bronze table: `batch_ingest-nyc_taxi-spark-iceberg` (or ensure it exists)
2. Ensure the `gold` Iceberg namespace exists: `scripts/register_iceberg.py`
3. Open either notebook on the Atlas stack (Trino coordinator at `trino:8080`) and run all sections
4. Verify: `spark-sql -e "SELECT * FROM lakehouse.gold.nyc_taxi_daily_trino LIMIT 10"`

## 7. Dependencies

- **Dataset:** NYC Taxi trips via `lakehouse.bronze.nyc_taxi_trips`
- **Atlas services:** A5-A7 (Trino, Trino coordinator, Iceberg REST catalog)
- **Other:** None

## 8. Known Issues & Caveats

Live execution is gated on Atlas #268 (Trino coordinator integration). The `%trino` interpreter is seeded by Atlas pointing to the Trino coordinator. The `lakehouse.gold` namespace must exist in the Iceberg REST catalog before the Write cell runs.
```

Create `docs/scenarios/join_optimization-tpch-spark-iceberg.md`:

```markdown
# join_optimization-tpch-spark-iceberg

Demonstrates Spark broadcast joins and Adaptive Query Execution (AQE) by joining TPC-H `orders` with `customer` and aggregating revenue by market segment.

## 1. Purpose

Understanding join optimization is essential for building performant Spark pipelines at scale. This scenario explores broadcast joins (pushing a dimension table to all executors instead of shuffling it) versus sort-merge joins, shows how AQE automatically selects efficient strategies, and demonstrates inspecting physical plans via `.explain()` to diagnose and tune join performance — including the `AQE` scale knob for tuning broadcast thresholds.

## 2. Data Model

### 2.1 Input Source

Source: TPC-H Parquet datasets in S3 (`s3a://landing/tpch/`), downloaded via `make datasets`.

**orders table** (`s3a://landing/tpch/orders`):

| Column | Type | Notes |
|---|---|---|
| `o_orderkey` | long | Order key (PK) |
| `o_custkey` | long | Customer FK |
| `o_totalprice` | double | Order total price |
| `o_orderpriority` | string | Order priority |

**customer table** (`s3a://landing/tpch/customer`):

| Column | Type | Notes |
|---|---|---|
| `c_custkey` | long | Customer key (PK) |
| `c_name` | string | Customer name |
| `c_mktsegment` | string | Market segment |

### 2.2 Output Tables

| Table | Layer | Key Columns |
|---|---|---|
| `lakehouse.gold.tpch_segment_revenue` | Gold | `market_segment`, `total_revenue`, `order_count` |

## 3. Architecture

![Architecture](architectures/join_optimization-tpch-spark-iceberg.html)

Data flows from two Parquet tables in S3 (`orders`, `customer`) through Spark batch processing. The `customer` dimension table is broadcast to all executors (using `broadcast()` hint) and joined with the larger `orders` table on `o_custkey = c_custkey`. The result is aggregated by market segment into total revenue and order count, written to gold. The physical plan `.explain()` output demonstrates `BroadcastHashJoin` optimization and AQE tuning.

## 4. Notebooks

- **Zeppelin (Scala):** Sections: Overview, Read Source Tables, Broadcast Join, Aggregate by Segment, Print Physical Plan, Write to Gold, Verify
- **Jupyter (PySpark):** Same sections; same join optimization logic using `broadcast()` hint and `groupBy().agg()` with `.explain()` output

Both languages implement identical join optimization: broadcast join, AQE demonstration, physical plan inspection, and verification.

## 5. Orchestration

Airflow DAG: `join_optimization_tpch` — a scheduled batch DAG.

## 6. Usage

1. Ensure the `gold` Iceberg namespace exists: `scripts/register_iceberg.py`
2. Populate the TPC-H dataset: `make datasets`
3. Open either notebook on the Atlas stack, or trigger the Airflow DAG:
   ```bash
   airflow dags trigger join_optimization_tpch
   ```
4. Verify:
   ```bash
   spark-sql -e "SELECT * FROM lakehouse.gold.tpch_segment_revenue ORDER BY total_revenue DESC"
   ```

## 7. Dependencies

- **Dataset:** TPC-H Parquet (`orders`, `customer`) from `s3a://landing/tpch/`
- **Atlas services:** A1-A4 (Spark, Iceberg, S3 catalog, lakehouse catalog)
- **Other:** None
- **Note:** Reads directly from S3 landing — no medallion intermediate layers

## 8. Known Issues & Caveats

Notebook execution and Scala/PySpark parity are live-gated on Atlas A1-A4. The `gold` namespace must exist in the Iceberg REST catalog; run `scripts/register_iceberg.py` before executing. `make datasets` is required to populate the TPC-H landing zone.
```

Create `docs/scenarios/feature_engineering-movielens-spark-iceberg.md`:

```markdown
# feature_engineering-movielens-spark-iceberg

Aggregates MovieLens ratings into user-level and movie-level feature marts for downstream machine learning recommendation models.

## 1. Purpose

Feature engineering for ML is a critical step between raw data ingestion and model training. This scenario demonstrates aggregating large rating datasets into tabular feature stores that can be consumed by collaborative filtering and other recommendation systems. User-level features include average rating and total ratings per user; movie-level features include average rating and popularity metrics.

## 2. Data Model

### 2.1 Input Source

Source: MovieLens ratings CSV from S3 (`s3a://landing/movielens/ratings.csv`), downloaded via `make datasets`.

| Column | Type | Notes |
|---|---|---|
| `userId` | int | User identifier |
| `movieId` | int | Movie identifier |
| `rating` | double | Rating value |
| `timestamp` | int | Unix timestamp |

### 2.2 Output Tables

| Table | Layer | Key Columns |
|---|---|---|
| `lakehouse.gold.ml_user_features` | Gold (feature mart) | `userId`, `avg_rating`, `num_ratings` |
| `lakehouse.gold.ml_movie_features` | Gold (feature mart) | `movieId`, `movie_avg_rating`, `popularity` |

## 3. Architecture

![Architecture](architectures/feature_engineering-movielens-spark-iceberg.html)

Data flows from MovieLens ratings CSV in S3 through Spark batch processing. Two aggregations are computed: user-level features (avg rating, num ratings per user) and movie-level features (avg rating, popularity per movie). Both are written as gold-layer Iceberg tables.

## 4. Notebooks

- **Zeppelin (Scala):** Sections: Overview, Read CSV, Aggregate by User, Aggregate by Movie, Write Feature Tables, Verify
- **Jupyter (PySpark):** Same sections; same feature engineering logic using `groupBy().agg()` for both user and movie aggregations

Both languages implement identical feature engineering: raw CSV ingestion, user/movie aggregation, and verification of aggregated output.

## 5. Orchestration

Airflow DAG: `feature_engineering_movielens` — a scheduled batch DAG.

## 6. Usage

1. Ensure the `gold` Iceberg namespace exists: `scripts/register_iceberg.py`
2. Populate the MovieLens dataset: `make datasets`
3. Open either notebook on the Atlas stack, or trigger the Airflow DAG:
   ```bash
   airflow dags trigger feature_engineering_movielens
   ```
4. Verify:
   ```bash
   spark-sql -e "SELECT * FROM lakehouse.gold.ml_user_features LIMIT 10"
   spark-sql -e "SELECT * FROM lakehouse.gold.ml_movie_features LIMIT 10"
   ```

## 7. Dependencies

- **Dataset:** MovieLens ratings CSV from `s3a://landing/movielens/ratings.csv`
- **Atlas services:** A1-A4 (Spark, Iceberg, S3 catalog, lakehouse catalog)
- **Other:** None
- **Note:** Reads directly from S3 landing — no medallion intermediate layers

## 8. Known Issues & Caveats

Notebook execution and Scala/PySpark parity are live-gated on Atlas A1-A4. The `gold` namespace must exist; run `scripts/register_iceberg.py` first. `make datasets` is required to populate the MovieLens landing zone before the notebook can read data.
```

Create `docs/scenarios/scd2-online_retail-spark-iceberg.md`:

```markdown
# scd2-online_retail-spark-iceberg

Implements Slowly Changing Dimension Type 2 (SCD2) to track historical changes on a customer dimension, preserving full history with effective timestamps and current flags.

## 1. Purpose

SCD2 is a cornerstone of dimensional data warehousing, enabling time-travel queries on dimension changes. This scenario demonstrates Iceberg's native row-level UPDATE and INSERT capabilities, making SCD2 practical: efficient history tracking without full table scans or partition rewrites. When a customer's segment changes, the old row is closed (setting `effective_to` and `is_current=false`) and a new row is opened with the updated segment (`effective_from`, `is_current=true`).

## 2. Data Model

### 2.1 Input Source

Source: Online retail dimension data (inline seed in notebooks — no external dataset required).

| Column | Type | Notes |
|---|---|---|
| `CustomerID` | double | Customer identifier |
| `Name` | string | Customer name |
| `Segment` | string | Customer segment (changes over time) |

### 2.2 Output Tables

| Table | Layer | Key Columns |
|---|---|---|
| `lakehouse.gold.dim_customer_scd2` | Gold (dimension) | `CustomerID`, `Name`, `Segment`, `effective_from`, `effective_to`, `is_current` |

## 3. Architecture

![Architecture](architectures/scd2-online_retail-spark-iceberg.html)

Data flows from inline seed data through Spark batch processing with SCD2 logic: the initial customer data is seeded, then a simulated segment change triggers the SCD2 pattern — closing the old row and opening a new row with updated segment and effective timestamps.

## 4. Notebooks

- **Zeppelin (Scala):** Sections: Overview, Seed Data, Apply SCD2 Logic (close old row, open new row), Query Historical Records, Verify
- **Jupyter (PySpark):** Same sections; same SCD2 logic using PySpark `when()` and DataFrame updates with `effective_from`/`effective_to`/`is_current` tracking

Both languages implement identical SCD2 logic with seeding, history tracking via effective timestamps and current flags, and verification sections.

## 5. Orchestration

Airflow DAG: `scd2_online_retail` — a scheduled batch DAG.

## 6. Usage

1. Ensure the `gold` Iceberg namespace exists: `scripts/register_iceberg.py`
2. Open either notebook on the Atlas stack, or trigger the Airflow DAG:
   ```bash
   airflow dags trigger scd2_online_retail
   ```
3. Verify:
   ```bash
   spark-sql -e "SELECT * FROM lakehouse.gold.dim_customer_scd2"
   ```

## 7. Dependencies

- **Dataset:** Online retail dimension data (inline seed)
- **Atlas services:** A1-A4 (Spark, Iceberg, S3 catalog, lakehouse catalog)
- **Other:** None
- **Note:** Iceberg row-level UPDATE is an SQL extension; ensure `iceberg.sql.extensions` is enabled in Spark configuration

## 8. Known Issues & Caveats

Notebook execution and Scala/PySpark parity are live-gated on Atlas A1-A4. The `gold` namespace must exist; run `scripts/register_iceberg.py` first. At scale, the inline seed can be replaced by the registered `online_retail` dataset. The notebook's seed INSERT is not guarded; re-running the full notebook accumulates seed rows — drop the target table first for a clean demo.
```

Create `docs/scenarios/incremental_upsert-online_retail-spark-iceberg.md`:

```markdown
# incremental_upsert-online_retail-spark-iceberg

Implements CDC-style incremental upserts using Iceberg's `MERGE INTO` to apply change sets idempotently without rewriting entire tables.

## 1. Purpose

Incremental upserts are essential for building efficient data pipelines that avoid full table rewrites. This scenario teaches how to merge delta batches into an existing Iceberg table while maintaining data consistency and idempotency. The same batch can be merged multiple times without data duplication — a pattern used in daily ETL pipelines and real-time CDC workflows.

## 2. Data Model

### 2.1 Input Source

Source: Online retail batch deltas (inline seed data + change-set batches in the notebooks — no external dataset required).

| Column | Type | Notes |
|---|---|---|
| `InvoiceNo` | string | Invoice number (unique key) |
| `StockCode` | string | Product code |
| `Description` | string | Product description |
| `Quantity` | int | Quantity ordered |
| `InvoiceDate` | timestamp | Invoice date and time |
| `UnitPrice` | double | Price per unit |
| `CustomerID` | double (nullable) | Customer identifier |
| `Country` | string | Customer country |

### 2.2 Output Tables

| Table | Layer | Key Columns |
|---|---|---|
| `lakehouse.silver.online_retail` | Silver | Same as input; updated and inserted rows reflect latest values |

## 3. Architecture

![Architecture](architectures/incremental_upsert-online_retail-spark-iceberg.html)

Data flows from inline seed data through Spark batch processing with `MERGE INTO`. The notebook seeds initial data, applies two change-set batches (one with an update, one with an insert). The same batch can be merged multiple times without duplication, demonstrating idempotent change-set application.

## 4. Notebooks

- **Zeppelin (Scala):** Sections: Overview, Seed Data, Merge Batch 1 (update), Merge Batch 2 (insert), Verify Idempotency (merge same batch twice), Verify
- **Jupyter (PySpark):** Same sections; same CDC logic using PySpark's `df.merge()` and `MERGE INTO` table SQL syntax

Both languages implement identical upsert logic with seeding, merge operations, and verification.

## 5. Orchestration

Airflow DAG: `incremental_upsert_online_retail` — a scheduled batch DAG.

## 6. Usage

1. Ensure the `silver` Iceberg namespace exists: `scripts/register_iceberg.py`
2. Open either notebook on the Atlas stack, or trigger the Airflow DAG:
   ```bash
   airflow dags trigger incremental_upsert_online_retail
   ```
3. Verify:
   ```bash
   spark-sql -e "SELECT InvoiceNo, Description, Quantity FROM lakehouse.silver.online_retail"
   ```

## 7. Dependencies

- **Dataset:** Online retail data (via `make datasets` for registered dataset)
- **Atlas services:** A1-A4 (Spark, Iceberg, S3 catalog, lakehouse catalog)
- **Other:** None

## 8. Known Issues & Caveats

Notebook execution and Scala/PySpark parity are live-gated on Atlas A1-A4. The `silver` namespace must exist; run `scripts/register_iceberg.py` first. The seed INSERT is not guarded — re-running the full notebook accumulates seed rows. Drop the target table first for a clean demo. At scale, the inline seed can be replaced by the registered `online_retail` dataset.
```

Create `docs/scenarios/json_flatten-gh_archive-spark-iceberg.md`:

```markdown
# json_flatten-gh_archive-spark-iceberg

Reads GitHub Archive nested JSON events, extracts and flattens nested fields, casts timestamps, and writes to a flat Iceberg silver table.

## 1. Purpose

Handling semi-structured nested data is a common ETL pattern in data engineering. This scenario demonstrates converting messy JSON into well-typed columns using Spark's built-in `get_json_object` and `col` dot-notation for extracting deeply nested fields (like `actor.login` and `repo.name`), casting `created_at` to a proper timestamp, and writing the result as a flat Iceberg table for downstream consumption.

## 2. Data Model

### 2.1 Input Source

Source: `s3a://landing/gh_archive/*.json.gz` (compressed JSON files from GitHub Archive, downloaded via `make datasets`).

| Column | Type | Source |
|---|---|---|
| `id` | long | JSON: `id` |
| `type` | string | JSON: `type` |
| `actor_login` | string | JSON: `actor.login` |
| `repo_name` | string | JSON: `repo.name` |
| `created_at` | timestamp | JSON: `created_at` (cast from string) |

### 2.2 Output Tables

| Table | Layer | Key Columns |
|---|---|---|
| `lakehouse.silver.gh_events` | Silver | `id`, `type`, `actor_login`, `repo_name`, `created_at` |

## 3. Architecture

![Architecture](architectures/json_flatten-gh_archive-spark-iceberg.html)

Data flows from compressed JSON files in S3 through Spark batch processing. Nested fields are extracted using dot notation (`col("actor.login")`), timestamps are cast to proper types, and the flattened result is written to an Iceberg silver table.

## 4. Notebooks

- **Zeppelin (Scala):** Sections: Overview, Read JSON from S3, Extract Nested Fields, Cast Timestamps, Write to Iceberg, Verify
- **Jupyter (PySpark):** Same sections; same JSON flatten logic using `col("actor.login")` syntax and `toTimestamp`

Both languages implement identical JSON flatten logic with source read, field extraction, type casting, and sink write.

## 5. Orchestration

Airflow DAG: `json_flatten_gh_archive` — a scheduled batch DAG.

## 6. Usage

1. Ensure the `silver` Iceberg namespace exists: `scripts/register_iceberg.py`
2. Populate the landing zone: `make datasets`
3. Open either notebook on the Atlas stack, or trigger the Airflow DAG:
   ```bash
   airflow dags trigger json_flatten_gh_archive
   ```
4. Verify output:
   ```bash
   spark-sql -e "SELECT COUNT(*) FROM lakehouse.silver.gh_events"
   ```

## 7. Dependencies

- **Dataset:** GitHub Archive compressed JSON from `s3a://landing/gh_archive/`
- **Atlas services:** A1-A4 (Spark, Iceberg, S3 catalog, lakehouse catalog)
- **Other:** None

## 8. Known Issues & Caveats

Notebook execution and Scala/PySpark parity are live-gated on Atlas A1-A4. The `silver` namespace must exist; run `scripts/register_iceberg.py` first. `make datasets` is required to populate the GitHub Archive landing zone before the notebook can read data.
```

Create `docs/scenarios/sessionization-gh_archive-spark-iceberg.md`:

```markdown
# sessionization-gh_archive-spark-iceberg

Detects user sessions from GitHub Archive events using window functions and gap-based sessionization with a 30-minute inactivity threshold.

## 1. Purpose

Sessionization is a foundational pattern in event-driven analytics, used to understand user behavior patterns, engagement, and activity flows. This scenario showcases advanced window function techniques: partitioning events by `actor_login`, ordering by timestamp, detecting inactivity gaps exceeding 30 minutes using the `LAG` window function, and assigning session IDs via a cumulative sum over gap indicators.

## 2. Data Model

### 2.1 Input Source

Source: `lakehouse.silver.gh_events` (populated by the upstream `json_flatten-gh_archive-spark-iceberg` scenario).

| Column | Type | Notes |
|---|---|---|
| `actor_login` | string | Partition key for session detection |
| `created_at` | timestamp | Used for ordering and gap detection |
| `type` | string | Event type |
| `repo_name` | string | Repository name |

### 2.2 Output Tables

| Table | Layer | Key Columns |
|---|---|---|
| `lakehouse.silver.gh_sessions` | Silver | `actor_login`, `session_id`, `created_at`, `type` |

## 3. Architecture

![Architecture](architectures/sessionization-gh_archive-spark-iceberg.html)

Data flows from the GitHub Events silver table through Spark batch processing. Events are partitioned by `actor_login`, ordered by timestamp, and the `LAG` window function detects gaps > 30 minutes between consecutive events. Sessions are assigned IDs via cumulative sum over gap indicators, and each output row includes the actor login and its session ID.

## 4. Notebooks

- **Zeppelin (Scala):** Sections: Overview, Read Events, Compute LAG, Detect Gaps (> 30 min), Assign Session IDs, Write to Iceberg, Verify
- **Jupyter (PySpark):** Same sections; same sessionization logic using `lag()`, `when()`, and `sum().over()` window operations

Both languages implement identical sessionization logic with gap detection, session assignment, and verification.

## 5. Orchestration

Airflow DAG: `sessionization_gh_archive` — a scheduled batch DAG.

## 6. Usage

1. Ensure the `silver` Iceberg namespace exists: `scripts/register_iceberg.py`
2. Populate GitHub Archive data and run the prerequisite JSON flatten scenario: `make datasets` followed by `airflow dags trigger json_flatten_gh_archive` (or `lakehouse.silver.gh_events` must exist)
3. Open either notebook on the Atlas stack, or trigger the Airflow DAG:
   ```bash
   airflow dags trigger sessionization_gh_archive
   ```
4. Verify:
   ```bash
   spark-sql -e "SELECT actor_login, COUNT(DISTINCT session_id) AS num_sessions FROM lakehouse.silver.gh_sessions GROUP BY actor_login LIMIT 10"
   ```

## 7. Dependencies

- **Dataset:** GitHub Archive events (via `lakehouse.silver.gh_events`)
- **Atlas services:** A1-A4 (Spark, Iceberg, S3 catalog, lakehouse catalog)
- **Other:** None

## 8. Known Issues & Caveats

The 30-minute gap threshold is hardcoded as 1800 seconds and not externally configurable. The `silver` namespace must exist; run `scripts/register_iceberg.py` first. Requires upstream data in `lakehouse.silver.gh_events`; ensure the JSON flatten scenario has run first.
```

- [ ] **Step 5: Verify all 19 scenario docs are written**

```bash
wc -l docs/scenarios/*.md | grep -v index.md
ls docs/scenarios/*.md | grep -v index.md | wc -l
```

Expected: `19` scenario docs.

- [ ] **Step 6: Verify MkDocs nav matches — run build dry run**

```bash
uv run --group dev mkdocs build --strict
```

Expected: successful build with no missing links. Check the output HTML files exist for all scenario pages.

---

### Task 4: Enhance spark-app docs with architecture diagrams and cross-links

**Files:**
- Modify: `docs/spark-apps/nyc-taxi-etl.md` (existing file, enhance)
- Modify: `docs/spark-apps/nyc-taxi-medallion.md` (existing file, enhance)
- Both files already exist. Add architecture diagram embedding at section "Architecture" if missing, and "See Also" cross-links at bottom.

- [ ] **Step 1: Read current content and enhance both docs**

Read the existing files and add:
1. Architecture diagram embedding in section 3 (if not present):
   ```markdown
   ## 3. Architecture
   
   ![Architecture](architectures/<app-name>.html)
   
   <Description of CI/CD flow>
   ```
2. "See Also" cross-links at bottom:
   ```markdown
   ## See Also
   
   - [Related scenario: batch_ingest-nyc_taxi-spark-iceberg](../scenarios/batch_ingest-nyc_taxi-spark-iceberg.md) — Produces the bronze table this app consumes.
   - [Lakehouse Architecture](../lakehouse.md)
   - [Datasets](../datasets.md)
   ```
3. Fix any typos (e.g., "medallion medallion" → "medallion")

Apply the same enhancement pattern to both docs.

- [ ] **Step 2: Verify spark-app docs render correctly**

```bash
uv run --group dev mkdocs build --strict
```

Expected: build succeeds, both app docs have architecture diagram references and See Also sections.

---

### Task 5: Enhance `docs/index.md` with cross-links and updated catalog

**Files:**
- Modify: `docs/index.md`

- [ ] **Step 1: Ensure the scenario catalog table is accurate**

Cross-reference the catalog with `mkdocs.yml` nav entries and the actual 19 scenarios. The index should present a clean overview table with scenario name, dataset, mode, layers, DAG, and key cross-links.

- [ ] **Step 2: Ensure cross-links are consistent**

The "Explore" section card links must point to the correct MkDocs pages. Verify all links in `docs/index.md` resolve during a build.

- [ ] **Step 3: Verify index renders correctly**

```bash
uv run --group dev mkdocs build --strict
```

---

### Task 6: Enhance `docs/lakehouse.md` and `docs/datasets.md` with cross-links

**Files:**
- Modify: `docs/lakehouse.md`
- Modify: `docs/datasets.md`

- [ ] **Step 1: Add cross-links to lakehouse.md**

Add a "Scenarios Using Lakehouse" section at the bottom listing relevant scenarios with links:
- Bronze-layer scenarios: batch_ingest, streaming_ingest (events + gh_archive)
- Silver-layer scenarios: data_quality, json_flatten, incremental_upsert, scd2, streaming_windows, cdc_streaming, streaming_ingest_gh_archive, schema_evolution, time_travel, table_maintenance, sessionization
- Gold-layer scenarios: medallion, star_schema, feature_engineering, bi_query, federated_query, join_optimization

- [ ] **Step 2: Add cross-links to datasets.md**

For each of the 5 datasets (NYC Taxi, TPC-H, MovieLens, Online Retail, GitHub Archive), add a "Used by these scenarios" section linking to the relevant scenario docs.

- [ ] **Step 3: Verify builds**

```bash
uv run --group dev mkdocs build --strict
```

---

### Task 7: Update build scripts

**Files:**
- Modify: `scripts/build_wiki.py` — Ensure scenario pages pull from `docs/scenarios/*.md` (not from `scenarios/*/README.md` after those become stubs)
- Create: `scripts/render_readme.py` — Extract content from `docs/index.md` to root `README.md`

- [ ] **Step 1: Update `build_wiki.py` to pull scenario docs from docs/ instead of READMEs**

Modify the `_build_scenarios` function:

Before (current behavior):
```python
def _build_scenarios() -> list[tuple[str, str]]:
    scenario_dirs = sorted((REPO_ROOT / "scenarios").iterdir())
    entries: list[tuple[str, str]] = []
    for d in scenario_dirs:
        readme = d / "README.md"
        if not d.is_dir() or not readme.exists():
            continue
        name = d.name
        wiki_name = f"Scenario-{name}.md"
        dest = WIKI_DIR / wiki_name
        title = _h1(readme) or _stem(name)
        section_url = SITE_URL + f"scenarios/{name}/"
        original = readme.read_text(encoding="utf-8")
        content = BANNER_TEMPLATE.format(url=section_url) + original
        _write(dest, content)
        entries.append((title, wiki_name))
    return entries
```

After (new behavior — pull from MkDocs docs instead):
```python
def _build_scenarios() -> list[tuple[str, str]]:
    docs_dir = REPO_ROOT / "docs" / "scenarios"
    entries: list[tuple[str, str]] = []
    for md_file in sorted(docs_dir.glob("*.md")):
        if md_file.name == "index.md":
            continue  # Skip index; it's handled separately
        name = md_file.stem
        wiki_name = f"Scenario-{name}.md"
        dest = WIKI_DIR / wiki_name
        title = _h1(md_file) or _stem(name)
        section_url = SITE_URL + f"scenarios/{name}/"
        original = md_file.read_text(encoding="utf-8")
        content = BANNER_TEMPLATE.format(url=section_url) + original
        _write(dest, content)
        entries.append((title, wiki_name))
    return entries
```

Similarly update `_build_apps` to pull from `docs/spark-apps/*.md` instead of `spark-apps/*/README.md`:

```python
def _build_apps() -> list[tuple[str, str]]:
    docs_dir = REPO_ROOT / "docs" / "spark-apps"
    entries: list[tuple[str, str]] = []
    for md_file in sorted(docs_dir.glob("*.md")):
        if md_file.name == "index.md":
            continue
        name = md_file.stem
        wiki_name = f"App-{name}.md"
        dest = WIKI_DIR / wiki_name
        title = _h1(md_file) or _stem(name)
        section_url = SITE_URL + f"spark-apps/{name}/"
        original = md_file.read_text(encoding="utf-8")
        content = BANNER_TEMPLATE.format(url=section_url) + original
        _write(dest, content)
        entries.append((title, wiki_name))
    return entries
```

- [ ] **Step 2: Write `render-readme.py` — extract content from `docs/index.md` to root `README.md`**

Create `scripts/render_readme.py`:

```python
"""Extract key sections from docs/index.md into the root README.md.

Run from repo root:
    uv run --group dev python scripts/render_readme.py

Output:
    README.md — auto-generated from docs/index.md content
"""
from pathlib import Path
import re

REPO_ROOT = Path(__file__).parent.parent

def main():
    source = REPO_ROOT / "docs" / "index.md"
    readme = REPO_ROOT / "README.md"

    if not source.exists():
        raise FileNotFoundError(f"Source not found: {source}")

    content = source.read_text(encoding="utf-8")

    # Extract: title (first H1), overview paragraph(s), "By the Numbers" table,
    # "Quick Start" code block, "Explore" section (first 2-3 subsections)
    lines = []
    in_quick_start = False
    in_by_numbers = False
    in_explore = False

    for line in content.splitlines():
        if re.match(r"^#\s+", line):
            lines.append(line)
        elif line.startswith("## ") and "Quick Start" in line:
            in_quick_start = True
            lines.append(line)
        elif line.startswith("## ") and in_quick_start:
            in_quick_start = False
            lines.append(line)  # Include the heading
        elif line.startswith("## ") and "Explore" in line:
            in_explore = True
            lines.append(line)
        elif line.startswith("## ") and in_explore:
            in_explore = False
        elif in_quick_start:
            lines.append(line)
        elif in_explore:
            lines.append(line)

    readme.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(f"Wrote {readme} ({len(lines)} lines)")


if __name__ == "__main__":
    main()
```

- [ ] **Step 3: Verify both scripts run correctly**

```bash
uv run --group dev python scripts/render_readme.py
uv run --group dev python scripts/build_wiki.py
```

Expected:
- `render_readme.py`: outputs `README.md` with content from `docs/index.md`
- `build_wiki.py`: outputs `wiki/` directory with all scenario docs, app docs, sidebar, and home page

Test: run a dry-run wiki sync to confirm the wiki content matches the new docs structure.

---

### Task 8: Update CI workflows

**Files:**
- Modify: `.github/workflows/docs-deploy.yml` — Add trigger for `scripts/render_readme.py` path
- Modify: `.github/workflows/wiki.yml` — Ensure it uses updated `build_wiki.py` and pulls from `docs/`

- [ ] **Step 1: Update `docs-deploy.yml` trigger paths**

Add `scripts/render_readme.py` to the `paths` trigger, and run the render script before deploying:

```yaml
# Add to paths:
- "scripts/render_readme.py"

# Add step (after mkdocs build, before deploy):
- run: uv run --group dev python scripts/render_readme.py
```

- [ ] **Step 2: Verify `wiki.yml` uses correct build_wiki.py**

Ensure the wiki sync workflow calls the updated `scripts/build_wiki.py` (which now pulls from `docs/` for scenarios and spark apps). No changes to the wiki workflow itself are needed unless the script interface changed.

- [ ] **Step 3: Verify all workflows parse**

```bash
# Check YAML syntax
python3 -f .github/workflows/docs-deploy.yml
python3 -f .github/workflows/wiki.yml
```

Expected: both parse without errors.

---

### Task 9: Write redirect stubs for scenario READMEs (Task 1 was planned but now deferred to after doc creation)

**Already done in Task 1 (Step 2).** See code above for the redirect stub writer that creates all 19 `scenarios/*/README.md` files as thin stubs pointing to `.io` docs site.

---

### Task 10: Final verification

**Files:**
- Run MkDocs build on the final docs
- Verify wiki build
- Verify README render
- Verify cross-links resolve

- [ ] **Step 1: Full MkDocs build with --strict**

```bash
uv run --group dev mkdocs build --strict
```

Expected: successful build. No warnings or errors.

- [ ] **Step 2: Verify wiki build**

```bash
uv run --group dev python scripts/build_wiki.py
ls wiki/ | wc -l
```

Expected: `wiki/` contains: `Home.md`, `_Sidebar.md`, 19 `Scenario-*.md`, 2 `App-*.md`, guide pages (lakehouse.md, datasets.md, etc.)

- [ ] **Step 3: Verify README render**

```bash
uv run --group dev python scripts/render_readme.py
head -5 README.md
```

Expected: output shows title, overview, and quick-start content from `docs/index.md`.

- [ ] **Step 4: Verify all scenario docs have architecture diagram references**

```bash
for f in docs/scenarios/*.md; do
  [ "$f" = "docs/scenarios/index.md" ] && continue
  if ! grep -q "architectures/" "$f"; then
    echo "MISSING diagram ref: $f"
  fi
done
```

Expected: zero "MISSING" lines.

- [ ] **Step 5: Verify cross-links in all docs**

```bash
# Check that all markdown links within docs/ resolve
uv run --group dev mkdocs build --strict 2>&1 | grep -i "warning\|error" || echo "No broken links"
```

Expected: no broken link warnings.

- [ ] **Step 6: Verify scenario README stubs**

```bash
for f in scenarios/*/README.md; do
  lines=$(wc -l < "$f")
  if [ "$lines" -gt 10 ]; then
    echo "TOO LONG: $f (expected redirect stub, ~7 lines)"
  fi
done
```

Expected: zero "TOO LONG" lines.

---

### Task 11: Commit and push all changes

**Files:**
- All files modified/created across Tasks 1-10

- [ ] **Step 1: Stage all changes**

```bash
git add \
  docs/scenarios/*.md \
  docs/spark-apps/*.md \
  docs/index.md \
  docs/lakehouse.md \
  docs/datasets.md \
  scripts/build_wiki.py \
  scripts/render_readme.py \
  scenarios/*/README.md \
  mkdocs.yml \
  .github/workflows/docs-deploy.yml \
  .github/workflows/wiki.yml
```

- [ ] **Step 2: Commit**

```bash
git commit -m "docs: overhaul documentation — 19 scenario docs, 2 app docs, redirect stubs, build scripts

- Create 19 scenario docs in docs/scenarios/ with hierarchical numbering,
  architecture diagrams, cross-links, and notebook walkthroughs
- Enhance 2 spark-app docs with architecture diagrams and See Also cross-links
- Update build_wiki.py to pull from docs/ instead of READMEs
- Add render_readme.py to auto-generate root README.md from docs/index.md
- Replace all 19 scenario READMEs with thin redirect stubs
- Update CI workflows to include render_readme.py in trigger paths"
```

- [ ] **Step 3: Push to remote**

```bash
git push origin <branch-name>
```

---

### Task 12: Post-deploy verification

**Files:**
- None (manual verification)

- [ ] **Step 1: Confirm `.io` site is live**

Visit `https://thekaveh.github.io/data-eng-lab/` and verify:
- All 19 scenario docs render
- Architecture diagrams load in each scenario page
- Cross-links resolve
- Nav structure is correct

- [ ] **Step 2: Confirm wiki is synced**

Visit the GitHub Wiki and verify:
- Home page lists all scenarios and apps
- Sidebar shows correct hierarchy
- Scenario pages render correctly

- [ ] **Step 3: Confirm README.md is current**

Visit the repo main page and verify:
- README.md shows updated overview, quick-start, scenario catalog
- No stale content from old version
