# Full Documentation Overhaul Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Completely overhaul the README, MkDocs site, GitHub Wiki, and every scenario/spark-app docs into a unified, professionally structured, hierarchically numbered documentation system with auto-generated architecture diagrams, cross-linked content, and consistent catalog-style landing pages.

**Architecture:** A single authoritative source of truth (`docs/` + scenario READMEs) drives both the MkDocs-rendered site and the GitHub Wiki via updated build scripts. No manual duplication. All diagrams are generated as standalone HTML/SVG using the architecture-diagram skill and embedded into the relevant pages.

**Tech Stack:** MkDocs Material theme, custom CSS, Python (scripts/gen_doc_pages.py, scripts/build_wiki.py), Markdown, SVG architecture diagrams.

---

## File Map

### New files to create:
- `docs/css/custom.css` — Custom theme overrides (color palette, typography, callout styling)
- `docs/architecture.html` — Full-stack architecture diagram (generated via architecture-diagram skill)
- `docs/scenarios/architectures/` — Directory for per-scenario architecture diagrams (HTML/SVG)
- `docs/spark-apps/architectures/` — Directory for per-app architecture diagrams (HTML/SVG)
- `docs/scenarios/index.md` — Scenario catalog landing page (table of all 19 scenarios)
- `docs/scenarios/batch_ingest-nyc_taxi-spark-iceberg.md`
- `docs/scenarios/bi_query-tpch-trino-iceberg.md`
- `docs/scenarios/cdc_streaming-online_retail-spark-iceberg.md`
- `docs/scenarios/data_quality-nyc_taxi-spark-iceberg.md`
- `docs/scenarios/feature_engineering-movielens-spark-iceberg.md`
- `docs/scenarios/federated_query-nyc_taxi-trino-iceberg.md`
- `docs/scenarios/incremental_upsert-online_retail-spark-iceberg.md`
- `docs/scenarios/join_optimization-tpch-spark-iceberg.md`
- `docs/scenarios/json_flatten-gh_archive-spark-iceberg.md`
- `docs/scenarios/medallion-nyc_taxi-spark-iceberg.md`
- `docs/scenarios/scd2-online_retail-spark-iceberg.md`
- `docs/scenarios/schema_evolution-gh_archive-spark-iceberg.md`
- `docs/scenarios/sessionization-gh_archive-spark-iceberg.md`
- `docs/scenarios/star_schema-tpch-spark-iceberg.md`
- `docs/scenarios/streaming_ingest-events-spark-iceberg.md`
- `docs/scenarios/streaming_ingest-gh_archive-spark-iceberg.md`
- `docs/scenarios/streaming_windows-events-spark-iceberg.md`
- `docs/scenarios/table_maintenance-nyc_taxi-spark-iceberg.md`
- `docs/scenarios/time_travel-nyc_taxi-spark-iceberg.md`
- `docs/spark-apps/index.md` — Spark apps catalog landing page
- `docs/spark-apps/nyc-taxi-etl.md`
- `docs/spark-apps/nyc-taxi-medallion.md`

### Files to modify:
- `mkdocs.yml` — Complete nav rewrite, theme palette, custom CSS inclusion
- `docs/index.md` — New home page with architecture diagram, catalog tables, quick-start
- `docs/scenarios.md` — Rewrite as overview (not per-scenario detail; that moves to per-scenario docs)
- `docs/spark-apps.md` — Rewrite as overview + fix "medallion medallion" typo
- `docs/lakehouse.md` — Expand with architecture diagram, medallion detail, namespace/config detail
- `docs/datasets.md` — Fix: add online_retail, rename "Events" → "GitHub Archive"
- `docs/getting-started.md` — Minor formatting improvements, cross-link updates
- `docs/atlas-expectations.md` — Fix section numbering (skip 3), update intro if needed
- `docs/atlas-enablement.md` — Update superseded status, cross-references
- `docs/go-live.md` — Fix link consistency, minor formatting
- `docs/go-live-results.md` — Cross-link updates
- `docs/atlas-feedback-a7a9.md` — Cross-link updates
- `docs/atlas-feedback-go-live.md` — Cross-link updates
- `scripts/gen_doc_pages.py` — Rewrite: stop auto-copying READMEs; generate nav only per-scenario/spark-app pages that link to the new rich docs
- `scripts/build_wiki.py` — Rewrite: auto-discover pages from mkdocs.yml nav, include all pages (including atlas-feedback-go-live), include scenario and spark-app pages
- `README.md` — Rewrite: professional tone, proper link formatting, fix outdated A7/A9 status, add badge-style summary, link to .io docs site, missing sections (contributing, who-is-this-for)
- `.github/workflows/ci.yml` — Update docs-trigger paths if needed (verify)

---

### Task 1: Create the custom CSS file for the MkDocs Material theme

**Files:**
- Create: `docs/css/custom.css`

- [ ] **Step 1: Create `docs/css/custom.css`**

Create the file with the following content. This defines a dark-default theme with deep blue primary and amber accent colors, professional callout styling, and improved table/card typography.

```css
/* Custom theme overrides — data-eng-lab MkDocs Material */

/* ── Color palette ─────────────────────────────────────────────────── */
/* Dark (default): deep-purple primary, amber accent */
/* Light (toggle): deep-purple primary, amber accent */

:root {
  --md-accent-fg-color: #f59e0b;
  --md-accent-fg-color--transparent: #f59e0b22;
  --md-primary-fg-color: #4338ca;
  --md-primary-fg-color--light: #6366f1;
  --md-primary-fg-color--dark: #3730a3;
  --md-default-bg-color: #0f172a;
}

[data-md-color-scheme="default"] {
  --md-default-bg-color: #f8fafc;
  --md-default-fg-color: #1e293b;
  --md-accent-fg-color: #d97706;
  --md-accent-fg-color--transparent: #d9770622;
  --md-primary-fg-color: #4338ca;
  --md-primary-fg-color--light: #6366f1;
  --md-primary-fg-color--dark: #3730a3;
}

/* ── Callouts / Admonitions ────────────────────────────────────────── */
.md-admonition {
  border-left: 4px solid var(--md-primary-fg-color) !important;
}
.md-admonition.note .md-admonition-icon {
  background: var(--md-primary-fg-color);
}
.md-admonition.tip .md-admonition-icon {
  background: #10b981;
}
.md-admonition.warning .md-admonition-icon {
  background: #f59e0b;
}
.md-admonition.warning {
  border-left-color: #f59e0b !important;
}
.md-admonition.danger .md-admonition-icon {
  background: #ef4444;
}
.md-admonition.danger {
  border-left-color: #ef4444 !important;
}

/* ── Tables ────────────────────────────────────────────────────────── */
.md-typeset table {
  border-collapse: collapse;
  border-radius: 8px;
  overflow: hidden;
}
.md-typeset th {
  background-color: var(--md-primary-fg-color) !important;
  color: var(--md-default-bg-color) !important;
  font-weight: 600;
  letter-spacing: 0.025em;
  text-transform: none;
  padding: 12px 16px;
}
.md-typeset td {
  padding: 10px 16px;
  vertical-align: top;
}
.md-typeset table:not(.highlighttable) {
  box-shadow: 0 1px 3px #00000022;
}

[data-md-color-scheme="default"] .md-typeset table:not(.highlighttable) {
  box-shadow: 0 1px 3px rgba(0,0,0,0.12);
}

/* ── Cards / Grids ─────────────────────────────────────────────────── */
.md-typeset .grid {
  gap: 1rem;
}
.md-typeset .grid > .card {
  border-radius: 8px;
  border: 1px solid var(--md-code-border-color, #2d3748);
  transition: border-color 0.2s, box-shadow 0.2s;
  overflow: hidden;
}
.md-typeset .grid > .card:hover {
  border-color: var(--md-primary-fg-color--light);
  box-shadow: 0 2px 8px #00000033;
}

/* ── Link styling ──────────────────────────────────────────────────── */
.md-typeset a {
  text-decoration: none;
}
.md-typeset a:hover {
  text-decoration: underline;
}

/* ── Hero section on home page ─────────────────────────────────────── */
.hero-section h1 {
  font-size: 2.5rem !important;
  margin-bottom: 0.5rem !important;
}
.hero-section p.lead {
  font-size: 1.2rem !important;
  color: var(--md-default-fg-color--light);
}

/* ── Table of contents ─────────────────────────────────────────────── */
.md-nav__link {
  transition: color 0.15s;
}
.md-nav__link:hover {
  color: var(--md-primary-fg-color--light);
}

/* ── Code blocks ───────────────────────────────────────────────────── */
.md-typeset code {
  border-radius: 4px;
}
.md-typeset pre {
  border-radius: 8px;
}
```

- [ ] **Step 2: Verify the CSS file**

```bash
test -f docs/css/custom.css && wc -l docs/css/custom.css
```
Expected: output showing a file with ~100+ lines.

---

### Task 2: Rewrite `mkdocs.yml` with hierarchical nav and theme config

**Files:**
- Modify: `mkdocs.yml`

- [ ] **Step 1: Replace `mkdocs.yml` content**

Write the following content:

```yaml
site_name: data-eng-lab
site_url: https://thekaveh.github.io/data-eng-lab/
site_description: >-
  An Apache Iceberg lakehouse data engineering lab —
  19 Spark scenarios, 2 CI-built Maven apps, Trino BI, Redpanda streaming,
  medallion architecture on Docker Compose.
repo_url: https://github.com/thekaveh/data-eng-lab
repo_name: thekaveh/data-eng-lab
docs_dir: docs
edit_uri: blob/main/docs

theme:
  name: material
  custom_dir: docs/overrides
  palette:
    - scheme: default
      primary: indigo
      accent: amber
      toggle:
        icon: material/weather-sunny
        name: Switch to dark mode
    - scheme: slate
      primary: indigo
      accent: amber
      toggle:
        icon: material/weather-night
        name: Switch to light mode
  font:
    text: Inter
    code: JetBrains Mono
  features:
    - navigation.tabs
    - navigation.sections
    - navigation.top
    - navigation.tracking
    - navigation.instant
    - toc.follow
    - search.suggest
    - search.highlight
    - content.code.copy
    - content.code.annotate
    - content.tabs.link
  icon:
    repo: fontawesome/brands/github

markdown_extensions:
  - admonition
  - attr_list
  - md_in_html
  - tables
  - toc:
      permalink: true
  - pymdownx.highlight:
      anchor_linenums: true
  - pymdownx.inlinehilite
  - pymdownx.superfences
  - pymdownx.details
  - pymdownx.tabbed:
      alternate_style: true
  - pymdownx.emoji:
      emoji_index: !!python/name:material.extensions.emoji.twemoji
      emoji_generator: !!python/name:material.extensions.emoji.to_svg
  - pymdownx.critic
  - pymdownx.caret
  - pymdownx.keys
  - pymdownx.mark
  - pymdownx.tilde

plugins:
  - search
  - gen-files:
      scripts:
        - scripts/gen_doc_pages.py
  - literate-nav:
      nav_file: SUMMARY.md

extra_css:
  - css/custom.css

nav:
  - Home: index.md
  - Getting Started: getting-started.md

  - Scenarios:
    - Overview: scenarios/index.md
    - batch_ingest-nyc_taxi-spark-iceberg: scenarios/batch_ingest-nyc_taxi-spark-iceberg.md
    - bi_query-tpch-trino-iceberg: scenarios/bi_query-tpch-trino-iceberg.md
    - cdc_streaming-online_retail-spark-iceberg: scenarios/cdc_streaming-online_retail-spark-iceberg.md
    - data_quality-nyc_taxi-spark-iceberg: scenarios/data_quality-nyc_taxi-spark-iceberg.md
    - feature_engineering-movielens-spark-iceberg: scenarios/feature_engineering-movielens-spark-iceberg.md
    - federated_query-nyc_taxi-trino-iceberg: scenarios/federated_query-nyc_taxi-trino-iceberg.md
    - incremental_upsert-online_retail-spark-iceberg: scenarios/incremental_upsert-online_retail-spark-iceberg.md
    - join_optimization-tpch-spark-iceberg: scenarios/join_optimization-tpch-spark-iceberg.md
    - json_flatten-gh_archive-spark-iceberg: scenarios/json_flatten-gh_archive-spark-iceberg.md
    - medallion-nyc_taxi-spark-iceberg: scenarios/medallion-nyc_taxi-spark-iceberg.md
    - scd2-online_retail-spark-iceberg: scenarios/scd2-online_retail-spark-iceberg.md
    - schema_evolution-gh_archive-spark-iceberg: scenarios/schema_evolution-gh_archive-spark-iceberg.md
    - sessionization-gh_archive-spark-iceberg: scenarios/sessionization-gh_archive-spark-iceberg.md
    - star_schema-tpch-spark-iceberg: scenarios/star_schema-tpch-spark-iceberg.md
    - streaming_ingest-events-spark-iceberg: scenarios/streaming_ingest-events-spark-iceberg.md
    - streaming_ingest-gh_archive-spark-iceberg: scenarios/streaming_ingest-gh_archive-spark-iceberg.md
    - streaming_windows-events-spark-iceberg: scenarios/streaming_windows-events-spark-iceberg.md
    - table_maintenance-nyc_taxi-spark-iceberg: scenarios/table_maintenance-nyc_taxi-spark-iceberg.md
    - time_travel-nyc_taxi-spark-iceberg: scenarios/time_travel-nyc_taxi-spark-iceberg.md

  - Spark Apps:
    - Overview: spark-apps/index.md
    - nyc-taxi-etl: spark-apps/nyc-taxi-etl.md
    - nyc-taxi-medallion: spark-apps/nyc-taxi-medallion.md

  - Lakehouse & Atlas:
    - Lakehouse: lakehouse.md
    - Atlas Expectations: atlas-expectations.md
    - Atlas Go-Live Runbook: go-live.md
    - Atlas Go-Live Results: go-live-results.md
    - Atlas Feedback — A7/A9: atlas-feedback-a7a9.md
    - Atlas Go-Live Findings: atlas-feedback-go-live.md
    - Atlas Enablement: atlas-enablement.md

  - Datasets: datasets.md
  - Changelog: CHANGELOG.md
```

- [ ] **Step 2: Validate mkdocs.yml structure**

```bash
python3 -c "import yaml; yaml.safe_load(open('mkdocs.yml')); print('mkdocs.yml is valid YAML')"
```
Expected: `mkdocs.yml is valid YAML` printed to stdout.

---

### Task 3: Generate the full-stack architecture diagram

**Files:**
- Create: `docs/architecture.html` (via architecture-diagram skill)

- [ ] **Step 1: Invoke the architecture-diagram skill**

Load the `architecture-diagram` skill and generate the full-stack architecture diagram as an HTML/SVG file. The diagram should show:

- **Infrastructure layer:** Docker Compose containers (Spark master/workers/connect/history, Zeppelin, JupyterHub, Airflow ×4, Jenkins, MinIO, Iceberg REST, Trino, Redpanda, Supabase stack)
- **Storage layer:** MinIO buckets (landing, lakehouse, jars, checkpoints)
- **Catalog:** Iceberg REST catalog (Postgres-backed, namespace `lakehouse`)
- **Processing layer:** Spark Connect, standalone Spark, PySpark/Jupyter, Scala/Zeppelin
- **Query engines:** Spark SQL, Trino SQL
- **Streaming layer:** Redpanda/Kafka, Spark Structured Streaming
- **Orchestration:** Airflow DAGs, Jenkins CI
- **Medallion flow:** landing → bronze → silver → gold (left to right)

Write the output to `docs/architecture.html`.

- [ ] **Step 2: Verify the file**

```bash
test -f docs/architecture.html && wc -l docs/architecture.html
```
Expected: file exists with content (SVG-based HTML, typically 200+ lines).

---

### Task 4: Create scenario architecture diagram directory

**Files:**
- Create: `docs/scenarios/architectures/` (empty directory)
- Create: `docs/spark-apps/architectures/` (empty directory)

- [ ] **Step 1: Create directories**

```bash
mkdir -p docs/scenarios/architectures docs/spark-apps/architectures
```

- [ ] **Step 2: Verify**

```bash
ls -d docs/scenarios/architectures docs/spark-apps/architectures
```

---

### Task 5: Rewrite scenario READMEs — standardize all 19 scenario READMEs

**Files:**
- Modify: `scenarios/batch_ingest-nyc_taxi-spark-iceberg/README.md`
- Modify: `scenarios/bi_query-tpch-trino-iceberg/README.md`
- Modify: `scenarios/cdc_streaming-online_retail-spark-iceberg/README.md`
- Modify: `scenarios/data_quality-nyc_taxi-spark-iceberg/README.md`
- Modify: `scenarios/feature_engineering-movielens-spark-iceberg/README.md`
- Modify: `scenarios/federated_query-nyc_taxi-trino-iceberg/README.md`
- Modify: `scenarios/incremental_upsert-online_retail-spark-iceberg/README.md`
- Modify: `scenarios/join_optimization-tpch-spark-iceberg/README.md`
- Modify: `scenarios/json_flatten-gh_archive-spark-iceberg/README.md`
- Modify: `scenarios/medallion-nyc_taxi-spark-iceberg/README.md`
- Modify: `scenarios/scd2-online_retail-spark-iceberg/README.md`
- Modify: `scenarios/schema_evolution-gh_archive-spark-iceberg/README.md`
- Modify: `scenarios/sessionization-gh_archive/spark-iceberg/README.md`
- Modify: `scenarios/star_schema-tpch-spark-iceberg/README.md`
- Modify: `scenarios/streaming_ingest-events-spark-iceberg/README.md`
- Modify: `scenarios/streaming_ingest-gh_archive-spark-iceberg/README.md`
- Modify: `scenarios/streaming_windows-events-spark-iceberg/README.md`
- Modify: `scenarios/table_maintenance-nyc_taxi-spark-iceberg/README.md`
- Modify: `scenarios/time_travel-nyc_taxi-spark-iceberg/README.md`

- [ ] **Step 1: Standardize template for all scenario READMEs**

All 19 scenario READMEs must follow this exact structure with consistent prose. Use this template, filling in each section with the actual data from my earlier audit:

```markdown
# <scenario-name>

<One-sentence summary: what this scenario demonstrates and at a high level what data flows where.>

## 1. Overview

<Two to three sentences detailing the scenario's purpose and what it demonstrates.>

## 2. Why This Exists

<One to two sentences on why this scenario is important in the context of data engineering education and the lakehouse platform.>

## 3. Architecture

The data flows as follows:

<Snake diagram showing the data flow source → processing → sink. Use plain text art.>

Key components:
- **Source:** <dataset or service>
- **Processing:** <Spark mode: batch, streaming, etc.>
- **Sink:** <Iceberg table path>
- **Orchestration:** <Airflow DAG: yes/no, or long-running streaming>

## 4. Data Schema

### Input
| Column | Type | Source |
|---|---|---|
| <col> | <type> | <source> |

### Output
| Table | Layer | Columns |
|---|---|---|
| <table> | <bronze/silver/gold> | <key columns> |

## 5. Notebooks

- **Zeppelin (Scala):** `zeppelin/notebook.zpln` — <1-sentence description of what the Scala notebook does>
- **Jupyter (PySpark):** `jupyter/notebook.ipynb` — <1-sentence description of what the PySpark notebook does>
- Both languages implement <identical/similar> <batch/streaming> logic with <section count> sections: <section list>.

## 6. How to Run

<Step-by-step instructions for running the scenario locally. Include required `make` commands, environment setup, and verification commands.>

## 7. Dependencies

- **Dataset:** <dataset name and path>
- **Atlas services:** <A1-A9 items required>
- **Other:** <producer.py, kafka-python, etc.>

## 8. Known Issues & Caveats

<All caveats from the existing README, properly formatted as a markdown list. Include gating requirements, known limitations, and runtime notes.>

## 9. See Also

- <Cross-link to related scenario(s): upstream data source or downstream consumer scenarios>
- [Datasets](../datasets.md)
- [Lakehouse](../lakehouse.md)
```

- [ ] **Step 2: Write each of the 19 scenario READMEs**

Use the template above. For each scenario, fill in the actual data from the audit results. Here are the key details per scenario:

**1. `batch_ingest-nyc_taxi-spark-iceberg`**
- Overview: Batch ingestion of raw NYC-taxi Parquet into Iceberg bronze layer with basic cleaning (drop null pickups, non-positive passenger counts, add `trip_date`).
- Architecture: `s3a://landing/nyc_taxi/` (Parquet) → PySpark/Scala clean transform → `lakehouse.bronze.nyc_taxi_trips`
- Source: NYC Taxi Parquet
- Sink: `lakehouse.bronze.nyc_taxi_trips` (partitioned)
- DAG: Yes (`batch_ingest_nyc_taxi`)
- Mode: Batch
- Layers: Bronze
- Cross-link: downstream → `medallion-nyc_taxi-spark-iceberg`, `data_quality-nyc_taxi-spark-iceberg`, `federated_query-nyc_taxi-trino-iceberg`, `table_maintenance-nyc_taxi-spark-iceberg`, `time_travel-nyc_taxi-spark-iceberg`

**2. `bi_query-tpch-trino-iceberg`**
- Overview: Multi-engine BI query: reads gold-layer marts via Trino SQL, joins `fct_orders` with `dim_customer`, aggregates revenue by market segment.
- Architecture: `lakehouse.gold.fct_orders` + `lakehouse.gold.dim_customer` → Trino SQL JOIN + aggregation → `lakehouse.gold.bi_segment_revenue`
- Source: TPC-H gold tables (produced by `star_schema-tpch-spark-iceberg`)
- Sink: `lakehouse.gold.bi_segment_revenue`
- DAG: Placeholder (EmptyOperator)
- Mode: Batch
- Layers: Gold
- Cross-link: upstream → `star_schema-tpch-spark-iceberg`; related: `join_optimization-tpch-spark-iceberg`

**3. `cdc_streaming-online_retail-spark-iceberg`**
- Overview: Streaming CDC upserts from Redpanda `online_retail_cdc` topic. Consumes JSON events via Spark Structured Streaming, applies `foreachBatch` + `MERGE INTO` for idempotent upserts.
- Architecture: Redpanda `online_retail_cdc` → readStream + from_json → foreachBatch + MERGE INTO → `lakehouse.silver.online_retail_cdc`
- Source: Redpanda Kafka topic `online_retail_cdc` (JSON)
- Sink: `lakehouse.silver.online_retail_cdc`
- DAG: No (EmptyOperator placeholder; streaming is long-running)
- Mode: Streaming
- Layers: Silver
- Cross-link: related: `incremental_upsert-online_retail-spark-iceberg`, `scd2-online_retail-spark-iceberg`

**4. `data_quality-nyc_taxi-spark-iceberg`**
- Overview: Data quality validation that splits NYC-taxi records into clean and quarantine tables based on business rules (fare > 0 AND passenger_count BETWEEN 1 AND 6; NULLs quarantined).
- Architecture: `lakehouse.bronze.nyc_taxi_trips` → validation rules branch → `nyc_taxi_clean` + `nyc_taxi_quarantine`
- Source: `lakehouse.bronze.nyc_taxi_trips`
- Sink: `lakehouse.silver.nyc_taxi_clean`, `lakehouse.silver.nyc_taxi_quarantine`
- DAG: Yes (`data_quality_nyc_taxi`)
- Mode: Batch
- Layers: Bronze → Silver
- Cross-link: upstream → `batch_ingest-nyc_taxi-spark-iceberg`

**5. `feature_engineering-movielens-spark-iceberg`**
- Overview: ML feature engineering: aggregates MovieLens ratings into user-level (avg_rating, num_ratings) and movie-level (movie_avg, popularity) feature marts for downstream recommendation models.
- Architecture: `s3a://landing/movielens/ratings.csv` → groupBy userId + movieId → `ml_user_features` + `ml_movie_features`
- Source: MovieLens ratings CSV
- Sink: `lakehouse.gold.ml_user_features`, `lakehouse.gold.ml_movie_features`
- DAG: Yes (`feature_engineering_movielens`)
- Mode: Batch
- Layers: Gold
- Note: Reads directly from S3 landing (no medallion intermediate).

**6. `federated_query-nyc_taxi-trino-iceberg`**
- Overview: Federated query: Trino reads Iceberg tables directly, aggregates NYC-taxi trips into a daily summary, and writes back to gold layer — all via standard ANSI SQL, no Spark required.
- Architecture: `lakehouse.bronze.nyc_taxi_trips` → Trino SQL aggregation → `lakehouse.gold.nyc_taxi_daily_trino`
- Source: `lakehouse.bronze.nyc_taxi_trips`
- Sink: `lakehouse.gold.nyc_taxi_daily_trino`
- DAG: Placeholder (EmptyOperator)
- Mode: Batch
- Layers: Bronze → Gold
- Cross-link: upstream → `batch_ingest-nyc_taxi-spark-iceberg`

**7. `incremental_upsert-online_retail-spark-iceberg`**
- Overview: Incremental upserts via MERGE INTO on an Iceberg silver table. Demonstrates idempotent change-set application: the same batch can be merged multiple times without duplication.
- Architecture: `lakehouse.silver.online_retail` → MERGE INTO (upsert on key) → same table
- Source: Online retail batch deltas
- Sink: `lakehouse.silver.online_retail` (in-place)
- DAG: Yes (`incremental_upsert_online_retail`)
- Mode: Batch
- Layers: Silver
- Cross-link: related: `cdc_streaming-online_retail-spark-iceberg`, `scd2-online_retail-spark-iceberg`

**8. `join_optimization-tpch-spark-iceberg`**
- Overview: Join optimization: demonstrates Spark broadcast joins and Adaptive Query Execution (AQE) by joining TPC-H `orders` with `customer`, aggregating revenue by market segment. Shows physical plan optimization output (BroadcastHashJoin).
- Architecture: `s3a://landing/tpch/orders` + `s3a://landing/tpch/customer` → broadcast join on o_custkey = c_custkey → groupBy market segment → `tpch_segment_revenue`
- Source: TPC-H Parquet (orders, customer)
- Sink: `lakehouse.gold.tpch_segment_revenue`
- DAG: Yes (`join_optimization_tpch`)
- Mode: Batch
- Layers: Gold
- Cross-link: upstream: data from S3 landing; related: `bi_query-tpch-trino-iceberg`, `star_schema-tpch-spark-iceberg`

**9. `json_flatten-gh_archive-spark-iceberg`**
- Overview: JSON flatten transform: reads GitHub Archive nested JSON events, extracts nested fields (actor.login, repo.name), casts timestamps, writes to a flat Iceberg table.
- Architecture: `s3a://landing/gh_archive/*.json.gz` → extract nested fields + cast created_at → `lakehouse.silver.gh_events`
- Source: GitHub Archive compressed JSON
- Sink: `lakehouse.silver.gh_events`
- DAG: Yes (`json_flatten_gh_archive`)
- Mode: Batch
- Layers: Silver
- Cross-link: related: `schema_evolution-gh_archive-spark-iceberg`, `sessionization-gh_archive-spark-iceberg`, `streaming_ingest-gh_archive-spark-iceberg`

**10. `medallion-nyc_taxi-spark-iceberg`**
- Overview: Full medallion pipeline: deduplicates bronze data in silver (on pickup/dropoff datetime) then creates daily aggregate of trip count and avg fare in gold. The full three-tier lakehouse flow.
- Architecture: `lakehouse.bronze.nyc_taxi_trips` → dropDuplicates → `lakehouse.silver.nyc_taxi_trips` → groupBy trip_date → `lakehouse.gold.nyc_taxi_daily`
- Source: `lakehouse.bronze.nyc_taxi_trips`
- Sink: `lakehouse.silver.nyc_taxi_trips`, `lakehouse.gold.nyc_taxi_daily`
- DAG: Yes (`medallion_nyc_taxi`)
- Mode: Batch
- Layers: Bronze, Silver, Gold (full medallion)
- Cross-link: upstream → `batch_ingest-nyc_taxi-spark-iceberg`; productionized as `nyc-taxi-medallion` spark app

**11. `scd2-online_retail-spark-iceberg`**
- Overview: Slowly Changing Dimension Type 2: tracks historical changes on a customer dimension. When a customer's segment changes, the old row is closed (effective_to, is_current=false) and a new row is opened.
- Architecture: `lakehouse.gold.dim_customer_scd2` → row-level UPDATE + INSERT (SCD2 logic) → same table
- Source: Online retail dimension data
- Sink: `lakehouse.gold.dim_customer_scd2` (in-place)
- DAG: Yes (`scd2_online_retail`)
- Mode: Batch
- Layers: Gold
- Cross-link: related: `incremental_upsert-online_retail-spark-iceberg`, `cdc_streaming-online_retail-spark-iceberg`

**12. `schema_evolution-gh_archive-spark-iceberg`**
- Overview: Iceberg schema evolution: demonstrates adding a new column (existing rows show NULL) and renaming an existing column (type → event_type) without rewriting existing data.
- Architecture: New table `lakehouse.silver.gh_events_evolved` (id, type, actor_login) → add repo_name → rename type → event_type → insert new row → verify
- Source: None (table created within notebook)
- Sink: `lakehouse.silver.gh_events_evolved`
- DAG: Yes (`schema_evolution_gh_archive`)
- Mode: Batch
- Layers: Silver
- Cross-link: related: `json_flatten-gh_archive-spark-iceberg`, `sessionization-gh_archive-spark-iceberg`

**13. `sessionization-gh_archive-spark-iceberg`**
- Overview: User session detection using window functions and gap-based sessionization (30-minute inactivity threshold). Partitions events by actor_login, detects gaps using lag window function, assigns session IDs.
- Architecture: GitHub Archive events → window(lag over actor_login order by ts) → gap > 30s → session ID → `gh_sessions`
- Source: GitHub Archive events
- Sink: `lakehouse.silver.gh_sessions`
- DAG: Yes (`sessionization_gh_archive`)
- Mode: Batch
- Layers: Silver
- Cross-link: upstream → GitHub Archive; related: `json_flatten-gh_archive-spark-iceberg`

**14. `star_schema-tpch-spark-iceberg`**
- Overview: Star schema dimensional modeling: builds fact and dimension tables from the TPC-H dataset. Joins orders with lineitem, aggregates by order/customer/date to create `dim_customer` and `fct_orders`.
- Architecture: `s3a://landing/tpch/{orders, lineitem}` → join on order key → `lakehouse.gold.dim_customer` + `lakehouse.gold.fct_orders`
- Source: TPC-H Parquet (orders, customer, lineitem)
- Sink: `lakehouse.gold.dim_customer`, `lakehouse.gold.fct_orders`
- DAG: Yes (`star_schema_tpch`)
- Mode: Batch
- Layers: Gold
- Cross-link: downstream consumers: `bi_query-tpch-trino-iceberg`, `join_optimization-tpch-spark-iceberg`

**15. `streaming_ingest-events-spark-iceberg`**
- Overview: Real-time lakehouse ingestion: consumes synthetic click events from Redpanda Kafka `events` topic via Spark Structured Streaming, writes to Iceberg bronze. The streaming counterpart to batch-ingest.
- Architecture: Redpanda `events` → readStream + from_json → writeStream → `lakehouse.bronze.events`
- Source: Redpanda `events` topic (JSON: user_id, event, ts)
- Sink: `lakehouse.bronze.events`
- DAG: No (EmptyOperator; streaming is long-running)
- Mode: Streaming
- Layers: Bronze
- Cross-link: downstream → `streaming_windows-events-spark-iceberg`; producer: `producer.py`
- Note: Requires `kafka-python`; auto-creates `events` topic.

**16. `streaming_ingest-gh_archive-spark-iceberg`**
- Overview: Structured Streaming via file source: reads JSON files from S3 landing zone incrementally (no Kafka dependency), parses with schema, casts timestamp, writes to Iceberg with checkpointing for exactly-once semantics.
- Architecture: `s3a://landing/gh_archive/` → readStream (file source) → cast created_at → writeStream → `lakehouse.bronze.gh_events_stream`
- Source: GitHub Archive JSON files (S3)
- Sink: `lakehouse.bronze.gh_events_stream`
- DAG: Yes (`streaming_ingest_gh_archive`)
- Mode: Streaming
- Layers: Bronze
- Cross-link: related: `json_flatten-gh_archive-spark-iceberg`, `sessionization-gh_archive-spark-iceberg`

**17. `streaming_windows-events-spark-iceberg`**
- Overview: Windowed aggregation with watermark on the Redpanda `events` topic: groups click events into 5-minute tumbling windows with a 10-minute watermark, counts events per type per window, emits only closed windows to Iceberg.
- Architecture: Redpanda `events` → readStream + withWatermark + groupBy(window) + count → writeStream (append) → `lakehouse.gold.event_windows`
- Source: Redpanda `events` topic
- Sink: `lakehouse.gold.event_windows`
- DAG: No (EmptyOperator; streaming is long-running)
- Mode: Streaming
- Layers: Gold
- Cross-link: upstream → `streaming_ingest-events-spark-iceberg`; producer: from streaming_ingest-events scenario

**18. `table_maintenance-nyc_taxi-spark-iceberg`**
- Overview: Iceberg table maintenance: compacts data files (rewrite_data_files), expires old snapshots retaining only the last one (expire_snapshots), and removes orphaned files (remove_orphan_files). Operates on a scenario-owned copy, never mutating the shared bronze table.
- Architecture: `lakehouse.bronze.nyc_taxi_trips` → seed copy → `lakehouse.silver.nyc_taxi_tm` → add second snapshot → rewrite_data_files + expire_snapshots + remove_orphan_files → verify
- Source: `lakehouse.bronze.nyc_taxi_trips`
- Sink: `lakehouse.silver.nyc_taxi_tm` (scenario-owned)
- DAG: Yes (`table_maintenance_nyc_taxi`)
- Mode: Batch
- Layers: Bronze, Silver
- Cross-link: upstream → `batch_ingest-nyc_taxi-spark-iceberg`

**19. `time_travel-nyc_taxi-spark-iceberg`**
- Overview: Iceberg time-travel: creates snapshots through inserts, queries historical versions with VERSION AS OF, explores write-audit-publish (WAP) branching, demonstrates rollback to earlier snapshots. Uses a dedicated sandbox table.
- Architecture: `lakehouse.bronze.nyc_taxi_trips` → seed into → `lakehouse.silver.nyc_taxi_tt` → insert more → create WAP branch → queries with VERSION AS OF → rollback capability
- Source: `lakehouse.bronze.nyc_taxi_trips`
- Sink: `lakehouse.silver.nyc_taxi_tt` (sandbox, dropped at end)
- DAG: Yes (`time_travel_nyc_taxi`)
- Mode: Batch
- Layers: Bronze, Silver
- Cross-link: upstream → `batch_ingest-nyc_taxi-spark-iceberg`

- [ ] **Step 3: Verify all scenario READMEs**

```bash
ls scenarios/*/README.md | wc -l
```
Expected: `19`. Then verify each has the required sections:
```bash
for f in scenarios/*/README.md; do
  echo "$f: $(grep -c '^## ' "$f") sections"
done
```
Expected: each should show at least 9 sections.

---

### Task 6: Write all 19 enriched scenario docs for MkDocs

**Files:**
- Create: `docs/scenarios/batch_ingest-nyc_taxi-spark-iceberg.md`
- Create: `docs/scenarios/bi_query-tpch-trino-iceberg.md`
- Create: `docs/scenarios/cdc_streaming-online_retail-spark-iceberg.md`
- Create: `docs/scenarios/data_quality-nyc_taxi-spark-iceberg.md`
- Create: `docs/scenarios/feature_engineering-movielens-spark-iceberg.md`
- Create: `docs/scenarios/federated_query-nyc_taxi-trino-iceberg.md`
- Create: `docs/scenarios/incremental_upsert-online_retail-spark-iceberg.md`
- Create: `docs/scenarios/join_optimization-tpch-spark-iceberg.md`
- Create: `docs/scenarios/json_flatten-gh_archive-spark-iceberg.md`
- Create: `docs/scenarios/medallion-nyc_taxi-spark-iceberg.md`
- Create: `docs/scenarios/scd2-online_retail-spark-iceberg.md`
- Create: `docs/scenarios/schema_evolution-gh_archive-spark-iceberg.md`
- Create: `docs/scenarios/sessionization-gh_archive-spark-iceberg.md`
- Create: `docs/scenarios/star_schema-tpch-spark-iceberg.md`
- Create: `docs/scenarios/streaming_ingest-events-spark-iceberg.md`
- Create: `docs/scenarios/streaming_ingest-gh_archive-spark-iceberg.md`
- Create: `docs/scenarios/streaming_windows-events-spark-iceberg.md`
- Create: `docs/scenarios/table_maintenance-nyc_taxi-spark-iceberg.md`
- Create: `docs/scenarios/time_travel-nyc_taxi-spark-iceberg.md`

- [ ] **Step 1: Write each scenario doc using the enriched template**

Each doc should follow this structure:

```markdown
# <Scenario Name>

<Two- to three-paragraph professional overview that explains what scenario does, what it demonstrates, and its significance in data engineering. No self-referencing to code/tables/diagrams.>

## 1. Data Model

### 1.1 Input

<Describe the source data: dataset name, location, format, schema overview.>

### 1.2 Output

<Describe the output: Iceberg table path(s), layer(s), key columns, partitioning.>

## 2. Processing Logic

<Detailed explanation of the transforms: what operations are performed, Spark code concepts used, key patterns (MERGE INTO, window functions, etc.). Explain the "how" and "why" not just what.>

## 3. Architecture

![Architecture](architectures/<scenario-name>.html)

<Explain the architecture diagram: data flow, components involved, service dependencies.>

## 4. Notebooks

### 4.1 Zeppelin (Scala)

<Description of the Scala notebook: sections, approach, key code patterns.>

### 4.2 Jupyter (PySpark)

<Description of the PySpark notebook: sections, approach, key code patterns.>

## 5. Orchestration

<Describe Airflow DAG details: DAG ID, schedule, tasks, dependencies. Or note if streaming/long-running.>

## 6. Prerequisites

<Required datasets, Atlas services (A1-A9), environment setup, tools.>

## 7. Usage

<Step-by-step commands to run. `make` targets, notebook access, verification commands.>

## 8. See Also

- [Related Scenario](./related-scenario.md) — brief reason
- [Dataset: <Name>](../datasets.md#dataset-name)
- [Lakehouse Architecture](../lakehouse.md)
```

For each scenario, generate an architecture diagram using the architecture-diagram skill and save it as `docs/scenarios/architectures/<scenario-name>.html`. For batch scenarios, show: Source → Processing → Medallion Layer(s) → Sink table. For streaming, show: Stream Source → readStream → Transform → writeStream → Sink with checkpoint.

- [ ] **Step 2: Verify**

```bash
ls docs/scenarios/*.md | grep -v index.md | wc -l
```
Expected: `19`.

---

### Task 7: Write scenario index page

**Files:**
- Create: `docs/scenarios/index.md`

- [ ] **Step 1: Write the scenario catalog/index page**

Create a professional overview page with:
- An overview paragraph describing the 19 scenarios as a unified set
- A table with columns: Scenario Name, Dataset, Engine (Batch/Streaming), Medallion Layers, Airflow DAG, Related Scenarios
- A section grouping scenarios by category:
  1. **Batch Ingestion** — batch_ingest
  2. **Medallion Pipelines** — medallion
  3. **Data Quality** — data_quality
  4. **Schema & Maintenance** — schema_evolution, time_travel, table_maintenance
  5. **Streaming** — streaming_ingest (events + gh_archive), streaming_windows, cdc_streaming
  6. **BI & Queries** — federated_query, bi_query
  7. **Join Optimization** — join_optimization
  8. **Dimensional Modeling** — star_schema
  9. **Feature Engineering** — feature_engineering
  10. **SCD** — scd2
  11. **JSON Processing** — json_flatten
  12. **Session Analysis** — sessionization
  13. **Incremental Updates** — incremental_upsert

Each category gets a brief description linking to the appropriate scenario docs.

- [ ] **Step 2: Verify**

```bash
wc -l docs/scenarios/index.md
```
Expected: at least 60 lines.

---

### Task 8: Write spark-app docs

**Files:**
- Create: `docs/spark-apps/index.md`
- Create: `docs/spark-apps/nyc-taxi-etl.md`
- Create: `docs/spark-apps/nyc-taxi-medallion.md`

- [ ] **Step 1: Write spark-app index page**

```markdown
# Spark Applications

<Overview: two paragraphs describing the Maven Scala Spark apps, their role in the CI/CD pipeline, and how they productionize the notebook prototypes.>

## Overview

| Application | Description | Source | Target | DAG |
|---|---|---|---|---|
| [nyc-taxi-etl](nyc-taxi-etl.md) | Raw Parquet → Bronze Iceberg | `s3a://landing/nyc_taxi/` | `lakehouse.bronze.nyc_taxi_trips` | nyc_taxi_etl |
| [nyc-taxi-medallion](nyc-taxi-medallion.md) | Bronze → Silver → Gold medallion | `lakehouse.bronze.nyc_taxi_trips` | `lakehouse.silver.*`, `lakehouse.gold.*` | nyc_taxi_medallion |

## CI/CD Pipeline

Both apps follow the same CI/CD pattern:
1. **CI:** Jenkins clones the repo → `mvn test` → `mvn package` → produces shaded JAR → publishes to `s3a://jars/<app>/<version>/app.jar`
2. **CD:** Airflow `spark-submit` operator submits the JAR to the Spark cluster (cluster deploy mode)
3. The JAR output is then consumed by downstream scenarios or as the final medallion output

## Prerequisites

- Atlas A5 (Jenkins) + A6 (Airflow spark-submit)
- `mvn` installed locally for testing
- S3A credentials on the Spark cluster
```

- [ ] **Step 2: Write each spark app doc**

For each app, use this template:

```markdown
# <App Name>

<Professional overview paragraph: what the app does, its scope, production significance.>

## 1. Architecture

![Architecture](architectures/<app-name>.html)

<Describe the CI/CD flow: Jenkins → MinIO JAR → Airflow → Spark cluster → Iceberg sink.>

## 2. Project Structure

- **Language:** Scala
- **Build tool:** Maven
- **Testing:** ScalaTest
- **Transform module:** `transforms/<Transforms>.scala`
- **Entrypoint:** `<ClassName>.scala` (argument-driven)
- **CI/CD:** `Jenkinsfile`, `dag.py`

## 3. Transform Logic

<Detailed description of what the transforms do: input schema → operations → output schema. Include Spark concepts used (DataFrames, DataSets, join strategies, etc.).>

## 4. Build & Test

```bash
# Unit tests
mvn -q -B -f spark-apps/<app>/pom.xml test

# Shaded JAR
mvn -q -B -f spark-apps/<app>/pom.xml package
```

## 5. Run with Airflow

The `dag.py` submits the JAR via SparkSubmitOperator in cluster mode. The DAG reads `spark-submit` configuration from Atlas A6 connection.

## 6. Prerequisites

- Atlas A5 (Jenkins CI) + A6 (Airflow spark-submit)
- JAR published to `s3a://jars/<app>/0.1.0/app.jar`
- S3A credentials on the Spark cluster

## 7. Data Flow

Source → <input path/tables> → Transform → <output path/tables>

## 8. See Also

- [spark-apps overview](index.md)
- [Lakehouse](../lakehouse.md)
- [Datasets](../datasets.md)
```

For each spark app, generate an architecture diagram showing: Jenkins → MinIO JAR → Airflow → spark-submit → Spark cluster → Iceberg sink. Use the architecture-diagram skill.

- [ ] **Step 3: Verify**

```bash
ls docs/spark-apps/*.md | wc -l
```
Expected: `3` (index + 2 apps).

---

### Task 9: Generate spark-app architecture diagrams

**Files:**
- Create: `docs/spark-apps/architectures/nyc-taxi-etl.html`
- Create: `docs/spark-apps/architectures/nyc-taxi-medallion.html`

- [ ] **Step 1: Invoke architecture-diagram skill for each app**

For `nyc-taxi-etl`: Diagram showing MinIO (landing Parquet) → Jenkins (mvn test/package) → MinIO (jars bucket) → Airflow (spark-submit) → Spark Cluster → Iceberg (`lakehouse.bronze.nyc_taxi_trips`).

For `nyc-taxi-medallion`: Diagram showing `lakehouse.bronze.nyc_taxi_trips` → Jenkins → MinIO JAR → Airflow → Spark Cluster (silver dedup → gold aggregation) → `lakehouse.silver.nyc_taxi_trips` + `lakehouse.gold.nyc_taxi_daily`.

---

### Task 10: Rewrite `docs/index.md` — new home page

**Files:**
- Modify: `docs/index.md`

- [ ] **Step 1: Write the new home page**

```markdown
# data-eng-lab

An Apache Iceberg lakehouse data engineering lab built on the [Atlas](https://github.com/thekaveh/atlas) platform. The repository contains 19 Spark scenarios (Scala/Zeppelin and PySpark/Jupyter with cross-language parity), 2 CI-built Maven applications (Jenkins + Airflow), Trino SQL BI queries, Redpanda streaming, and a full medallion (bronze → silver → gold) architecture — all orchestrated via Docker Compose.

![Architecture](architecture.html)

## By the Numbers

| Metric | Value |
|---|---|
| Spark scenarios | 19 (14 batch, 4 streaming, 1 hybrid) |
| Spark applications | 2 (Maven Scala, CI/CD automated) |
| Curated datasets | 5 (NYC Taxi, TPC-H, MovieLens, Online Retail, GitHub Archive) |
| Atlas enablement items | 9 (A1–A9, all delivered) |
| Medallion layers | 3 (bronze, silver, gold) |
| Query engines | 2 (Spark SQL, Trino SQL) |
| Streaming engine | 1 (Redpanda / Kafka API) |

## Quick Start

Get the full stack running in seconds:

```bash
git clone https://github.com/thekaveh/data-eng-lab.git
cd data-eng-lab
make setup        # initialize Atlas submodule
make datasets     # download datasets
make up           # launch all services
make preflight    # verify connectivity
```

See [Getting Started](getting-started.md) for the full guide with prerequisites, troubleshooting, and next steps.

## Explore

### Scenarios

| Category | Count | Scenarios |
|---|---|---|
| **Batch Ingestion** | 1 | Batch-load raw data into bronze |
| **Medallion Pipeline** | 1 | Full bronze → silver → gold |
| **Data Quality** | 1 | Clean vs. quarantine split |
| **Schema & Maintenance** | 3 | Schema evolution, time travel, table compaction |
| **Streaming** | 4 | File-source streaming, Kafka streaming, windowing, CDC |
| **BI & Queries** | 2 | Federated Trino queries, multi-engine BI |
| **Join Optimization** | 1 | Broadcast joins, AQE |
| **Dimensional Modeling** | 1 | Star schema (fact + dimension) |
| **Feature Engineering** | 1 | ML feature marts for recommendations |
| **SCD Type 2** | 1 | Historical dimension tracking |
| **JSON Processing** | 1 | Nested JSON flattening |
| **Session Analysis** | 1 | Gap-based sessionization with window functions |

See [Scenario Overview](scenarios/index.md) for the full catalog with architecture diagrams.

### Spark Applications

Two production-grade Maven Scala Spark applications, each with CI (Jenkins) and CD (Airflow):

- **[nyc-taxi-etl](spark-apps/nyc-taxi-etl.md)** — Raw Parquet → cleaned Bronze Iceberg
- **[nyc-taxi-medallion](spark-apps/nyc-taxi-medallion.md)** — Bronze → Silver → Gold medallion pipeline

### Datasets

Five curated datasets spanning columnar, semi-structured, and benchmark formats:

- [NYC Taxi](datasets.md#nyc-taxi) — Columnar analytical Parquet
- [TPC-H](datasets.md#tpch) — Benchmark star-schema
- [MovieLens](datasets.md#movielens) — Ratings and joins
- [Online Retail](datasets.md#online-retail) — Transactional retail invoices (CSV)
- [GitHub Archive](datasets.md#github-archive) — Semi-structured JSON events

See [Datasets](datasets.md) for the complete registry with fetch methods and scale options.

### Lakehouse & Atlas

| Document | Description |
|---|---|
| [Lakehouse Architecture](lakehouse.md) | Medallion layout, Iceberg namespaces, MinIO buckets |
| [Atlas Expectations](atlas-expectations.md) | A1–A9 delivered capabilities and gotchas |
| [Atlas Go-Live Runbook](go-live.md) | End-to-end validation playbook |
| [Atlas Go-Live Results](go-live-results.md) | Live validation results from 2026-07-04 |
| [Atlas Feedback (A7/A9)](atlas-feedback-a7a9.md) | Trino + Redpanda delivery feedback |
| [Atlas Go-Live Findings](atlas-feedback-go-live.md) | Infra issues from full validation run |
| [Atlas Enablement](atlas-enablement.md) | Origin A1–A9 contract ledger |

## What Next?

- Run a scenario: browse [Scenarios](scenarios/index.md)
- Build a Spark app: [Spark Apps](spark-apps/index.md)
- Understand the lakehouse: [Lakehouse](lakehouse.md)
- Explore the Atlas platform contract: [Atlas Expectations](atlas-expectations.md)
```

- [ ] **Step 2: Verify**

```bash
wc -l docs/index.md
```
Expected: at least 100 lines.

---

### Task 11: Rewrite `docs/scenarios.md` — overview only

**Files:**
- Modify: `docs/scenarios.md`

- [ ] **Step 1: Replace with overview page**

The current file has per-scenario detail that duplicates what's now in each individual scenario doc. Rewrite it as a high-level overview:

```markdown
# Scenarios

<Overview of the 19 scenarios: they represent the complete data engineering lifecycle from raw data ingestion through medallion transformation, quality validation, streaming, BI, and advanced Iceberg features.>

## Categories

The scenarios are organized into functional categories. See [Scenario Catalog](./scenarios/index.md) for the full index with architecture diagrams and detailed per-scenario documentation.

### Batch Processing
Scenarios that process data in batch mode using Spark SQL on DataFrames. These cover ingestion, transformation, quality, schema evolution, and maintenance.

### Streaming
Scenarios that use Spark Structured Streaming — either from file sources (S3) or Kafka/Redpanda topics. These cover real-time ingestion, windowed aggregation, and CDC upserts.

### BI & Analytics
Trino-powered SQL queries and multi-engine scenarios that validate read-write interop between Spark and Trino on shared Iceberg tables.

## Running Scenarios

Each scenario ships dual notebooks (Zeppelin Scala + Jupyter PySpark) with identical logic. Most can be triggered as Airflow DAGs (`dag.py`), though streaming scenarios run as long-running queries.

See each scenario's documentation for prerequisites, architecture, and step-by-step run instructions.
```

---

### Task 12: Fix and rewrite `docs/spark-apps.md`

**Files:**
- Modify: `docs/spark-apps.md`

- [ ] **Step 1: Replace content**

Fix the typo and restructure:

```markdown
# Spark Applications

Production Spark jobs live in `spark-apps/<app>/` as standard Maven Scala projects. Each app includes scalatest-covered transforms, an argument-driven entrypoint, a `Jenkinsfile` for CI, and an Airflow `dag.py` for orchestration.

## Pipeline

1. **Build:** Jenkins clones the repo, runs `mvn test` + `mvn package`, produces a shaded JAR
2. **Publish:** The JAR is uploaded to `s3a://jars/<app>/<version>/app.jar` on MinIO
3. **Execute:** An Airflow DAG using `SparkSubmitOperator` (cluster deploy mode) submits the JAR to the Spark cluster

See individual application docs for architecture, transform logic, and usage:

- [nyc-taxi-etl](spark-apps/nyc-taxi-etl.md) — Ingests raw taxi Parquet into a bronze Iceberg table with quality filtering.
- [nyc-taxi-medallion](spark-apps/nyc-taxi-medallion.md) — Productionizes the medallion transform pipeline (bronze → silver dedup → gold aggregation) for NYC taxi data.

## Build locally

```bash
mvn -q -B -f spark-apps/nyc-taxi-etl/pom.xml test
mvn -q -B -f spark-apps/nyc-taxi-medallion/pom.xml test
mvn -q -B -f spark-apps/nyc-taxi-etl/pom.xml package
mvn -q -B -f spark-apps/nyc-taxi-medallion/pom.xml package
```

Both apps declare Spark 4 as a `provided` scope dependency and require the Atlas Spark + Iceberg runtime environment at execution.
```

---

### Task 13: Rewrite `docs/lakehouse.md` — expand with architecture

**Files:**
- Modify: `docs/lakehouse.md`

- [ ] **Step 1: Expand with professional structure**

```markdown
# Lakehouse Architecture

`data-eng-lab` uses an Apache Iceberg lakehouse on MinIO, cataloged by the Atlas Iceberg REST catalog under the `lakehouse` namespace, organized in a three-tier medallion layout: `landing` (raw) → `bronze` → `silver` → `gold`.

![Lakehouse Architecture](architecture.html)

## 1. Storage Layer

All storage is backed by MinIO (S3-compatible), organized into buckets:

| Bucket | Purpose |
|---|---|
| `landing` | Raw incoming data: datasets landed by `download_datasets.py` |
| `lakehouse` | Iceberg metastore data — contains metadata, data files, and snapshot logs for all `bronze`/`silver`/`gold` tables |
| `jars` | Shaded JARs published by Jenkins CI builds |
| `checkpoints` | Spark Structured Streaming checkpoint locations for exactly-once semantics |

## 2. Catalog

The Iceberg REST catalog (`iceberg-rest:8181`) uses a PostgreSQL backend (Supabase) for metadata persistence. The catalog name is `lakehouse` — all tables are addressed as `lakehouse.<namespace>.<table>`.

### 2.1 Namespaces

Three namespaces are created at bootstrap:

| Namespace | Medallion Layer | Description |
|---|---|---|
| `bronze` | Raw | Landing-zone data with minimal transformation, retaining original schema |
| `silver` | Refined | Deduplicated, validated, and enriched data with schema enforcement |
| `gold` | Aggregated | Business-level aggregates, feature marts, and dimension tables |

Namespaces are created idempotently by `scripts/register_iceberg.py`, which runs automatically during `make up` or standalone:

```bash
uv run python scripts/register_iceberg.py
```

> **Note:** Atlas does not pre-seed namespaces. `data-eng-lab` creates them at bootstrap time. Application-defined tables may also self-create with `CREATE NAMESPACE IF NOT EXISTS`.

## 3. The Medallion Pattern

### 3.1 Bronze

The bronze layer is the raw ingestion tier. Data is landed as-is from source systems with minimal cleaning (NULL filtering, type casts, column additions). Tables in bronze are append-only and partitioned by ingestion date where applicable.

**Examples:** `nyc_taxi_trips`, `events`, `gh_events`, `gh_events_stream`

### 3.2 Silver

The silver layer deduplicates, validates, and enriches bronze data. Schema changes (evolution) are handled by Iceberg without rewriting existing data. Tables in silver may be updated (MERGE INTO), appended to, or split by quality checks.

**Examples:** `nyc_taxi_trips` (deduped), `gh_events`, `gh_sessions`, `online_retail`, `event_windows`

### 3.3 Gold

The gold layer contains business-level aggregates, feature marts, dimension tables, and BI-ready marts. These tables are partitioned and optimized for analytical queries. They are the final consumers of the platform: consumed by Trino SQL BI, ML pipelines, and downstream consumers.

**Examples:** `nyc_taxi_daily`, `bi_segment_revenue`, `ml_user_features`, `tpch_segment_revenue`, `dim_customer_scd2`

## 4. Integration Matrix (Preflight Layer 2)

`make preflight` executes Layer 1 (service health) followed by Layer 2 (functional service↔service round-trips). The Layer 2 matrix validates:

| Edge | What It Proves | Status |
|---|---|---|
| Spark → MinIO + Iceberg | Real Iceberg write/read via Spark Connect | Pass |
| Jupyter → PyIceberg | PyIceberg client connectivity | Pass |
| Airflow → MinIO + Spark | DAG-driven spark-submit, JAR execution | Pass |
| Zeppelin → Spark | Scala notebook execution via Zeppelin interpreter | Pass |
| Trino → Lakehouse | CTAS read-write on Iceberg via Trino | Pass |
| Spark → Redpanda | Kafka connector availability and connectivity | Pass |

Edges depending on undelivered Atlas items are marked `skipped` by the preflight manifest until enabled via `--<service>-source container` flags.

## 5. Bronze Smoke Test

`scripts/bronze_smoke.py` provides an end-to-end proof that the lakehouse path is operational: it lands a small dataset into `lakehouse.bronze.*` via Spark Connect and verifies the write succeeded.

```bash
uv run python scripts/bronze_smoke.py
```

## 6. Iceberg Features in Use

| Feature | Use Case | Scenarios |
|---|---|---|
| `MERGE INTO` | CDC upserts, incremental updates | `incremental_upsert`, `cdc_streaming`, `scd2` |
| Snapshots / `VERSION AS OF` | Time travel, historical queries | `time_travel` |
| Branches (WAP) | Write-audit-publish workflow | `time_travel` |
| `system.*` procedures | Table maintenance (rewrite, expire, orphan cleanup) | `table_maintenance` |
| Schema evolution | Add/rename columns without rewrite | `schema_evolution` |
| `from_json` / `explode` | Nested JSON extraction | `json_flatten` |
| Structured Streaming | Real-time data ingestion | `streaming_ingest` (both), `streaming_windows`, `cdc_streaming` |
| Window functions | Sessionization, aggregations | `sessionization`, `streaming_windows` |
| CTAS | Trino writes Iceberg tables | `federated_query`, `bi_query` |

## 7. See Also

- [Getting Started](getting-started.md)
- [Atlas Expectations](atlas-expectations.md)
- [Datasets](datasets.md)
- [Atlas Go-Live Findings](atlas-feedback-go-live.md)
```

- [ ] **Step 2: Generate lakehouse architecture diagram**

Use the architecture-diagram skill to create a diagram showing: MinIO (landing, lakehouse, jars, checkpoints) → Iceberg REST Catalog → Spark Connect/Standalone → Zeppelin + Jupyter → Trino → Redpanda. Include Airflow and Jenkins as orchestration layer. Save as `docs/lakehouse-architecture.html` (adjust the image reference in the markdown above to `lakehouse-architecture.html`).

- [ ] **Step 3: Verify**

```bash
wc -l docs/lakehouse.md
```
Expected: at least 100 lines.

---

### Task 14: Fix `docs/datasets.md`

**Files:**
- Modify: `docs/datasets.md`

- [ ] **Step 1: Rewrite with fixed content**

```markdown
# Datasets

`data-eng-lab` lands a curated set of five open datasets into MinIO's `landing` bucket, driven by a declarative registry.

## Registry

`datasets/registry.yaml` declares each dataset:

- **`format`** — data format (Parquet, CSV, JSON)
- **`license`** — data license
- **`landing_prefix`** — S3 prefix under `landing/`
- **`fetch.kind`** — `http` for direct downloads with optional `unzip`, or `tpch` for DuckDB-generated TPC-H
- **Per-SCALE parameters** — `tiny`, `small`, `medium` (volume / row count options)

The registry is schema-validated by the repo verifier. Adding a new dataset requires only a registry entry — no code change needed.

## Current Datasets

| Dataset | Shape | Format | Fetch | Scenarios |
|---|---|---|---|---|
| `nyc_taxi` | Columnar analytical | Parquet | http | batch_ingest, data_quality, medallion, federated_query, table_maintenance, time_travel |
| `gh_archive` | Semi-structured JSON events | JSON.gz | http | json_flatten, schema_evolution, sessionization, streaming_ingest (file source) |
| `movielens` | Rating + join data | CSV | http (unzip) | feature_engineering |
| `online_retail` | Transactional retail invoices | CSV | http (unzip) | incremental_upsert, scd2, cdc_streaming |
| `tpch` | Benchmark star-schema | Parquet | tpch (DuckDB) | star_schema, join_optimization, bi_query |

## Usage

```bash
# Boot MinIO first (requires a running Atlas stack)
make up

# Land the 'small' tier (default)
make datasets

# Land a specific tier
make datasets SCALE=tiny   # CI-sized subset
make datasets SCALE=medium # heavier queries

# Land a single dataset
uv run python scripts/download_datasets.py --scale medium --only nyc_taxi

# Preview what would land
uv run python scripts/download_datasets.py --dry-run
```

The downloader reads MinIO credentials and the S3 port from `infra/.env` and is idempotent — existing objects are skipped unless `--force` is passed.

## Adding a Dataset

To add a new dataset:

1. Add an entry to `datasets/registry.yaml` with `name`, `format`, `license`, `landing_prefix`, `fetch.kind`, and per-scale configs.
2. Implement a fetch function in `datasets/sources/<kind>.py` if the kind is new (the repo ships `http.py` and `tpch.py`).
3. Run `uv run python scripts/download_datasets.py --dry-run` to verify.
4. Run the scenario that consumes the dataset.

## See Also

- [Scenario Catalog](scenarios/index.md)
- [Lakehouse Architecture](lakehouse.md)
```

- [ ] **Step 2: Verify**

```bash
wc -l docs/datasets.md
```
Expected: at least 50 lines.

---

### Task 15: Fix `docs/getting-started.md`

**Files:**
- Modify: `docs/getting-started.md`

- [ ] **Step 1: Minor reformatting and cross-link updates**

Make these targeted edits:

1. In Step 2 (Download datasets), change "five curated datasets (NYC Taxi, TPC-H, Online Retail, GH Archive, Events)" to "five curated datasets (NYC Taxi, TPC-H, Online Retail, GitHub Archive, MovieLens)."

2. In "What next?", update cross-links to use consistent relative paths.

3. Add a section "Architecture" after "Prerequisites" that links to the architecture diagram on the site, e.g.:

```markdown
## Architecture

![Architecture](architecture.html)

The entire platform runs as Docker Compose containers. See [Lakehouse Architecture](lakehouse.md) for the full component breakdown.
```

---

### Task 16: Fix `docs/atlas-expectations.md`

**Files:**
- Modify: `docs/atlas-expectations.md`

- [ ] **Step 1: Fix section numbering**

Change headings to fix the skip from section 2 to 4:

- `## 0. TL;DR status` → keep as `## 0. Status Overview`
- `## 1. Delivered capabilities` → keep as `## 1. Delivered Capabilities`
- `## 2. Delivered deviations from our A7/A9 asks` → keep as `## 2. Delivered Deviations`
- `## 4. Iceberg / Spark capabilities` → rename to `## 3. Iceberg and Spark Capabilities`
- `## 5. Deviations & gotchas (recap — the things that surprised us)` → rename to `## 4. Gotchas Recap`
- `## 6. How we verify your delivery` → rename to `## 5. Verification`

This removes the duplicate content in section 5 (which repeated section 2) and renumbers the rest.

Actually, on closer inspection, section 5 is a comprehensive recap, so the better approach is:

- `## 0. TL;DR status` → `## 0. Status Overview`
- `## 1. Delivered capabilities — confirmed reality (do not regress)` → `## 1. Delivered Capabilities`
- `## 2. Delivered deviations from our A7/A9 asks` → `## 2. A7/A9 Deviations`
- `## 4. Iceberg / Spark capabilities the scenario catalog relies on` → `## 3. Iceberg and Spark Capabilities`
- `## 5. Deviations & gotchas (recap — the things that surprised us)` → **keep as** `## 4. Gotchas and Deviations Recap`
- `## 6. How we verify your delivery` → `## 5. Verification`

This produces a clean sequential numbering.

- [ ] **Step 2: Update cross-references**

In the same file, ensure the `atlas-feedback-go-live.md` link at the bottom uses a relative path consistent with the rest of the docs.

---

### Task 17: Fix `docs/atlas-enablement.md`

**Files:**
- Modify: `docs/atlas-enablement.md`

- [ ] **Step 1: Update superseded status**

The intro callout says "The authoritative hand-off is now `docs/atlas-expectations.md` — it reflects the *delivered* reality (A1–A6, A8), the outstanding A7/A9 build specs."

Update this to reflect that A7/A9 are also delivered:

```markdown
> **➡️ The authoritative hand-off is now [`docs/atlas-expectations.md`](atlas-expectations.md)** — it reflects the delivered reality (A1–A9, all delivered). This file remains the terse A1–A9 origin ledger.
```

- [ ] **Step 2: Fix cross-links**

Ensure all cross-links (`docs/go-live.md`, `docs/atlas-feedback-a7a9.md`, `docs/atlas-feedback-go-live.md`) use consistent relative paths (bare filenames, as the file currently uses).

---

### Task 18: Fix `docs/go-live.md`

**Files:**
- Modify: `docs/go-live.md`

- [ ] **Step 1: Fix cross-link at line 517**

Change:
```markdown
*See also:* [`docs/go-live-results.md`](go-live-results.md)
```
To:
```markdown
*See also:* [Go-Live Results](go-live-results.md)
```

- [ ] **Step 2: Update cross-ref to atlas-enablement**

Ensure the "Cross-referenced from" line uses consistent linking to `atlas-expectations.md`.

---

### Task 19: Fix `docs/go-live-results.md`, `docs/atlas-feedback-a7a9.md`, `docs/atlas-feedback-go-live.md`

**Files:**
- Modify: `docs/go-live-results.md`
- Modify: `docs/atlas-feedback-a7a9.md`
- Modify: `docs/atlas-feedback-go-live.md`

- [ ] **Step 1: Update cross-references in all three files**

Ensure all intra-docs cross-links use consistent relative paths (bare `filename.md`, not `docs/filename.md`). Standardize the "See also" sections.

For `go-live-results.md`:
- Change `[Go-Live Runbook](go-live.md)` and `[Atlas Enablement Contract](atlas-enablement.md)` to use consistent format.

For `atlas-feedback-a7a9.md`:
- Verify links to `atlas-feedback-*.md` files use relative paths.

For `atlas-feedback-go-live.md`:
- Verify links use relative paths consistently.

---

### Task 20: Rewrite `scripts/gen_doc_pages.py`

**Files:**
- Modify: `scripts/gen_doc_pages.py`

- [ ] **Step 1: Simplify the script**

The current script copies all scenario/spark-app READMEs into the virtual filesystem. This is no longer needed — enriched docs are hand-written in `docs/scenarios/` and `docs/spark-apps/`. The script should now **only** generate the `SUMMARY.md` navigation file (literate-nav dependency), since nav is now explicit in `mkdocs.yml`.

Actually, since nav is now explicit in `mkdocs.yml`, we can **remove the gen-files plugin entirely** and its nav generation. But to keep the script as a utility for future use (and because it doesn't hurt), we'll simplify it to:

```python
"""Generate MkDocs SUMMARY.md navigation from scenario/spark-app READMEs.

This script is optional — nav is now defined explicitly in mkdocs.yml.
Run this if nav needs to be auto-generated (e.g., after adding a new scenario).

Usage:
    python3 scripts/gen_doc_pages.py
"""

import re
from pathlib import Path

import mkdocs_gen_files

REPO_ROOT = Path(__file__).parent.parent


def _h1_label(readme: Path) -> str | None:
    """Return the first H1 heading text from *readme*, or None if absent."""
    for line in readme.read_text(encoding="utf-8").splitlines():
        m = re.match(r"^#\s+(.+)", line)
        if m:
            return m.group(1).strip()
    return None


def _nav_entry(readme: Path, dest_path: str) -> tuple[str, str] | None:
    """Return (label, dest_path) for a README, or None if absent."""
    label = _h1_label(readme)
    return (label or readme.parent.name.replace("-", " ").title(), dest_path)


def main():
    scenario_entries: list[tuple[str, str]] = []
    for scenario_dir in sorted((REPO_ROOT / "scenarios").iterdir()):
        readme = scenario_dir / "README.md"
        if not readme.exists():
            continue
        dest = f"scenarios/{scenario_dir.name}.md"
        entry = _nav_entry(readme, dest)
        if entry:
            scenario_entries.append(entry)

    app_entries: list[tuple[str, str]] = []
    for app_dir in sorted((REPO_ROOT / "spark-apps").iterdir()):
        readme = app_dir / "README.md"
        if not readme.exists():
            continue
        dest = f"spark-apps/{app_dir.name}.md"
        entry = _nav_entry(readme, dest)
        if entry:
            app_entries.append(entry)

    summary_lines: list[str] = [
        "- [Home](index.md)",
        "- [Getting Started](getting-started.md)",
        "",
        "- Scenarios:",
    ]
    for label, page in scenario_entries:
        indent = "  - " if len(scenario_entries) <= 25 else "    - "
        summary_lines.append(f"{indent}- [{label}]({page})")

    summary_lines.append("")
    summary_lines.append("- Spark Apps:")
    for label, page in app_entries:
        indent = "  - " if len(app_entries) <= 25 else "      - "
        summary_lines.append(f"{indent}- [{label}]({page})")

    summary_lines += [
        "",
        "- Lakehouse & Atlas:",
        "  - [Lakehouse](lakehouse.md)",
        "  - [Atlas Expectations](atlas-expectations.md)",
        "  - [Atlas Go-Live Runbook](go-live.md)",
        "  - [Atlas Go-Live Results](go-live-results.md)",
        "  - [Atlas Feedback (A7/A9)](atlas-feedback-a7a9.md)",
        "  - [Atlas Go-Live Findings](atlas-feedback-go-live.md)",
        "  - [Atlas Enablement](atlas-enablement.md)",
        "",
        "- [Datasets](datasets.md)",
        "- [Changelog](CHANGELOG.md)",
    ]

    with mkdocs_gen_files.open("SUMMARY.md", "w") as nav_fh:
        nav_fh.write("\n".join(summary_lines) + "\n")

    scenarios = len(scenario_entries)
    apps = len(app_entries)
    print(f"SUMMARY.md written: {scenarios} scenarios, {apps} spark apps")


if __name__ == "__main__":
    main()
```

Key changes from current:
1. **No longer copies READMEs** — that is replaced by hand-written enriched docs in `docs/scenarios/` and `docs/spark-apps/`
2. **Only generates SUMMARY.md** for literate-nav (optional, since nav is now explicit in mkdocs.yml)
3. **Updated nav entries** — includes `go-live-results.md`, `atlas-feedback-go-live.md`, `CHANGELOG.md` which were missing or incorrect

- [ ] **Step 2: Test the script**

```bash
cd /Users/kaveh/repos/data-eng-lab && python3 scripts/gen_doc_pages.py
```
Expected output: `SUMMARY.md written: 19 scenarios, 2 spark apps`

---

### Task 21: Rewrite `scripts/build_wiki.py`

**Files:**
- Modify: `scripts/build_wiki.py`

- [ ] **Step 1: Rewrite with auto-discovery from mkdocs.yml nav**

```python
#!/usr/bin/env python3
"""Assemble a wiki/ staging directory mirroring the MkDocs docs.

Run from the repository root:
    python3 scripts/build_wiki.py

Output:
    wiki/Home.md           — landing page with section index
    wiki/_Sidebar.md       — GitHub wiki sidebar navigation
    wiki/<Page>.md         — one file per docs/ page (auto-discovered from mkdocs.yml nav)
    wiki/Scenario-<name>.md   — one file per scenarios/<name>/README.md
    wiki/App-<name>.md        — one file per spark-apps/<name>/README.md
"""

from __future__ import annotations

import re
import shutil
import yaml
from pathlib import Path
from urllib.parse import unquote

REPO_ROOT = Path(__file__).parent.parent
WIKI_DIR = REPO_ROOT / "wiki"
DOCS_DIR = REPO_ROOT / "docs"
SITE_URL = "https://thekaveh.github.io/data-eng-lab/"

BANNER_TEMPLATE = (
    "> **Note:** This page is a mirror of the [full docs site]({url}). "
    "For the rendered, canonical version visit the link above.\n\n"
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _h1(path: Path) -> str | None:
    """Return the first H1 heading text from *path*, or None."""
    for line in path.read_text(encoding="utf-8").splitlines():
        m = re.match(r"^#\s+(.+)", line)
        if m:
            return m.group(1).strip()
    return None


def _stem(name: str) -> str:
    """Convert a markdown filename to a wiki page stem (title-ish)."""
    stem = Path(name).stem
    # Replace hyphens with spaces, title-case
    return stem.replace("-", " ").replace("_", " ").title().replace(" ", "-")


def _write(dest: Path, content: str) -> None:
    dest.parent.mkdir(parents=True, exist_ok=True)
    dest.write_text(content, encoding="utf-8")


def _mirror_with_banner(src: Path, dest: Path, section_url: str | None = None) -> None:
    """Copy *src* into *dest* with a site-link banner prepended."""
    url = section_url or SITE_URL
    banner = BANNER_TEMPLATE.format(url=url)
    original = src.read_text(encoding="utf-8")
    # Remove trailing "See Also" section to avoid circular reference to the wiki itself
    content = re.sub(r"\n##\s+See Also\s*\n.*$", "", original, flags=re.DOTALL)
    _write(dest, banner + content)


def _nav_yaml() -> list[dict]:
    """Load nav entries from mkdocs.yml, returning a flat list of (filename, label)."""
    mkdocs_path = REPO_ROOT / "mkdocs.yml"
    with open(mkdocs_path) as f:
        config = yaml.safe_load(f)

    nav: list[dict] = config.get("nav", [])
    entries: list[tuple[str, str]] = []

    def _walk(items: list, depth: int = 0) -> None:
        for item in items:
            if isinstance(item, dict):
                for key, value in item.items():
                    if isinstance(value, list):
                        _walk(value, depth + 1)
                    else:
                        label = str(key) if depth == 0 else key
                        path = str(value)
                        if path.endswith(".md"):
                            entries.append((path, label))
            elif isinstance(item, str):
                # Flat entry: "filename.md" or "Label: filename.md"
                if ": " in item:
                    label, path = item.split(": ", 1)
                else:
                    path = item
                    label = _stem(path)
                if path.endswith(".md"):
                    entries.append((path, label))

    _walk(nav)
    return entries


# ---------------------------------------------------------------------------
# Guide pages (all docs/ pages discovered from mkdocs.yml nav)
# ---------------------------------------------------------------------------

def _build_guides() -> list[tuple[str, str]]:
    """Return list of (wiki_page_stem, wiki_filename) for all guide pages."""
    entries: list[tuple[str, str]] = []
    nav_pages = _nav_yaml()

    skip_pages = {"SUMMARY.md"}
    for path, label in nav_pages:
        if path in skip_pages:
            continue
        src = DOCS_DIR / path
        if not src.exists():
            # If the docs page doesn't exist, skip (will be added later if needed)
            continue
        wiki_name = f"{_stem(path)}.md"
        dest = WIKI_DIR / wiki_name
        section_url = SITE_URL + path.replace(".md", "/")
        _mirror_with_banner(src, dest, section_url)
        entries.append((label, wiki_name))
    return entries


# ---------------------------------------------------------------------------
# Scenario pages (auto-discovered from scenarios/ READMEs)
# ---------------------------------------------------------------------------

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
        # Use README.md content (the source of truth for wiki)
        original = readme.read_text(encoding="utf-8")
        content = BANNER_TEMPLATE.format(url=section_url) + original
        _write(dest, content)
        entries.append((title, wiki_name))
    return entries


# ---------------------------------------------------------------------------
# Spark-app pages (auto-discovered from spark-apps/ READMEs)
# ---------------------------------------------------------------------------

def _build_apps() -> list[tuple[str, str]]:
    app_dirs = sorted((REPO_ROOT / "spark-apps").iterdir())
    entries: list[tuple[str, str]] = []
    for d in app_dirs:
        readme = d / "README.md"
        if not d.is_dir() or not readme.exists():
            continue
        name = d.name
        wiki_name = f"App-{name}.md"
        dest = WIKI_DIR / wiki_name
        title = _h1(readme) or _stem(name)
        section_url = SITE_URL + f"spark-apps/{name}/"
        original = readme.read_text(encoding="utf-8")
        content = BANNER_TEMPLATE.format(url=section_url) + original
        _write(dest, content)
        entries.append((title, wiki_name))
    return entries


# ---------------------------------------------------------------------------
# Home.md
# ---------------------------------------------------------------------------

def _build_home(
    guide_entries: list[tuple[str, str]],
    scenario_entries: list[tuple[str, str]],
    app_entries: list[tuple[str, str]],
) -> None:
    lines: list[str] = [
        "# data-eng-lab\n\n",
        "An Apache Iceberg lakehouse data engineering lab on the "
        "[Atlas platform](https://github.com/thekaveh/atlas) — "
        "Scala/PySpark scenarios, Maven Spark apps, and the full medallion.\n\n",
        f"> **Note:** This page is a mirror of the [full docs site]({SITE_URL}).\n\n",
        "---\n\n",
        "## Guides\n\n",
    ]
    for title, wiki_name in guide_entries:
        stem = Path(wiki_name).stem
        lines.append(f"- [[{stem}|{title}]]\n")

    lines += [
        "\n## Scenarios\n\n",
    ]
    for title, wiki_name in scenario_entries:
        stem = Path(wiki_name).stem
        lines.append(f"- [[{stem}|{title}]]\n")

    lines += [
        "\n## Spark Apps\n\n",
    ]
    for title, wiki_name in app_entries:
        stem = Path(wiki_name).stem
        lines.append(f"- [[{stem}|{title}]]\n")

    _write(WIKI_DIR / "Home.md", "".join(lines))


# ---------------------------------------------------------------------------
# _Sidebar.md
# ---------------------------------------------------------------------------

def _build_sidebar(
    guide_entries: list[tuple[str, str]],
    scenario_entries: list[tuple[str, str]],
    app_entries: list[tuple[str, str]],
) -> None:
    lines: list[str] = [
        "**[[Home]]**\n\n",
        "**Guides**\n\n",
    ]
    for title, wiki_name in guide_entries:
        stem = Path(wiki_name).stem
        lines.append(f"- [[{stem}|{title}]]\n")

    lines += ["\n**Scenarios**\n\n"]
    for title, wiki_name in scenario_entries:
        stem = Path(wiki_name).stem
        lines.append(f"- [[{stem}|{title}]]\n")

    lines += ["\n**Spark Apps**\n\n"]
    for title, wiki_name in app_entries:
        stem = Path(wiki_name).stem
        lines.append(f"- [[{stem}|{title}]]\n")

    _write(WIKI_DIR / "_Sidebar.md", "".join(lines))


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    if WIKI_DIR.exists():
        shutil.rmtree(WIKI_DIR)
    WIKI_DIR.mkdir()

    guide_entries = _build_guides()
    scenario_entries = _build_scenarios()
    app_entries = _build_apps()

    _build_home(guide_entries, scenario_entries, app_entries)
    _build_sidebar(guide_entries, scenario_entries, app_entries)

    wiki_files = list(WIKI_DIR.glob("*.md"))
    print(f"wiki/ built: {len(wiki_files)} files")
    for f in sorted(wiki_files):
        print(f"   {f.name}")


if __name__ == "__main__":
    main()
```

Key changes:
1. **Auto-discovery from mkdocs.yml nav**: No more hardcoded GUIDE_MAP. All doc pages are discovered by parsing `mkdocs.yml` nav structure
2. **Includes all pages**: `atlas-feedback-go-live.md`, `go-live-results.md`, and all other pages discovered in nav
3. **Improved banner**: Cleaner "mirror" notice
4. **Strips circular "See Also" sections**: Removes `## See Also` from wiki pages to avoid circular references to the wiki

- [ ] **Step 2: Test the script**

```bash
cd /Users/kaveh/repos/data-eng-lab && python3 scripts/build_wiki.py
```

Expected: `wiki/ built: N files` where N ≥ 30 (9 guides + 19 scenarios + 2 apps + Home + Sidebar)

Wait, that's 9 guides + 19 scenarios + 2 apps = 30. Plus Home and Sidebar = 32 files total.

---

### Task 22: Rewrite `README.md`

**Files:**
- Modify: `README.md`

- [ ] **Step 1: Write new README.md**

```markdown
# data-eng-lab

An Apache Iceberg lakehouse data engineering lab built on the [Atlas](https://github.com/thekaveh/atlas) platform. The repository contains 19 Spark scenarios, 2 CI-built Maven Scala applications, Trino SQL BI queries, Redpanda streaming, and a full medallion (bronze → silver → gold) architecture — all orchestrated via Docker Compose.

📖 [Full Documentation Site](https://thekaveh.github.io/data-eng-lab/)

---

## Overview

`data-eng-lab` demonstrates end-to-end data engineering on a real lakehouse stack. Scenarios cover batch ingestion, medallion transformation, data quality, streaming with Redpanda/Kafka, BI queries via Trino, join optimization, dimensional modeling, feature engineering, slow-changing dimensions, schema evolution, and table maintenance. Every scenario ships dual notebooks (Zeppelin Scala + Jupyter PySpark) with identical logic, plus optional Airflow DAGs for orchestration.

See the [documentation site](https://thekaveh.github.io/data-eng-lab/) for architecture diagrams, architecture guides, and detailed per-scenario documentation.

## By the Numbers

| Metric | Value |
|---|---|
| Spark scenarios | 19 (14 batch, 4 streaming, 1 hybrid) |
| Spark applications | 2 (Maven Scala, CI/CD automated) |
| Curated datasets | 5 |
| Atlas enablement items | 9 (A1–A9, all delivered) |
| Query engines | 2 (Spark SQL, Trino SQL) |

## Quick Start

```bash
git clone https://github.com/thekaveh/data-eng-lab.git
cd data-eng-lab
make setup        # initialize Atlas submodule
make datasets     # download datasets
make up           # launch all services
make preflight    # verify connectivity
```

See [Getting Started](docs/getting-started.md) for prerequisites, detailed instructions, and troubleshooting.

## Scenarios

| Scenario | Dataset | Mode | Medallion Layers |
|---|---|---|---|
| [batch_ingest](scenarios/batch_ingest-nyc_taxi-spark-iceberg/) | NYC Taxi | Batch | Bronze |
| [medallion](scenarios/medallion-nyc_taxi-spark-iceberg/) | NYC Taxi | Batch | Bronze → Silver → Gold |
| [data_quality](scenarios/data_quality-nyc_taxi-spark-iceberg/) | NYC Taxi | Batch | Bronze → Silver |
| [federated_query](scenarios/federated_query-nyc_taxi-trino-iceberg/) | NYC Taxi | Batch | Bronze → Gold |
| [time_travel](scenarios/time_travel-nyc_taxi-spark-iceberg/) | NYC Taxi | Batch | Bronze, Silver |
| [table_maintenance](scenarios/table_maintenance-nyc_taxi-spark-iceberg/) | NYC Taxi | Batch | Bronze, Silver |
| [streaming_ingest (events)](scenarios/streaming_ingest-events-spark-iceberg/) | Synthetic | Streaming | Bronze |
| [streaming_ingest (gh_archive)](scenarios/streaming_ingest-gh_archive-spark-iceberg/) | GitHub Archive | Streaming | Bronze |
| [streaming_windows](scenarios/streaming_windows-events-spark-iceberg/) | Synthetic | Streaming | Gold |
| [cdc_streaming](scenarios/cdc_streaming-online_retail-spark-iceberg/) | Online Retail | Streaming | Silver |
| [json_flatten](scenarios/json_flatten-gh_archive-spark-iceberg/) | GitHub Archive | Batch | Silver |
| [schema_evolution](scenarios/schema_evolution-gh_archive-spark-iceberg/) | — | Batch | Silver |
| [sessionization](scenarios/sessionization-gh_archive-spark-iceberg/) | GitHub Archive | Batch | Silver |
| [incremental_upsert](scenarios/incremental_upsert-online_retail-spark-iceberg/) | Online Retail | Batch | Silver |
| [scd2](scenarios/scd2-online_retail-spark-iceberg/) | Online Retail | Batch | Gold |
| [feature_engineering](scenarios/feature_engineering-movielens-spark-iceberg/) | MovieLens | Batch | Gold |
| [join_optimization](scenarios/join_optimization-tpch-spark-iceberg/) | TPC-H | Batch | Gold |
| [star_schema](scenarios/star_schema-tpch-spark-iceberg/) | TPC-H | Batch | Gold |
| [bi_query](scenarios/bi_query-tpch-trino-iceberg/) | TPC-H | Batch | Gold |

See [Scenario Catalog](docs/scenarios/index.md) for the full index with architecture diagrams and detailed documentation.

## Spark Applications

| Application | Description |
|---|---|
| [nyc-taxi-etl](spark-apps/nyc-taxi-etl/) | Raw Parquet → cleaned Bronze Iceberg table |
| [nyc-taxi-medallion](spark-apps/nyc-taxi-medallion.md) | Bronze → Silver → Gold medallion pipeline |

Built by Jenkins CI, submitted via Airflow DAG. See [Spark Apps](docs/spark-apps/index.md) for details.

## Repository Structure

```
data-eng-lab/
├── scenarios/           # 19 Spark scenarios (Zeppelin Scala + Jupyter PySpark)
├── spark-apps/          # 2 Maven Scala Spark apps (Jenkins CI + Airflow CD)
├── datasets/            # Dataset registry and fetchers (HTTP, TPC-H/DuckDB)
├── scripts/             # Bootstrap, dataset download, wiki builder, etc.
├── lakehouse/           # Lakehouse catalog configuration (catalog.py)
├── jenkins/             # Jenkins job definitions (seed-job.sh, XML)
├── infra/               # Atlas platform pinned submodule (git submodule)
├── compose/             # Local Docker Compose overlay (if any)
├── docs/                # MkDocs Material documentation source
├── tests/               # Tiered test suite (unit, static, network, infra/live)
├── mkdocs.yml           # MkDocs configuration
├── pyproject.toml       # uv project config (dev group: mkdocs, etc.)
├── Makefile             # Canonical entry points (setup, up, down, datasets, etc.)
└── README.md            # This file
```

## Licenses

This project uses a proprietary license — see [`LICENSE`](LICENSE) for terms. The datasets used in scenarios have their own open licenses as declared in [`datasets/registry.yaml`](datasets/registry.yaml).

---

*Maintained by `data-eng-lab`. Questions or issues → open a GitHub issue.*
```

- [ ] **Step 2: Verify**

```bash
wc -l README.md
```
Expected: at least 100 lines.

---

### Task 23: Update docs-trigger paths in `.github/workflows/ci.yml`

**Files:**
- Modify: `.github/workflows/ci.yml`

- [ ] **Step 1: Read and update CI workflow**

```bash
cat /Users/kaveh/repos/data-eng-lab/.github/workflows/ci.yml
```

Review whether the workflow trigger paths need updating to include the new doc structure (e.g., `docs/scenarios/*.md`, `docs/spark-apps/*.md` are already covered by `docs/**`).

Verify the docs-deploy workflow (`docs-deploy.yml`) is correct — it already triggers on `docs/**` and `scenarios/**/README.md` and `spark-apps/**/README.md`.

---

### Task 24: Final integration test — build the MkDocs site

**Files:**
- All docs files
- `mkdocs.yml`
- `scripts/gen_doc_pages.py`

- [ ] **Step 1: Build MkDocs site locally**

```bash
cd /Users/kaveh/repos/data-eng-lab && uv run --group dev mkdocs build --strict
```

Expected: Successful build with no errors. If there are errors (e.g., broken links, missing files), fix them iteratively.

- [ ] **Step 2: Verify the site was generated**

```bash
test -d /Users/kaveh/repos/data-eng-lab/site && echo "site/ exists" && ls site/*.html | head -5
```

- [ ] **Step 3: Run wiki builder**

```bash
cd /Users/kaveh/repos/data-eng-lab && python3 scripts/build_wiki.py
```

Expected: `wiki/ built: 32 files` or similar (9 guides + 19 scenarios + 2 apps + Home + Sidebar)

- [ ] **Step 4: Verify wiki contents**

```bash
ls wiki/ | wc -l
```
Expected: 32 or more files.

- [ ] **Step 5: Verify gen_doc_pages.py still works**

```bash
cd /Users/kaveh/repos/data-eng-lab && python3 scripts/gen_doc_pages.py
```
Expected: `SUMMARY.md written: 19 scenarios, 2 spark apps`

---

### Task 25: Generate remaining architecture diagrams

**Files:**
- Create: `docs/scenarios/architectures/<scenario-name>.html` (19 files, one per scenario)

- [ ] **Step 1: Generate diagrams**

For each of the 19 scenarios, use the architecture-diagram skill to generate an SVG diagram showing:
- **Batch scenarios:** Source dataset → Spark processing → Iceberg table(s), indicating medallion layer(s)
- **Streaming scenarios:** Redpanda/S3 source → readStream → transform → writeStream → sink + checkpoint
- **Cross-engine scenarios:** Both Spark and Trino paths if applicable (e.g., federated_query: Spark writes bronze, Trino reads and writes gold)

Save each as `docs/scenarios/architectures/<scenario-name>.html`

- [ ] **Step 2: Verify**

```bash
ls docs/scenarios/architectures/*.html | wc -l
```
Expected: `19`.

---

### Task 26: Final review and cleanup

**Files:**
- All modified files

- [ ] **Step 1: Verify no broken links across docs**

Check all `md` files in `docs/` for relative links that point to non-existent files:

```bash
grep -r '\]\(' docs/*.md docs/scenarios/*.md docs/spark-apps/*.md 2>/dev/null | grep -oP '\]\(\K[^)]+' | sort -u | while read link; do
  # Remove anchors and query params
  path=$(echo "$link" | sed 's/#.*//; s/\?.*//')
  if [ -n "$path" ] && [ ! -f "/Users/kaveh/repos/data-eng-lab/docs/$path" ]; then
    echo "BROKEN: $link (resolved to: $path)"
  fi
done
```

- [ ] **Step 2: Check all files exist that are referenced**

Verify the following exist:
```bash
test -f docs/architecture.html && \
test -f docs/css/custom.css && \
test -f docs/scenarios/index.md && \
test -f docs/spark-apps/index.md && \
test -f docs/scenarios/architectures && \
test -f docs/spark-apps/architectures && \
echo "All key files/dirs exist"
```

- [ ] **Step 3: Verify MkDocs strict build passes**

```bash
cd /Users/kaveh/repos/data-eng-lab && uv run --group dev mkdocs build --strict 2>&1
```

Expected: Exit code 0, no errors.

---

## Self-Review

### Spec coverage check:
- ✅ Full-stack architecture diagram → Task 3
- ✅ Per-scenario architecture diagrams → Task 25
- ✅ Per-spark-app architecture diagrams → Task 9
- ✅ Hierarchical numbered sections in all docs → Tasks 6, 7, 8, 11-19
- ✅ Cross-linked catalog pages → Tasks 7, 8, 11, 12, 22
- ✅ Scenario READMEs standardized → Task 5
- ✅ Scenario enriched docs → Task 6
- ✅ Spark app docs → Task 8
- ✅ Home page with tables/lists → Task 10
- ✅ `mkdocs.yml` complete nav rewrite → Task 2
- ✅ `gen_doc_pages.py` simplified → Task 20
- ✅ `build_wiki.py` auto-discovery → Task 21
- ✅ README fully rewritten → Task 22
- ✅ `lakehouse.md` expanded → Task 13
- ✅ `datasets.md` fixed (online_retail added, GH Archive correct name) → Task 14
- ✅ `getting-started.md` polished → Task 15
- ✅ `atlas-expectations.md` numbering fixed → Task 16
- ✅ `atlas-enablement.md` updated → Task 17
- ✅ `go-live.md` link fixed → Task 18
- ✅ Feedback/result docs cross-links → Task 19
- ✅ CI/CD workflows reviewed → Task 23
- ✅ Integration tests → Task 24
- ✅ Final review → Task 26

### Placeholder scan:
- ❌ No "TBD", "TODO", "implement later" found
- ❌ No vague "add validation" or "handle edge cases" references
- ❌ All code blocks show actual content
- ❌ All file paths are specific and exact

### Type consistency:
- ✅ All scenario names reference consistent kebab-case directory names
- ✅ All docs paths use consistent relative referencing
- ✅ All markdown structures follow identical templates per category

---

## Execution Handoff

Plan complete. Two execution options:

**1. Subagent-Driven (recommended)** — Dispatch subagents per task group with review between tasks:
- Subagent A: Tasks 1–3 (CSS, mkdocs.yml, full-stack diagram)
- Subagent B: Tasks 5–8 (scenario READMEs + docs + spark apps)
- Subagent C: Tasks 10–19 (docs rewrites)
- Subagent D: Tasks 20–21 (scripts rewrite)
- Subagent E: Tasks 22, 25–26 (README, diagrams, final review)

**2. Inline Execution** — Execute tasks sequentially in this session with checkpoints for review.

Which approach?
