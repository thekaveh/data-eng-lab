# Full Documentation Overhaul — Design Spec

**Topic:** Complete overhaul of `data-eng-lab`'s documentation ecosystem (README, MkDocs `.io` site, GitHub Wiki) into a unified system with one source of truth.

**Date:** 2026-07-05

**Author:** User + Opencode (brainstorming skill design phase)

---

## Goals

1. **Eliminate all drift surfaces.** No two documentation systems maintain the same content independently. There is exactly one canonical document source; everything else renders from it.
2. **Professional quality across all surfaces.** Site, wiki, and GitHub README are equally polished — no single surface being a low-effort afterthought.
3. **Cross-linked and navigable.** Every page links to related pages (upstream, downstream, sibling scenarios); every parent page has a table of its children; every child documents the link back in "See Also."
4. **Auto-generated architecture diagrams.** Every scenario, spark app, and overview page has an inline architecture diagram; all generated via the `architecture-diagram` skill with dark-themed aesthetic.
5. **Hierarchically numbered sections everywhere.** Book-style numbering (`1 Overview`, `2 Data Model (2.1 Input / 2.2 Output)`) that auto-number from Markdown headings. Numbers are never hand-maintained.

---

## Architecture: Source of Truth + Two Renderings

Everything is built around one principle: **the source tree wins**. Both the `.io` docs site and the GitHub Wiki are *renderings* of the same content — not parallel, independent documentation sets that must be kept in sync by hand.

```
                        Source of truth
              ┌───────────────────────────────────────┐
              │   MkDocs Material site (docs/ raw md) │
              │   + per-scenario docs                 │
              │   + per-spark-app docs                │
              └─────┬──────────────────────┬──────────┘
                    │                      │
            builds via MkDocs       builds via sync_wiki.py
                    │                      │
                     v                      v
          ┌─────────────────────┐  ┌──────────────────────┐
          │ .io docs site       │  │ GitHub Wiki repo     │
          │ (thekaveh.github.io)│  │ (.wiki clone pushed  │
          │                     │  │  by CI on every      │
          │                     │  │  deploy to main)     │
          └─────────────────────┘  └──────────────────────┘
```

### Why MkDocs-Driven (Architecture A — chosen over B)

- **The existing pipeline is proven.** Phase 14 already has MkDocs Material with a working deployment flow. No reason to replace it.
- **All documentation artifacts are first-class repo objects** with full git history, PR review ability, and local preview via `mkdocs serve`.
- **Cross-linking between pages is trivial with MkDocs**' literal-nav plugin (file-based navigation) combined with relative Markdown links — no manual number tracking needed.

### Wiki-as-mirror behavior

The wiki repo (`thekaveh/data-eng-lab.wiki.git`) contains only raw GitHub Wiki Markdown files (per-page `.md` content files, an `_Sidebar.md`, and any `wiki/` subdirectories). The build script maps each MkDocs nav entry back to its canonical Markdown source. There is no "wiki" content path that isn't simultaneously a `.io` site page.

This also means: **adding a new scenario automatically adds it to the wiki** because the scenario doc goes into `docs/scenarios/<name>.md`, which already exists in the nav, so the sync script renders its page and sidebar entry too. No two-step process needed.

---

## Page Taxonomy & Hierarchical Numbering System

### Document Hierarchy (Book-Style)

All top-level pages and their subsections are numbered starting from Section 1 at root level:

| Section | Path | Contents |
|---------|------|----------|
| **1** Overview | `docs/index.md` — rendered at `/` on `.io`, and mirrored to GitHub Wiki as `_Sidebar.md` "Home" entry. Serves also as the canonical source for the repo's root README.md (thin redirect stub). | Architecture diagram inline, stat table ("By The Numbers"), quick navigation cards linking to every section, getting-started command block |
| **2** Getting Started | `docs/getting-started.md` — rendered at `/getting-started/` on `.io`, mirrored in wiki under "Getting Started" sub-section. | Prerequisites (Docker Compose, Java 17+, Node.js 20+, git submodule support), step-by-step: clone → `make setup` → `make datasets` → `make up` → `make preflight`. Includes troubleshooting appendix with atlas feedback references linked inline. |
| **3** Architecture & Lakehouse | `docs/lakehouse.md` — rendered at `/lakehouse/`, wiki mirror. | Medallion layout, Iceberg namespace design (bronze/silver/gold), storage layer breakdown per MinIO bucket, Iceberg REST catalog config (`lakehouse.*.properties`). Full-stack architecture.html diagram embedded inline. |
| **4** Scenario Catalog | `docs/scenarios/index.md` — rendered at `/scenarios/`, wiki mirror. 19 scenario documents linked below; each also has its own section in left nav and sidebar. | Table of all 19 scenarios (columns: name, dataset, mode, medallion layers, Airflow DAG, upstream/downstream cross-links), plus category grouping listing the categories described in this design spec. Each sibling links to the related scenario pages as "see also." |
| **5** Spark Applications | `docs/spark-apps/index.md` — rendered at `/spark-apps/`, wiki mirror. 2 application documents linked below with architecture diagrams inline per app. | Two Maven Scala Spark apps, CI flow (Jenkins → shade → MinIO). Per-app docs contain: architecture diagram, project structure, transform logic walkthrough, and build/run instructions. |
| **6** Datasets | `docs/datasets.md` — rendered at `/datasets/`, wiki mirror. | Catalog of 5 curated datasets with table: name, format (Parquet/csv/json.gz), fetch method (`make datasets --all` or per-dataset download script argument), registered schema link-back to scenario usages. |
| **7** Platform Enablement & Go-Live | `docs/atlas-*.md` (A1–A9) — rendered at `/platform/`, wiki mirror as a multi-page sub-section. | Atlas expectations, go-live runbook, go-live results from validation runs (2026-07-04), feedback on A7/A9 delivery issues and resolution paths. Grouped under top-level "Platform" section of nav + sidebar because all content tracks the Atlas enablement contract. |
| **8** Changelog | `CHANGELOG.md` — rendered at `/changelog/`, wiki mirror as final sub-entry. | Semantic-version-style changelog entries for data-eng-lab itself (not infra). |

### Numbering Mechanics & Source-Of-Truth Behavior

Section numbers are **computed from Markdown headings**, not hand-typed:

- MkDocs Material's `toc.follow` and sidebar display shows section numbers by virtue of rendering the nav tree in order.
- All per-scenario pages use a "numbered" outline (e.g., 4.1, 4.2 for the scenarios subsection) — this is computed at render time by a small Python script that processes each scenario doc's source Markdown to find its section position within the parent `docs/scenarios/index.md` nav tree and prepends numbers automatically.
- Anchor links in "see also" sections use auto-numbered slug (`#4-architecture`) so references always stay valid even if you reorder scenarios (the number is derived from document order, not hard-coded text).

### What Each Per-Scenario Page Contains

Every scenario doc shares the same template structure (19 consistent layouts):

```markdown
# <scenario-name>

**One-sentence overview:** what this does and high-level data flow.

## 1. Overview — Purpose
Two to three sentences explaining *why* this scenario exists in the set:
what problem it solves, how it maps onto standard data-engineering practice,
and its role relative to sibling scenarios (upstream/downstream relationship).

## 2. Data Model
### 2.1 Input Schema
Describe input dataset format + relevant columns / key column choices for upstream joins.
#### Required datasets & fetch commands
Include `make datasets` command if fetching is needed.

### 2.2 Output Schema (Iceberg path(s))
Iceberg table path (`lakehouse.<namespace>.<table>`), medallion layer, partitioning,
and what other scenarios use this output as input — **bidirectional cross-link**.

## 3. Architecture
![Architecture diagram](architectures/<scenario-name><dataset>.html)
Description of diagram: data flow source → processing step → sink; services required; dependencies on Atlas infrastructure items (A1, A5, etc.).

## 4. Notebooks — Walkthroughs
### 4.1 Zeppelin Scala Notebook
One-paragraph walkthrough covering steps performed, key patterns used
(e.g., structured streaming with watermarks, MERGE INTO with upsert semantics).

### 4.2 Jupyter PySpark Notebook
Same format for the PySpark equivalent. Both notebooks implement identical logic.

## 5. Orchestration — Airflow / Long-running Streaming
DAG file location, DAG ID, schedule (cron-style), Airflow connection used by task. For long-standing streaming flows: explain checkpoint configuration and how you stop it safely.

## 6. Usage
Step-by-step commands to run this scenario locally (`make <target>`, `docker exec` against Jupyter or Zeppelin container — whichever is appropriate). Also include verification commands after a successful run (output should match X, query Y on Trino returns Z rows).

## See Also

### Upstream Sources
- [Source Scenario Name](../scenarios/foo-scenario.md) — produces the data this scenario consumes.
- → **Dataset: <Name>** (datasets.md#dataset-name) — dataset description + fetch command.

### Downstream Consumers
- [Consumer Scenario Name](../scenarios/bar-scenario.md) — consumes output of this scenario as input.

### Related Scenarios / Adjacent Topics
- [Related Topic](../topic/section.md)

All cross-links are **bidirectional**: if A lists B as upstream, B lists A under "downstream consumers." This is verified by the test suite (see Section 4 below).
```

No embedded code blocks in per-scenario docs — professional prose + architecture diagram only. Code references stay in Getting Started / Usage context.

### Notebooks Within Scenarios — Walkthrough Treatment

Each walkthrough section (`4.1` and `4.2`) is a **concise narrative**: what the notebook does step by step, not reproducing every cell's output or code. The reader already has access to the notebook via `.io` links; the document exists to explain *what* the notebook demonstrates — not to be a duplicate of it.

### Spark-App Docs Structure (5 documents total)

| File | Pages | Purpose |
|------|-------|---------|
| `docs/spark-apps/index.md` | ~20 lines | Catalog table: app name, source → target tables, CI/CD pattern summary |
| `docs/spark-apps/nyc-taxi-etl.md` | ~35 pages | Per-app doc (architecture diagram + project structure + transform logic + build/run) |
| `docs/spark-apps/nyc-taxi-medallion.md` | ~35 pages | Same template as above for medallion app, with Bronze → Silver → Gold pipeline explanation included in "transform logic" section |

Same numbering system — each app doc is numbered under parent topic 5:
- **5.1 nyc-taxi-etl** at `spark-apps/nyc-taxi-etl.md`
- **5.2 nyc-taxi-medallion** at `spark-apps/nyc-taxi-medallion.md`

---

## Diagram Generation Approach & Layout Specification

### All Architecture diagrams generated with architecture-diagram skill — dark theme by default, light mode toggle as secondary scheme (dark is the MkDocs Material scheme used in production)

| Scope | Files Generated | Location on `.io` site |
|-------|-----------------|------------------------|
| **Full stack / overview** (`data-eng-lab/main/Overview`) | 1 SVG architecture diagram showing all components — MinIO (4 buckets), Spark, Zeppelin, JupyterHub, Airflow, Redpanda, Iceberg REST catalog with namespace `lakehouse`, Trino, Kafka, Medallion layout. | Embedded in docs/index.md and lakehouse.md; shown on .io home page above "By the Numbers" table |
| **Per-scenario layouts** (`data-eng-lab/scenarios/batch_ingest-nyc_taxi-spark-iceberg/Overview`) | 19 diagrams, each showing that scenario's specific data flow (source → processing step → sink + services) with relevant services highlighted. For streaming scenarios: show Kafka producer and source-to-sink with writeStream checkpoint markers explicitly included | Each embedded inline at section "3 Architecture" of its owning scenario page; stored in `docs/scenarios/architectures/<name>.html` |
| **Per-spark-app layouts** (`data-eng-lab/spark-apps/nyc-taxi-medallion/Overview`) | 2 diagrams showing: (1) Jenkins → shade JAR → MinIO/Jars bucket, then Spark submit path → Iceberg sink; (2) Same for medallion app with Bronze → Silver → Gold processing layer flow | Inline at top of each spark-app doc (`docs/spark-apps/architectures/<name>.html`) |

All generated files are embedded inside `<figure>` tags with `img-fluid` class so they scale correctly on `.io`, rendered side-by-side for comparison, and always in the same position (left aligned by default, no floating). Dark-theme color palette matches MkDocs primary (indigo) + accent (amber); light mode uses lighter variants of each.

### Architecture Diagrams — Rendering Location & Cross-Linking Behavior

The architecture diagram is shown at section "3" of every scenario and spark-app document **before** the notebook walkthrough section `4`. This way: reader sees data flow visually first, then reads textual explanation; reading order matches layout. No need to reference external files for context — everything in view on that one page.

---

## Category Grouping (Per-Scenario Index Page)

The 19 scenarios are grouped into categories above the catalog table. Each category section links out:

| Category | Scenarios Included | Rationale / Topic |
|----------|--------------------|----|
| **1. Batch Ingestion** | `batch_ingest-nyc_taxi-spark-iceberg` (1 scenario) | Raw Parquet/CSV → Iceberg Bronze layer, no additional transforms beyond cleaning. Foundation for downstream scenarios. |
| **2. Medallion Pipeline** | `medallion-nyc_taxi-spark-iceberg` (1 scenario) | Full three-tier lake flow; demonstrates dedup, enrichment, aggregation within one pipeline. Production-grade counterpart is nyc-taxi-medallion spark app in `spark-apps/`. |
| **3. Data Quality** | `data_quality-nyc_taxi-spark-iceberg` (1 scenario) | Splits records into clean vs quarantine tables per business rules. Foundation for downstream quality dashboards. |
| **4. Schema & Maintenance** | `schema_evolution-gh_archive-spark-iceberg`, `time_travel-nyc_taxi-spark-iceberg`, `table_maintenance-nyc_taxi-spark-iceberg` (3 scenarios) | Iceberg features — schema evolution + time-travel snapshots + lifecycle table maintenance. No production DAG equivalent needed; used as reference for operational practices. |
| **5. Streaming Ingest** | `streaming_ingest-events-spark-iceberg`, `streaming_ingest-gh_archive-spark-iceberg` (2 scenarios) | Two streaming ingest patterns — file-source vs Kafka source with checkpoint configuration explained in their architecture diagrams above. |
| **6. Streaming & CDC Transformations** | `cdc_streaming-online_retail-spark-iceberg`, `streaming_windows-events-spark-iceberg` (2 scenarios) | CDC upserts using MERGE INTO + foreachBatch; windowed aggregations with watermark for bounded stream processing. |
| **7. Feature Engineering & SCD Type 2** | `feature_engineering-movielens-spark-iceberg`, `scd2-online_retail-spark-iceberg` (2 scenarios) | ML feature marts from recommendations datasets + slow-changing-dimension tracking — advanced Iceberg capabilities for historical dimension updates. |
| **8. Incremental Upsert & Time Travel** | `incremental_upsert-online_retail-spark-iceberg`, `time_travel-nyc_taxi-spark-iceberg` (2 scenarios) | Merge into upsert semantics, plus versioning / rollback — related by "how to safely update Iceberg tables without duplication". |
| **9. BI & Analytics Queries** | `bi_query-tpch-trino-iceberg`, `federated_query-nyc_taxi-trino-iceberg`, `join_optimization-tpch-spark-iceberg` (3 scenarios) | Trino-powered querying + multi-engine read-write interop; benchmark-style TPC-H queries. Validates Spark writes → Trino reads flow works. |
| **10. Dimensional Modeling** | `star_schema-tpch-spark-iceberg` (1 scenario) | Star schema design — fact/dimension separation with dim_customer/fct_orders for analytical use cases on Iceberg data written by Spark. |

Category titles and ordering are defined inline in MkDocs nav (Section 4 below shows structure) so they never get out of step with the catalog table content, which renders them automatically from source. This is also why category reordering or adding a new scenario means updating one location — `mkdocs.yml` nav key — instead of four places across docs and wiki files.

---

## Wiki Sync Pipeline & CI Integration (Section 4 of the document)

### How Changes Flow End-to-End

```
You edit docs/X/Y.md (the source of truth)
    ↓ git push to origin/main
GitHub Actions triggers the docs-deploy workflow:
    ├── build_www.py     → runs `mkdocs build` into site/       │ deploys .io via GitHub Pages Action
    └── sync_wiki.py     → parses mkdocs.yml nav tree, writes  │ pushes wiki content to wiki repo git remote
                                matching .md file per page
                                + auto-generates _Sidebar.md from mkdocs.yml title structure hierarchy
```

One git push. One CI job with two parallel jobs. Both sites update simultaneously — zero manual trigger needed. If a scenario doc exists it's mirrored; if you delete a page (in practice never for the 19 scenarios under design), same applies: both sides remove reference to it. No drift between wiki and .io ever.

### What `sync_wiki.py` Actually Does on Input from mkdocs.yml + Source Tree Under docs/

| Output | Location / Format | Source In mkdocs.yml For It |
|--------|-------------------|---------------------------- |
| Per-page markdown files (raw GitHub Wiki format) | `<wiki>/foo.md`, `<wiki>/(subfolder)/scenario-name-spark-iceberg/bar` (mirror structure of docs/ nav tree exactly, no nesting deeper than 1 folder for scenario sub-sections) | One file per mkdocs.yml navigation entry whose doc_path matches an actual source markdown path; sidebar auto-generated from structured title hierarchy. Each page is a literal copy of its `docs/<name>.md` with no re-rendering or transformation needed (we keep content Markdown everywhere so `.io` and wiki see identical text — only rendered differently per site default themes/layouts.)
| `_Sidebar.md` (table of contents) | `<wiki>/_Sidebar.md` | Computed from mkdocs.yml's nav: key directly: the sidebar mirrors exactly what appears in MkDocs left rail. Same indentation pattern across `.io` and GitHub Wiki — identical content structure but rendered by two different UI contexts (one is local browser, another is Github wiki)
| Per-HTML page exports  | `<wiki>/<name>.wiki format (not used here; we use markdown only for wiki repo content source)` | Omitted in current design because it's unnecessary duplication

### Why Markdown Over HTML For Wiki Repo

The GitHub Wiki only accepts raw Markdown and image files when pushed through `git` remote. There is no "HTML export" path available on push. So: keep content as Markdown everywhere (`.io` renders via MkDocs + Material theme into styled HTML with search + dark/light mode toggle, wiki repo serves markdown content directly to GitHub's default Wiki renderer) — same underlying Markdown documents, two different rendering contexts for end user experience.

### CI Build / Deploy Workflow (what runs where in `.github/workflows`)

```yaml
on:
  push: { branches: [main] }  # triggers on every `git push main`
  pull_request:                # also run before merge to make sure docs pass — wiki preview step included as dry-run only; no actual wiki update happens for unmerged state.
    branches: [main]

jobs:

  deploy-docs-to-pages:       # builds MkDocs, deploys to GitHub Pages (.io site under /data-eng-lab/ URL)
    steps:
      - checkout-repo-with-submodules-and-wiki-token-set-env-pat-if-needed-before-push     # needs GITHUB_TOKEN from environment with permissions
                                                         # Note: also pushes .wiki git remote via dedicated personal access token stored as repo secrets for wiki push operation.
      - uv_install_and_activate_python_environment   # sets up python + pip packages needed (mkdocs-material, mkdocs-gen-files, mkdocs-literate-nav) including running mkdocs CLI after all deps are loaded into virtualenv.
      - python scripts/build_www.py                  # runs actual mkdocs build → produces static site/ dir containing CSS overrides, inline diagrams in /architectures/, scenario docs, etc.
      - deploy-to-github-pages-action                # final step: pushes built site to gh-pages branch so GitHub Pages renders live docs at thekaveh.github.io/data-eng-lab/.

  sync-wiki:               # second parallel job runs AFTER first completes successfully (needs deployment worked fine — no use updating wiki mirror with broken content)
    needs: [deploy-docs-to-pages]     # wait for deploy step to finish before triggering wiki refresh
    steps:
      - checkout-repo-with-submodules                                # re-checkouts source tree so sync script uses most recent docs/ state (in case there's any difference between local vs build-time — always use freshly compiled site not locally modified copy).
      - python scripts/sync_wiki.py                                  # generates wiki-tree content /tmp/<build-wiki-temp>/ directory structure mirroring exactly what goes into wiki repo files (Markdown + sidebar) based on parsed mkdocs nav hierarchy (no manual mapping required because navigation key already mirrors actual file paths from docs/ source tree — one location, two surfaces).
      - push-to-git-remote-wiki-https-using-token-from-secrets       # pushes generated content back up to https://github.com/thekaveh/data-eng-lab.wiki.git using same PAT used for repo clone operation but authenticated specifically toward the wiki remote instead of main repo origin.
```

### Parallel Job Reasoning For CI / GitHub Actions Layouts

Splitting into two jobs ensures neither blocks each other — if `sync_wiki.py` fails (e.g., network hiccup pushing to `.wiki`), the `.io` site still deploys; conversely, a MkDocs build failure doesn't prevent wiki refresh for cases where only some docs were edited. Failure notifications post under the PR as comments with which page or set of pages weren't synced so maintainer can triage what happened.

### Permissions Required For Wiki Repo Push Operation (auth)

- Read-write access to `data-eng-lab.wiki.git` remote — GitHub's default fine-grained personal access token grants this out of the box when you give it repo-level permissions (`repo`, `contents`).
- If an existing PAT exists in your GitHub account for data-eng-lab operations (which we should check), use that; otherwise generate new one via GH CLI with scopes [repo, write:repository].
- Add to repository secrets as `WIKI_PUSH_TOKEN` so sync script uses it without exposing raw credentials. Avoid storing long-lived tokens in environment variables directly — always pull from GitHub secrets at runtime by reading `$GITHUB_ENV` after job starts.

### CI / Docs Test Gating (what stops unmerged content)

Before merging to main, PR must pass two test suites that catch broken docs early:

1. **MkDocs local build check** runs in `deploy-docs-to-pages` step — exits with error code if any document has a syntax problem so you can fix locally before merge attempt rather than find out after the change went into CI.
2. **Wiki sync dry-run validation check** (`python scripts/sync_wiki.py --dry-run --check-only`) runs as part of PR CI too — compares expected file list against what currently exists on wiki remote (only to verify absence/presence of orphan pages and sidebar entry consistency, not full content diff). Failures stop the merge.

These two checks together mean: if someone deletes a scenario doc without updating MkDocs nav tree correctly first, both build-time tests catch it *before* merge happens — no ghost wiki page ever appears (orphan reference) on either side of the mirror because source-of-truth always remains in `docs/<name>.md` under git version control.

---

## Exclusions From Wiki Sync Scope

- **Infra services README** (`infra/services/*/README.md`) and other sub-project documentation — out of scope for data-eng-lab's docs system (these belong to the Atlas infra project, a completely separate repo; touching them would introduce coupling between docs overhaul work in this repo vs. unrelated infra code changes).
- **Internal planning notes**: `docs/superpowers/plans/*.md`, `infra/docs/strategy/*.md` — they're part of internal workflow documentation not external-facing material, excluded from both the .io site rendering and wiki mirror via existing MkDocs `_build-*ignore_dir*` ignore mechanism already in effect before design spec approval.
- **Scattered README stubs at** `scenarios/*/README.md`, `spark-apps/*/README.md` and similar — remain as thin redirect stubs (3–5 line file explaining where to find canonical documentation) per your instructions; the full content stays authoritative on .io site AND wiki mirror both through one unified source under `docs/`.

---

## Design Verification Checklist

- [x] Every page has a single, well-defined purpose
- [x] Parent pages include table-of-children catalog (19 scenarios, 2 spark-apps)
- [x] Each document links to related docs with explicit cross-link descriptions — bidirectional relationship guaranteed via test suite check-before-merge verification gating mechanism
- [x] Numbering is hierarchical from Section 1 Overview to Section 8 Changelog — no gaps between consecutive sections; all scenarios use same template outline structure (19 consistent layouts per per-scenario page spec)
- [x] Every section has auto-generated content — no manual number maintenance anywhere in docs source tree or in build scripts (because nav key drives everything through its order in mkdocs.yml + parsed sidebar entries)
- [x] Architecture diagrams use dark theme color palette matching MkDocs default scheme (indigo primary, amber accent); light-mode toggle provided for users who prefer lighter backgrounds when browsing .io site with Material UI switch control.
- [x] Wiki and `.io` doc sites have same content — any edit anywhere updates both simultaneously because of one source tree being shared between them via build script that parses nav from MkDocs config into wiki markdown structure output format (same content; just two different display mechanisms: browser-rendered vs raw-file-on-repo-display)
- [x] Infra services excluded explicitly from sync scope per user instruction — no accidental coupling with unrelated repo
- [x] Scattered README stubs remain thin redirect stub (3-liners pointing at canonical site documentation location — always .io docs link visible to readers even if they somehow browse by directory structure instead of following nav)

---

## Appendix: File Map Overview (what changes in the implementation phase we're transitioning toward next via writing-plans skill invocation after user approval of this spec document itself — not part of design section but listed here for completeness so all work scope is known upfront):

The full implementation plan will be written as part of the **writing-plans** sub-step which comes AFTER you, the user, approve finalization of this design document in conversation with me (current session). The implementation file list covers every change including:
- `mkdocs.yml` rewrite to include proper hierarchical nav structure with section number support via literal-nav + gen-files plugins working together for per-scenario page generation automation flow.
- Custom CSS overrides under docs/css/custom.css so both `.io` site UI styling matches the same palette (indigo primary, amber accent color) used in architecture diagram dark theme aesthetic — consistent visual identity across all surfaces of documentation system we're designing here including wiki sidebar mirror which uses GitHub's Wiki default rendering but with text styling that mimics indigo+amber accent look too so users don't see "two different looking documents" while navigating .io and github-wiki back-and-forth during work sessions on same data-eng-lab project content set across both surfaces side-by-side.
- Per-scenario README stubs at `scenarios/*/README.md` — rewritten to point readers toward canonical docs (one line with link to .io; no duplicated content because source of truth lives only under `docs/<name>.md`).
- 19 per-scenario Markdown documents generated from standardized template outline above. Each contains: numbered sections, embedded inline architecture diagram, cross-links in structured See Also block at bottom per-page. Same for the two spark-app docs (nyc-taxi-etl.md and nyc-taxi-medallion.md).
- Scripts directory under `scripts/` that will now be split into three new files replacing current gen_doc_pages.py with: one build_www.py that invokes MkDocs CLI to compile site/, another sync_wiki.py which handles wiki repo push operation (with separate auth token for wiki remote), a third test_docs.py file containing verification checks (MkDocs syntax OK, cross-links resolve, no orphan pages). See "Verification Gating" section above.
- A `.github/workflows/ci.yml` update for docs-trigger paths + adding new workflow `docs-deploy-into-main.yaml`. Two jobs: one runs build_www.py then deploys .io site; second runs sync_wiki.py only after first finishes successfully before push happens (no orphan pages possible because source file deletion gets caught by tests too).

---

## Conclusion: Next Step Forward (writing-plans sub-step invocation after user approves this document)

I've presented the full design spec here so we both know what's being built. The next phase is **implementation planning** via an implementation plan that translates all decisions above into actionable, ordered tasks — written as a superpowers/plans file (`docs/superpowers/plans/2025-XX-YY-docs-overhaul.md` or similar) with checkbox-based steps so we can verify each completed piece.

Before invoking writing-plans, let me know if you have any adjustments to the design itself before I formalize into a plan document.
**Approving this spec means transitioning directly onward toward invocation of superpowers:writing-plans skill next.**

