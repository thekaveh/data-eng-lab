# Documentation Overhaul — Design Spec (v2)

**Topic:** Re-architect the `data-eng-lab` documentation into three self-contained, fully-synced surfaces (in-repo READMEs, GitHub Wiki, `.io` MkDocs site) generated from a single source of truth — with embedded diagrams, per-notebook docs, and an Orbital/sci-fi theme.

**Date:** 2026-07-07
**Branch:** `docs/overhaul-2026-07-06` (continues the in-flight overhaul work)
**Status:** Design approved 2026-07-07; supersedes `2026-07-06-docs-overhaul-design.md` and the `2026-07-06-docs-overhaul-implementation.md` plan.
**Scope:** Top-level `data-eng-lab` repo only. The `infra/` (Atlas) submodule is **out of scope** — it has its own `mkdocs.yml`, wiki, and site.

---

## 1. Background & motivation

The repo ships documentation on three surfaces:

1. **In-repo markdown** — the README files users see browsing the repo on GitHub (root `README.md`, `scenarios/*/README.md`, `spark-apps/*/README.md`).
2. **GitHub Wiki** — `wiki/` (built locally by a script, pushed to `*.wiki.git`).
3. **`.io` doc site** — built by MkDocs Material from `docs/`, deployed to GitHub Pages.

A prior overhaul (`2026-07-06`) established `docs/` as the source of truth and wrote 19 scenario docs, 2 spark-app docs, an 8-section numbered template, a `custom.css`, and the `build_wiki.py` / `render_readme.py` generators. That foundation is solid, but an audit on 2026-07-07 found the current state **violates the project owner's hard requirements**:

- **Cross-surface linking is pervasive.** `build_wiki.py` prepends an `.io` banner on every wiki page; `render_readme.py` footers the root README with an `.io` link; scenario READMEs link back into `docs/` (`../../docs/...`). Requirement: *none* of the three surfaces may link to the others.
- **Diagrams are linked, not embedded — and broken.** Docs reference diagrams via `![alt](architectures/x.html)`, but HTML is not an image (renders nowhere as an image). `build_wiki.py` never copies diagram files, so every wiki diagram reference dangles. Requirement: diagrams generated once, embedded (not linked) in all three surfaces.
- **No per-notebook documentation.** 19 scenarios × {Jupyter `.ipynb`, Zeppelin `.zpln`} = 38 notebooks; only mentioned in prose. Requirement: each scenario's notebooks get a comprehensive, hierarchically-numbered doc, linked from the first page, in all three surfaces.
- **Dead CI / dead code.** `render_readme.py` runs in `docs-deploy.yml` with `contents: read` and never commits (the generated README is discarded). `gen_doc_pages.py` + literate-nav are shadowed by the explicit `nav:` in `mkdocs.yml`; `SUMMARY.md` does not exist on disk.
- **Coverage gaps.** `go-live-results.md`, `atlas-feedback-go-live.md`, and `CHANGELOG.md` are in the nav but never mirrored to the wiki.
- **Orphan diagrams.** `docs/spark-apps/architectures/` contains 3 stray files matching no real app (`medallion-lakehouse-spark-iceberg`, `spark-cdc-spark-iceberg`, `streaming-data-engineering-spark-iceberg`).
- **Spec ↔ reality drift.** The prior spec names scripts that do not exist (`render-wiki.py`, `generate-diagrams.py`) and a diagram layout that does not match disk.
- **Theme is "clean modern," not the requested aesthetic.** Current `custom.css` is indigo/amber Material; the project owner wants clean, sleek, minimalistic, modern, sci-fi-esque.

A separate content-level concern (out of the structural overhaul but noted): some scenario docs disagree with their notebooks (e.g. `batch_ingest` docs say "no transformation" while the notebook cleans). This is addressed as a per-scenario accuracy pass (§9, deferred item).

---

## 2. Locked decisions

| # | Decision | Choice |
|---|---|---|
| 1 | Source of truth & surface model | `docs/` is the single authored SOT and renders the `.io` site. READMEs and Wiki are **generated, self-contained copies** (full content, embedded diagrams, zero cross-surface links). |
| 2 | Notebook documentation | **One `notebooks.md` doc per scenario (19)**, auto-extracted by parsing both the `.ipynb` and `.zpln` cells. Documents the shared numbered section structure, embeds the code/SQL per section, includes a Scala/PySpark parity table. |
| 3 | Diagram format | **Standalone SVG**, landscape orientation, one canonical asset per diagram. Embedded (copied) into each surface. |
| 4 | `.io` theme | **Orbital / Sci-Fi Terminal** — flat dark canvas (`#0a0f1f`) + faint cyan corner glow; cyan accent (`#22d3ee` dark / `#0e7490` light); Space Grotesk (display) + IBM Plex Mono (code/HUD) + Inter (body); dark default + light toggle. **No grid/crossed-line background.** |

---

## 3. Architecture: source of truth & three surfaces

`docs/` is authored by humans and is the **only** place content is written. Three surfaces are produced from it; two are generated.

| Surface | Source | Produced by | "First page" (links to everything, incl. notebook docs) | Internal links allowed |
|---|---|---|---|---|
| **.io site** | `docs/**` | `mkdocs build` | `docs/index.md` | only to other `docs/` pages |
| **In-repo markdown** | derived from `docs/` | `scripts/build_docs.py` (README stage) | root `README.md` | only between READMEs and `notebooks.md` files within the repo |
| **GitHub Wiki** | derived from `docs/` | `scripts/build_docs.py` (wiki stage) | `wiki/Home.md` | only between wiki pages |

### Hard rules (enforced by the generator + CI)

1. **No cross-surface links.** No `.io` URLs in any README or wiki page. No `../../docs/...` (or any `docs/`-relative) path in any README. No repo URLs in any wiki page. No banners.
2. **Diagrams embedded, never linked across surfaces.** Each surface holds its own copy of every SVG it uses.
3. **Comprehensive parity.** Every page in the `.io` nav has a counterpart in the other two surfaces (scenario → README + wiki page; notebook doc → `notebooks.md` + wiki page; concept → root-README section + wiki page).
4. **Generated files are output-only.** READMEs and wiki pages are never hand-edited; a header comment in each states this.

### In-repo surface structure (explicit)

The in-repo surface has no `docs/`-parallel tree (that would duplicate the SOT). Instead it is a small set of GitHub-browsable files:
- **Root `README.md`** — the single comprehensive entry document: a table of contents, then numbered sections for Overview, Getting Started, Lakehouse Architecture, Datasets, the full Scenario catalog (with internal links to each scenario README and its `notebooks.md`), Spark Apps catalog, Atlas Operations summary, and Changelog. Deep concept content (Lakehouse, Datasets) lives **as numbered sections of this README** — deliberately, so a GitHub visitor gets the whole story from one file and the "first page links to everything" rule holds.
- **`scenarios/<name>/README.md`** × 19 — full scenario doc, internal cross-links to sibling scenario READMEs and to its own `notebooks.md`.
- **`scenarios/<name>/notebooks.md`** × 19 — auto-extracted notebook doc.
- **`spark-apps/<name>/README.md`** × 2 — full app doc.

If a concept section grows too large for the root README, it may be split into a top-level file (e.g. `DATASETS.md`) that the root README links to internally — but the default is one comprehensive README.

### Data flow

```
                 docs/  (authored SOT — humans edit only here)
                    │
        ┌───────────┼────────────────────────────┐
        │           │                            │
   mkdocs build    build_docs.py              (canonical SVGs live
        │           │                            in docs/architectures/)
        ▼           ├─► READMEs (root +        │
   .io site         │     scenarios/*/README   │
   (GitHub Pages)   │     + */notebooks.md     ├─► copied into
                    │     + spark-apps/*)      │    scenarios/*/architectures/
                    │                           │    (for READMEs)
                    ├─► wiki/                   │
                    │   (Home, Scenario-*,      └─► copied into wiki repo
                    │    Notebook-*, App-*,          (for wiki pages)
                    │    concepts, _Sidebar)
                    └─► copy SVG assets → README dirs + wiki repo
```

---

## 4. Content hierarchy & numbering (generic → specific)

The `.io` site and Wiki present a **top-level numbered hierarchy** from overview to specific. The in-repo root README mirrors it as a numbered overview + catalog. Proposed tree:

```
1.  Overview                         (docs/index.md · README §1 · wiki Home)
2.  Getting Started                  (docs/getting-started.md · README §2 · wiki Getting-Started)
3.  Lakehouse Architecture           (docs/lakehouse.md · README §3 · wiki Lakehouse)
4.  Datasets                         (docs/datasets.md · README §4 · wiki Datasets)
5.  Scenarios
       5.1  Catalog (by category)    (docs/scenarios/index.md · README §5 · wiki Home catalog)
       5.2–5.20  the 19 scenarios     (docs/scenarios/*.md · scenarios/*/README.md · wiki Scenario-*)
6.  Notebooks                        (docs/notebooks/*.md · scenarios/*/notebooks.md · wiki Notebook-*)
7.  Spark Apps                       (docs/spark-apps/*.md · spark-apps/*/README.md · wiki App-*)
8.  Atlas Operations                 (docs/{go-live,atlas-*}.md · README §8 · wiki Atlas-* / Go-Live-*)
9.  Changelog                        (docs/CHANGELOG.md · README footer · wiki Changelog)
```

**Scenario category groupings** (used in 5.1 and for ordering 5.2–5.20):
- **Batch ingest:** batch_ingest, medallion
- **Streaming:** streaming_ingest-events, streaming_ingest-gh_archive, streaming_windows-events, cdc_streaming-online_retail
- **Data quality / modeling:** data_quality, schema_evolution, star_schema, feature_engineering, scd2
- **Ops / governance:** time_travel, table_maintenance, incremental_upsert
- **SQL / analytics:** bi_query, federated_query, join_optimization
- **Semi-structured:** json_flatten, sessionization

### Document templates

**Scenario doc** (8 numbered sections, unchanged from prior template): 1. Purpose · 2. Data Model (2.1 Input / 2.2 Output) · 3. Architecture (embedded SVG) · 4. Notebooks (summary + link to `notebooks` doc) · 5. Orchestration · 6. Usage · 7. Dependencies · 8. Known Issues & Caveats · See Also (internal cross-links only).

**Notebook doc** (`docs/notebooks/<scenario>.md`), auto-extracted:
```
# Notebooks — <scenario>
## 1. Overview            (scenario summary + the two notebooks' shared purpose)
## 2. Section map         (table: section # → title → Scala cell → PySpark cell)
## 3. Walkthrough
       3.1 … 3.N          (one subsection per numbered notebook section;
                            embeds the code/SQL from both notebooks side by side)
## 4. Scala / PySpark parity   (diff-style notes on language-specific differences)
## 5. How to run          (open in Zeppelin / Jupyter; prerequisites)
```

**Concept docs** (Overview, Getting Started, Lakehouse, Datasets) get numbered subsections (`3.1`, `3.2`, …) — most are un-numbered today, so this is new authoring.

### Nav mechanism

**Keep the explicit `nav:` in `mkdocs.yml`** (full control over the hand-curated numbering). **Delete `scripts/gen_doc_pages.py`** and the literate-nav plugin + `SUMMARY.md` reference (dead code, shadowed by explicit nav). Alternatives considered and rejected: literate-nav/SUMMARY.md (inert today), awesome-pages auto-discovery (less control).

---

## 5. Diagram system

- **Format:** standalone SVG, **landscape orientation** (hard requirement). Script-free so GitHub renders it via `<img>`.
- **Canonical location:** `docs/architectures/` (standardized). Fixes today's drift across `docs/architecture.html`, `docs/lakehouse-architecture.html`, `docs/scenarios/architectures/`, `docs/spark-apps/architectures/`.
- **Inventory (canonical assets to produce):**
  - `overview.svg` — full-stack (home/README).
  - `lakehouse.svg` — medallion flow.
  - `scenarios/<name>.svg` × 19 — per-scenario data flow.
  - `apps/<name>.svg` × 2 — spark-app CI/CD flow.
- **Generation:** the `architecture-diagram` skill, palette-synced to Orbital:
  - Dark variant: cyan `#22d3ee` primary, slate node fills, `#0a0f1f` background.
  - Light variant: `#0e7490` primary on `#f6f8fb`, for the light theme.
  - Each diagram declares its orientation as landscape and its source inputs (notebook `dag.py`, `pom.xml`/`Jenkinsfile`, configs) in frontmatter the generator reads.
- **Embedding (generated once, copied everywhere):**
  - `.io`: embed canonical SVG from `docs/architectures/`.
  - READMEs: generator **copies** the SVG into `scenarios/<name>/architectures/<name>.svg` (and `spark-apps/<name>/architectures/`) and the README references the local copy.
  - Wiki: generator **copies** the SVG into the cloned wiki repo and the wiki page references it.
- **Cleanup:** delete the 3 orphan diagrams in `docs/spark-apps/architectures/` that match no real app.

---

## 6. Theme & site (Orbital — locked)

Delivered via `mkdocs.yml` (palette + fonts + features), an expanded `docs/css/custom.css` (replaces the current indigo/amber file), and a new `docs/overrides/main.html` (adds the corner-glow background layer and HUD-style breadcrumb).

- **Palette:** dark default `scheme: slate`-derived with `primary: custom` (`#22d3ee` cyan) — implemented via CSS variables; light scheme with `#0e7490`. Toggle preserved.
- **Background:** flat `#0a0f1f` (dark) / `#f6f8fb` (light) + a single faint radial cyan glow in one corner. **No grid, no crossed lines.**
- **Type:** Space Grotesk (headings/display, loaded via `main.html`), IBM Plex Mono (code + HUD labels/breadcrumbs), Inter (body).
- **Accent cues:** `◢` brand mark, mono uppercase breadcrumbs, cyan accent rules under headers, soft glow only on the brand and active nav item.
- **Existing Material features retained:** `navigation.tabs`, `navigation.sections`, `navigation.tracking`, `toc.follow`, `search.suggest/highlight`, `content.code.copy`, `content.code.annotate`, `content.tabs.link`.
- **`mkdocs.yml` changes:** swap `font` to the three families, set `palette` to the Orbital dark/light schemes, point `extra_css` at the new `custom.css`, keep `custom_dir: docs/overrides` (now populated).

---

## 7. Generation & sync pipeline

Consolidate `build_wiki.py` + `render_readme.py` into **one orchestrator: `scripts/build_docs.py`**, backed by a shared `scripts/docslib/` parser module. One run updates all surfaces atomically (the two-script split today lets README and wiki drift).

### Stages (each idempotent; `--check` runs a dry-run diff)

1. **Parse** `docs/` into an in-memory model (pages, frontmatter, section anchors, See-Also graphs, notebook cell trees).
2. **README stage** — write root `README.md` (comprehensive numbered overview + catalog + quick-start, internal links only), `scenarios/<name>/README.md` (full scenario doc, internal cross-links to sibling READMEs), `scenarios/<name>/notebooks.md` (auto-extracted), `spark-apps/<name>/README.md`. Rewrite every link to an internal README/notebook target; localize diagram paths.
3. **Wiki stage** — write `wiki/Home.md` + `Scenario-*.md` + `Notebook-*.md` + `App-*.md` + concept pages (`Lakehouse.md`, `Datasets.md`, `Getting-Started.md`, `Atlas-*.md`, `Go-Live-*.md`, `Changelog.md`) + `_Sidebar.md` (mirrors the §4 hierarchy). **No `.io` banner.**
4. **Asset stage** — copy each canonical SVG into the README directories and the wiki repo clone.
5. **Notebook extraction** — `scripts/docslib/notebooks.py` parses `.ipynb` (JSON cells) and `.zpln` (Zeppelin paragraphs), aligns them by numbered section, emits the `notebooks.md` template.

### CI

- **New workflow `.github/workflows/docs-sync.yml`** (on push to `main`, after a successful `mkdocs build`): runs `build_docs.py`, commits the regenerated READMEs back to the repo (`contents: write`), and pushes `wiki/` to `*.wiki.git` (replacing today's `wiki.yml`). Keeps `docs-deploy.yml` for the Pages deploy only.
- **`ci.yml`** keeps `mkdocs build --strict` as the PR gate and adds `build_docs.py --check` (asserts a clean run produces no diff vs. committed output).
- **New `scripts/check_surfaces.py`** CI assertion: fails if (a) any generated surface contains a link to another surface, (b) any diagram is referenced but not embedded locally, or (c) any nav page lacks a counterpart in all three surfaces.

### Files removed

- `scripts/gen_doc_pages.py` (dead; nav is explicit).
- `scripts/render_readme.py` and `scripts/build_wiki.py` (subsumed by `build_docs.py`; their logic moves into `docslib/`).
- literate-nav plugin + `SUMMARY.md` reference in `mkdocs.yml`.
- The 3 orphan diagrams.

---

## 8. Issue remediation map (audit → fix)

| # | Finding (2026-07-07 audit) | Fix |
|---|---|---|
| 1 | Wiki `.io` banner + README `.io` footer + README→`docs/` links | Generator strips all cross-surface links; `check_surfaces.py` asserts none remain |
| 2 | Diagrams linked (HTML), dangling in wiki | Regenerate as landscape SVG; copy+embed into all 3 surfaces |
| 3 | No per-notebook docs | Generate 19 `notebooks.md`, linked from scenario §4 + first page, in all 3 surfaces |
| 4 | `render_readme.py` dead in CI (`contents: read`, never commits) | `docs-sync.yml` with `contents: write` + real commit/push |
| 5 | Wiki missing `go-live-results`, `atlas-feedback-go-live`, `CHANGELOG` | Mirror map covers full nav |
| 6 | Dead `gen_doc_pages.py` + literate-nav | Remove; explicit nav only |
| 7 | 3 orphan spark-app diagrams | Delete |
| 8 | Prior spec names nonexistent scripts; layout drift | This spec supersedes; standardize `docs/architectures/` |
| 9 | Theme not sci-fi | Orbital redesign (§6) |
| 10 | Docs-vs-notebook content drift (e.g. `batch_ingest`) | Per-scenario accuracy pass (§9, deferred) |

---

## 9. Scope, branch, and deferred items

- **In scope:** all top-level `data-eng-lab` docs — three surfaces, diagram system, notebook docs, Orbital theme, generation pipeline, CI, and the remediation in §8 (items 1–9).
- **Out of scope:** `infra/` (Atlas) submodule.
- **Branch:** continue on `docs/overhaul-2026-07-06` (preserves the 19 scenario docs + diagrams already written). If a clean start is preferred, branch fresh off `main` — the spec is branch-agnostic.
- **Deferred (separate effort, tracked here):** item #10 — a per-scenario content-accuracy pass reconciling docs with notebook code. Flagged because it is content work, not structural, and is large (19 scenarios). It can proceed in parallel once the structural overhaul lands.

---

## 10. Verification & testing

Every change proves itself before it is called done:

- `mkdocs build --strict` — the `.io` site builds with no warnings or broken links.
- `scripts/check_surfaces.py` (CI) — asserts: no cross-surface links; every diagram embedded locally; every nav page present in all three surfaces.
- `scripts/build_docs.py --check` — a clean re-run produces an empty diff against committed READMEs/wiki.
- `scripts/docslib/notebooks.py` unit test — deterministically parses a sample `.ipynb` and `.zpln`, asserts the section map and parity table.
- Manual: visually confirm the `.io` site renders the Orbital theme in dark and light; confirm a scenario README and its wiki page are self-contained (open each with cross-surface links disabled).
- Diagram check: every generated SVG is landscape and renders via `<img>` on GitHub (script-free).

---

## Design verification checklist

- [x] Single source of truth (`docs/`); READMEs and Wiki are generated, never hand-edited.
- [x] Three surfaces are self-contained: no cross-surface links; diagrams embedded, not linked.
- [x] Comprehensive, hierarchically-numbered coverage from overview to specific (§4 tree).
- [x] Each scenario's notebooks get a comprehensive numbered doc, linked from the first page, in all three surfaces.
- [x] Diagrams generated once as landscape SVG, embedded in all three surfaces under the correct section.
- [x] `.io` site is clean, sleek, minimalistic, modern, sci-fi-esque (Orbital), dark default + light toggle, nice font + accent.
- [x] One `mkdocs`-based mechanism with a deterministic generator keeping everything in sync; CI asserts sync.
- [x] infra/ submodule excluded.
- [x] Every audit finding (§8) has a concrete fix.
