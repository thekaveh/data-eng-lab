# Scenario Execution-Mode Matrix Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Define, validate, and publish one truthful execution contract for each of the 19 scenarios while removing every discoverable no-op scenario DAG.

**Architecture:** A closed-schema YAML file is the execution-mode authority. A typed Python module validates it against scenario directories and real production DAGs, renders a deterministic manifest-owned Markdown table, and exposes a verifier/docs check. Public READMEs, canonical pages, indexes, and diagrams are corrected to the same reviewed row contracts.

**Tech Stack:** Python 3.11, PyYAML, pytest, Ruff, Airflow DAG static/import contracts, MkDocs Material, existing three-surface documentation scripts.

## Global Constraints

- Work only on `codex/82-scenario-execution-matrix`, based on `origin/develop` commit `8b3a31a11b3a3503e67ab7f7ecb872b4839ce0ef`.
- Do not implement any child production DAG in this branch.
- Use only the five classification strings approved in issue #82 and the design.
- Preserve exactly 19 paired-notebook scenario directories and exactly two production Spark-app DAGs.
- Delete all 19 scenario-local no-op DAGs; do not replace them with inert Python stubs.
- Preserve the `spark-apps/` Airflow mount and prove both production DAGs remain importable and discoverable.
- Use strict RED then GREEN TDD and capture the failing and passing commands in `.superpowers/sdd/issue-82-report.md` without staging that report.
- Edit canonical documentation only; regenerate and check site/wiki projections through the existing three-surface pipeline.
- Do not modify Atlas source or the `infra` gitlink; nested Atlas status remains clean.
- Never touch or stage `docs/superpowers/plans/2026-07-21-atlas-submodule-modernization.md`; its SHA-256 remains `f7eb55036e3020f419d9acb42bbc502bfe0528219a980a6973ed083591d2ba66`.
- Do not change `uv.lock`; its SHA-256 remains `a376ce1b5bd5621290aaded68c22572690395419876da41814e28469bb4186b1`.

---

## File and responsibility map

| Path | Responsibility |
|---|---|
| `scenarios/execution-modes.yaml` | Canonical 19-row execution-mode inventory |
| `scripts/scenario_execution.py` | Closed-schema parsing, semantic validation, Markdown rendering, CLI check/render |
| `tests/scenarios/test_execution_modes.py` | Matrix schema, counts, directory, DAG, child, mount, projection, and public-claim contracts |
| `scripts/verify_repo.py` | Aggregate matrix validation |
| `scripts/new_scenario.py` | Notebook-first scaffolding with no placeholder DAG option |
| `docs/scenarios/execution-modes.md` | Deterministic committed projection for repo/site/wiki |
| `docs/manifest.yaml` | Three-surface page ownership and navigation |
| `scenarios/*/README.md`, `docs/scenarios/*.md` | Scenario-local and canonical execution guidance |
| `docs/diagrams/*.html`, `docs/diagrams/img/*` | Correct execution labels and regenerated projections |
| `README.md`, `docs/scenarios/index.md`, `docs/notebooks/index.md`, `docs/CHANGELOG.md` | Public inventory and navigation |

### Task 1: Freeze the executable matrix contract

**Files:**
- Create: `tests/scenarios/test_execution_modes.py`
- Create: `scenarios/execution-modes.yaml`
- Create: `scripts/scenario_execution.py`
- Modify: `scripts/verify_repo.py`

**Interfaces:**
- Produces: `ExecutionModeError`, `ExecutionMode`, `load_execution_modes(path, root)`, `validate_execution_modes(modes, root)`, `render_markdown(modes)`, `check_projection(root)`

- [ ] **Step 1: Write RED tests** that import the wished-for loader and assert the exact 19-directory set, five-value classification enum, all required fields, count distribution `2/7/7/3/0`, exact existing entrypoints, unscheduled invariants, child requirements, two production DAGs, zero scenario DAGs/`EmptyOperator` runtime imports, and byte-identical Markdown projection.
- [ ] **Step 2: Run RED:** `uv run pytest tests/scenarios/test_execution_modes.py -q`; expect collection failure because `scripts.scenario_execution` and the matrix do not exist.
- [ ] **Step 3: Implement the minimal strict loader/validator/renderer** with frozen dataclasses, explicit key-set checks, safe relative paths, scenario-directory equality, classification-specific semantic checks, and stable row order.
- [ ] **Step 4: Populate all 19 approved rows** and run `uv run pytest tests/scenarios/test_execution_modes.py -q`; expected remaining failures are the current no-op DAG tree, missing children, projection, and public claims.

### Task 2: Remove no-op DAG discovery and create child boundaries

**Files:**
- Delete: `scenarios/*/dag.py` (exactly 19 files)
- Modify: `scripts/new_scenario.py`
- Modify: `tests/scenarios/test_new_scenario.py`
- Modify: `tests/test_atlas_usage_contract.py`
- Modify: `tests/test_dag_catalog_conf.py`
- Modify: `compose/data-eng-lab.yml` only if the now-empty scenario DAG mount is proven unnecessary

**Interfaces:**
- Consumes: Task 1 classifications and entrypoints
- Produces: notebook-only scenario scaffolding; exactly two mounted/discoverable production DAGs

- [ ] **Step 1: Extend RED** so default and CLI scaffolding never create `dag.py`, the obsolete DAG option is rejected, the scenario tree contains no DAG or `EmptyOperator`, the Spark-app mount remains, and both production DAGs pass the existing network-free import guard.
- [ ] **Step 2: Run RED:** `uv run pytest tests/scenarios/test_new_scenario.py tests/test_atlas_usage_contract.py tests/test_dag_catalog_conf.py -q`; expect failures on 19 scenario DAGs and scaffold behavior.
- [ ] **Step 3: Delete the 19 no-op DAGs and remove DAG generation/CLI flags** while preserving paired notebook scaffolding and verifier success.
- [ ] **Step 4: Create/reuse exactly five child issues**, add new children to Project #7 with Todo status, dependency/order/priority fields, link all five to #82 with GitHub sub-issue relationships when supported, and put the exact child numbers into the seven approved rows.
- [ ] **Step 5: Run GREEN:** the focused DAG/scaffold/matrix tests pass and both production DAGs remain the only Airflow artifacts.

### Task 3: Project the matrix and correct every public claim

**Files:**
- Create: `docs/scenarios/execution-modes.md`
- Modify: `docs/manifest.yaml`
- Modify: `README.md`
- Modify: `docs/scenarios/index.md`
- Modify: `docs/notebooks/index.md`
- Modify: `scenarios/*/README.md`
- Modify: `docs/scenarios/*.md`
- Modify: `docs/diagrams/*.html` and regenerated `docs/diagrams/img/*`
- Modify: `docs/CHANGELOG.md`
- Modify: documentation content-contract tests

**Interfaces:**
- Consumes: `render_markdown(modes)`
- Produces: one manifest-owned execution page and matching repo/site/wiki claims

- [ ] **Step 1: Extend RED** to reject published scenario DAG triggers except `nyc_taxi_etl` and `nyc_taxi_medallion`, `EmptyOperator` claims, Atlas #268 as a current blocker, missing matrix links, row/readme/page mismatches, stale diagram execution labels, and projection drift.
- [ ] **Step 2: Run RED:** focused documentation tests fail on the existing public claims.
- [ ] **Step 3: Render the matrix page and update canonical prose/diagram labels** so existing rows name real entrypoints, approved rows name children and no current DAG, notebook-only rows name paired notebooks, and continuous rows explicitly remain unscheduled.
- [ ] **Step 4: Regenerate diagrams/site/wiki** with the existing documentation commands; never hand-edit generated trees.
- [ ] **Step 5: Run GREEN:** focused matrix/content/docs tests pass with deterministic projections and strict links.

### Task 4: Verify and hand off the reviewed change

**Files:**
- Create ignored: `.superpowers/sdd/issue-82-report.md`
- Update ignored: `.superpowers/sdd/progress.md`

**Interfaces:**
- Consumes: Tasks 1-3
- Produces: evidence for independent spec and quality review

- [ ] **Step 1: Run focused gates:** matrix, scenario scaffolding, DAG catalog/import, docs content, manifest/build/check tests, Ruff, YAML/JSON parsing, and `git diff --check`.
- [ ] **Step 2: Run aggregate gates:** `make lint`, `make test`, `make verify`, `make docs-check`, `make docs-wiki`, and deterministic docs diff checks.
- [ ] **Step 3: Prove protected invariants:** historical plan and `uv.lock` hashes unchanged, Atlas gitlink unchanged and nested status clean, no generated trees staged.
- [ ] **Step 4: Stage exact issue files and commit:** `git commit -m "docs(scenarios): define execution-mode matrix (#82)"`.
- [ ] **Step 5: Record the commit, RED/GREEN commands, child issue links, and gate results** in the ignored report for independent review; do not push or open PRs in this task.
