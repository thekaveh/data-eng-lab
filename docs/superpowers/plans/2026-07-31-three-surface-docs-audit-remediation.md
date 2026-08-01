# Three-Surface Documentation Audit Remediation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Resolve every important and minor finding from the consolidated three-surface documentation audit, promote the verified work through `develop` and `main`, and leave both long-lived branches synchronized with local `develop` checked out.

**Architecture:** Keep `docs/*.md` as neutral, GitHub-compatible canonical Markdown and project it unchanged except for links and local image paths. Harden the existing manifest/build/check pipeline with content contracts, deterministic committed diagram publication, a live notebook-reproducibility entry point, and supported MkDocs dependency bounds. Do not edit `infra/` or change the Atlas submodule pin.

**Tech Stack:** Python 3.11, pytest, MkDocs Material 9.x, Markdown, GitHub Actions, Make, Git/GitHub CLI.

## Global Constraints

- Work only on `codex/fix-three-surface-docs-audit`, created from synchronized `develop`.
- Preserve `docs/superpowers/plans/2026-07-21-atlas-submodule-modernization.md` untracked and byte-identical.
- Preserve Atlas submodule pin `985918ce8c805081947d53b1c48bb80610237a5b`; never edit `infra/`.
- Canonical public pages must use Markdown that renders correctly in GitHub repository views, GitHub Wiki, and MkDocs.
- Build/deploy jobs must publish committed fingerprint-verified PNGs; they must not regenerate host-dependent PNG bytes.
- Use test-first changes and run the full offline and documentation gates before either PR promotion.
- Promote feature → `develop`, then `develop` → `main`; reconcile `develop` from `main`, clean merged/dangling branches and PRs, and finish on local `develop`.

---

### Task 1: Lock the Audit Findings into Executable Contracts

**Files:**
- Modify: `tests/test_docs_content_contract.py`
- Modify: `tests/scripts/docs/test_check_docs.py`
- Modify: `tests/test_makefile.py`
- Modify: `tests/scripts/docs/test_workflows.py`

**Interfaces:**
- Consumes: canonical `README.md`, manifest-owned Markdown, build workflow text, and Make targets.
- Produces: regression assertions for opener parity, executive-summary facts, neutral Markdown, streaming/dataset truth, onboarding order, current Atlas metadata, deterministic publication, heading/fence policy, and the notebook reproducibility gate.

- [ ] **Step 1: Write failing opener and cross-surface content tests**

  Assert that the canonical landing page has project H1 `# data-eng-lab`, the architecture image appears before its first H2, the opener contains `make up`, `atlas.consumer.yml`, `Data Engineering`, the development/default profile, and a grouped stack summary. Assert root and canonical taglines/summaries are equal after surface-relative links are normalized.

- [ ] **Step 2: Write failing semantic-truth tests**

  Assert that only the three broker-backed scenarios are described as Redpanda consumers; GH Archive incremental ingest is described as file-backed. Assert MovieLens is the fifth curated dataset and dataset-to-scenario mappings match the actual scenario directory names. Assert the Getting Started sequence launches the stack before `make datasets`, requires Java 17, and Atlas enablement says the repository is public and Airflow is 3.3.0.

- [ ] **Step 3: Write failing portability and publication tests**

  Reject `<div class="grid cards"`, `:material-`, `:octicons-`, `!!!`, and `===` in manifest-owned public Markdown. Reject unlabeled opening fences. Assert deployment and wiki targets do not pass `--force-png`; assert they run the docs gate before publication. Assert MkDocs and Material are bounded below their next major versions.

- [ ] **Step 4: Write the failing notebook-gate contract**

  Assert Make exposes `notebooks-reproducibility`, the live test enumerates exactly the 19 paired scenario folders, and the test executes both Zeppelin and Jupyter notebooks through the shared live execution helpers.

- [ ] **Step 5: Run focused tests and record RED**

  Run: `uv run --group dev pytest tests/test_docs_content_contract.py tests/scripts/docs/test_check_docs.py tests/test_makefile.py tests/scripts/docs/test_workflows.py -q`

  Expected: failures corresponding to the audit findings above, with no unrelated collection error.

---

### Task 2: Rebuild the Opener and Executive Summary as Neutral Markdown

**Files:**
- Modify: `README.md`
- Modify: `docs/index.md`
- Modify: `scripts/docs/check_docs.py`
- Modify: `tests/scripts/docs/test_check_docs.py`

**Interfaces:**
- Consumes: the existing `overview.png`, the Atlas consumer contract, and verified project inventory.
- Produces: a project-first opener that is physically copied to site/wiki and a numbering checker that permits the special project landing H1 while retaining baked numbering everywhere else.

- [ ] **Step 1: Promote the existing overview visual into the poster position**

  Place the architecture image immediately after `# data-eng-lab` on both repository surfaces and before the tagline/summary. Remove its duplicate placement below the Architecture H2.

- [ ] **Step 2: Replace the opener copy**

  Use one exact tagline and one exact executive-summary paragraph on both files. Name the Atlas submodule/manifest contract, one-command `make up`, default development profile, display name `Data Engineering`, dual notebook runtimes, Iceberg/MinIO, Airflow/Jenkins, Trino, and Redpanda without claiming the file-backed GH Archive scenario uses a broker.

- [ ] **Step 3: Add a self-contained grouped technology visual**

  Add a compact Markdown table grouped as platform, compute/notebooks, lakehouse/storage, orchestration/delivery, and query/streaming. This replaces remote badges and remains readable on all three surfaces.

- [ ] **Step 4: Replace Material-only navigation/admonitions**

  Replace grid cards with a normal Markdown table/list and block admonitions with blockquotes. Keep the same destinations using surface-appropriate relative links.

- [ ] **Step 5: Remove duplicate README catalogs and correct counts**

  Keep one scenario catalog and one Spark-app entry point. Describe the 19 scenarios as paired notebook implementations and separately state the accurate source modes (three Redpanda-backed streams plus one file-backed incremental stream) instead of the ungrounded 14/4/1 split.

- [ ] **Step 6: Special-case only the overview H1 in numbering checks**

  In `check_numbering`, expect `# data-eng-lab` for manifest id `overview`; retain `# {number}. {title}` for every other leaf. Add fixture coverage proving other unnumbered H1s still fail.

- [ ] **Step 7: Run focused tests and record GREEN**

  Run: `uv run --group dev pytest tests/test_docs_content_contract.py tests/scripts/docs/test_check_docs.py -q`

  Expected: opener, portability, parity, and numbering assertions pass.

---

### Task 3: Correct the Semantic Documentation Drift

**Files:**
- Modify: `docs/getting-started.md`
- Modify: `docs/scenarios/index.md`
- Modify: `docs/datasets.md`
- Modify: `docs/atlas-feedback-a7a9.md`
- Modify: `docs/atlas-enablement.md`
- Modify: affected manifest-owned Markdown containing tabs, admonitions, inconsistent H2 numbering, or unlabeled fences

**Interfaces:**
- Consumes: scenario READMEs, dataset registry, `atlas.consumer.yml`, POM release level, Atlas pin metadata, and current repository visibility.
- Produces: one internally consistent public documentation corpus.

- [ ] **Step 1: Make onboarding executable in reading order**

  Require Docker, Git, `uv`, and Java 17; start Atlas with `make up` before `make datasets`; use neutral headings/code blocks in place of MkDocs tabs; retain verification and shutdown steps.

- [ ] **Step 2: Correct streaming taxonomy**

  Name `streaming_ingest-events`, `streaming_windows-events`, and `cdc_streaming-online_retail` as Redpanda-backed. Name `streaming_ingest-gh_archive` as incremental file-source Structured Streaming with no broker dependency. Remove GH Archive from A9 validation claims.

- [ ] **Step 3: Correct dataset inventory and mappings**

  Include MovieLens in the five curated datasets. Restrict NYC Taxi, TPC-H, Online Retail, GH Archive, MovieLens, and synthetic Events to their real scenario lists; add schema evolution to GH Archive and remove event-topic scenarios from that dataset section.

- [ ] **Step 4: Refresh Atlas metadata**

  Mark the consumer repository public, preserve the reviewed pin text, and state Airflow 3.3.0 consistently with Atlas. Do not modify Atlas or its submodule pointer.

- [ ] **Step 5: Normalize headings and fences**

  Use document-local sequential H2 numbering throughout manifest-owned pages; label every fenced block (`bash`, `text`, `yaml`, `python`, `scala`, `sql`, or `json`). Preserve all H1 numbers except the project landing page.

- [ ] **Step 6: Run semantic and gate tests**

  Run: `uv run --group dev pytest tests/test_docs_content_contract.py tests/scripts/docs/test_check_docs.py -q`

  Expected: all semantic, heading, and fence contracts pass.

---

### Task 4: Make Diagram Publication Reproducible and MkDocs Supported

**Files:**
- Modify: `.github/workflows/docs-deploy.yml`
- Modify: `Makefile`
- Modify: `pyproject.toml`
- Modify: `uv.lock`
- Modify: `tests/scripts/docs/test_workflows.py`
- Modify: `tests/test_makefile.py`

**Interfaces:**
- Consumes: committed diagram PNGs and their renderer fingerprints.
- Produces: CI/site/wiki publication that validates but never rewrites committed PNG bytes, plus bounded MkDocs dependencies without the upstream MkDocs-2 warning.

- [ ] **Step 1: Remove forced PNG rendering from publication**

  Replace deploy-time `render_diagrams --force-png` with `python -m scripts.docs.check_docs --root .` and then surface builds. Remove forced rendering from `docs-wiki`; retain explicit local `--force-png` as the intentional maintainer-only refresh mechanism.

- [ ] **Step 2: Bound documentation dependencies**

  Set `mkdocs>=1.6,<2`, `mkdocs-material>=9.5,<10`, and the corresponding compatible plugin bounds. Refresh `uv.lock` through `uv lock`.

- [ ] **Step 3: Suppress only Material's acknowledged MkDocs-2 compatibility banner**

  Set the supported `NO_MKDOCS_2_WARNING=1` environment variable on local Make invocations and CI build steps; do not silence normal MkDocs warnings or remove `--strict`.

- [ ] **Step 4: Run publication/dependency tests**

  Run: `uv run --group dev pytest tests/scripts/docs/test_workflows.py tests/test_makefile.py -q`

  Expected: deterministic-publication and supported-version assertions pass.

---

### Task 5: Add the Separate Live Notebook Reproducibility Gate

**Files:**
- Create: `tests/scenarios/test_notebook_reproducibility_live.py`
- Modify: `Makefile`
- Modify: `tests/test_makefile.py`
- Modify: `docs/go-live.md`

**Interfaces:**
- Consumes: `tests/scenarios/live_exec.py::run_zeppelin_note`, `run_jupyter_note`, the 19 paired scenario directories, and a running Atlas data-eng stack.
- Produces: `make notebooks-reproducibility`, an opt-in `RUN_INFRA=1` live gate that executes both notebook formats for every scenario and a runbook entry documenting prerequisites and expected evidence.

- [ ] **Step 1: Write the parameterized live test**

  Discover exactly the 19 directories containing both notebook formats, assert the count, and parameterize execution. Mark the suite `infra`; skip unless `RUN_INFRA=1`; call both shared execution helpers for each scenario.

- [ ] **Step 2: Add the Make entry point**

  Add `notebooks-reproducibility` to `.PHONY` and run only the new suite with `RUN_INFRA=1 uv run --group live pytest ... -v`.

- [ ] **Step 3: Document the gate**

  In the go-live runbook, require `make up`, `make datasets`, namespace registration, and sufficient runtime; distinguish this exhaustive gate from the representative PR-safe offline suite.

- [ ] **Step 4: Verify test collection and offline skip behavior**

  Run: `uv run --group live pytest tests/scenarios/test_notebook_reproducibility_live.py --collect-only -q`

  Expected: 19 parameterized tests collected.

  Run: `uv run --group live pytest tests/scenarios/test_notebook_reproducibility_live.py -q`

  Expected: all 19 skipped without `RUN_INFRA=1`.

---

### Task 6: Verify All Three Surfaces and Repository Safety

**Files:**
- Verify only: all changed files, `infra`, generated projections, local user-owned plan

**Interfaces:**
- Consumes: completed Tasks 1–5.
- Produces: fresh evidence suitable for PR review and promotion.

- [ ] **Step 1: Run focused docs tests**

  Run: `uv run --group dev pytest tests/scripts/docs tests/test_docs_content_contract.py tests/test_makefile.py -q`

- [ ] **Step 2: Run full documentation gate**

  Run: `make docs-check`

  Expected: deterministic repo/site/wiki build and strict MkDocs build pass with no compatibility warning.

- [ ] **Step 3: Run full offline repository gates**

  Run: `make verify`, `make lint`, and `make test`.

  Expected: all commands exit 0.

- [ ] **Step 4: Inspect generated opener and asset parity**

  Confirm generated site `index.md` and wiki `Home.md` share the canonical H1/tagline/summary and neutral Markdown. Hash all generated wiki PNGs against `docs/diagrams/img/*.png` and require exact equality.

- [ ] **Step 5: Confirm protected state**

  Assert `git submodule status infra` still reports `985918ce8c805081947d53b1c48bb80610237a5b`; assert the user-owned plan SHA-256 remains `f7eb55036e3020f419d9acb42bbc502bfe0528219a980a6973ed083591d2ba66`; assert no `infra/` path is changed.

---

### Task 7: Review, Commit, Promote, Reconcile, and Clean Up

**Files:**
- Commit: only intentional feature-branch changes; explicitly exclude the user-owned untracked plan.

**Interfaces:**
- Consumes: verified feature branch and GitHub branch protections.
- Produces: merged feature → `develop` PR, merged `develop` → `main` PR, synchronized long-lived branches, no dangling merged feature branch/PR, and local `develop` checkout.

- [ ] **Step 1: Review the complete diff**

  Run `git diff --check`, inspect `git diff --stat`, inspect every changed path, and confirm no Atlas internal or pointer change.

- [ ] **Step 2: Commit and push the feature branch**

  Stage explicit paths only, commit with `docs: resolve three-surface audit findings`, and push `codex/fix-three-surface-docs-audit`.

- [ ] **Step 3: Open and merge feature → develop**

  Create a ready PR with audit context, findings resolved, verification evidence, Atlas-pin protection, and notebook-live-gate caveat. Wait for required checks, then merge using the repository's accepted method and delete the remote feature branch.

- [ ] **Step 4: Open and merge develop → main**

  Create the second ready PR only after the first merge and updated `origin/develop` verification. Wait for checks and merge without bypassing protections.

- [ ] **Step 5: Reconcile and clean**

  Fetch/prune; fast-forward local `main`; merge `origin/main` back into `develop` if the promotion method created a distinct merge commit; push only if reconciliation changed `develop`. Close only clearly obsolete/dangling PRs, delete only fully merged feature branches, and preserve unrelated work.

- [ ] **Step 6: Final-state proof**

  Require `origin/main` and `origin/develop` to have identical trees, no relevant open PRs or unmerged feature branches, clean tracked state, unchanged user plan hash, unchanged Atlas pin, and local branch `develop` checked out.
