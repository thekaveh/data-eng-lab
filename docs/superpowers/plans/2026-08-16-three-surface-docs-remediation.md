# Three-Surface Documentation Remediation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Correct every validated three-surface documentation finding and leave the canonical repository, generated MkDocs site, and GitHub wiki projections deterministic, self-contained, and synchronized.

**Architecture:** Preserve the existing canonical-source pipeline. Strengthen contracts at the source boundary (`build_docs.py`, opener constants/tests, diagram master, and workflow tests), regenerate only derived artifacts, and use the existing content-hash build to prove parity.

**Tech Stack:** Python 3.12, pytest, MkDocs Material, YAML GitHub Actions, HTML/SVG, CairoSVG, Ruff, GNU Make.

## Global Constraints

- `README.md`, canonical `docs/`, diagram HTML masters, and committed diagram PNGs are the only human-edited public documentation artifacts.
- `generated/site/`, `generated/wiki/`, root `mkdocs.yml`, and `site/` remain generated and ignored.
- Retain the isolated ephemeral `GITHUB_TOKEN` wiki publisher; do not add a deploy key or a second authentication path.
- Do not edit the pinned Atlas submodule or any runtime, scenario, DAG, Spark application, dataset, retention, observability-service, or release behavior.
- Every behavioral correction starts with a failing focused regression.
- The final worktree must pass strict MkDocs, docs/wiki generation, docs tests, repository verification, Ruff lint/format, diff checks, and the complete A–L audit.

---

## File map

- `scripts/docs/build_docs.py` — generated MkDocs metadata and navigation.
- `tests/scripts/docs/test_build_docs.py` — source-backed metadata regression.
- `README.md`, `docs/index.md` — hand-authored shared opener.
- `tests/test_docs_content_contract.py` — exact opener parity, grounding, and word-count contract.
- `docs/diagrams/overview.html` — canonical full-stack architecture master.
- `docs/diagrams/img/overview.png`, `docs/diagrams/img/overview.sha256` — committed raster and fingerprint.
- `tests/scripts/docs/test_render_diagrams.py` — overview content and authoring-leak regression.
- `.github/workflows/docs-deploy.yml` — isolated wiki publisher and explanatory contract comments.
- `tests/scripts/docs/test_workflows.py` — exact publisher authentication/permission boundary.
- `docs/superpowers/specs/2026-08-16-three-surface-docs-remediation-design.md` — approved design record.

### Task 1: Bind MkDocs metadata to the six application inventory

**Files:**
- Modify: `tests/scripts/docs/test_build_docs.py`
- Modify: `scripts/docs/build_docs.py:27-35`

**Interfaces:**
- Consumes: `render_mkdocs_yml(manifest) -> str` and repository `spark-apps/*/pom.xml` inventory.
- Produces: rendered `site_description` containing the exact six-app public claim.

- [x] **Step 1: Write the failing source-backed metadata test**

Add to `tests/scripts/docs/test_build_docs.py`:

```python
def test_repository_mkdocs_description_matches_maven_app_inventory():
    repo_root = Path(__file__).resolve().parents[3]
    manifest = load_manifest(repo_root / "docs/manifest.yaml", repo_root)
    app_count = len(tuple((repo_root / "spark-apps").glob("*/pom.xml")))

    assert app_count == 6
    assert f"{app_count} CI-built Maven apps" in render_mkdocs_yml(manifest)
```

- [x] **Step 2: Run the focused RED**

Run:

```bash
uv run --group dev pytest tests/scripts/docs/test_build_docs.py::test_repository_mkdocs_description_matches_maven_app_inventory -q
```

Expected: FAIL because the description contains `5 CI-built Maven apps`.

- [x] **Step 3: Correct the canonical template**

Change the description fragment in `scripts/docs/build_docs.py` to:

```python
  19 paired scenarios, 17 Scala/PySpark parity pairs, 6 CI-built Maven apps,
```

- [x] **Step 4: Run focused and module tests**

Run:

```bash
uv run --group dev pytest tests/scripts/docs/test_build_docs.py -q
```

Expected: all tests pass.

- [x] **Step 5: Commit the metadata correction**

```bash
git add scripts/docs/build_docs.py tests/scripts/docs/test_build_docs.py
git commit -m "docs: bind site metadata to app inventory"
```

### Task 2: Expand and ground the exact shared opener

**Files:**
- Modify: `tests/test_docs_content_contract.py:84-90,208-280`
- Modify: `README.md:36`
- Modify: `docs/index.md:36`

**Interfaces:**
- Consumes: `_opener_block`, `_executive_summary`, `HERO_EXECUTIVE_SUMMARY`, `atlas.consumer.yml`, and six `spark-apps/*/pom.xml` roots.
- Produces: one exact 100–150-word summary present on both hand-authored surfaces.

- [x] **Step 1: Add failing opener depth and inventory assertions**

Extend `test_opener_is_centered_badged_and_identical_across_canonical_surfaces`:

```python
    words = executive_summary.split()
    assert 100 <= len(words) <= 150
    assert "six CI-built Maven applications" in executive_summary
    assert "Prometheus and Grafana" in executive_summary
    assert len(tuple((ROOT / "spark-apps").glob("*/pom.xml"))) == 6
```

Extend the required-term tuple with `"six CI-built Maven applications"`, `"Prometheus"`, and `"Grafana"`.

- [x] **Step 2: Run the focused RED**

Run:

```bash
uv run --group dev pytest tests/test_docs_content_contract.py::test_opener_is_centered_badged_and_identical_across_canonical_surfaces -q
```

Expected: FAIL because the existing summary is 53 words and omits the new grounded terms.

- [x] **Step 3: Define the expanded canonical summary**

Replace `HERO_EXECUTIVE_SUMMARY` and the identical paragraph in both public sources with this exact copy:

```text
`data-eng-lab` consumes Atlas as its pinned `infra/` git submodule through `atlas.consumer.yml`, so `make up` launches the default development profile as the **Data Engineering** workspace. The lab integrates storage, compute, orchestration, delivery, and observability instead of leaving users to wire independent services together: Iceberg tables live on MinIO, Spark runs batch and streaming workloads, Airflow coordinates production DAGs, Jenkins publishes six CI-built Maven applications, Trino serves analytical SQL, and Prometheus and Grafana monitor the Iceberg REST boundary. Nineteen paired Zeppelin and Jupyter scenarios provide 17 Scala/PySpark implementations plus two Trino client pairs, while Redpanda supplies three broker-backed streams. The same locked datasets and catalog contracts support notebook exploration and deployable application paths.
```

- [x] **Step 4: Run opener and complete content-contract tests**

Run:

```bash
uv run --group dev pytest tests/test_docs_content_contract.py -q
```

Expected: all tests pass and exact opener parity remains intact.

- [x] **Step 5: Commit the opener correction**

```bash
git add README.md docs/index.md tests/test_docs_content_contract.py
git commit -m "docs: strengthen the shared project opener"
```

### Task 3: Bring the full-stack diagram into observability parity

**Files:**
- Modify: `tests/scripts/docs/test_render_diagrams.py`
- Modify: `docs/diagrams/overview.html`
- Regenerate: `docs/diagrams/img/overview.png`
- Regenerate: `docs/diagrams/img/overview.sha256`

**Interfaces:**
- Consumes: `extract_svg(html) -> str`, `scripts.docs.render_diagrams`, the fixed labels `iceberg-rest-probe`, `Prometheus`, and `Grafana`.
- Produces: accessible overview SVG/PNG showing probe→metrics→dashboard flow with no authoring metadata.

- [x] **Step 1: Write the failing diagram-content test**

Add to `tests/scripts/docs/test_render_diagrams.py`:

```python
def test_overview_diagram_includes_observability_and_no_authoring_metadata():
    root = Path(__file__).resolve().parents[3]
    svg = extract_svg((root / "docs/diagrams/overview.html").read_text(encoding="utf-8"))

    for label in ("iceberg-rest-probe", "Prometheus", "Grafana", "metrics", "dashboard"):
        assert label in svg
    for leaked in ("Orbital theme", "landscape"):
        assert leaked not in svg
```

- [x] **Step 2: Run the focused RED**

Run:

```bash
uv run --group dev pytest tests/scripts/docs/test_render_diagrams.py::test_overview_diagram_includes_observability_and_no_authoring_metadata -q
```

Expected: FAIL because observability labels are absent and authoring labels are present.

- [x] **Step 3: Update the canonical SVG master**

Revise `docs/diagrams/overview.html` while preserving its accessible `<title>`/`<desc>` and existing flows. Use the right-side area for an observability group with these semantic elements:

```html
<text ...>Observability</text>
<rect .../>
<text ...>iceberg-rest-probe</text>
<text ...>bounded catalog probe</text>
<rect .../>
<text ...>Prometheus</text>
<text ...>30s scrape · alert rules</text>
<rect .../>
<text ...>Grafana</text>
<text ...>dashboard · alert view</text>
<path ... marker-end="url(#ah-cyan)"/>
<text ...>metrics</text>
<path ... marker-end="url(#ah-cyan)"/>
<text ...>dashboard</text>
```

Add a dashed probe edge from Iceberg REST to `iceberg-rest-probe`, a metrics edge to Prometheus, and a datasource/dashboard edge to Grafana. Update the verification comment to 2026-08-16 and include `docs/iceberg-rest-observability.md`. Remove both `Orbital theme`/`landscape` text nodes.

- [x] **Step 4: Run the focused GREEN**

Run:

```bash
uv run --group dev pytest tests/scripts/docs/test_render_diagrams.py -q
```

Expected: all diagram tests pass.

- [x] **Step 5: Regenerate the committed projection**

Run:

```bash
uv run --group dev python -m scripts.docs.render_diagrams --root . --force-png
```

Expected: `overview.png` and `overview.sha256` change; unrelated PNG bytes remain unchanged.

- [x] **Step 6: Visually inspect the regenerated overview**

Open `docs/diagrams/img/overview.png` and verify labels do not overlap, every arrow has an unambiguous direction, and the existing data/metadata/retention flows remain legible.

- [x] **Step 7: Commit the diagram correction**

```bash
git add docs/diagrams/overview.html docs/diagrams/img/overview.png docs/diagrams/img/overview.sha256 tests/scripts/docs/test_render_diagrams.py
git commit -m "docs: add observability to the full-stack diagram"
```

### Task 4: Codify the ephemeral wiki publisher contract

**Files:**
- Modify: `tests/scripts/docs/test_workflows.py:162-202`
- Modify: `.github/workflows/docs-deploy.yml:104-127`
- Modify: `docs/superpowers/specs/2026-08-16-three-surface-docs-remediation-design.md`

**Interfaces:**
- Consumes: parsed `docs-deploy.yml` and exact privileged `wiki` job.
- Produces: one explicit repository-specific publisher contract with no SSH/deploy-key fallback.

- [x] **Step 1: Add a failing exact authentication-boundary assertion**

Extend `test_publish_workflow_pushes_generated_wiki_after_pages_deploy`:

```python
    assert wiki["permissions"] == {"contents": "write"}
    assert push_step["env"] == {
        "WIKI_REMOTE": (
            "https://x-access-token:${{ secrets.GITHUB_TOKEN }}@github.com/"
            "${{ github.repository }}.wiki.git"
        )
    }
    workflow_text = (WORKFLOWS / "docs-deploy.yml").read_text(encoding="utf-8")
    assert "Ephemeral repository-scoped publisher" in workflow_text
    assert "WIKI_DEPLOY_KEY" not in workflow_text
    assert "WIKI_SSH_KEY" not in workflow_text
```

- [x] **Step 2: Run the focused RED**

Run:

```bash
uv run --group dev pytest tests/scripts/docs/test_workflows.py::test_publish_workflow_pushes_generated_wiki_after_pages_deploy -q
```

Expected: FAIL because the approved publisher rationale is not recorded in the workflow.

- [x] **Step 3: Document the isolated publisher at the enforcement point**

Add immediately above the `wiki` job:

```yaml
  # Ephemeral repository-scoped publisher: this isolated job receives only
  # contents:write, verifies the privileged script digest, and intentionally
  # uses GITHUB_TOKEN instead of a persistent deploy key or fallback path.
```

Update the design status to `Implemented` only after all verification passes; until then retain the review status.

- [x] **Step 4: Run workflow and push-wiki tests**

Run:

```bash
uv run --group dev pytest tests/scripts/docs/test_workflows.py tests/scripts/docs/test_push_wiki.py -q
```

Expected: all tests pass.

- [x] **Step 5: Commit the publisher contract**

```bash
git add .github/workflows/docs-deploy.yml tests/scripts/docs/test_workflows.py docs/superpowers/specs/2026-08-16-three-surface-docs-remediation-design.md
git commit -m "docs: codify the ephemeral wiki publisher"
```

### Task 5: Regenerate, audit, and close every remaining finding

**Files:**
- Modify only if a newly reproduced in-scope finding requires it.
- Update: `docs/superpowers/specs/2026-08-16-three-surface-docs-remediation-design.md`
- Update: this plan's checkboxes as work completes.

**Interfaces:**
- Consumes: final canonical sources and all repository docs gates.
- Produces: clean generated site/wiki trees and an evidence-backed final audit.

- [x] **Step 1: Run focused documentation suites**

```bash
uv run --group dev pytest tests/scripts/docs tests/test_docs_content_contract.py -q
```

Expected: all tests pass.

- [x] **Step 2: Run canonical generation and strict builds**

```bash
make docs-check
uv run --group dev mkdocs build --strict
make docs-wiki
```

Expected: all commands exit 0; MkDocs emits zero warnings.

- [x] **Step 3: Run repository hygiene gates**

```bash
make verify
uv run ruff check scripts/docs tests/scripts/docs tests/test_docs_content_contract.py
uv run ruff format --check scripts/docs tests/scripts/docs tests/test_docs_content_contract.py
git diff --check
```

Expected: zero findings and no formatting drift.

- [x] **Step 4: Run structural A–H and K–L scans**

Verify all generated roots are ignored; root `mkdocs.yml` is untracked; no cross-surface origins, placeholders, empty files/directories, flat notebook subsections, adjacent duplicate headings, empty fences, authoring prose, or missing diagram projections exist. Confirm all 24 masters have repo PNG, site SVG, and wiki PNG files.

- [x] **Step 5: Audit all changed claims and diagram elements against source**

Confirm six POM roots, 19 paired scenario roots, `atlas.consumer.yml`'s Data Engineering/dev/Prometheus/Grafana values, the Iceberg probe Compose service, Prometheus scrape interval, rule loading, Grafana datasource/dashboard provisioning, and every changed diagram edge.

- [x] **Step 6: Address additional findings through microscopic TDD**

For each newly reproduced defect, add one focused failing regression, run it to RED, apply the smallest canonical-source correction, rerun to GREEN, and include the changed file in the final verification. Do not alter behavior based on a speculative or ungrounded concern.

- [x] **Step 7: Mark the design implemented and run final verification again**

Change the design status to `Implemented`, then repeat Tasks 5.1–5.3. Expected: all green on the final bytes.

- [x] **Step 8: Commit final evidence/hygiene changes**

```bash
git add docs/superpowers/specs/2026-08-16-three-surface-docs-remediation-design.md \
  docs/superpowers/plans/2026-08-16-three-surface-docs-remediation.md
git commit -m "docs: complete three-surface remediation"
```

- [x] **Step 9: Prepare completion evidence**

Record exact HEAD, clean status, test counts, strict-build result, diagram projection counts, and the post-change audit result. Explicitly state that the live 19-scenario notebook reproducibility suite was not run because notebook execution/content was unchanged and it requires a prepared live stack.
