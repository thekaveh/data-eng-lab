# data-eng-lab — Phase 2a: Scenario Framework & Scaffolding — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make adding a scenario **enforced and mechanical** — extend the repo verifier with scenario-structure checks, add a scenario **scaffolder** that generates a conventional scenario folder (6-section README + valid Zeppelin `.zpln` + valid Jupyter `.ipynb` + optional DAG), a **cross-language parity** test harness, and seed the first exemplar scenario. All **CI-green now** (structural, off-stack); the scenarios' live Spark logic is filled in Phase 2b once Atlas delivers A1–A4.

**Architecture:** The scaffolder (`scripts/new_scenario.py`) generates notebook JSON programmatically (`.ipynb` via `nbformat`, `.zpln` as a Zeppelin-format dict) so no large JSON is hand-authored. The verifier (`scripts/verify_repo.py`) auto-discovers `scenarios/*/` folders and validates naming, required files, README sections, and notebook-JSON validity. The parity harness is a framework (unit-tested with fakes; live comparison is `@pytest.mark.infra`). One exemplar scenario is scaffolded and committed so the verifier has a real target and contributors have a copy-me pattern.

**Tech Stack:** Python 3.11, `uv`, `ruff`, `pytest`, `nbformat`, `PyYAML`.

## Global Constraints

- **Never edit anything under `infra/`** (the Atlas submodule).
- **Scenario convention:** flat folders `scenarios/<pattern>-<dataset>-<engine>-<format>/`, each with `README.md` (6 sections), `zeppelin/notebook.zpln`, `jupyter/notebook.ipynb`, and an optional `dag.py`. Names match `^[a-z0-9_]+-[a-z0-9_]+-[a-z0-9_]+-[a-z0-9_]+$`.
- **README 6 sections (exact H2 titles):** `1. Scenario summary`, `2. Why this exists`, `3. What's in the notebooks`, `4. How to run`, `5. Data & dependencies`, `6. Known issues & caveats`.
- **Notebook 6 sections (both `.zpln` and `.ipynb`, as headings):** `1. Overview`, `2. Setup`, `3. Read`, `4. Transform`, `5. Write`, `6. Verify`.
- **This phase is structural/CI-green** — no live stack needed. The exemplar scenario's Spark code is representative/minimal (authored against the assumed contract); its *execution* is a Phase 2b concern. Do not add any `@pytest.mark.infra` test that must pass here.
- **Python 3.11**, ruff `line-length = 120`, `select = ["E","F","W","I"]`. New deps in `[dependency-groups] dev`. Package imports for modules with dataclasses (never importlib-by-path for those).
- **Branch/PR:** `main` is protected; land via feature branch → PR with `static-and-unit` green.

## File Structure

- `scripts/verify_repo.py` — replace the two config-driven scenario checks with auto-discovery checks (`_discover_scenarios`, `_check_scenarios`).
- `scripts/verify_repo_config.yaml` — `scenario_name_regex`, `scenario_required_files`, `scenario_readme_sections`, `notebook_sections` (drop `active_scenario_dirs`).
- `scripts/new_scenario.py` — the scaffolder + `nbformat` dep.
- `scenarios/batch_ingest-nyc_taxi-spark-iceberg/` — the first exemplar (scaffolded).
- `tests/scenarios/` — parity harness (`parity.py`) + its unit test.
- `Makefile` (`new-scenario` target), `CONTRIBUTING.md` (adding-a-scenario section), `docs/scenarios.md`.

---

### Task 1: Scenario-structure verifier checks (auto-discovery)

**Files:**
- Modify: `scripts/verify_repo.py`, `scripts/verify_repo_config.yaml`
- Modify: `tests/test_verify_repo.py`

**Interfaces:**
- Produces: `_discover_scenarios(root: Path) -> list[str]` (dir names under `scenarios/` excluding those starting with `.` or `_`); `_check_scenarios(root, cfg) -> list[Finding]` — for each discovered scenario emits `error` Findings for: name violating `scenario_name_regex` (`scenario.naming`), any missing `scenario_required_files` (`scenario.files`), any missing README section (`scenario.readme`), and any `.zpln`/`.ipynb` that is not valid JSON (`scenario.notebook_json`).

- [ ] **Step 1: Write the failing test**

Replace the scenario-related tests in `tests/test_verify_repo.py` (keep `test_registry_check_flags_invalid_registry`) with:

```python
def _make_valid_scenario(root: Path, name: str = "batch_ingest-nyc_taxi-spark-iceberg"):
    d = root / "scenarios" / name
    (d / "zeppelin").mkdir(parents=True)
    (d / "jupyter").mkdir(parents=True)
    sections = ["1. Scenario summary", "2. Why this exists", "3. What's in the notebooks",
                "4. How to run", "5. Data & dependencies", "6. Known issues & caveats"]
    (d / "README.md").write_text("# t\n\n" + "\n".join(f"## {s}\n\ntext\n" for s in sections))
    (d / "zeppelin" / "notebook.zpln").write_text('{"paragraphs": []}')
    (d / "jupyter" / "notebook.ipynb").write_text('{"cells": [], "nbformat": 4, "nbformat_minor": 5}')
    return d


CFG = {
    "scenario_name_regex": r"^[a-z0-9_]+-[a-z0-9_]+-[a-z0-9_]+-[a-z0-9_]+$",
    "scenario_required_files": ["README.md", "zeppelin/notebook.zpln", "jupyter/notebook.ipynb"],
    "scenario_readme_sections": ["1. Scenario summary", "2. Why this exists", "3. What's in the notebooks",
                                 "4. How to run", "5. Data & dependencies", "6. Known issues & caveats"],
}


def test_valid_scenario_passes(tmp_path: Path):
    _make_valid_scenario(tmp_path)
    errors = [f for f in verify_repo.run_checks(tmp_path, CFG) if f.severity == "error"]
    assert errors == [], errors


def test_bad_scenario_name_flags_error(tmp_path: Path):
    _make_valid_scenario(tmp_path, "BadName")
    findings = verify_repo.run_checks(tmp_path, CFG)
    assert any(f.check == "scenario.naming" and f.severity == "error" for f in findings), findings


def test_missing_notebook_flags_error(tmp_path: Path):
    d = _make_valid_scenario(tmp_path)
    (d / "jupyter" / "notebook.ipynb").unlink()
    findings = verify_repo.run_checks(tmp_path, CFG)
    assert any(f.check == "scenario.files" and f.severity == "error" for f in findings), findings


def test_missing_readme_section_flags_error(tmp_path: Path):
    d = _make_valid_scenario(tmp_path)
    (d / "README.md").write_text("# t\n\n## 1. Scenario summary\n\ntext\n")  # only 1 of 6
    findings = verify_repo.run_checks(tmp_path, CFG)
    assert any(f.check == "scenario.readme" and f.severity == "error" for f in findings), findings


def test_invalid_notebook_json_flags_error(tmp_path: Path):
    d = _make_valid_scenario(tmp_path)
    (d / "zeppelin" / "notebook.zpln").write_text("{not json")
    findings = verify_repo.run_checks(tmp_path, CFG)
    assert any(f.check == "scenario.notebook_json" and f.severity == "error" for f in findings), findings


def test_no_scenarios_dir_is_ok(tmp_path: Path):
    errors = [f for f in verify_repo.run_checks(tmp_path, CFG) if f.severity == "error"]
    assert errors == []
```

Remove the old `test_valid_scenario_name_passes`, `test_bad_scenario_name_flags_error` (old form), and `test_missing_declared_dir_flags_error` tests.

- [ ] **Step 2: Run to verify RED**

Run: `uv run pytest tests/test_verify_repo.py -q`
Expected: FAIL — the new checks don't exist yet.

- [ ] **Step 3: Refactor the verifier**

In `scripts/verify_repo.py`, remove `_check_naming` and `_check_declared_dirs_exist`, and add:

```python
def _discover_scenarios(root: Path) -> list[str]:
    base = root / "scenarios"
    if not base.is_dir():
        return []
    return sorted(
        p.name for p in base.iterdir()
        if p.is_dir() and not p.name.startswith((".", "_"))
    )


def _check_scenarios(root: Path, cfg: dict) -> list[Finding]:
    import json as _json

    findings: list[Finding] = []
    regex = re.compile(cfg["scenario_name_regex"])
    required = cfg.get("scenario_required_files", [])
    sections = cfg.get("scenario_readme_sections", [])
    for name in _discover_scenarios(root):
        sdir = root / "scenarios" / name
        if not regex.fullmatch(name):
            findings.append(Finding("scenario.naming", "error",
                                    f"scenario '{name}' violates the naming convention"))
        for rel in required:
            if not (sdir / rel).exists():
                findings.append(Finding("scenario.files", "error",
                                        f"scenario '{name}' is missing '{rel}'"))
        readme = sdir / "README.md"
        if readme.exists():
            text = readme.read_text(encoding="utf-8")
            for sec in sections:
                if f"## {sec}" not in text:
                    findings.append(Finding("scenario.readme", "error",
                                            f"scenario '{name}' README missing section '## {sec}'"))
        for rel in required:
            if rel.endswith((".zpln", ".ipynb")) and (sdir / rel).exists():
                try:
                    _json.loads((sdir / rel).read_text(encoding="utf-8"))
                except Exception as exc:  # noqa: BLE001
                    findings.append(Finding("scenario.notebook_json", "error",
                                            f"scenario '{name}' file '{rel}' is not valid JSON: {exc}"))
    return findings
```

Update the registry: `CHECKS = [_check_scenarios, _check_dataset_registry]`.

Update `scripts/verify_repo_config.yaml`:

```yaml
# Single edit point for repo-structure verification.
scenario_name_regex: "^[a-z0-9_]+-[a-z0-9_]+-[a-z0-9_]+-[a-z0-9_]+$"
scenario_required_files:
  - README.md
  - zeppelin/notebook.zpln
  - jupyter/notebook.ipynb
scenario_readme_sections:
  - "1. Scenario summary"
  - "2. Why this exists"
  - "3. What's in the notebooks"
  - "4. How to run"
  - "5. Data & dependencies"
  - "6. Known issues & caveats"
notebook_sections:
  - "1. Overview"
  - "2. Setup"
  - "3. Read"
  - "4. Transform"
  - "5. Write"
  - "6. Verify"
```

- [ ] **Step 4: Run tests + verifier + lint**

Run: `uv run pytest tests/test_verify_repo.py -q` → PASS.
Run: `uv run python scripts/verify_repo.py --root .` → `0 error(s)`, exit 0 (no scenarios yet).
Run: `uv run pytest -m "not infra" -q` and `uv run ruff check .` → green.

- [ ] **Step 5: Commit**

```bash
git add scripts/verify_repo.py scripts/verify_repo_config.yaml tests/test_verify_repo.py
git commit -m "feat(verify): auto-discover + validate scenario structure (naming/files/readme/notebook-json)"
```

---

### Task 2: Scenario scaffolder (`scripts/new_scenario.py`)

**Files:**
- Create: `scripts/new_scenario.py`
- Modify: `pyproject.toml` (add `nbformat`)
- Test: `tests/scenarios/__init__.py`, `tests/scenarios/test_new_scenario.py`

**Interfaces:**
- Produces:
  - `readme_text(name: str) -> str` (a 6-section README)
  - `zeppelin_notebook(name: str) -> dict` (valid Zeppelin `.zpln` with the 6 notebook sections as `%md` + `%spark` paragraphs)
  - `jupyter_notebook(name: str) -> nbformat.NotebookNode` (6 markdown sections + representative PySpark code cells)
  - `scaffold(root: Path, name: str, with_dag: bool = True) -> Path` (writes the folder; raises `ValueError` on a bad name or if it exists)
  - `main()` CLI: `new_scenario.py <name> [--no-dag]`

- [ ] **Step 1: Add the nbformat dependency**

In `pyproject.toml` `[dependency-groups] dev`, add `"nbformat>=5.9"`. Then `uv sync`.

- [ ] **Step 2: Write the failing test**

Create `tests/scenarios/__init__.py` (empty) and `tests/scenarios/test_new_scenario.py`:

```python
import importlib.util
import json
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
_spec = importlib.util.spec_from_file_location("new_scenario", ROOT / "scripts" / "new_scenario.py")
ns = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(ns)

NAME = "batch_ingest-nyc_taxi-spark-iceberg"


def test_scaffold_creates_valid_structure(tmp_path: Path):
    d = ns.scaffold(tmp_path, NAME, with_dag=True)
    assert (d / "README.md").exists()
    assert (d / "zeppelin" / "notebook.zpln").exists()
    assert (d / "jupyter" / "notebook.ipynb").exists()
    assert (d / "dag.py").exists()
    # both notebooks are valid JSON
    json.loads((d / "zeppelin" / "notebook.zpln").read_text())
    json.loads((d / "jupyter" / "notebook.ipynb").read_text())


def test_scaffold_output_passes_the_verifier(tmp_path: Path):
    ns.scaffold(tmp_path, NAME)
    vspec = importlib.util.spec_from_file_location("verify_repo", ROOT / "scripts" / "verify_repo.py")
    verify = importlib.util.module_from_spec(vspec)
    vspec.loader.exec_module(verify)
    import yaml
    cfg = yaml.safe_load((ROOT / "scripts" / "verify_repo_config.yaml").read_text())
    errors = [f for f in verify.run_checks(tmp_path, cfg) if f.severity == "error"]
    assert errors == [], errors


def test_scaffold_rejects_bad_name(tmp_path: Path):
    with pytest.raises(ValueError):
        ns.scaffold(tmp_path, "BadName")


def test_scaffold_refuses_overwrite(tmp_path: Path):
    ns.scaffold(tmp_path, NAME)
    with pytest.raises(ValueError):
        ns.scaffold(tmp_path, NAME)


def test_no_dag_flag(tmp_path: Path):
    d = ns.scaffold(tmp_path, NAME, with_dag=False)
    assert not (d / "dag.py").exists()
```

- [ ] **Step 3: Run to verify RED**

Run: `uv run pytest tests/scenarios/test_new_scenario.py -q`
Expected: FAIL — `new_scenario.py` missing.

- [ ] **Step 4: Implement `scripts/new_scenario.py`**

```python
#!/usr/bin/env python3
"""Scaffold a conventional scenario folder (README + Zeppelin .zpln + Jupyter .ipynb + optional DAG)."""
from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path

import nbformat

NAME_RE = re.compile(r"^[a-z0-9_]+-[a-z0-9_]+-[a-z0-9_]+-[a-z0-9_]+$")

README_SECTIONS = [
    "1. Scenario summary", "2. Why this exists", "3. What's in the notebooks",
    "4. How to run", "5. Data & dependencies", "6. Known issues & caveats",
]
NB_SECTIONS = ["1. Overview", "2. Setup", "3. Read", "4. Transform", "5. Write", "6. Verify"]


def readme_text(name: str) -> str:
    body = "\n".join(f"## {s}\n\n_TODO (Phase 2b)_\n" for s in README_SECTIONS)
    return f"# {name}\n\n> Scaffolded scenario. Fill the notebook logic in Phase 2b.\n\n{body}"


def _scala_cell(section: str) -> str:
    # Zeppelin `%spark` paragraphs are SCALA — use Scala placeholders.
    return {
        "2. Setup": "// spark is pre-bound by the Atlas Zeppelin interpreter\nspark.version",
        "3. Read": '// val df = spark.read.parquet("s3a://landing/nyc_taxi/")',
        "4. Transform": "// TODO (Phase 2b): scenario transform",
        "5. Write": '// df.writeTo("lakehouse.bronze.nyc_taxi").using("iceberg").createOrReplace()',
        "6. Verify": '// spark.table("lakehouse.bronze.nyc_taxi").count()',
    }.get(section, "// TODO (Phase 2b)")


def _py_cell(section: str) -> str:
    # Jupyter cells are PySpark (Python).
    return {
        "3. Read": 'df = spark.read.parquet("s3a://landing/nyc_taxi/")',
        "4. Transform": "# TODO (Phase 2b): scenario transform",
        "5. Write": '# df.writeTo("lakehouse.bronze.nyc_taxi").using("iceberg").createOrReplace()',
        "6. Verify": '# spark.table("lakehouse.bronze.nyc_taxi").count()',
    }.get(section, "# TODO (Phase 2b)")


def zeppelin_notebook(name: str) -> dict:
    paragraphs = []
    for sec in NB_SECTIONS:
        paragraphs.append({"title": sec, "text": f"%md\n## {sec}", "config": {}, "settings": {"params": {}, "forms": {}}})
        if sec != "1. Overview":
            paragraphs.append({"title": f"{sec} (code)", "text": f"%spark\n{_scala_cell(sec)}",
                               "config": {}, "settings": {"params": {}, "forms": {}}})
    return {"paragraphs": paragraphs, "name": name, "id": name, "noteParams": {}, "config": {}, "info": {}}


def jupyter_notebook(name: str) -> nbformat.NotebookNode:
    nb = nbformat.v4.new_notebook()
    nb.cells.append(nbformat.v4.new_markdown_cell(f"# {name}"))
    for sec in NB_SECTIONS:
        nb.cells.append(nbformat.v4.new_markdown_cell(f"## {sec}"))
        if sec == "2. Setup":
            nb.cells.append(nbformat.v4.new_code_cell(
                "from pyspark.sql import SparkSession\n"
                'spark = SparkSession.builder.remote("sc://spark-connect:15002").getOrCreate()'))
        elif sec != "1. Overview":
            nb.cells.append(nbformat.v4.new_code_cell(_py_cell(sec)))
    nb.metadata["language_info"] = {"name": "python"}
    return nb


def _dag_text(name: str) -> str:
    return (
        '"""Airflow DAG for the ' + name + ' scenario (Phase 2b)."""\n'
        "from __future__ import annotations\n\n"
        "# TODO (Phase 2b): define the DAG that orchestrates this scenario.\n"
    )


def scaffold(root: Path, name: str, with_dag: bool = True) -> Path:
    if not NAME_RE.fullmatch(name):
        raise ValueError(f"scenario name '{name}' must match {NAME_RE.pattern}")
    d = Path(root) / "scenarios" / name
    if d.exists():
        raise ValueError(f"scenario '{name}' already exists at {d}")
    (d / "zeppelin").mkdir(parents=True)
    (d / "jupyter").mkdir(parents=True)
    (d / "README.md").write_text(readme_text(name), encoding="utf-8")
    (d / "zeppelin" / "notebook.zpln").write_text(json.dumps(zeppelin_notebook(name), indent=2), encoding="utf-8")
    nbformat.write(jupyter_notebook(name), str(d / "jupyter" / "notebook.ipynb"))
    if with_dag:
        (d / "dag.py").write_text(_dag_text(name), encoding="utf-8")
    return d


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="Scaffold a new scenario folder.")
    ap.add_argument("name", help="scenario name: <pattern>-<dataset>-<engine>-<format>")
    ap.add_argument("--no-dag", action="store_true")
    ap.add_argument("--root", default=".")
    args = ap.parse_args(argv)
    d = scaffold(Path(args.root), args.name, with_dag=not args.no_dag)
    print(f"scaffolded {d}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
```

- [ ] **Step 5: Run tests + lint**

Run: `uv run pytest tests/scenarios/test_new_scenario.py -q` → PASS (incl. the verifier round-trip test).
Run: `uv run pytest -m "not infra" -q` and `uv run ruff check .` → green.

- [ ] **Step 6: Commit**

```bash
git add scripts/new_scenario.py pyproject.toml uv.lock tests/scenarios/
git commit -m "feat(scenarios): scaffolder generating conventional README + .zpln + .ipynb (+ verifier round-trip test)"
```

---

### Task 3: Seed the first exemplar scenario + `make new-scenario`

**Files:**
- Create: `scenarios/batch_ingest-nyc_taxi-spark-iceberg/…` (via the scaffolder)
- Modify: `Makefile`
- Test: `tests/test_makefile.py`

**Interfaces:**
- Produces: a committed exemplar scenario that the repo verifier validates; `make new-scenario NAME=<name>` runs the scaffolder.

- [ ] **Step 1: Scaffold the exemplar**

Run: `uv run python scripts/new_scenario.py batch_ingest-nyc_taxi-spark-iceberg --root .`
Then verify it: `uv run python scripts/verify_repo.py --root .` → `0 error(s)`, exit 0.

- [ ] **Step 2: Add the Makefile target + its test**

Add to `tests/test_makefile.py`:

```python
def test_new_scenario_target_runs_scaffolder():
    text = subprocess.run(["make", "-npq"], cwd=ROOT, capture_output=True, text=True).stdout
    assert "new_scenario.py" in text
```

Add `new-scenario` to the `TARGETS` list in `test_all_targets_defined`.

In the `Makefile`, add:

```makefile
new-scenario: ## Scaffold a scenario folder: make new-scenario NAME=pattern-dataset-engine-format
	uv run python scripts/new_scenario.py $(NAME) --root .
```

Add `new-scenario` to the `.PHONY` line.

- [ ] **Step 3: Run tests + verifier + lint**

Run: `uv run pytest tests/test_makefile.py -q` → PASS.
Run: `uv run pytest -m "not infra" -q`, `uv run ruff check .`, `uv run python scripts/verify_repo.py --root .` → all green/clean/exit 0.

- [ ] **Step 4: Commit**

```bash
git add scenarios/ Makefile tests/test_makefile.py
git commit -m "feat(scenarios): seed exemplar scenario + make new-scenario target"
```

---

### Task 4: Cross-language parity harness + docs

**Files:**
- Create: `tests/scenarios/parity.py`, `tests/scenarios/test_parity.py`
- Modify: `CONTRIBUTING.md`, create `docs/scenarios.md`, `README.md` (link)

**Interfaces:**
- Produces: `parity.tables_equivalent(a: dict, b: dict) -> tuple[bool, str]` — compares two lightweight "table snapshots" (`{"schema": [...], "row_count": int, "checksum": str}`) and reports whether the Scala and PySpark outputs of a scenario match. Pure + unit-tested; the live capture (running both notebooks and snapshotting their output tables) is a Phase 2b `@pytest.mark.infra` concern that will import this comparator.

- [ ] **Step 1: Write the failing test**

Create `tests/scenarios/test_parity.py`:

```python
import importlib.util
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
_spec = importlib.util.spec_from_file_location("parity", ROOT / "tests" / "scenarios" / "parity.py")
parity = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(parity)


def _snap(schema, n, checksum):
    return {"schema": schema, "row_count": n, "checksum": checksum}


def test_identical_snapshots_are_equivalent():
    a = _snap(["id:int", "v:string"], 10, "abc")
    ok, detail = parity.tables_equivalent(a, dict(a))
    assert ok, detail


def test_schema_mismatch_detected():
    ok, detail = parity.tables_equivalent(_snap(["id:int"], 10, "abc"), _snap(["id:bigint"], 10, "abc"))
    assert not ok and "schema" in detail


def test_row_count_mismatch_detected():
    ok, detail = parity.tables_equivalent(_snap(["id:int"], 10, "abc"), _snap(["id:int"], 11, "abc"))
    assert not ok and "row_count" in detail


def test_checksum_mismatch_detected():
    ok, detail = parity.tables_equivalent(_snap(["id:int"], 10, "abc"), _snap(["id:int"], 10, "xyz"))
    assert not ok and "checksum" in detail
```

- [ ] **Step 2: Run to verify RED**

Run: `uv run pytest tests/scenarios/test_parity.py -q`
Expected: FAIL — `parity` missing.

- [ ] **Step 3: Implement `tests/scenarios/parity.py`**

```python
"""Compare Scala vs PySpark scenario outputs by lightweight table snapshots.

A snapshot is {"schema": list[str], "row_count": int, "checksum": str}. Phase 2b captures
these from the live-executed notebooks; this comparator is the pure, unit-tested core.
"""
from __future__ import annotations


def tables_equivalent(a: dict, b: dict) -> tuple[bool, str]:
    if a.get("schema") != b.get("schema"):
        return False, f"schema differs: {a.get('schema')} != {b.get('schema')}"
    if a.get("row_count") != b.get("row_count"):
        return False, f"row_count differs: {a.get('row_count')} != {b.get('row_count')}"
    if a.get("checksum") != b.get("checksum"):
        return False, f"checksum differs: {a.get('checksum')} != {b.get('checksum')}"
    return True, "equivalent"
```

- [ ] **Step 4: Docs**

Create `docs/scenarios.md`:

```markdown
# Scenarios

Each scenario lives in a flat folder `scenarios/<pattern>-<dataset>-<engine>-<format>/` and is
**self-contained**: a 6-section `README.md`, a Scala Spark `zeppelin/notebook.zpln`, a PySpark
`jupyter/notebook.ipynb`, and an optional Airflow `dag.py`. Structure is enforced by the repo verifier.

## Add a scenario
```bash
make new-scenario NAME=medallion-nyc_taxi-spark-iceberg
uv run python scripts/verify_repo.py --root .   # validates structure
```
Then fill the notebook sections (`1. Overview` … `6. Verify`) with the scenario's Scala/PySpark logic.
The **Scala and PySpark notebooks must produce equivalent output** — Phase 2b captures a snapshot of
each and compares them with `tests/scenarios/parity.py`.

## Status
The scenario framework (scaffolder + verifier + parity harness) is in place; the curated scenario
notebooks are authored in Phase 2b (once the Atlas lakehouse — A1–A4 — is live).
```

Add an "Adding a scenario" pointer to `CONTRIBUTING.md` and a README §2 link to `docs/scenarios.md`.

- [ ] **Step 5: Run everything**

Run: `uv run pytest -m "not infra" -q` → all green. `uv run ruff check .` → clean. `uv run python scripts/verify_repo.py --root .` → exit 0.

- [ ] **Step 6: Commit**

```bash
git add tests/scenarios/parity.py tests/scenarios/test_parity.py docs/scenarios.md CONTRIBUTING.md README.md
git commit -m "feat(scenarios): cross-language parity harness + docs"
```

---

## Phase 2a exit criteria

- [ ] `uv run pytest -m "not infra" -q` → all pass (verifier scenario checks, scaffolder + verifier round-trip, parity harness).
- [ ] `uv run python scripts/verify_repo.py --root .` → exit 0 (the exemplar scenario validates).
- [ ] `uv run ruff check .` → clean (and — per the Phase 1b fix — actually lints `tests/`).
- [ ] `make new-scenario NAME=<x>` scaffolds a verifier-passing folder.
- [ ] PR into `main`, `static-and-unit` green, squash-merge.

## Self-review (against the design spec §6/§7)

**Spec coverage:** scenario-centric flat convention + enforced structure (Task 1 ✓); the "adding a task/scenario" mechanical recipe (Task 2/3 ✓, the ml-lab-style scaffolder); cross-language parity as a first-class guarantee (Task 4 ✓); docs (Task 4 ✓). The actual ★ core-10 notebooks are **Phase 2b** (live-gated), which this framework makes mechanical.

**Now-green vs later:** everything here is structural and CI-green; no live stack. The exemplar's notebook code is representative/minimal against the assumed contract; real per-scenario logic + live parity capture is Phase 2b.

**Placeholder scan:** the `_TODO (Phase 2b)` markers are inside *generated scenario scaffolding* (a deliberate template placeholder for downstream authors), NOT in the plan's own steps — every plan step has complete, runnable content.

**Type/name consistency:** `_discover_scenarios`/`_check_scenarios`, `readme_text`/`zeppelin_notebook`/`jupyter_notebook`/`scaffold`, `tables_equivalent` are used identically across defining and consuming steps.
