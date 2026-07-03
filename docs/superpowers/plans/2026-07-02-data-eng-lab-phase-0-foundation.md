# data-eng-lab — Phase 0: Foundation & Harness — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Stand up the `data-eng-lab` repository as a fully-initialized **private GitHub repo** (`thekaveh/data-eng-lab`) with the Atlas submodule, the launch harness (overlay + bootstrap scripts), base tooling (uv/ruff/pytest, Makefile, CI), a verifier skeleton, and infra-preflight **Layer 1** — such that `make up` boots Atlas's `data-eng` track and `make preflight` proves the base services exist and are initialized.

**Architecture:** `data-eng-lab` is a pure downstream consumer of Atlas (vendored as a pinned git submodule at `infra/`, never edited). It adds one compose overlay symlinked into Atlas's `services/_user/` slot, injects config via `infra/.env`, and runs bootstrap steps (`docker`/`mc`) after launch. All Phase 0 tests run under **pytest** (Python units + subprocess tests of shell scripts); shell scripts are made testable via overridable root/command env vars.

**Tech Stack:** Python 3.11, `uv`, `ruff`, `pytest`; Bash; Docker + `mc`; `gh`; GitHub Actions. (Atlas provides Spark 4.1 / Zeppelin / JupyterHub / Airflow / MinIO.)

## Global Constraints

- **Atlas is a pinned git submodule at `infra/` and is NEVER edited.** Ruff excludes it (`extend-exclude = ["infra"]`).
- **Phase 0 targets Atlas's *existing* `data-eng` track only** (`spark, airflow, jupyterhub, zeppelin, minio, weaviate, neo4j`). Iceberg-REST / Jenkins / Redpanda / Trino are **not** expected yet (A1–A9 pending); the preflight manifest marks them `disabled` until enabled.
- **Phase 0 pins the submodule to Atlas `main`.** Re-pin to `feat/data-eng-lab-enablement` in Phase 1 once that branch exists.
- **`PROJECT_NAME=data-eng-lab`** (Docker project + network `data-eng-lab-network`).
- **Buckets to create:** `landing`, `lakehouse`, `jars`, `checkpoints`, `lakehouse-test`.
- **MinIO internal endpoint:** `http://minio:9000`; credentials read from `infra/.env` (`MINIO_ROOT_USER`, `MINIO_ROOT_PASSWORD`) — **never hardcoded, never committed** (`infra/.env` lives inside the submodule and is gitignored by Atlas).
- **Python 3.11**, ruff `line-length = 120`, `select = ["E","F","W","I"]`.
- **Repo owner:** `thekaveh`; repo is **private**.
- **Idempotent bootstrap:** every script is safe to re-run.
- Shell scripts accept overrides for testability: `INFRA_DIR`, `MC_CMD`, and a health-probe hook.

---

### Task 1: Project scaffolding & tooling config

**Files:**
- Create: `pyproject.toml`, `.python-version`, `.gitignore`, `LICENSE`, `README.md`, `CONTRIBUTING.md`, `CHANGELOG.md`
- Create: `tests/__init__.py`, `tests/test_repo_structure.py`

**Interfaces:**
- Produces: a `uv`-managed dev environment; `pytest` discovers `tests/`; ruff configured. Later tasks assume `uv run pytest` and `uv run ruff check .` work and that top-level scaffolding files exist.

- [ ] **Step 1: Write the failing test**

Create `tests/__init__.py` (empty) and `tests/test_repo_structure.py`:

```python
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

REQUIRED_FILES = [
    "pyproject.toml",
    ".python-version",
    ".gitignore",
    "LICENSE",
    "README.md",
    "CONTRIBUTING.md",
    "CHANGELOG.md",
    "docs/atlas-enablement.md",
    "docs/superpowers/specs/2026-07-02-data-eng-lab-design.md",
]


def test_required_top_level_files_exist():
    missing = [f for f in REQUIRED_FILES if not (ROOT / f).exists()]
    assert not missing, f"missing required files: {missing}"


def test_python_version_is_311():
    assert (ROOT / ".python-version").read_text().strip() == "3.11"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_repo_structure.py -q`
Expected: FAIL — `pyproject.toml`/`.python-version` etc. missing (and `uv` may need `--no-project`; if so create `pyproject.toml` first in Step 3, then this passes in Step 4).

- [ ] **Step 3: Create the scaffolding files**

`pyproject.toml`:

```toml
[project]
name = "data-eng-lab"
version = "0.1.0"
description = "Iceberg-lakehouse data-engineering lab on the Atlas platform"
requires-python = ">=3.11"

[dependency-groups]
dev = [
  "pytest>=8",
  "pytest-cov>=5",
  "pyyaml>=6",
  "boto3>=1.34",
  "requests>=2.32",
]

[tool.ruff]
line-length = 120
target-version = "py311"
extend-exclude = ["infra"]

[tool.ruff.lint]
select = ["E", "F", "W", "I"]

[tool.pytest.ini_options]
testpaths = ["tests"]
addopts = "-ra"
markers = [
  "infra: requires a live Atlas data-eng stack (deselect with -m 'not infra')",
]
```

`.python-version`:

```
3.11
```

`.gitignore`:

```
__pycache__/
*.pyc
.venv/
.pytest_cache/
.ruff_cache/
*.egg-info/
.coverage
htmlcov/
.DS_Store
# local data (never committed)
datasets/raw/
datasets/generated/
**/data/
**/runs/
# preflight/report artifacts
.preflight/
```

`LICENSE` (private/proprietary — adjust later if desired):

```
Copyright (c) 2026 Kaveh Razavi. All rights reserved.

This repository is private and proprietary. No license is granted to use,
copy, modify, or distribute this software without explicit written permission.
```

`README.md` (real skeleton — numbered sections mirror ml-lab; fill deepens in Phase 6):

```markdown
# data-eng-lab

An Iceberg-lakehouse **data-engineering lab** built on the [Atlas](https://github.com/thekaveh/atlas)
platform. Curated Spark scenarios in **Scala (Zeppelin)** and **PySpark (Jupyter)**, orchestrated with
**Airflow**, plus **Maven Scala Spark** apps built by **Jenkins** and published to **MinIO** — all over
an **Apache Iceberg** lakehouse (REST catalog) on MinIO.

## 1. Overview
See the design spec: [`docs/superpowers/specs/2026-07-02-data-eng-lab-design.md`](docs/superpowers/specs/2026-07-02-data-eng-lab-design.md).

## 2. Repository layout
- `infra/` — Atlas platform (git submodule; never edited)
- `compose/` — the single compose overlay merged into Atlas
- `scenarios/` — flat scenario folders (Zeppelin Scala + Jupyter PySpark [+ DAG])
- `spark-apps/` — Maven Scala Spark projects (+ Jenkinsfile + DAG)
- `datasets/` — dataset registry + downloader
- `scripts/` — bootstrap & tooling
- `tests/` — comprehensive tiered tests (incl. `tests/infra/` preflight)
- `docs/` — design, Atlas contract, per-topic docs

## 3. Quick start
```bash
make setup      # init the Atlas submodule
make up         # launch Atlas data-eng track + bootstrap buckets
make preflight  # prove the stack is up, initialized, and integrated
make down       # tear down
```

## 4. Atlas contract
This repo consumes an enhanced Atlas; required upstream enhancements are tracked in
[`docs/atlas-enablement.md`](docs/atlas-enablement.md).

## 5. License
Private / proprietary — see [`LICENSE`](LICENSE).
```

`CONTRIBUTING.md`:

```markdown
# Contributing to data-eng-lab

## Workflow
1. Branch off `main`.
2. Make the change (follow existing patterns; keep files focused).
3. `make verify` (repo structure/oracle) must pass.
4. `make lint` and `make test` (static + unit) must pass.
5. Against a live stack: `make preflight` and relevant `make test-int` must pass.
6. Open a PR — one concern per PR.

## Conventions
- Never edit `infra/` (the Atlas submodule).
- Scenario folders: `[pattern]-[dataset]-[engine]-[format]`, flat under `scenarios/`.
- Every scenario ships both a Zeppelin Scala and a Jupyter PySpark notebook + a 6-section README.
- Bootstrap scripts are idempotent and read secrets from `infra/.env` (never hardcode).
```

`CHANGELOG.md`:

```markdown
# Changelog

All notable changes to this project are documented here (Keep a Changelog format).

## [Unreleased]
### Added
- Phase 0: repository foundation, Atlas submodule, launch harness, base tooling,
  verifier skeleton, and infra-preflight Layer 1.
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_repo_structure.py -q`
Expected: PASS (2 passed).

- [ ] **Step 5: Verify lint is clean**

Run: `uv run ruff check .`
Expected: `All checks passed!`

- [ ] **Step 6: Commit**

```bash
git add pyproject.toml .python-version .gitignore LICENSE README.md CONTRIBUTING.md CHANGELOG.md tests/
git commit -m "chore: project scaffolding, tooling config, and repo-structure test"
```

---

### Task 2: Atlas submodule + private GitHub repo

**Files:**
- Create: `.gitmodules` (via `git submodule add`), `infra/` (submodule gitlink)
- Create: `tests/test_submodule.py`

**Interfaces:**
- Produces: `infra/` present as a submodule; a private `thekaveh/data-eng-lab` GitHub repo with `origin` remote and `main` pushed. Later tasks assume `infra/start.sh` and `infra/services/_user/` exist after `make setup`.

- [ ] **Step 1: Write the failing test**

Create `tests/test_submodule.py`:

```python
import configparser
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def test_gitmodules_points_at_atlas():
    gm = ROOT / ".gitmodules"
    assert gm.exists(), ".gitmodules missing"
    cp = configparser.ConfigParser()
    cp.read_string(gm.read_text())
    sections = {s: dict(cp.items(s)) for s in cp.sections()}
    infra = next((v for v in sections.values() if v.get("path") == "infra"), None)
    assert infra is not None, "no submodule at path 'infra'"
    assert "thekaveh/atlas" in infra["url"], infra["url"]
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_submodule.py -q`
Expected: FAIL — `.gitmodules missing`.

- [ ] **Step 3: Add the Atlas submodule (pinned to main)**

```bash
git submodule add https://github.com/thekaveh/atlas infra
git submodule update --init --recursive infra
```

Expected: `infra/` populated; `.gitmodules` created.

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/test_submodule.py -q`
Expected: PASS.

- [ ] **Step 5: Commit locally**

```bash
git add .gitmodules infra tests/test_submodule.py
git commit -m "chore: vendor Atlas as pinned submodule at infra/"
```

- [ ] **Step 6: Create the private GitHub repo and push**

```bash
gh repo create thekaveh/data-eng-lab \
  --private \
  --source=. \
  --remote=origin \
  --push \
  --description "Iceberg-lakehouse data-engineering lab on the Atlas platform"
```

Expected: repo created; `main` pushed.

- [ ] **Step 7: Verify the repo is private and wired**

```bash
gh repo view thekaveh/data-eng-lab --json visibility,nameWithOwner -q '.visibility + " " + .nameWithOwner'
git remote -v
```

Expected: `PRIVATE thekaveh/data-eng-lab`; `origin` points at the new repo.

---

### Task 3: Shared shell library (`scripts/lib.sh`)

**Files:**
- Create: `scripts/lib.sh`
- Create: `tests/scripts/__init__.py`, `tests/scripts/test_lib.py`

**Interfaces:**
- Produces (sourced Bash functions): `log <msg>`; `envval <KEY> <file>` (prints value of `KEY=` in an env file, last wins); `set_env <KEY> <VALUE> <file>` (idempotent upsert); `set_env_default <KEY> <VALUE> <file>` (only if absent); `wait_healthy <service...>` using probe command in `HEALTH_PROBE` (default = docker label check). Later scripts source this file.

- [ ] **Step 1: Write the failing test**

Create `tests/scripts/__init__.py` (empty) and `tests/scripts/test_lib.py`:

```python
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
LIB = ROOT / "scripts" / "lib.sh"


def _run(script: str, cwd: Path) -> subprocess.CompletedProcess:
    return subprocess.run(["bash", "-c", f'set -e; source "{LIB}"; {script}'],
                          cwd=cwd, capture_output=True, text=True)


def test_envval_reads_last_value(tmp_path: Path):
    env = tmp_path / ".env"
    env.write_text("FOO=one\nBAR=two\nFOO=three\n")
    out = _run(f'envval FOO "{env}"', tmp_path)
    assert out.returncode == 0, out.stderr
    assert out.stdout.strip() == "three"


def test_set_env_is_idempotent(tmp_path: Path):
    env = tmp_path / ".env"
    env.write_text("PROJECT_NAME=old\n")
    _run(f'set_env PROJECT_NAME data-eng-lab "{env}"', tmp_path)
    _run(f'set_env PROJECT_NAME data-eng-lab "{env}"', tmp_path)
    lines = [ln for ln in env.read_text().splitlines() if ln.startswith("PROJECT_NAME=")]
    assert lines == ["PROJECT_NAME=data-eng-lab"], env.read_text()


def test_set_env_default_does_not_overwrite(tmp_path: Path):
    env = tmp_path / ".env"
    env.write_text("KEEP=mine\n")
    _run(f'set_env_default KEEP theirs "{env}"', tmp_path)
    assert "KEEP=mine" in env.read_text()
    assert "KEEP=theirs" not in env.read_text()
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/scripts/test_lib.py -q`
Expected: FAIL — `scripts/lib.sh` missing (source error).

- [ ] **Step 3: Implement `scripts/lib.sh`**

```bash
#!/usr/bin/env bash
# Shared helpers for data-eng-lab bootstrap scripts. Source this file.

log() { printf '\033[1;36m[data-eng-lab]\033[0m %s\n' "$*" >&2; }
err() { printf '\033[1;31m[data-eng-lab]\033[0m %s\n' "$*" >&2; }

# envval KEY FILE -> prints the value of the last KEY= line, or empty.
envval() {
  local key="$1" file="$2"
  [ -f "$file" ] || return 0
  grep -E "^${key}=" "$file" | tail -n1 | cut -d= -f2-
}

# set_env KEY VALUE FILE -> idempotent upsert (replaces existing or appends).
set_env() {
  local key="$1" value="$2" file="$3"
  touch "$file"
  if grep -qE "^${key}=" "$file"; then
    # portable in-place edit (BSD + GNU sed): use a temp file
    grep -vE "^${key}=" "$file" > "${file}.tmp"
    printf '%s=%s\n' "$key" "$value" >> "${file}.tmp"
    mv "${file}.tmp" "$file"
  else
    printf '%s=%s\n' "$key" "$value" >> "$file"
  fi
}

# set_env_default KEY VALUE FILE -> set only if KEY absent.
set_env_default() {
  local key="$1" value="$2" file="$3"
  touch "$file"
  grep -qE "^${key}=" "$file" || printf '%s=%s\n' "$key" "$value" >> "$file"
}

# wait_healthy SERVICE... -> polls until each compose service (of project
# PROJECT_NAME) is healthy/Up, or times out (HEALTH_TIMEOUT secs, default 300).
# Override the probe with HEALTH_PROBE="my_probe_fn" for tests.
_default_health_probe() {
  local svc="$1"
  docker ps \
    --filter "label=com.docker.compose.project=${PROJECT_NAME:-data-eng-lab}" \
    --filter "label=com.docker.compose.service=${svc}" \
    --format '{{.Status}}' | head -n1
}

wait_healthy() {
  local probe="${HEALTH_PROBE:-_default_health_probe}"
  local timeout="${HEALTH_TIMEOUT:-300}"
  local start now status svc
  start="${SECONDS}"
  for svc in "$@"; do
    while true; do
      status="$("$probe" "$svc")"
      case "$status" in
        *healthy*|Up*) log "service '$svc' ready ($status)"; break ;;
      esac
      now="${SECONDS}"
      if [ $(( now - start )) -ge "$timeout" ]; then
        err "timeout waiting for '$svc' (last status: '${status:-none}')"
        return 1
      fi
      sleep 3
    done
  done
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/scripts/test_lib.py -q`
Expected: PASS (3 passed).

- [ ] **Step 5: Commit**

```bash
git add scripts/lib.sh tests/scripts/
git commit -m "feat: shared shell lib (envval/set_env/wait_healthy) with pytest coverage"
```

---

### Task 4: Compose overlay + `setup-overlay.sh`

**Files:**
- Create: `compose/data-eng-lab.yml`
- Create: `scripts/setup-overlay.sh`
- Create: `scenarios/.gitkeep`, `spark-apps/.gitkeep`, `datasets/.gitkeep`
- Create: `tests/scripts/test_setup_overlay.py`

**Interfaces:**
- Consumes: `scripts/lib.sh` (`set_env`, `set_env_default`, `log`).
- Produces: `setup-overlay.sh` symlinks `compose/data-eng-lab.yml` → `${INFRA_DIR}/services/_user/data-eng-lab/compose.yml` and injects keys into `${INFRA_DIR}/.env`. `INFRA_DIR` defaults to `<root>/infra`, overridable for tests.

- [ ] **Step 1: Write the failing test**

Create `tests/scripts/test_setup_overlay.py`:

```python
import os
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "scripts" / "setup-overlay.sh"


def _run(tmp_infra: Path) -> subprocess.CompletedProcess:
    env = {**os.environ, "INFRA_DIR": str(tmp_infra)}
    return subprocess.run(["bash", str(SCRIPT)], cwd=ROOT, env=env,
                          capture_output=True, text=True)


def test_creates_symlink_and_env(tmp_path: Path):
    infra = tmp_path / "infra"
    (infra / "services").mkdir(parents=True)
    (infra / ".env").write_text("BASE_PORT=63000\n")

    out = _run(infra)
    assert out.returncode == 0, out.stderr

    slot = infra / "services" / "_user" / "data-eng-lab" / "compose.yml"
    assert slot.is_symlink(), "overlay symlink not created"
    assert slot.resolve() == (ROOT / "compose" / "data-eng-lab.yml").resolve()

    env_text = (infra / ".env").read_text()
    assert "PROJECT_NAME=data-eng-lab" in env_text
    assert "ICEBERG_REST_URI=http://iceberg-rest:8181" in env_text


def test_is_idempotent(tmp_path: Path):
    infra = tmp_path / "infra"
    (infra / "services").mkdir(parents=True)
    (infra / ".env").write_text("")
    _run(infra)
    _run(infra)
    env_text = (infra / ".env").read_text()
    assert env_text.count("PROJECT_NAME=data-eng-lab") == 1
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/scripts/test_setup_overlay.py -q`
Expected: FAIL — script missing.

- [ ] **Step 3: Create the overlay and script**

`compose/data-eng-lab.yml` (merges into existing Atlas services; joins the external network; bind-mounts our trees for later phases):

```yaml
# data-eng-lab overlay — merged into Atlas services via services/_user/ auto-discovery.
# Phase 0: joins the shared network and mounts our (initially empty) trees so Airflow
# discovers DAGs recursively and datasets are available in-container. No new services yet
# (Iceberg-REST/Jenkins/Redpanda arrive from Atlas via A1/A5/A9).
services:
  airflow-scheduler:
    volumes:
      - ../scenarios:/opt/airflow/dags/data_eng_lab_scenarios:ro
      - ../spark-apps:/opt/airflow/dags/data_eng_lab_spark_apps:ro
      - ../datasets:/opt/data-eng-lab/datasets:ro

  airflow-dag-processor:
    volumes:
      - ../scenarios:/opt/airflow/dags/data_eng_lab_scenarios:ro
      - ../spark-apps:/opt/airflow/dags/data_eng_lab_spark_apps:ro

  jupyterhub:
    volumes:
      - ../datasets:/home/jovyan/data-eng-lab/datasets:ro
```

`scripts/setup-overlay.sh`:

```bash
#!/usr/bin/env bash
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$HERE/.." && pwd)"
source "$HERE/lib.sh"

INFRA_DIR="${INFRA_DIR:-$ROOT/infra}"
ENV_FILE="$INFRA_DIR/.env"
SLOT_DIR="$INFRA_DIR/services/_user/data-eng-lab"

log "linking overlay into Atlas user slot: $SLOT_DIR"
mkdir -p "$SLOT_DIR"
# Relative link so it resolves the same on any checkout: slot -> repo compose file.
ln -sf "../../../../compose/data-eng-lab.yml" "$SLOT_DIR/compose.yml"

log "injecting config into $ENV_FILE"
set_env         PROJECT_NAME     data-eng-lab                     "$ENV_FILE"
set_env         ICEBERG_REST_URI http://iceberg-rest:8181         "$ENV_FILE"
set_env_default BRAND_NAME       "data-eng-lab"                   "$ENV_FILE"

log "setup-overlay complete"
```

Create the placeholder trees:

```bash
mkdir -p scenarios spark-apps datasets
touch scenarios/.gitkeep spark-apps/.gitkeep datasets/.gitkeep
chmod +x scripts/setup-overlay.sh
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/scripts/test_setup_overlay.py -q`
Expected: PASS (2 passed).

- [ ] **Step 5: Commit**

```bash
git add compose/ scripts/setup-overlay.sh scenarios/.gitkeep spark-apps/.gitkeep datasets/.gitkeep tests/scripts/test_setup_overlay.py
git commit -m "feat: compose overlay + idempotent setup-overlay.sh (symlink + .env injection)"
```

---

### Task 5: Bucket bootstrap (`scripts/create_buckets.sh`)

**Files:**
- Create: `scripts/create_buckets.sh`
- Create: `tests/scripts/test_create_buckets.py`

**Interfaces:**
- Consumes: `scripts/lib.sh` (`log`, `envval`); `INFRA_DIR/.env` for MinIO creds; `PROJECT_NAME` for network.
- Produces: creates buckets `landing lakehouse jars checkpoints lakehouse-test` via `mc`. The `mc` invocation is the `MC_CMD` env var (default = transient `docker run … minio/mc`), overridable for tests.

- [ ] **Step 1: Write the failing test**

Create `tests/scripts/test_create_buckets.py`:

```python
import os
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "scripts" / "create_buckets.sh"
EXPECTED = ["landing", "lakehouse", "jars", "checkpoints", "lakehouse-test"]


def test_creates_all_expected_buckets(tmp_path: Path):
    infra = tmp_path / "infra"
    infra.mkdir()
    (infra / ".env").write_text("MINIO_ROOT_USER=minioadmin\nMINIO_ROOT_PASSWORD=secret\n")

    # Stub 'mc' that records each invocation's args to a log file.
    rec = tmp_path / "mc_calls.log"
    stub = tmp_path / "mc_stub.sh"
    stub.write_text(f'#!/usr/bin/env bash\necho "$@" >> "{rec}"\n')
    stub.chmod(0o755)

    env = {**os.environ, "INFRA_DIR": str(infra), "MC_CMD": str(stub), "PROJECT_NAME": "data-eng-lab"}
    out = subprocess.run(["bash", str(SCRIPT)], cwd=ROOT, env=env, capture_output=True, text=True)
    assert out.returncode == 0, out.stderr

    calls = rec.read_text()
    for bucket in EXPECTED:
        assert bucket in calls, f"bucket '{bucket}' not created; calls:\n{calls}"
    assert "--ignore-existing" in calls, "bucket creation must be idempotent"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/scripts/test_create_buckets.py -q`
Expected: FAIL — script missing.

- [ ] **Step 3: Implement `scripts/create_buckets.sh`**

```bash
#!/usr/bin/env bash
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$HERE/.." && pwd)"
source "$HERE/lib.sh"

INFRA_DIR="${INFRA_DIR:-$ROOT/infra}"
ENV_FILE="$INFRA_DIR/.env"
PROJECT_NAME="${PROJECT_NAME:-$(envval PROJECT_NAME "$ENV_FILE")}"
PROJECT_NAME="${PROJECT_NAME:-data-eng-lab}"

USER="$(envval MINIO_ROOT_USER "$ENV_FILE")"
PASS="$(envval MINIO_ROOT_PASSWORD "$ENV_FILE")"
BUCKETS=(landing lakehouse jars checkpoints lakehouse-test)

# Default: a transient mc container on the Atlas network, aliased to the in-network MinIO.
# Overridable via MC_CMD (used by tests and by callers that already have mc configured).
MC_CMD="${MC_CMD:-docker run --rm --network ${PROJECT_NAME}-network \
  -e MC_HOST_local=http://${USER}:${PASS}@minio:9000 minio/mc}"

log "creating buckets: ${BUCKETS[*]}"
for b in "${BUCKETS[@]}"; do
  # 'local/' alias when using the default docker mc; stub ignores the alias prefix.
  $MC_CMD mb --ignore-existing "local/${b}"
done
log "buckets ready"
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/scripts/test_create_buckets.py -q`
Expected: PASS.

Then: `chmod +x scripts/create_buckets.sh`

- [ ] **Step 5: Commit**

```bash
git add scripts/create_buckets.sh tests/scripts/test_create_buckets.py
git commit -m "feat: idempotent MinIO bucket bootstrap (mc) with pytest coverage"
```

---

### Task 6: Verifier skeleton (`scripts/verify_repo.py`)

**Files:**
- Create: `scripts/verify_repo.py`, `scripts/verify_repo_config.yaml`
- Create: `tests/test_verify_repo.py`

**Interfaces:**
- Produces: `verify_repo.py` with `run_checks(root: Path, config: dict) -> list[Finding]` where `Finding = namedtuple("Finding", "check severity message")`; CLI exits 0 iff no `error`-severity findings. Config lists `active_scenario_dirs` and the naming regex. Later phases add checks (notebooks, DAGs, registry) to this same module/config.

- [ ] **Step 1: Write the failing test**

Create `tests/test_verify_repo.py`:

```python
import importlib.util
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location("verify_repo", ROOT / "scripts" / "verify_repo.py")
verify_repo = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(verify_repo)


def test_valid_scenario_name_passes(tmp_path: Path):
    (tmp_path / "scenarios" / "medallion-nyc_taxi-spark-iceberg").mkdir(parents=True)
    cfg = {"active_scenario_dirs": ["medallion-nyc_taxi-spark-iceberg"],
           "scenario_name_regex": r"^[a-z0-9_]+-[a-z0-9_]+-[a-z0-9_]+-[a-z0-9_]+$"}
    findings = verify_repo.run_checks(tmp_path, cfg)
    errors = [f for f in findings if f.severity == "error"]
    assert errors == [], errors


def test_bad_scenario_name_flags_error(tmp_path: Path):
    (tmp_path / "scenarios" / "BadName").mkdir(parents=True)
    cfg = {"active_scenario_dirs": ["BadName"],
           "scenario_name_regex": r"^[a-z0-9_]+-[a-z0-9_]+-[a-z0-9_]+-[a-z0-9_]+$"}
    findings = verify_repo.run_checks(tmp_path, cfg)
    assert any(f.severity == "error" and "naming" in f.check for f in findings), findings


def test_missing_declared_dir_flags_error(tmp_path: Path):
    cfg = {"active_scenario_dirs": ["ghost-scenario-spark-iceberg"],
           "scenario_name_regex": r"^[a-z0-9_]+-[a-z0-9_]+-[a-z0-9_]+-[a-z0-9_]+$"}
    findings = verify_repo.run_checks(tmp_path, cfg)
    assert any(f.severity == "error" and "exists" in f.check for f in findings), findings
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_verify_repo.py -q`
Expected: FAIL — `verify_repo.py` missing.

- [ ] **Step 3: Implement `scripts/verify_repo.py` and config**

`scripts/verify_repo_config.yaml`:

```yaml
# Single edit point for repo structure verification. Phases add keys here.
scenario_name_regex: "^[a-z0-9_]+-[a-z0-9_]+-[a-z0-9_]+-[a-z0-9_]+$"
active_scenario_dirs: []   # populated as scenarios land (Phase 2+)
```

`scripts/verify_repo.py`:

```python
#!/usr/bin/env python3
"""Config-driven repo verifier (skeleton). Exit 0 iff no error-severity findings."""
from __future__ import annotations

import argparse
import json
import re
import sys
from collections import namedtuple
from pathlib import Path

import yaml

Finding = namedtuple("Finding", "check severity message")


def _check_naming(root: Path, cfg: dict) -> list[Finding]:
    findings: list[Finding] = []
    regex = re.compile(cfg["scenario_name_regex"])
    for name in cfg.get("active_scenario_dirs", []):
        if not regex.match(name):
            findings.append(Finding("scenario.naming", "error",
                                    f"scenario dir '{name}' violates naming convention"))
    return findings


def _check_declared_dirs_exist(root: Path, cfg: dict) -> list[Finding]:
    findings: list[Finding] = []
    for name in cfg.get("active_scenario_dirs", []):
        if not (root / "scenarios" / name).is_dir():
            findings.append(Finding("scenario.exists", "error",
                                    f"declared scenario '{name}' has no folder under scenarios/"))
    return findings


CHECKS = [_check_naming, _check_declared_dirs_exist]


def run_checks(root: Path, cfg: dict) -> list[Finding]:
    findings: list[Finding] = []
    for check in CHECKS:
        findings.extend(check(root, cfg))
    return findings


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", default=".", type=Path)
    ap.add_argument("--config", default=None, type=Path)
    ap.add_argument("--json", action="store_true")
    args = ap.parse_args(argv)

    root = args.root.resolve()
    cfg_path = args.config or (root / "scripts" / "verify_repo_config.yaml")
    cfg = yaml.safe_load(cfg_path.read_text())

    findings = run_checks(root, cfg)
    errors = [f for f in findings if f.severity == "error"]

    if args.json:
        print(json.dumps([f._asdict() for f in findings], indent=2))
    else:
        for f in findings:
            print(f"[{f.severity.upper()}] {f.check}: {f.message}")
        print(f"\n{len(findings)} finding(s), {len(errors)} error(s)")
    return 1 if errors else 0


if __name__ == "__main__":
    sys.exit(main())
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_verify_repo.py -q`
Expected: PASS (3 passed).

- [ ] **Step 5: Run the verifier on the real repo**

Run: `uv run python scripts/verify_repo.py --root .`
Expected: `0 finding(s), 0 error(s)` (no active scenarios yet) — exit 0.

- [ ] **Step 6: Commit**

```bash
git add scripts/verify_repo.py scripts/verify_repo_config.yaml tests/test_verify_repo.py
git commit -m "feat: config-driven repo verifier skeleton with self-tests"
```

---

### Task 7: Infra preflight — Layer 1 (existence & initialization)

**Files:**
- Create: `tests/infra/__init__.py`, `tests/infra/manifest.py`, `tests/infra/preflight.py`
- Create: `tests/infra/test_preflight_unit.py`
- Create: `tests/infra/test_preflight_live.py`

**Interfaces:**
- Produces:
  - `manifest.py`: `EXPECTED_SERVICES: list[ServiceSpec]` where `ServiceSpec = namedtuple("ServiceSpec", "name enabled init_check")`; `enabled` gates by env (base services on; iceberg-rest/jenkins/redpanda/trino off until their `*_ENABLED` env is truthy).
  - `preflight.py`: `run_layer1(services, docker_ok, checks) -> list[Result]` (`Result = namedtuple("Result", "name status detail")`, status ∈ `pass|fail|blocked|skipped`); `render_matrix(results) -> str`; `main()` returns non-zero on any `fail`.
- Consumes (live): Docker + `infra/.env`. Unit tests inject fakes.

- [ ] **Step 1: Write the failing unit test**

Create `tests/infra/__init__.py` (empty) and `tests/infra/test_preflight_unit.py`:

```python
import importlib.util
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def _load(mod):
    spec = importlib.util.spec_from_file_location(mod, ROOT / "tests" / "infra" / f"{mod}.py")
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    return m


preflight = _load("preflight")
manifest = _load("manifest")


def test_disabled_service_is_skipped():
    services = [manifest.ServiceSpec("redpanda", enabled=False, init_check=lambda: (True, "ok"))]
    results = preflight.run_layer1(services, docker_ok=True)
    assert results[0].status == "skipped"


def test_docker_down_blocks_all():
    services = [manifest.ServiceSpec("minio", enabled=True, init_check=lambda: (True, "ok"))]
    results = preflight.run_layer1(services, docker_ok=False)
    assert results[0].status == "blocked"


def test_failed_init_check_reports_fail():
    services = [manifest.ServiceSpec("minio", enabled=True,
                                     init_check=lambda: (False, "buckets missing"))]
    results = preflight.run_layer1(services, docker_ok=True)
    assert results[0].status == "fail"
    assert "buckets missing" in results[0].detail


def test_render_matrix_contains_status():
    results = [preflight.Result("minio", "pass", "ok")]
    out = preflight.render_matrix(results)
    assert "minio" in out and "pass" in out.lower()
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/infra/test_preflight_unit.py -q`
Expected: FAIL — modules missing.

- [ ] **Step 3: Implement `manifest.py` and `preflight.py`**

`tests/infra/manifest.py`:

```python
"""Declarative expected-service manifest for the data-eng track (Phase 0: Layer 1)."""
from __future__ import annotations

import os
import shutil
import subprocess
from collections import namedtuple

ServiceSpec = namedtuple("ServiceSpec", "name enabled init_check")

PROJECT = os.environ.get("PROJECT_NAME", "data-eng-lab")


def _truthy(var: str) -> bool:
    return os.environ.get(var, "").lower() in ("1", "true", "yes", "on")


def _container_status(service: str) -> str:
    if not shutil.which("docker"):
        return ""
    out = subprocess.run(
        ["docker", "ps",
         "--filter", f"label=com.docker.compose.project={PROJECT}",
         "--filter", f"label=com.docker.compose.service={service}",
         "--format", "{{.Status}}"],
        capture_output=True, text=True,
    )
    return out.stdout.strip().splitlines()[0] if out.stdout.strip() else ""


def _up(service: str):
    status = _container_status(service)
    ok = ("healthy" in status) or status.startswith("Up")
    return ok, status or "not running"


def _minio_ready():
    ok, detail = _up("minio")
    # Deeper bucket check lands in Phase 1 (needs mc/boto3 against the live endpoint).
    return ok, detail


# Base data-eng services always expected; A1/A5/A9/A7 services gated until enabled.
EXPECTED_SERVICES = [
    ServiceSpec("minio", True, _minio_ready),
    ServiceSpec("spark-master", True, lambda: _up("spark-master")),
    ServiceSpec("spark-connect", True, lambda: _up("spark-connect")),
    ServiceSpec("zeppelin", True, lambda: _up("zeppelin")),
    ServiceSpec("jupyterhub", True, lambda: _up("jupyterhub")),
    ServiceSpec("airflow-webserver", True, lambda: _up("airflow-webserver")),
    ServiceSpec("airflow-scheduler", True, lambda: _up("airflow-scheduler")),
    ServiceSpec("iceberg-rest", _truthy("ICEBERG_REST_ENABLED"), lambda: _up("iceberg-rest")),
    ServiceSpec("jenkins", _truthy("JENKINS_ENABLED"), lambda: _up("jenkins")),
    ServiceSpec("redpanda", _truthy("REDPANDA_ENABLED"), lambda: _up("redpanda")),
    ServiceSpec("trino", _truthy("TRINO_ENABLED"), lambda: _up("trino")),
]
```

`tests/infra/preflight.py`:

```python
#!/usr/bin/env python3
"""Infra preflight — Layer 1 (existence & initialization). Fail-loud stack doctor."""
from __future__ import annotations

import shutil
import sys
from collections import namedtuple

Result = namedtuple("Result", "name status detail")


def run_layer1(services, docker_ok: bool) -> list[Result]:
    results: list[Result] = []
    for svc in services:
        if not svc.enabled:
            results.append(Result(svc.name, "skipped", "disabled by manifest (A-item pending)"))
            continue
        if not docker_ok:
            results.append(Result(svc.name, "blocked", "docker unavailable — root cause upstream"))
            continue
        ok, detail = svc.init_check()
        results.append(Result(svc.name, "pass" if ok else "fail", detail))
    return results


def render_matrix(results: list[Result]) -> str:
    icon = {"pass": "✅", "fail": "❌", "blocked": "⛔", "skipped": "⏭️"}
    width = max((len(r.name) for r in results), default=4)
    lines = ["Infra preflight — Layer 1 (existence & initialization)", "-" * 60]
    for r in results:
        lines.append(f"{icon.get(r.status,'?')} {r.name.ljust(width)}  {r.status.upper():8} {r.detail}")
    fails = [r for r in results if r.status == "fail"]
    lines.append("-" * 60)
    lines.append(f"{len(results)} services · {len(fails)} FAIL")
    return "\n".join(lines)


def main() -> int:
    from manifest import EXPECTED_SERVICES  # local import so unit tests can inject their own
    docker_ok = shutil.which("docker") is not None
    results = run_layer1(EXPECTED_SERVICES, docker_ok)
    print(render_matrix(results))
    return 1 if any(r.status == "fail" for r in results) else 0


if __name__ == "__main__":
    sys.path.insert(0, str(__import__("pathlib").Path(__file__).resolve().parent))
    sys.exit(main())
```

- [ ] **Step 4: Run unit tests to verify they pass**

Run: `uv run pytest tests/infra/test_preflight_unit.py -q`
Expected: PASS (4 passed).

- [ ] **Step 5: Add the live (stack-gated) test**

Create `tests/infra/test_preflight_live.py`:

```python
import importlib.util
import os
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]

pytestmark = pytest.mark.infra


def _load(mod):
    spec = importlib.util.spec_from_file_location(mod, ROOT / "tests" / "infra" / f"{mod}.py")
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    return m


@pytest.mark.skipif(os.environ.get("RUN_INFRA") != "1",
                    reason="set RUN_INFRA=1 with a live Atlas data-eng stack")
def test_layer1_all_pass_against_live_stack():
    preflight = _load("preflight")
    manifest = _load("manifest")
    results = preflight.run_layer1(manifest.EXPECTED_SERVICES, docker_ok=True)
    print("\n" + preflight.render_matrix(results))
    fails = [r for r in results if r.status == "fail"]
    assert not fails, f"preflight L1 failures: {fails}"
```

- [ ] **Step 6: Verify the live test is skipped without a stack**

Run: `uv run pytest tests/infra/test_preflight_live.py -q`
Expected: `1 skipped` (RUN_INFRA unset).

- [ ] **Step 7: Commit**

```bash
git add tests/infra/
git commit -m "feat: infra preflight Layer 1 (existence/init) — fail-loud manifest + matrix"
```

---

### Task 8: Launch harness (`start-all.sh` / `stop-all.sh`)

**Files:**
- Create: `scripts/start-all.sh`, `scripts/stop-all.sh`
- Create: `tests/scripts/test_start_all_smoke.py`

**Interfaces:**
- Consumes: `scripts/lib.sh`, `scripts/setup-overlay.sh`, `scripts/create_buckets.sh`, `tests/infra/preflight.py`; `infra/start.sh`/`infra/stop.sh`.
- Produces: `make up`/`make down` entrypoints. `start-all.sh` supports `--dry-run` (print the plan without executing Docker) for CI-safe verification.

- [ ] **Step 1: Write the failing test**

Create `tests/scripts/test_start_all_smoke.py`:

```python
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
START = ROOT / "scripts" / "start-all.sh"
STOP = ROOT / "scripts" / "stop-all.sh"


def test_start_all_dry_run_lists_plan():
    out = subprocess.run(["bash", str(START), "--dry-run"], cwd=ROOT,
                         capture_output=True, text=True)
    assert out.returncode == 0, out.stderr
    text = out.stdout + out.stderr
    for token in ["setup-overlay", "--track data-eng", "create_buckets", "preflight"]:
        assert token in text, f"dry-run plan missing '{token}':\n{text}"


def test_stop_all_dry_run():
    out = subprocess.run(["bash", str(STOP), "--dry-run"], cwd=ROOT,
                         capture_output=True, text=True)
    assert out.returncode == 0, out.stderr
    assert "stop.sh" in (out.stdout + out.stderr)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/scripts/test_start_all_smoke.py -q`
Expected: FAIL — scripts missing.

- [ ] **Step 3: Implement `scripts/start-all.sh`**

```bash
#!/usr/bin/env bash
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$HERE/.." && pwd)"
source "$HERE/lib.sh"

INFRA_DIR="${INFRA_DIR:-$ROOT/infra}"
DRY_RUN=0
[ "${1:-}" = "--dry-run" ] && DRY_RUN=1

run() { if [ "$DRY_RUN" = 1 ]; then echo "+ $*"; else eval "$@"; fi; }

log "1/5 wiring overlay + .env (setup-overlay)"
run "\"$HERE/setup-overlay.sh\""

log "2/5 launching Atlas data-eng track"
# Non-interactive; backgrounded to avoid the start.py 'logs -f' block (see Atlas reuse notes).
ATLAS_START="cd \"$INFRA_DIR\" && ./start.sh --track data-eng --no-tui \
  --spark-source container --zeppelin-source container --airflow-source container \
  --minio-source container --jupyterhub-source container"
if [ "$DRY_RUN" = 1 ]; then echo "+ $ATLAS_START &"; else eval "$ATLAS_START" & fi

log "3/5 waiting for core services to be healthy"
export PROJECT_NAME="${PROJECT_NAME:-data-eng-lab}"
run "wait_healthy minio spark-master spark-connect airflow-webserver zeppelin jupyterhub"

log "4/5 creating buckets"
run "\"$HERE/create_buckets.sh\""

log "5/5 preflight (stack doctor)"
run "uv run python \"$ROOT/tests/infra/preflight.py\""

log "data-eng-lab is up. Consoles: see 'cd infra && ./start.sh --list-tracks' / infra/.env for ports."
```

- [ ] **Step 4: Implement `scripts/stop-all.sh`**

```bash
#!/usr/bin/env bash
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$HERE/.." && pwd)"
source "$HERE/lib.sh"
INFRA_DIR="${INFRA_DIR:-$ROOT/infra}"

DRY_RUN=0
COLD=""
for a in "$@"; do
  [ "$a" = "--dry-run" ] && DRY_RUN=1
  [ "$a" = "--cold" ] && COLD="--cold"
done

CMD="cd \"$INFRA_DIR\" && ./stop.sh $COLD"
if [ "$DRY_RUN" = 1 ]; then echo "+ $CMD"; else eval "$CMD"; fi
log "data-eng-lab stopped ${COLD:+(volumes wiped)}"
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `uv run pytest tests/scripts/test_start_all_smoke.py -q`
Expected: PASS (2 passed). Then `chmod +x scripts/start-all.sh scripts/stop-all.sh`.

- [ ] **Step 6: Commit**

```bash
git add scripts/start-all.sh scripts/stop-all.sh tests/scripts/test_start_all_smoke.py
git commit -m "feat: launch harness (start-all/stop-all) with dry-run smoke tests"
```

---

### Task 9: Makefile, CI workflow, and finalize

**Files:**
- Create: `Makefile`, `.github/workflows/ci.yml`
- Create: `tests/test_makefile.py`

**Interfaces:**
- Consumes: everything above.
- Produces: `make setup|up|down|datasets|verify|test|preflight|lint|fmt` targets; a PR-gate CI workflow (static + unit only, no Docker).

- [ ] **Step 1: Write the failing test**

Create `tests/test_makefile.py`:

```python
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
TARGETS = ["setup", "up", "down", "verify", "test", "preflight", "lint", "fmt"]


def test_all_targets_defined():
    out = subprocess.run(["make", "-npq"], cwd=ROOT, capture_output=True, text=True)
    text = out.stdout
    missing = [t for t in TARGETS if f"\n{t}:" not in text]
    assert not missing, f"Makefile missing targets: {missing}"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_makefile.py -q`
Expected: FAIL — Makefile missing.

- [ ] **Step 3: Create the Makefile**

```makefile
.DEFAULT_GOAL := help
SHELL := /bin/bash

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN{FS=":.*?## "}{printf "  %-12s %s\n",$$1,$$2}'

setup: ## Initialize the Atlas submodule
	git submodule update --init --recursive infra

up: ## Launch the Atlas data-eng track + bootstrap
	./scripts/start-all.sh

down: ## Tear down (add COLD=1 to wipe volumes)
	./scripts/stop-all.sh $(if $(COLD),--cold,)

datasets: ## Download datasets into MinIO (Phase 1)
	@echo "datasets: implemented in Phase 1"

verify: ## Run the repo verifier
	uv run python scripts/verify_repo.py --root .

test: ## Static + unit tests (no live stack)
	uv run pytest -m "not infra" -q

preflight: ## Infra preflight against a live stack
	uv run python tests/infra/preflight.py

lint: ## Lint (ruff; shell/yaml lint if installed)
	uv run ruff check .
	@command -v shellcheck >/dev/null && shellcheck scripts/*.sh || echo "shellcheck not installed — skipping"

fmt: ## Auto-format Python
	uv run ruff format .
	uv run ruff check --fix .
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/test_makefile.py -q`
Expected: PASS.

- [ ] **Step 5: Create the CI workflow**

`.github/workflows/ci.yml`:

```yaml
name: CI
on:
  push:
    branches: [main]
  pull_request:
    branches: [main]
  workflow_dispatch:

permissions:
  contents: read

jobs:
  static-and-unit:
    runs-on: ubuntu-24.04
    steps:
      - uses: actions/checkout@v4
        with:
          submodules: false          # base CI does not need the Atlas submodule
          persist-credentials: false
      - uses: astral-sh/setup-uv@v5
      - name: Set up Python
        run: uv python install 3.11
      - name: Lint
        run: uv run ruff check .
      - name: Verify repo structure
        run: uv run python scripts/verify_repo.py --root .
      - name: Unit + static tests (no live stack)
        run: uv run pytest -m "not infra" -q
```

- [ ] **Step 6: Run the full non-infra suite locally**

Run: `uv run pytest -m "not infra" -q`
Expected: PASS (all Phase 0 unit tests green).

- [ ] **Step 7: Commit and push; confirm CI green**

```bash
git add Makefile .github/workflows/ci.yml tests/test_makefile.py
git commit -m "feat: Makefile targets + PR-gate CI (static + unit)"
git push -u origin main
gh run watch --exit-status || gh run list --limit 1
```

Expected: the `CI / static-and-unit` job passes on GitHub.

---

## Phase 0 exit criteria (verify all)

- [ ] `gh repo view thekaveh/data-eng-lab --json visibility -q .visibility` → `PRIVATE`.
- [ ] `git submodule status` shows `infra` populated.
- [ ] `uv run pytest -m "not infra" -q` → all pass.
- [ ] `uv run ruff check .` → clean.
- [ ] `uv run python scripts/verify_repo.py --root .` → exit 0.
- [ ] `./scripts/start-all.sh --dry-run` → prints the 5-step plan.
- [ ] GitHub Actions `CI` workflow green on `main`.
- [ ] **Local integration (manual, needs Docker):** `make up` boots the data-eng track; `RUN_INFRA=1 uv run pytest tests/infra/test_preflight_live.py -q` → preflight L1 all `pass`; `make down` tears down.

---

## Self-review (against the spec)

**Spec coverage (Phase 0 rows of §9):** repo + private GitHub repo (Task 2 ✓); submodule (Task 2 ✓); overlay + setup-overlay (Task 4 ✓); start-all/stop-all (Task 8 ✓); bucket creation (Task 5 ✓); base tooling — Makefile/uv/ruff/verifier+CI skeletons/README/CONTRIBUTING/CHANGELOG (Tasks 1, 6, 9 ✓); preflight Layer 1 (Task 7 ✓). Exit criteria match §9 Phase 0 row.

**Deferred to later phase plans (correctly out of Phase 0 scope):** `register_iceberg.py` + preflight Layer 2 integration matrix (Phase 1); `download_datasets.py` + registry (Phase 1); scenarios/notebooks (Phase 2); spark-apps + Jenkins (Phase 3); orchestration e2e (Phase 4); Redpanda/Trino/roadmap (Phase 5); full docs/diagram (Phase 6). The `make datasets` target is a labeled stub, not a placeholder gap.

**Placeholder scan:** no TBD/TODO; every code step has complete, runnable content; the one stub (`make datasets`) prints an explicit "implemented in Phase 1" message.

**Type/name consistency:** `Finding(check, severity, message)`, `ServiceSpec(name, enabled, init_check)`, `Result(name, status, detail)`, `run_layer1(...)`, `render_matrix(...)` are used identically across their defining and consuming steps.
