# Atlas Consumer Modernization Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

> **Supersession (2026-07-28):** The original `881df596` reviewed pin was the
> focused live-gate baseline. Atlas #850 was then fixed upstream; this plan's
> current immutable target is `af7713ee43f71e140e57735488001bc1cfb09245`. The
> earlier SHA remains historical evidence, not the current pin.

**Goal:** Pin Atlas at `af7713ee43f71e140e57735488001bc1cfb09245`, align this repository with the current consumer runbook, and prove the full data-eng catalog remains compatible through static checks and focused live smoke.

**Architecture:** Atlas stays a read-only submodule. The parent `atlas.consumer.yml` and Compose overlay remain the only committed integration configuration. New parent endpoint helpers consume Atlas's exported MinIO host endpoint and resolve unexported data-eng host ports through explicit overrides or `infra/.env`, with no port arithmetic.

**Tech Stack:** Git submodules, Atlas `start.sh`, Docker Compose, Bash, Python 3.11, pytest, PyYAML, boto3, PyIceberg, kafka-python, Trino, MkDocs, GitHub Actions.

## Global Constraints

- Use `codex/atlas-consumer-modernization`, based on `develop`.
- Preserve and never stage `docs/superpowers/plans/2026-07-21-atlas-submodule-modernization.md`.
- Pin the exact SHA `af7713ee43f71e140e57735488001bc1cfb09245`; do not chase `origin/main`.
- Never edit a file inside `infra/`.
- Keep identity, `BASE_PORT: auto`, sources, storage, profile overrides, and overlay selection in `atlas.consumer.yml`.
- Keep in-network DNS inside DAGs/notebooks. Host code uses explicit override or resolved config, never a fixed port or a base-port offset.
- Assert only `ATLAS_MINIO_HOST_ENDPOINT`. Target Atlas does not export host endpoints for Iceberg REST, Trino, Redpanda, Zeppelin, or Airflow.
- Acceptance is complete static catalog coverage plus focused live smoke, not live execution of all 19 scenarios.
- Promotions are feature PR → `develop`, then `develop` PR → `main`; required checks must pass at each gate.

---

## File map

| Path | Responsibility |
| --- | --- |
| `infra` gitlink | Records the immutable Atlas source. |
| `.gitignore` | Excludes generated `atlas-consumer.env`. |
| `lakehouse/atlas_endpoints.py` | Parses values and resolves host endpoints with documented precedence. |
| `lakehouse/catalog.py`, `datasets/s3.py` | Consume the MinIO export while retaining local credentials. |
| `tests/scenarios/live_exec.py`, streaming producer, live tests, Layer 2 | Resolve non-exported data-eng host endpoints safely. |
| `scripts/start-all.sh` | Canonical consumer lifecycle and endpoint export/assertion. |
| `tests/test_atlas_usage_contract.py` | Full catalog no-host-port guard. |
| `.github/workflows/ci.yml` | Pinned, non-live consumer validation job. |
| `docs` source files | Current pin, image rebuild, endpoint boundary, #791 status. |
| README and wiki outputs | Generated only with `scripts/build_docs.py`. |

### Task 1: Verify the worktree and pin the reviewed Atlas commit

**Files:**
- Modify: `infra` gitlink
- Modify: `tests/test_submodule.py`
- Test: `tests/test_submodule.py`

**Interfaces:**
- Consumes the Atlas object `af7713ee43f71e140e57735488001bc1cfb09245`.
- Produces a parent-tree pin test that works even when CI does not initialize the submodule.

- [ ] **Step 1: Verify branch and preserve user work**

Run:

```bash
git switch codex/atlas-consumer-modernization
git fetch --prune origin
git status --short --branch
git diff -- docs/superpowers/plans/2026-07-21-atlas-submodule-modernization.md
```

Expected: only the historical untracked plan appears; it is neither modified nor staged.

- [ ] **Step 2: Write the failing gitlink test**

Append to `tests/test_submodule.py`:

```python
import subprocess

ATLAS_PIN = "af7713ee43f71e140e57735488001bc1cfb09245"


def test_infra_gitlink_is_the_reviewed_atlas_commit():
    out = subprocess.run(
        ["git", "ls-files", "-s", "--", "infra"],
        cwd=ROOT,
        capture_output=True,
        check=True,
        text=True,
    ).stdout.strip()
    mode, sha, stage, path = out.split()
    assert (mode, stage, path) == ("160000", "0", "infra")
    assert sha == ATLAS_PIN
```

- [ ] **Step 3: Run it to prove the current staged pin fails**

Run: `uv run pytest tests/test_submodule.py::test_infra_gitlink_is_the_reviewed_atlas_commit -q`

Expected: FAIL because the staged `infra` gitlink is still `2d006cae`.

- [ ] **Step 4: Fetch, verify ancestry, and checkout the immutable SHA**

```bash
git -C infra fetch --prune origin
git -C infra merge-base --is-ancestor af7713ee43f71e140e57735488001bc1cfb09245 origin/main
git -C infra checkout --detach af7713ee43f71e140e57735488001bc1cfb09245
git submodule status infra
```

Expected: ancestry exits 0, and status starts with `af7713ee` without `+` or `-`.

- [ ] **Step 5: Stage the updated gitlink, verify, and commit**

```bash
git add infra tests/test_submodule.py
uv run pytest tests/test_submodule.py -q
git commit -m "chore: bump Atlas pin to af7713ee"
```

Expected: test PASS; commit contains only gitlink and test.

### Task 2: Create the parent endpoint resolver

**Files:**
- Create: `lakehouse/atlas_endpoints.py`
- Create: `tests/lakehouse/test_atlas_endpoints.py`
- Modify: `lakehouse/catalog.py`
- Modify: `datasets/s3.py`
- Modify: `tests/lakehouse/test_catalog.py`
- Modify: `tests/datasets/test_s3.py`

**Interfaces:**
- `read_env_file(path: Path) -> dict[str, str]`
- `env_value(key: str, *, env: Mapping[str, str], env_file: Path) -> str`
- `resolve_http_endpoint(override_key: str, port_key: str, *, env: Mapping[str, str] | None, env_file: Path, export_key: str | None, export_file: Path | None) -> str`
- Precedence: explicit override → supported export → environment or `infra/.env` port. A missing value raises `RuntimeError` naming the two accepted controls.

- [ ] **Step 1: Write failing resolver tests**

Create `tests/lakehouse/test_atlas_endpoints.py`:

```python
from lakehouse.atlas_endpoints import read_env_file, resolve_http_endpoint


def test_read_env_file_uses_last_assignment(tmp_path):
    path = tmp_path / ".env"
    path.write_text("# ignored\nMINIO_PORT=63020\nMINIO_PORT=64020\n")
    assert read_env_file(path) == {"MINIO_PORT": "64020"}


def test_explicit_override_wins_over_export_and_port(tmp_path):
    export = tmp_path / "atlas-consumer.env"
    export.write_text("ATLAS_MINIO_HOST_ENDPOINT=http://localhost:63020\n")
    env_file = tmp_path / ".env"
    env_file.write_text("MINIO_PORT=64020\n")
    value = resolve_http_endpoint(
        "MINIO_HOST_ENDPOINT", "MINIO_PORT",
        env={"MINIO_HOST_ENDPOINT": "http://example.test:9000"},
        env_file=env_file,
        export_key="ATLAS_MINIO_HOST_ENDPOINT",
        export_file=export,
    )
    assert value == "http://example.test:9000"


def test_supported_export_wins_over_local_port(tmp_path):
    export = tmp_path / "atlas-consumer.env"
    export.write_text("ATLAS_MINIO_HOST_ENDPOINT=http://localhost:63120\n")
    env_file = tmp_path / ".env"
    env_file.write_text("MINIO_PORT=63020\n")
    value = resolve_http_endpoint(
        "MINIO_HOST_ENDPOINT", "MINIO_PORT", env={}, env_file=env_file,
        export_key="ATLAS_MINIO_HOST_ENDPOINT", export_file=export,
    )
    assert value == "http://localhost:63120"


def test_unexported_service_uses_resolved_port(tmp_path):
    env_file = tmp_path / ".env"
    env_file.write_text("TRINO_PORT=63140\n")
    assert resolve_http_endpoint(
        "TRINO_HOST_ENDPOINT", "TRINO_PORT", env={}, env_file=env_file,
    ) == "http://localhost:63140"
```

- [ ] **Step 2: Prove the test fails**

Run: `uv run pytest tests/lakehouse/test_atlas_endpoints.py -q`

Expected: FAIL with `ModuleNotFoundError`.

- [ ] **Step 3: Implement the smallest resolver**

Create `lakehouse/atlas_endpoints.py`:

```python
from __future__ import annotations

import os
from collections.abc import Mapping
from pathlib import Path


def read_env_file(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    if not path.exists():
        return values
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        values[key] = value.strip()
    return values


def env_value(key: str, *, env: Mapping[str, str], env_file: Path) -> str:
    if key in env and env[key].strip():
        return env[key].strip()
    return read_env_file(env_file).get(key, "").strip()


def resolve_http_endpoint(
    override_key: str,
    port_key: str,
    *,
    env: Mapping[str, str] | None = None,
    env_file: Path,
    export_key: str | None = None,
    export_file: Path | None = None,
) -> str:
    values = os.environ if env is None else env
    override = values.get(override_key, "").strip()
    if override:
        return override
    if export_key and export_file:
        exported = read_env_file(export_file).get(export_key, "").strip()
        if exported:
            return exported
    port = env_value(port_key, env=values, env_file=env_file)
    if port:
        return f"http://localhost:{port}"
    raise RuntimeError(
        f"{override_key} or {port_key} is required; start Atlas and resolve its endpoint first"
    )
```

- [ ] **Step 4: Run unit tests**

Run: `uv run pytest tests/lakehouse/test_atlas_endpoints.py -q`

Expected: 4 PASS.

- [ ] **Step 5: Use the supported MinIO export, and test precedence**

In `lakehouse/catalog.py` and `datasets/s3.py`, derive `export_file = Path(infra_dir).parent / "atlas-consumer.env"`. Replace only host MinIO URL construction with:

```python
minio_endpoint = resolve_http_endpoint(
    "MINIO_HOST_ENDPOINT",
    "MINIO_PORT",
    env_file=env_file,
    export_key="ATLAS_MINIO_HOST_ENDPOINT",
    export_file=export_file,
)
```

Keep `ICEBERG_REST_PORT`, MinIO root credentials, and MinIO region behavior unchanged.

Add these tests:

```python
def test_catalog_prefers_exported_minio_endpoint(tmp_path):
    infra = _write_env(
        tmp_path,
        ICEBERG_REST_PORT="64110",
        MINIO_PORT="64093",
        MINIO_ROOT_USER="minioadmin",
        MINIO_ROOT_PASSWORD="secret",
    )
    (tmp_path / "atlas-consumer.env").write_text(
        "ATLAS_MINIO_HOST_ENDPOINT=http://localhost:65120\n"
    )
    assert catalog._catalog_config(infra)["s3.endpoint"] == "http://localhost:65120"
```

```python
def test_client_prefers_exported_minio_endpoint(tmp_path):
    infra = tmp_path / "infra"
    infra.mkdir()
    (infra / ".env").write_text(
        "MINIO_ROOT_USER=minioadmin\nMINIO_ROOT_PASSWORD=secret\nMINIO_PORT=64093\n"
    )
    (tmp_path / "atlas-consumer.env").write_text(
        "ATLAS_MINIO_HOST_ENDPOINT=http://localhost:65120\n"
    )
    assert s3mod.s3_client_from_env(infra).meta.endpoint_url == "http://localhost:65120"
```

- [ ] **Step 6: Verify and commit**

```bash
uv run pytest tests/lakehouse/test_atlas_endpoints.py tests/lakehouse/test_catalog.py tests/datasets/test_s3.py -q
git add lakehouse/atlas_endpoints.py lakehouse/catalog.py datasets/s3.py \
  tests/lakehouse/test_atlas_endpoints.py tests/lakehouse/test_catalog.py tests/datasets/test_s3.py
git commit -m "feat: resolve Atlas consumer endpoints explicitly"
```

Expected: PASS; no `.env` artifact is staged.

### Task 3: Migrate live helpers and streaming producer configuration

**Files:**
- Modify: `tests/scenarios/live_exec.py`
- Modify: `tests/scenarios/test_live_exec_unit.py`
- Modify: `tests/scenarios/test_trino_query_live.py`
- Modify: `tests/scenarios/test_streaming_live.py`
- Modify: `tests/infra/layer2.py`
- Modify: `scenarios/streaming_ingest-events-spark-iceberg/producer.py`
- Create: `tests/scenarios/test_streaming_producer.py`

**Interfaces:**
- `live_exec._http_endpoint(override_key: str, port_key: str) -> str`
- `producer._resolve_bootstrap() -> str`
- Explicit host controls: `ICEBERG_REST_HOST_ENDPOINT`, `ZEPPELIN_HOST_ENDPOINT`, `TRINO_HOST_ENDPOINT`, and `REDPANDA_BOOTSTRAP`.

- [ ] **Step 1: Write failing override tests**

Add to `tests/scenarios/test_live_exec_unit.py`:

```python
def test_http_endpoint_prefers_explicit_override(monkeypatch):
    monkeypatch.setenv("ZEPPELIN_HOST_ENDPOINT", "http://example.test:8890")
    assert le._http_endpoint("ZEPPELIN_HOST_ENDPOINT", "ZEPPELIN_PORT") == "http://example.test:8890"
```

Create `tests/scenarios/test_streaming_producer.py`:

```python
import importlib.util
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
PRODUCER = ROOT / "scenarios" / "streaming_ingest-events-spark-iceberg" / "producer.py"


def _load():
    spec = importlib.util.spec_from_file_location("event_producer", PRODUCER)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_bootstrap_uses_explicit_override(monkeypatch):
    monkeypatch.setenv("REDPANDA_BOOTSTRAP", "broker.example.test:19092")
    assert _load()._resolve_bootstrap() == "broker.example.test:19092"
```

- [ ] **Step 2: Confirm failure**

Run: `uv run pytest tests/scenarios/test_live_exec_unit.py tests/scenarios/test_streaming_producer.py -q`

Expected: FAIL because `_http_endpoint` is absent and producer resolves live configuration at import.

- [ ] **Step 3: Implement the resolver integration**

In `tests/scenarios/live_exec.py`, add the repository root to `sys.path`, import `resolve_http_endpoint`, and add:

```python
def _http_endpoint(override_key: str, port_key: str) -> str:
    return resolve_http_endpoint(override_key, port_key, env_file=INFRA_ENV)
```

Use it for Iceberg REST and Zeppelin. For MinIO, add `export_key="ATLAS_MINIO_HOST_ENDPOINT"` and `export_file=ROOT / "atlas-consumer.env"`.

In the producer, add root import setup and replace global bootstrap calculation with:

```python
def _resolve_bootstrap() -> str:
    explicit = os.environ.get("REDPANDA_BOOTSTRAP", "").strip()
    if explicit:
        return explicit
    endpoint = resolve_http_endpoint(
        "REDPANDA_HOST_ENDPOINT",
        "REDPANDA_KAFKA_PORT",
        env_file=INFRA_ENV,
    )
    return endpoint.removeprefix("http://").removeprefix("https://")


def main(count: int = 100) -> None:
    producer = KafkaProducer(
        bootstrap_servers=_resolve_bootstrap(),
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
    )
```

- [ ] **Step 4: Update direct live callers**

Use `_live_exec()._http_endpoint("TRINO_HOST_ENDPOINT", "TRINO_PORT")` in `test_trino_query_live.py`, parse it with `urllib.parse.urlparse`, and pass host and port to `trino.dbapi.connect`.

Keep `REDPANDA_BOOTSTRAP` as first choice in `test_streaming_live.py`; otherwise resolve `"REDPANDA_HOST_ENDPOINT"` plus `"REDPANDA_KAFKA_PORT"` and remove its HTTP scheme for `KafkaAdminClient`.

Use the resolver in `tests/infra/layer2.py` `_zeppelin_probe()` before requesting `{endpoint}/api/interpreter/setting`.

- [ ] **Step 5: Verify and commit**

```bash
uv run pytest tests/scenarios/test_live_exec_unit.py tests/scenarios/test_streaming_producer.py \
  tests/scenarios/test_trino_query_live.py tests/scenarios/test_streaming_live.py \
  tests/infra/test_layer2_unit.py -q
git add tests/scenarios/live_exec.py tests/scenarios/test_live_exec_unit.py \
  tests/scenarios/test_trino_query_live.py tests/scenarios/test_streaming_live.py \
  tests/infra/layer2.py scenarios/streaming_ingest-events-spark-iceberg/producer.py \
  tests/scenarios/test_streaming_producer.py
git commit -m "refactor: centralize Atlas host endpoint resolution"
```

Expected: non-live tests PASS; live tests remain skipped without `RUN_INFRA=1`.

### Task 4: Align launcher, manifest, and Compose validation

**Files:**
- Modify: `.gitignore`
- Modify: `scripts/start-all.sh`
- Modify: `tests/scripts/test_start_all_smoke.py`
- Modify: `tests/scripts/test_consumer_manifest.py`
- Modify: `compose/data-eng-lab.yml` only if target validation identifies a concrete incompatibility

**Interfaces:**
- Produces ignored `atlas-consumer.env`.
- Every Atlas lifecycle command receives `--consumer "$MANIFEST"`.
- The launcher asserts only `ATLAS_MINIO_HOST_ENDPOINT`.

- [ ] **Step 1: Write failing launcher contract tests**

Replace the dry-run required tokens in `tests/scripts/test_start_all_smoke.py`:

```python
required = [
    "_user/data-eng-lab", "env backfill", "compose validate", "doctor",
    "--consumer", "--track data-eng", "--detach", "endpoints export",
    "atlas-consumer.env", "ATLAS_MINIO_HOST_ENDPOINT",
    "register_iceberg", "preflight", "layer2",
]
for token in required:
    assert token in text, f"dry-run plan missing {token!r}:\n{text}"
```

Add:

```python
def test_start_all_asserts_only_supported_endpoint_contract():
    text = START.read_text(encoding="utf-8")
    assert "ATLAS_MINIO_HOST_ENDPOINT" in text
    for unsupported in (
        "ATLAS_ICEBERG_REST_HOST_ENDPOINT",
        "ATLAS_TRINO_HOST_ENDPOINT",
        "ATLAS_REDPANDA_HOST_ENDPOINT",
        "ATLAS_ZEPPELIN_HOST_ENDPOINT",
        "ATLAS_AIRFLOW_HOST_ENDPOINT",
    ):
        assert unsupported not in text
```

In `tests/scripts/test_consumer_manifest.py`, parse `compose/data-eng-lab.yml` and assert its `services` mapping contains `airflow-scheduler`, `airflow-dag-processor`, and `jupyterhub`; also assert `"services/_user"` is absent from overlay text.

- [ ] **Step 2: Confirm the missing phases fail the test**

Run: `uv run pytest tests/scripts/test_start_all_smoke.py tests/scripts/test_consumer_manifest.py -q`

Expected: FAIL because compose validation, export, and assertion are absent.

- [ ] **Step 3: Implement canonical eight-phase startup**

Append to `.gitignore`:

```gitignore
# Atlas endpoint contract generated by scripts/start-all.sh
atlas-consumer.env
```

In `scripts/start-all.sh`, preserve legacy symlink cleanup and use:

```bash
log "3/8 consumer compose validation"
run "(cd \"$INFRA_DIR\" && ./start.sh --consumer \"$MANIFEST\" compose validate)"

log "4/8 consumer doctor"
run "(cd \"$INFRA_DIR\" && ./start.sh --consumer \"$MANIFEST\" doctor --format json)"

log "5/8 launching Atlas data-eng track (detached; Atlas waits on health gates)"
run "(cd \"$INFRA_DIR\" && ./start.sh --consumer \"$MANIFEST\" --track data-eng --no-tui --detach)"

log "6/8 exporting and asserting the supported endpoint contract"
run "(cd \"$INFRA_DIR\" && ./start.sh --consumer \"$MANIFEST\" endpoints export --format env --output \"$ROOT/atlas-consumer.env\")"
run "(cd \"$INFRA_DIR\" && ./start.sh --consumer \"$MANIFEST\" endpoints assert --require ATLAS_MINIO_HOST_ENDPOINT)"
```

Renumber namespace registration to phase 7 and Layer 1/2 preflight to phase 8. Keep source choices solely in the manifest.

- [ ] **Step 4: Validate target overlay compatibility**

```bash
[ -f infra/.env ] || cp infra/.env.example infra/.env
(cd infra && ./start.sh env backfill)
(cd infra && ./start.sh --consumer ../atlas.consumer.yml compose validate)
(cd infra && ./start.sh --consumer ../atlas.consumer.yml doctor --format json)
```

Expected: all exit 0. If a specific target incompatibility appears, fix the smallest parent overlay line, extend the structural test, and rerun. Do not edit `infra`.

- [ ] **Step 5: Verify and commit**

```bash
uv run pytest tests/scripts/test_start_all_smoke.py tests/scripts/test_consumer_manifest.py -q
bash scripts/start-all.sh --dry-run
git add .gitignore scripts/start-all.sh tests/scripts/test_start_all_smoke.py \
  tests/scripts/test_consumer_manifest.py compose/data-eng-lab.yml
git commit -m "feat: validate and export Atlas consumer endpoints"
```

Expected: PASS. Omit the overlay from staging if it did not change.

### Task 5: Guard all executable artifacts and the CI consumer contract

**Files:**
- Create: `tests/test_atlas_usage_contract.py`
- Create: `tests/test_ci_atlas_contract.py`
- Modify: `.github/workflows/ci.yml`

**Interfaces:**
- Full catalog: 19 scenario DAGs, 2 Spark-app DAGs, 19 Zeppelin notebooks, 19 Jupyter notebooks.
- CI `atlas-consumer-contract` initializes the pinned submodule and runs non-live Atlas validation.

- [ ] **Step 1: Write failing CI and catalog tests**

Create `tests/test_atlas_usage_contract.py`:

```python
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def _artifacts() -> list[Path]:
    return (
        sorted((ROOT / "scenarios").rglob("dag.py"))
        + sorted((ROOT / "spark-apps").rglob("dag.py"))
        + sorted((ROOT / "scenarios").rglob("notebook.zpln"))
        + sorted((ROOT / "scenarios").rglob("notebook.ipynb"))
    )


def test_catalog_has_expected_atlas_artifacts():
    assert len(sorted((ROOT / "scenarios").rglob("dag.py"))) == 19
    assert len(sorted((ROOT / "spark-apps").rglob("dag.py"))) == 2
    assert len(sorted((ROOT / "scenarios").rglob("notebook.zpln"))) == 19
    assert len(sorted((ROOT / "scenarios").rglob("notebook.ipynb"))) == 19


def test_executable_artifacts_do_not_hardcode_host_ports():
    offenders = [
        path.relative_to(ROOT).as_posix()
        for path in _artifacts()
        if "localhost:" in path.read_text(encoding="utf-8")
        or "127.0.0.1:" in path.read_text(encoding="utf-8")
    ]
    assert not offenders, f"host endpoint literals in executable artifacts: {offenders}"
```

Create `tests/test_ci_atlas_contract.py`:

```python
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def test_ci_has_a_pinned_atlas_consumer_contract_job():
    text = (ROOT / ".github/workflows/ci.yml").read_text(encoding="utf-8")
    for required in (
        "atlas-consumer-contract:",
        "submodules: recursive",
        "cp infra/.env.example infra/.env",
        "./start.sh env backfill",
        "--consumer ../atlas.consumer.yml compose validate",
        "--consumer ../atlas.consumer.yml doctor --format json",
    ):
        assert required in text
```

- [ ] **Step 2: Establish the missing CI job**

Run: `uv run pytest tests/test_atlas_usage_contract.py tests/test_ci_atlas_contract.py -q`

Expected: catalog test PASS; CI test FAIL.

- [ ] **Step 3: Add the non-live GitHub Actions job**

Add under `jobs:` in `.github/workflows/ci.yml`:

```yaml
  atlas-consumer-contract:
    runs-on: ubuntu-24.04
    steps:
      - uses: actions/checkout@34e114876b0b11c390a56381ad16ebd13914f8d5
        with:
          submodules: recursive
          persist-credentials: false
      - uses: astral-sh/setup-uv@d4b2f3b6ecc6e67c4457f6d3e41ec42d3d0fcb86
        with:
          enable-cache: true
      - name: Validate the pinned Atlas consumer contract
        run: |
          cp infra/.env.example infra/.env
          cd infra
          ./start.sh env backfill
          ./start.sh --consumer ../atlas.consumer.yml compose validate
          ./start.sh --consumer ../atlas.consumer.yml doctor --format json
```

Do not start containers or call `endpoints assert` in hosted CI.

- [ ] **Step 4: Verify and commit**

```bash
uv run pytest tests/test_atlas_usage_contract.py tests/test_ci_atlas_contract.py \
  tests/test_dag_catalog_conf.py tests/test_submodule.py -q
git add tests/test_atlas_usage_contract.py tests/test_ci_atlas_contract.py .github/workflows/ci.yml
git commit -m "test: guard Atlas consumer contract"
```

Expected: PASS.

### Task 6: Update documentation and generated surfaces

**Files:**
- Modify: `docs/atlas-pin-bump-runbook.md`
- Modify: `docs/atlas-feedback-go-live.md`
- Modify: `docs/go-live.md`
- Modify: `docs/atlas-expectations.md`
- Modify: `docs/getting-started.md`
- Modify: `docs/atlas-enablement.md` when launcher wording differs
- Modify: `docs/CHANGELOG.md`
- Modify: only source diagrams with exact stale #791 text
- Regenerate: root README, scenario docs, app docs, wiki

**Interfaces:**
- `docs/` is the source of truth.
- #791 is resolved at target pin pending Task 7 proof; #792 remains documented unless live evidence shows otherwise.

- [ ] **Step 1: Add failing docs assertions to `tests/test_atlas_usage_contract.py`**

```python
def test_current_docs_do_not_describe_atlas_791_as_pending():
    docs = [
        ROOT / "docs" / "atlas-feedback-go-live.md",
        ROOT / "docs" / "go-live.md",
    ]
    stale = [
        path.relative_to(ROOT).as_posix()
        for path in docs
        if "awaiting the upstream compose change" in path.read_text(encoding="utf-8")
        or "DAG execution is currently blocked upstream (atlas#791)" in path.read_text(encoding="utf-8")
    ]
    assert not stale


def test_pin_bump_runbook_describes_automatic_target_rebuild():
    text = (ROOT / "docs" / "atlas-pin-bump-runbook.md").read_text(encoding="utf-8")
    assert ".atlas-build-state" in text
    assert "automatically" in text
    assert "atlas#506, open" not in text
```

- [ ] **Step 2: Confirm docs currently fail**

Run: `uv run pytest tests/test_atlas_usage_contract.py -q`

Expected: FAIL on pending #791 and manual #506 wording.

- [ ] **Step 3: Update canonical docs**

- `docs/atlas-pin-bump-runbook.md`: replace manual “#506, open” cold-build step with automatic `--build` after a changed source commit, tracked in ignored `.atlas-build-state`; retain cold reset only for destructive volume reset or uncommitted Atlas Dockerfile edits. Add consumer Compose validation and post-start `endpoints assert --require ATLAS_MINIO_HOST_ENDPOINT`.
- `docs/atlas-feedback-go-live.md`: retain the dated `881df596` live-gate finding, then add an `af7713ee` update that #850 shares a durable API JWT secret across the Airflow services. Require Task 7 DAG evidence before claiming success. Keep #792 open.
- `docs/go-live.md`: replace “pending #791” with `nyc_taxi_etl` success verification; retain the distinct SparkSubmitHook/#792 caveat. Update Pre-Execute troubleshooting to verify target config and link feedback.
- `docs/atlas-expectations.md`: set current pin to `af7713ee`, preserving historic SHAs including the `881df596` live-gate baseline.
- `docs/getting-started.md` and `docs/atlas-enablement.md`: document eight launcher phases, ignored `atlas-consumer.env`, MinIO export, and explicit-override plus `infra/.env` fallback for unexported data-eng ports.
- `docs/CHANGELOG.md`: add Unreleased entry for the new pin, automatic rebuild, MinIO export, and #791 verification.
- Search diagrams for `atlas#791`, `localhost:8080/execution`, or “pending”; change only exact stale claims.

- [ ] **Step 4: Regenerate and verify surfaces**

```bash
uv run --group dev python scripts/build_docs.py --root .
uv run --group dev python scripts/build_docs.py --root . --check
uv run --group dev python scripts/check_surfaces.py --root .
uv run --group dev python scripts/check_diagrams.py --root .
uv run --group dev mkdocs build --strict
uv run pytest tests/test_atlas_usage_contract.py tests/scripts/test_build_docs.py \
  tests/scripts/test_check_surfaces.py -q
```

Expected: all PASS; review generated diffs before staging.

- [ ] **Step 5: Commit only canonical docs and attributable outputs**

```bash
git add docs/atlas-pin-bump-runbook.md docs/atlas-feedback-go-live.md \
  docs/go-live.md docs/atlas-expectations.md docs/getting-started.md \
  docs/atlas-enablement.md docs/CHANGELOG.md tests/test_atlas_usage_contract.py
git add -u README.md scenarios spark-apps architectures
git diff --cached --name-only
git commit -m "docs: update Atlas consumer operations"
```

Expected: `wiki/` remains generated and ignored for the docs-sync workflow; no runtime artifact or historical planning draft is staged.

### Task 7: Perform focused live validation

**Files:**
- Modify only when a live failure has a new parent-side regression test and smallest compatible fix.
- Record runtime evidence in the feature PR; do not commit env files, endpoint artifact, build marker, logs, volumes, or data.

- [ ] **Step 1: Start target Atlas and assert supported endpoint export**

```bash
make down
make up
test -s atlas-consumer.env
grep '^ATLAS_MINIO_HOST_ENDPOINT=http://localhost:' atlas-consumer.env
(cd infra && ./start.sh --consumer ../atlas.consumer.yml endpoints assert --require ATLAS_MINIO_HOST_ENDPOINT)
```

Expected: all eight phases complete; Atlas rebuilds stale images when its old build marker differs from target; assertion reports required field present.

- [ ] **Step 2: Prove Layer 1 and Layer 2**

```bash
RUN_INFRA=1 uv run python tests/infra/preflight.py
RUN_INFRA=1 uv run python tests/infra/layer2.py
RUN_INFRA=1 uv run pytest tests/infra/test_preflight_live.py tests/infra/test_layer2_live.py -q
```

Expected: no `FAIL` row.

- [ ] **Step 3: Prove #791 behavior through one real DAG**

Bootstrap datasets and JARs through the repository’s normal commands, trigger `nyc_taxi_etl` in Airflow, and capture run ID plus task log.

Expected: task success without `http://localhost:8080/execution/`, `ConnectionError`, or supervisor `SIGKILL`. If #792 driver-status behavior persists, record it separately and keep `waitAppCompletion` documentation.

- [ ] **Step 4: Exercise representative notebook, SQL, and streaming paths**

```bash
RUN_INFRA=1 uv run pytest tests/scenarios/test_scenario_execution_live.py -q
RUN_INFRA=1 uv run pytest tests/scenarios/test_trino_query_live.py tests/scenarios/test_streaming_live.py -q
uv run python scenarios/streaming_ingest-events-spark-iceberg/producer.py 10
```

Expected: Zeppelin/Spark and Jupyter/PyIceberg paths, Trino, and Redpanda metadata succeed.

- [ ] **Step 5: Confirm isolation and cleanliness**

```bash
grep '^BASE_PORT=' infra/.env
docker ps --filter 'name=data-eng-lab-' --format '{{.Names}} {{.Ports}}'
docker ps --filter 'name=data-eng-lab-ollama' --format '{{.Names}}'
git status --short
```

Expected: durable non-default port block, project-namespaced containers, no containerized Ollama, no tracked submodule changes, and only the preserved historical plan as pre-existing untracked material.

- [ ] **Step 6: Fix only evidence-backed parent defects**

If a smoke fails, write the failing regression test, make the smallest parent-only fix, re-run the exact smoke, and commit it. Do not make an empty runtime-validation commit.

### Task 8: Run release verification and promote with cleanup

**Files:**
- No product change expected.

- [ ] **Step 1: Run release-quality checks**

```bash
uv run ruff check .
shellcheck scripts/*.sh
uv run python scripts/verify_repo.py --root .
uv run pytest -m "not infra and not network" -q
uv run pytest -m network -q
uv run --group dev python scripts/build_docs.py --root . --check
uv run --group dev python scripts/check_surfaces.py --root .
uv run --group dev python scripts/check_diagrams.py --root .
uv run --group dev mkdocs build --strict
for pom in $(find spark-apps -name pom.xml -print); do mvn -q -B -f "$pom" package; done
git diff --check
git status --short
```

Expected: every command exits 0; no runtime artifacts or historic plan staged.

- [ ] **Step 2: Push and merge the feature PR into develop**

```bash
git push -u origin codex/atlas-consumer-modernization
gh pr create --base develop --head codex/atlas-consumer-modernization \
  --title "feat: modernize Atlas consumer integration" \
  --body "Pins Atlas to af7713ee, aligns consumer validation and endpoint resolution, adds contract guards, and records focused live evidence."
```

Wait for required checks and review, then:

```bash
gh pr merge --merge --delete-branch
git switch develop
git pull --ff-only origin develop
git fetch --prune origin
git branch -d codex/atlas-consumer-modernization
```

Expected: merged feature is absent locally and remotely, while `develop` contains it.

- [ ] **Step 3: Promote develop through main and finish cleanup**

```bash
gh pr create --base main --head develop \
  --title "release: promote Atlas consumer modernization" \
  --body "Promotes validated Atlas pin af7713ee and the parent consumer integration."
```

After required checks pass:

```bash
gh pr merge --merge
git fetch --prune origin
git switch main
git pull --ff-only origin main
git switch develop
git pull --ff-only origin develop
gh pr list --state open --limit 100 --json number,title,headRefName,baseRefName,url
git branch -vv
git branch -r
git worktree list --porcelain
git status --short --branch
```

Expected: no open migration PR, no migration feature branch, `develop` checked out, and no deletion of `main`, `develop`, user worktrees, or untracked planning material.

## Plan self-review

- **Spec coverage:** Tasks 1–4 cover pin, consumer configuration, endpoint boundary, launcher, and overlay; Task 5 covers all 21 DAGs and 38 notebooks plus CI; Task 6 covers docs/derived surfaces; Task 7 is the agreed live smoke; Task 8 implements Gitflow and safe cleanup.
- **Endpoint consistency:** only `ATLAS_MINIO_HOST_ENDPOINT` is exported/asserted. Iceberg, Trino, Redpanda, Zeppelin, and Airflow use explicit override then `infra/.env`.
- **Preservation:** staging commands name paths explicitly and exclude the user-owned historical plan.
