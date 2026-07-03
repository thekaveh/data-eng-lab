# data-eng-lab — Phase 1b: Lakehouse + Integration Matrix — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Register the Iceberg medallion namespaces (`bronze`/`silver`/`gold`) in the Atlas Iceberg REST catalog, add the infra-preflight **Layer 2 integration matrix** (real service↔service round-trips), and land one dataset as an Iceberg `bronze` table end-to-end — completing the lakehouse foundation.

**Architecture:** Authored **against the assumed enhanced-Atlas contract** (A1/A2/A3/A4/A6 — see Global Constraints). Stack-independent logic (namespace registration, the Layer 2 framework, harness wiring) is built and unit-tested **now** with mocks/fakes (green in CI). The parts that require the live enhanced stack (the concrete edge probes and the bronze smoke) are authored now, marked `@pytest.mark.infra`, and go green only once Atlas delivers A1–A6. The infra preflight is the executable form of the A1–A9 contract; Layer 2 turns each service-integration edge into a pass/fail cell.

**Tech Stack:** Python 3.11, `uv`, `ruff`, `pytest`; `pyiceberg[s3fs]`; Docker (`docker exec` for in-network probes); the Atlas Iceberg REST catalog + Spark Connect + Zeppelin + JupyterHub + Airflow.

## Global Constraints

- **Never edit anything under `infra/`** (the Atlas submodule).
- **Assumed enhanced-Atlas contract** (from `docs/atlas-enablement.md`) — this plan is written against it; where an item is undelivered, the corresponding live test stays `infra`-marked (deselected in CI) until it lands:
  - **A1:** Iceberg REST catalog service `iceberg-rest`, in-network `http://iceberg-rest:8181`, host-published `ICEBERG_REST_PORT` in `infra/.env`; catalog name **`lakehouse`**, warehouse `s3a://lakehouse/`. Buckets `lakehouse`/`jars`/`checkpoints` exist.
  - **A2:** Spark image carries `iceberg-spark-runtime`; Spark Connect (`sc://spark-connect:15002`) has default catalog `lakehouse` configured.
  - **A3:** Zeppelin `spark` interpreter auto-seeded; Zeppelin REST on host-published `ZEPPELIN_PORT`.
  - **A4:** JupyterHub image has `boto3`/`s3fs`/`pyiceberg`/`duckdb` and `SPARK_REMOTE=sc://spark-connect:15002`.
  - **A6:** Airflow seeds `minio_default`/`spark_default` and can drive `spark-submit`.
- **Medallion namespaces:** `bronze`, `silver`, `gold` (in the `lakehouse` catalog).
- **Host vs in-network access:** the host can reach MinIO and iceberg-rest via **published ports** (`MINIO_PORT`, `ICEBERG_REST_PORT`) but **cannot** reach Spark Connect (no host port). Therefore: `register_iceberg` (namespace metadata only — no S3 I/O) runs **from the host** against the published REST port; Layer-2 edges that use Spark Connect / in-network hostnames run **inside a container** via `docker exec` (JupyterHub for pyspark/pyiceberg/boto3; Airflow for `S3Hook`/`spark_default`), and Zeppelin is probed via its REST API on the published port.
- **Preflight taxonomy** (reused from Layer 1): `pass | fail | blocked | skipped`. Fail-loud; a disabled-by-manifest edge is `skipped`, a down stack blocks its edges.
- **Python 3.11**, ruff `line-length = 120`, `select = ["E","F","W","I"]`. New deps in `[dependency-groups] dev`.
- **Tests never hit the network or a live stack in the CI gate.** Live tests are `@pytest.mark.infra` (already deselected by `make test`/CI's `-m "not infra"`). Use PACKAGE imports for any module that defines dataclasses/namedtuples (never importlib-by-path — see the Phase 1a `registry.py` lesson).
- **Branch/PR:** `main` is protected; land via feature branch → PR with `static-and-unit` green.

## File Structure

- `lakehouse/__init__.py`, `lakehouse/catalog.py` — REST catalog builder + idempotent `ensure_namespaces`.
- `scripts/register_iceberg.py` — CLI that creates the medallion namespaces.
- `tests/infra/layer2.py` — the Layer-2 edge framework (`Edge`, `run_layer2`, `render_matrix`, `EDGES`).
- `tests/infra/probes/` — the in-container probe scripts (mounted into JupyterHub/Airflow; run via `docker exec`).
- `scripts/bronze_smoke.py` — the landing→Iceberg-bronze end-to-end job (Spark Connect).
- `tests/infra/` — unit tests (fakes) + `@pytest.mark.infra` live tests.
- `scripts/setup-overlay.sh`, `scripts/start-all.sh` — wire in `register_iceberg` + `*_ENABLED` flags + Layer 2.
- `compose/data-eng-lab.yml` — mount `tests/infra/probes` + `scripts/bronze_smoke.py` into JupyterHub/Airflow.
- `pyproject.toml`, `Makefile`, `docs/lakehouse.md`.

---

### Task 1: REST catalog helper + `register_iceberg.py` (medallion namespaces)

**Files:**
- Create: `lakehouse/__init__.py`, `lakehouse/catalog.py`, `scripts/register_iceberg.py`
- Modify: `pyproject.toml` (add `pyiceberg[s3fs]`)
- Test: `tests/lakehouse/__init__.py`, `tests/lakehouse/test_catalog.py`, `tests/lakehouse/test_register_iceberg.py`

**Interfaces:**
- Produces:
  - `catalog._catalog_config(infra_dir: Path) -> dict` (pure; builds the pyiceberg REST config from `infra/.env`; raises `RuntimeError` if `ICEBERG_REST_PORT` / MinIO creds missing)
  - `catalog.rest_catalog_from_env(infra_dir: Path)` (builds a `RestCatalog` from that config)
  - `catalog.ensure_namespaces(cat, names: list[str]) -> list[str]` (idempotent; returns the namespaces actually created)
  - `register_iceberg.run(infra_dir, namespaces, catalog=None) -> list[str]` + a `main()` CLI (default namespaces `["bronze","silver","gold"]`; `catalog` injectable for tests)

- [ ] **Step 1: Add the pyiceberg dependency**

In `pyproject.toml` `[dependency-groups] dev`, add `"pyiceberg[s3fs]>=0.7"`. Then `uv sync`.

- [ ] **Step 2: Write the failing test (catalog helper)**

Create `tests/lakehouse/__init__.py` (empty) and `tests/lakehouse/test_catalog.py`:

```python
from pathlib import Path

import pytest

from lakehouse import catalog


def _write_env(tmp_path: Path, **kv) -> Path:
    infra = tmp_path / "infra"
    infra.mkdir()
    (infra / ".env").write_text("".join(f"{k}={v}\n" for k, v in kv.items()))
    return infra


def test_catalog_config_from_env(tmp_path: Path):
    infra = _write_env(tmp_path, ICEBERG_REST_PORT="64110", MINIO_PORT="64093",
                       MINIO_ROOT_USER="minioadmin", MINIO_ROOT_PASSWORD="secret")
    cfg = catalog._catalog_config(infra)
    assert cfg["uri"] == "http://localhost:64110"
    assert cfg["s3.endpoint"] == "http://localhost:64093"
    assert cfg["s3.access-key-id"] == "minioadmin"
    assert cfg["s3.path-style-access"] == "true"


def test_catalog_config_missing_port_raises(tmp_path: Path):
    infra = _write_env(tmp_path, MINIO_PORT="64093")
    with pytest.raises(RuntimeError):
        catalog._catalog_config(infra)


class _FakeCatalog:
    def __init__(self, existing):
        self._ns = [(n,) for n in existing]
        self.created: list[str] = []

    def list_namespaces(self):
        return list(self._ns)

    def create_namespace(self, name):
        self._ns.append((name,))
        self.created.append(name)


def test_ensure_namespaces_creates_missing():
    fake = _FakeCatalog(["bronze"])
    created = catalog.ensure_namespaces(fake, ["bronze", "silver", "gold"])
    assert created == ["silver", "gold"]
    assert fake.created == ["silver", "gold"]


def test_ensure_namespaces_idempotent():
    fake = _FakeCatalog(["bronze", "silver", "gold"])
    assert catalog.ensure_namespaces(fake, ["bronze", "silver", "gold"]) == []
```

- [ ] **Step 3: Run test to verify it fails**

Run: `uv run pytest tests/lakehouse/test_catalog.py -q`
Expected: FAIL — `lakehouse.catalog` missing.

- [ ] **Step 4: Implement the catalog helper**

`lakehouse/__init__.py`: empty file.

`lakehouse/catalog.py`:

```python
"""Iceberg REST catalog helpers for the medallion lakehouse (host-side access)."""
from __future__ import annotations

from pathlib import Path


def _envval(key: str, env_file: Path) -> str:
    if not env_file.exists():
        return ""
    val = ""
    for line in env_file.read_text(encoding="utf-8").splitlines():
        if line.startswith(f"{key}="):
            val = line.split("=", 1)[1].strip()  # last wins
    return val


def _catalog_config(infra_dir: Path) -> dict:
    env_file = Path(infra_dir) / ".env"
    port = _envval("ICEBERG_REST_PORT", env_file)
    minio_port = _envval("MINIO_PORT", env_file)
    user = _envval("MINIO_ROOT_USER", env_file)
    password = _envval("MINIO_ROOT_PASSWORD", env_file)
    if not (port and minio_port and user and password):
        raise RuntimeError(
            f"Iceberg REST / MinIO settings missing in {env_file} — start the enhanced stack first."
        )
    return {
        "uri": f"http://localhost:{port}",
        "warehouse": "s3a://lakehouse/",
        "s3.endpoint": f"http://localhost:{minio_port}",
        "s3.access-key-id": user,
        "s3.secret-access-key": password,
        "s3.path-style-access": "true",
    }


def rest_catalog_from_env(infra_dir: Path):
    from pyiceberg.catalog.rest import RestCatalog

    cfg = _catalog_config(infra_dir)
    return RestCatalog(name="lakehouse", **cfg)


def ensure_namespaces(cat, names: list[str]) -> list[str]:
    """Create any of `names` not already present. Idempotent; returns those created."""
    existing = {ns[0] if isinstance(ns, tuple) else ns for ns in cat.list_namespaces()}
    created: list[str] = []
    for name in names:
        if name not in existing:
            cat.create_namespace(name)
            created.append(name)
    return created
```

- [ ] **Step 5: Write the failing test (CLI) and implement it**

Create `tests/lakehouse/test_register_iceberg.py`:

```python
import importlib.util
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
_spec = importlib.util.spec_from_file_location("register_iceberg", ROOT / "scripts" / "register_iceberg.py")
reg = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(reg)


class _FakeCatalog:
    def __init__(self):
        self._ns = []

    def list_namespaces(self):
        return list(self._ns)

    def create_namespace(self, name):
        self._ns.append((name,))


def test_run_creates_medallion_namespaces():
    fake = _FakeCatalog()
    created = reg.run(infra_dir=Path("/unused"), namespaces=["bronze", "silver", "gold"], catalog=fake)
    assert created == ["bronze", "silver", "gold"]
    # idempotent second run
    assert reg.run(infra_dir=Path("/unused"), namespaces=["bronze", "silver", "gold"], catalog=fake) == []
```

`scripts/register_iceberg.py`:

```python
#!/usr/bin/env python3
"""Create the medallion namespaces (bronze/silver/gold) in the Iceberg REST catalog."""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from lakehouse.catalog import ensure_namespaces, rest_catalog_from_env  # noqa: E402

DEFAULT_NAMESPACES = ["bronze", "silver", "gold"]


def run(infra_dir, namespaces=None, catalog=None) -> list[str]:
    namespaces = namespaces or DEFAULT_NAMESPACES
    if catalog is None:
        catalog = rest_catalog_from_env(Path(infra_dir))
    created = ensure_namespaces(catalog, namespaces)
    for ns in created:
        print(f"+ created namespace lakehouse.{ns}")
    for ns in namespaces:
        if ns not in created:
            print(f"= namespace lakehouse.{ns} already present")
    return created


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="Register medallion namespaces in the Iceberg REST catalog.")
    ap.add_argument("--infra-dir", default=str(ROOT / "infra"))
    ap.add_argument("--namespace", action="append", help="namespace (repeatable; default bronze/silver/gold)")
    args = ap.parse_args(argv)
    run(args.infra_dir, args.namespace or DEFAULT_NAMESPACES)
    return 0


if __name__ == "__main__":
    sys.exit(main())
```

- [ ] **Step 6: Run tests + lint**

Run: `uv run pytest tests/lakehouse/ -q` → all pass.
Run: `uv run pytest -m "not infra" -q` (no regressions) and `uv run ruff check .` (clean).

- [ ] **Step 7: Commit**

```bash
git add lakehouse/ scripts/register_iceberg.py tests/lakehouse/ pyproject.toml uv.lock
git commit -m "feat(lakehouse): Iceberg REST catalog helper + register_iceberg (medallion namespaces)"
```

---

### Task 2: Harness wiring — enable Iceberg in preflight + register namespaces at boot

**Files:**
- Modify: `scripts/setup-overlay.sh`, `scripts/start-all.sh`
- Test: `tests/scripts/test_setup_overlay.py`, `tests/scripts/test_start_all_smoke.py`

**Interfaces:**
- Produces: `setup-overlay.sh` writes `ICEBERG_REST_ENABLED=true` into `infra/.env`; `start-all.sh` exports the `*_ENABLED` flags (so the preflight manifest gates on them) and runs `register_iceberg` after bucket creation, before preflight.

- [ ] **Step 1: Write the failing tests**

Add to `tests/scripts/test_setup_overlay.py` (`test_creates_symlink_and_env`), after the existing `.env` assertions:

```python
    assert "ICEBERG_REST_ENABLED=true" in env_text
```

Add to `tests/scripts/test_start_all_smoke.py` (`test_start_all_dry_run_lists_plan`), extend the token list:

```python
    for token in ["setup-overlay", "--track data-eng", "create_buckets", "register_iceberg", "preflight"]:
        assert token in text, f"dry-run plan missing '{token}':\n{text}"
```

- [ ] **Step 2: Run to verify RED**

Run: `uv run pytest tests/scripts/test_setup_overlay.py tests/scripts/test_start_all_smoke.py -q`
Expected: FAIL (missing `ICEBERG_REST_ENABLED` / `register_iceberg`).

- [ ] **Step 3: Wire `setup-overlay.sh`**

After the existing `set_env` lines, add:

```bash
# Enable the enhanced-Atlas services the preflight manifest gates on (A1 delivered).
set_env         ICEBERG_REST_ENABLED true                             "$ENV_FILE"
```

- [ ] **Step 4: Wire `start-all.sh`**

After the `create_buckets` step and before the preflight step, insert a registration step, and export the enable-flag so the preflight manifest (which reads `os.environ`) sees it. Replace the preflight section so it reads:

```bash
log "4/6 creating buckets"
run "\"$HERE/create_buckets.sh\""

log "5/6 registering Iceberg medallion namespaces"
export ICEBERG_REST_ENABLED=true
run "uv run python \"$ROOT/scripts/register_iceberg.py\""

log "6/6 preflight (stack doctor)"
run "uv run python \"$ROOT/tests/infra/preflight.py\""
```

(Update the earlier step counters from `N/5` to `N/6`.)

- [ ] **Step 5: Run tests + verify RED→GREEN, lint**

Run: `uv run pytest tests/scripts/ -q` → pass.
Run: `uv run pytest -m "not infra" -q` and `uv run ruff check .` → green/clean.
Run: `./scripts/start-all.sh --dry-run` → the plan now lists `register_iceberg` between `create_buckets` and `preflight`.

- [ ] **Step 6: Commit**

```bash
git add scripts/setup-overlay.sh scripts/start-all.sh tests/scripts/
git commit -m "feat(lakehouse): wire register_iceberg into boot + enable iceberg-rest in preflight"
```

---

### Task 3: Preflight Layer 2 framework (edge registry + runner + matrix)

**Files:**
- Create: `tests/infra/layer2.py`
- Test: `tests/infra/test_layer2_unit.py`

**Interfaces:**
- Produces:
  - `Edge = namedtuple("Edge", "name enabled probe")` — `probe(exec_fn) -> tuple[bool, str]`
  - `run_layer2(edges, exec_fn, docker_ok) -> list[Result]` (reuses `preflight.Result`; status `pass|fail|blocked|skipped`)
  - `render_matrix(results) -> str`
  - `default_exec(container, argv) -> tuple[int, str]` — runs `docker exec <container> <argv…>` with a timeout; returns `(rc, combined_output)`
- Consumes: `tests/infra/preflight.py`'s `Result`.

- [ ] **Step 1: Write the failing test**

Create `tests/infra/test_layer2_unit.py`:

```python
import importlib.util
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def _load(mod):
    spec = importlib.util.spec_from_file_location(mod, ROOT / "tests" / "infra" / f"{mod}.py")
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    return m


layer2 = _load("layer2")


def _ok_edge(name):
    return layer2.Edge(name, True, lambda exec_fn: (True, "ok"))


def _bad_edge(name):
    return layer2.Edge(name, True, lambda exec_fn: (False, "boom"))


def test_disabled_edge_skipped():
    e = layer2.Edge("x", False, lambda f: (True, "ok"))
    assert layer2.run_layer2([e], exec_fn=None, docker_ok=True)[0].status == "skipped"


def test_docker_down_blocks():
    assert layer2.run_layer2([_ok_edge("x")], exec_fn=None, docker_ok=False)[0].status == "blocked"


def test_pass_and_fail_branches():
    res = layer2.run_layer2([_ok_edge("a"), _bad_edge("b")], exec_fn=lambda c, a: (0, ""), docker_ok=True)
    by = {r.name: r.status for r in res}
    assert by == {"a": "pass", "b": "fail"}


def test_probe_receives_exec_fn():
    captured = {}

    def probe(exec_fn):
        captured["rc"], captured["out"] = exec_fn("jupyterhub", ["echo", "hi"])
        return captured["rc"] == 0, captured["out"]

    fake_exec = lambda container, argv: (0, f"{container}:{' '.join(argv)}")
    res = layer2.run_layer2([layer2.Edge("e", True, probe)], exec_fn=fake_exec, docker_ok=True)
    assert res[0].status == "pass"
    assert captured["out"] == "jupyterhub:echo hi"


def test_render_matrix_contains_status():
    out = layer2.render_matrix([layer2.Result("a", "pass", "ok")])
    assert "a" in out and "PASS" in out
```

- [ ] **Step 2: Run to verify RED**

Run: `uv run pytest tests/infra/test_layer2_unit.py -q`
Expected: FAIL — `layer2` missing.

- [ ] **Step 3: Implement `tests/infra/layer2.py`**

```python
#!/usr/bin/env python3
"""Infra preflight — Layer 2 (service<->service integration matrix).

Each Edge's probe performs a real round-trip via `exec_fn` (default: `docker exec`
into a container that has in-network access + the right client libs). Unit tests
inject a fake exec_fn; live runs use the real one.
"""
from __future__ import annotations

import subprocess
import sys
from collections import namedtuple
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from preflight import Result, render_matrix as _render  # noqa: E402

Edge = namedtuple("Edge", "name enabled probe")


def default_exec(container: str, argv: list[str]) -> tuple[int, str]:
    """Run `docker exec <container> <argv...>`; return (rc, combined stdout+stderr)."""
    import os

    project = os.environ.get("PROJECT_NAME", "data-eng-lab")
    full = ["docker", "exec", f"{project}-{container}", *argv]
    try:
        out = subprocess.run(full, capture_output=True, text=True, timeout=120)
    except subprocess.TimeoutExpired:
        return 124, "<timeout>"
    except FileNotFoundError:
        return 127, "<docker-missing>"
    return out.returncode, (out.stdout + out.stderr)


def run_layer2(edges, exec_fn, docker_ok) -> list[Result]:
    results: list[Result] = []
    for e in edges:
        if not e.enabled:
            results.append(Result(e.name, "skipped", "disabled by manifest (A-item pending)"))
            continue
        if not docker_ok:
            results.append(Result(e.name, "blocked", "docker/stack unavailable"))
            continue
        try:
            ok, detail = e.probe(exec_fn)
        except Exception as exc:  # noqa: BLE001
            ok, detail = False, f"probe error: {exc}"
        results.append(Result(e.name, "pass" if ok else "fail", detail))
    return results


def render_matrix(results) -> str:
    return _render(results).replace("Layer 1 (existence & initialization)",
                                    "Layer 2 (integration matrix)")
```

*(Note: `render_matrix` reuses Layer 1's renderer; the header swap keeps a single formatting implementation.)*

- [ ] **Step 4: Run tests + lint**

Run: `uv run pytest tests/infra/test_layer2_unit.py -q` → pass.
Run: `uv run pytest -m "not infra" -q` and `uv run ruff check .` → green.

- [ ] **Step 5: Commit**

```bash
git add tests/infra/layer2.py tests/infra/test_layer2_unit.py
git commit -m "feat(preflight): Layer 2 integration-matrix framework (edges + runner + render)"
```

---

### Task 4: Concrete edge probes + `EDGES` registry (assumed contract, live-gated)

**Files:**
- Create: `tests/infra/probes/probe_spark.py`, `tests/infra/probes/probe_pyiceberg.py`, `tests/infra/probes/probe_airflow.py`
- Modify: `tests/infra/layer2.py` (add `EDGES` + Zeppelin REST probe), `compose/data-eng-lab.yml` (mount probes)
- Test: `tests/infra/test_layer2_edges_unit.py` (unit, fakes); `tests/infra/test_layer2_live.py` (`@pytest.mark.infra`)

**Interfaces:**
- Produces: `layer2.EDGES` — the concrete matrix; each probe is authored against the assumed contract and runs inside the named container (or via Zeppelin REST for the zeppelin edge).

**NOTE:** the probe *scripts* below are authored against the assumed A1–A6 contract; their exact client APIs (pyspark-connect, pyiceberg, Airflow providers, Zeppelin REST) must be validated when Atlas delivers, and adjusted if the delivered versions differ. Their unit-testable surface is the `EDGES` wiring; the round-trips themselves are `@pytest.mark.infra`.

- [ ] **Step 1: Write the probe scripts** (mounted read-only into the containers)

`tests/infra/probes/probe_spark.py` (runs in JupyterHub; proves Spark→MinIO + Spark→Iceberg):

```python
"""Probe: Spark Connect -> MinIO (s3a) + Iceberg round-trip. Exit 0 on success."""
import sys
import uuid

from pyspark.sql import SparkSession

spark = SparkSession.builder.remote("sc://spark-connect:15002").getOrCreate()
tbl = f"lakehouse.bronze._preflight_{uuid.uuid4().hex[:8]}"
try:
    spark.sql(f"CREATE TABLE {tbl} (id INT) USING iceberg")
    spark.sql(f"INSERT INTO {tbl} VALUES (1)")
    n = spark.sql(f"SELECT count(*) c FROM {tbl}").collect()[0]["c"]
    assert n == 1, f"expected 1 row, got {n}"
    print("spark->iceberg OK")
finally:
    spark.sql(f"DROP TABLE IF EXISTS {tbl}")
sys.exit(0)
```

`tests/infra/probes/probe_pyiceberg.py` (runs in JupyterHub; proves PyIceberg→REST catalog):

```python
"""Probe: PyIceberg loads the REST catalog and lists the medallion namespaces. Exit 0 on success."""
from pyiceberg.catalog import load_catalog

cat = load_catalog("lakehouse", **{"type": "rest", "uri": "http://iceberg-rest:8181"})
names = {ns[0] if isinstance(ns, tuple) else ns for ns in cat.list_namespaces()}
assert {"bronze", "silver", "gold"} <= names, f"missing namespaces: {names}"
print("pyiceberg->rest OK")
```

`tests/infra/probes/probe_airflow.py` (runs in Airflow; proves Airflow→MinIO via `S3Hook` + `spark_default` presence):

```python
"""Probe: Airflow S3Hook round-trip to MinIO + spark_default connection present. Exit 0 on success."""
import uuid

from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.hooks.base import BaseHook

hook = S3Hook(aws_conn_id="minio_default")
key = f"_preflight/{uuid.uuid4().hex}.txt"
hook.load_string("ok", key=key, bucket_name="landing", replace=True)
assert hook.check_for_key(key, bucket_name="landing"), "S3Hook round-trip failed"
hook.delete_objects(bucket="landing", keys=[key])
BaseHook.get_connection("spark_default")  # raises if unset
print("airflow->minio+spark OK")
```

- [ ] **Step 2: Add the `EDGES` registry + Zeppelin REST probe to `layer2.py`**

Append to `tests/infra/layer2.py`:

```python
def _truthy(var: str) -> bool:
    import os

    return os.environ.get(var, "").lower() in ("1", "true", "yes", "on")


def _run_in(container: str, script_path: str):
    def probe(exec_fn):
        rc, out = exec_fn(container, ["python", script_path])
        tail = out.strip().splitlines()[-1] if out.strip() else f"rc={rc}"
        return rc == 0, tail
    return probe


def _zeppelin_probe(exec_fn):
    """Zeppelin is probed from the host via its REST API (published port)."""
    import os

    import requests

    infra_env = Path(__file__).resolve().parents[2] / "infra" / ".env"
    port = ""
    if infra_env.exists():
        for line in infra_env.read_text(encoding="utf-8").splitlines():
            if line.startswith("ZEPPELIN_PORT="):
                port = line.split("=", 1)[1].strip()
    port = os.environ.get("ZEPPELIN_PORT", port)
    if not port:
        return False, "ZEPPELIN_PORT not found"
    r = requests.get(f"http://localhost:{port}/api/interpreter/setting", timeout=30)
    ok = r.status_code == 200 and "spark" in r.text
    return ok, f"interpreter API status={r.status_code}, spark-present={'spark' in r.text}"


ICEBERG_ON = _truthy("ICEBERG_REST_ENABLED")

EDGES = [
    Edge("spark->minio+iceberg", ICEBERG_ON, _run_in("jupyterhub", "/opt/probes/probe_spark.py")),
    Edge("jupyter->pyiceberg", ICEBERG_ON, _run_in("jupyterhub", "/opt/probes/probe_pyiceberg.py")),
    Edge("airflow->minio+spark", True, _run_in("airflow-scheduler", "/opt/probes/probe_airflow.py")),
    Edge("zeppelin->spark", True, _zeppelin_probe),
]


def main() -> int:
    from manifest import daemon_ok  # reuse Layer 1's real daemon check

    results = run_layer2(EDGES, default_exec, daemon_ok())
    print(render_matrix(results))
    return 1 if any(r.status == "fail" for r in results) else 0


if __name__ == "__main__":
    sys.exit(main())
```

- [ ] **Step 3: Mount the probes into the containers** (`compose/data-eng-lab.yml`)

Add to the `jupyterhub` service volumes: `- ../tests/infra/probes:/opt/probes:ro`. Add the same mount to `airflow-scheduler`. (These join the existing volume lists.)

- [ ] **Step 4: Write the unit test (wiring) and the live test (gated)**

`tests/infra/test_layer2_edges_unit.py` — proves `EDGES` gating without a stack:

```python
import importlib.util
import os
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def _load(mod):
    spec = importlib.util.spec_from_file_location(mod, ROOT / "tests" / "infra" / f"{mod}.py")
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    return m


def test_iceberg_edges_gated_off_when_flag_unset(monkeypatch):
    monkeypatch.delenv("ICEBERG_REST_ENABLED", raising=False)
    layer2 = _load("layer2")
    names = {e.name: e.enabled for e in layer2.EDGES}
    assert names["spark->minio+iceberg"] is False
    assert names["airflow->minio+spark"] is True  # base edge always on


def test_iceberg_edges_enabled_when_flag_set(monkeypatch):
    monkeypatch.setenv("ICEBERG_REST_ENABLED", "true")
    layer2 = _load("layer2")
    assert {e.name for e in layer2.EDGES if e.enabled} >= {"spark->minio+iceberg", "jupyter->pyiceberg"}
```

`tests/infra/test_layer2_live.py`:

```python
import importlib.util
import os
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
pytestmark = pytest.mark.infra


@pytest.mark.skipif(os.environ.get("RUN_INFRA") != "1",
                    reason="set RUN_INFRA=1 with a live enhanced-Atlas stack")
def test_layer2_matrix_all_pass():
    spec = importlib.util.spec_from_file_location("layer2", ROOT / "tests" / "infra" / "layer2.py")
    layer2 = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(layer2)
    from importlib import import_module  # manifest for daemon_ok
    results = layer2.run_layer2(layer2.EDGES, layer2.default_exec, docker_ok=True)
    print("\n" + layer2.render_matrix(results))
    assert not [r for r in results if r.status == "fail"], results
```

- [ ] **Step 5: Run unit tests + lint**

Run: `uv run pytest tests/infra/test_layer2_edges_unit.py -q` → pass.
Run: `uv run pytest tests/infra/test_layer2_live.py -q` → `1 skipped` (RUN_INFRA unset).
Run: `uv run pytest -m "not infra" -q` and `uv run ruff check .` → green.

- [ ] **Step 6: Commit**

```bash
git add tests/infra/probes/ tests/infra/layer2.py tests/infra/test_layer2_edges_unit.py tests/infra/test_layer2_live.py compose/data-eng-lab.yml
git commit -m "feat(preflight): Layer 2 edges + probes (spark/jupyter/airflow/zeppelin) — live-gated"
```

---

### Task 5: Bronze smoke — `landing` → Iceberg `bronze` (assumed contract, live-gated)

**Files:**
- Create: `scripts/bronze_smoke.py`
- Test: `tests/lakehouse/test_bronze_smoke.py` (`@pytest.mark.infra` live + a unit test for the pure transform)

**Interfaces:**
- Produces: `bronze_smoke.build_bronze(spark, landing_uri, table) -> int` (reads Parquet from `landing_uri`, writes an Iceberg table, returns row count) + a `main()` that connects to Spark Connect and loads `lakehouse.bronze.nyc_taxi_trips` from `s3a://landing/nyc_taxi/`.

- [ ] **Step 1: Write the pure-logic unit test + the live test**

`tests/lakehouse/test_bronze_smoke.py`:

```python
import importlib.util
import os
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
_spec = importlib.util.spec_from_file_location("bronze_smoke", ROOT / "scripts" / "bronze_smoke.py")
bs = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(bs)


def test_bronze_table_name_default():
    # pure helper: derives the target table for a landing prefix
    assert bs.bronze_table("nyc_taxi") == "lakehouse.bronze.nyc_taxi"


@pytest.mark.infra
@pytest.mark.skipif(os.environ.get("RUN_INFRA") != "1", reason="needs a live enhanced-Atlas stack")
def test_build_bronze_end_to_end():
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.remote("sc://spark-connect:15002").getOrCreate()
    n = bs.build_bronze(spark, "s3a://landing/nyc_taxi/", bs.bronze_table("nyc_taxi_trips"))
    assert n > 0
```

- [ ] **Step 2: Implement `scripts/bronze_smoke.py`**

```python
#!/usr/bin/env python3
"""Bronze smoke: load a landing dataset into an Iceberg bronze table via Spark Connect."""
from __future__ import annotations

import sys


def bronze_table(name: str) -> str:
    return f"lakehouse.bronze.{name}"


def build_bronze(spark, landing_uri: str, table: str) -> int:
    df = spark.read.parquet(landing_uri)
    (df.writeTo(table).using("iceberg").createOrReplace())
    return spark.table(table).count()


def main(argv=None) -> int:
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.remote("sc://spark-connect:15002").getOrCreate()
    n = build_bronze(spark, "s3a://landing/nyc_taxi/", bronze_table("nyc_taxi_trips"))
    print(f"bronze lakehouse.bronze.nyc_taxi_trips: {n} rows")
    return 0


if __name__ == "__main__":
    sys.exit(main())
```

- [ ] **Step 3: Run the unit test (live test skips) + lint**

Run: `uv run pytest tests/lakehouse/test_bronze_smoke.py -q` → 1 passed, 1 skipped.
Run: `uv run pytest -m "not infra" -q` and `uv run ruff check .` → green.

- [ ] **Step 4: Commit**

```bash
git add scripts/bronze_smoke.py tests/lakehouse/test_bronze_smoke.py
git commit -m "feat(lakehouse): bronze smoke (landing -> Iceberg bronze via Spark Connect) — live-gated"
```

---

### Task 6: Wire Layer 2 into the readiness gate + docs

**Files:**
- Modify: `scripts/start-all.sh` (run Layer 2 after Layer 1), `Makefile` (add `preflight-l2`), `docs/lakehouse.md` (create), `README.md` (link)
- Test: `tests/scripts/test_start_all_smoke.py`

**Interfaces:**
- Produces: `make preflight` runs Layer 1 then Layer 2; `start-all.sh` runs both as the extended readiness gate; `docs/lakehouse.md` documents the catalog + medallion + how to run the matrix.

- [ ] **Step 1: Extend the start-all smoke test**

In `tests/scripts/test_start_all_smoke.py`, extend the token list to include `layer2`:

```python
    for token in ["setup-overlay", "--track data-eng", "create_buckets", "register_iceberg", "preflight", "layer2"]:
        assert token in text
```

- [ ] **Step 2: Run RED**

Run: `uv run pytest tests/scripts/test_start_all_smoke.py -q` → FAIL (no `layer2`).

- [ ] **Step 3: Wire `start-all.sh` + Makefile**

In `start-all.sh`, change the preflight step to run both layers:

```bash
log "6/6 preflight (stack doctor: layer 1 + layer 2)"
run "uv run python \"$ROOT/tests/infra/preflight.py\""
run "uv run python \"$ROOT/tests/infra/layer2.py\""
```

In the `Makefile`, update the `preflight` recipe:

```makefile
preflight: ## Infra preflight (layer 1 existence + layer 2 integration) against a live stack
	uv run python tests/infra/preflight.py
	uv run python tests/infra/layer2.py
```

- [ ] **Step 4: Create `docs/lakehouse.md`**

```markdown
# Lakehouse

`data-eng-lab` uses an **Apache Iceberg** lakehouse on MinIO, cataloged by the Atlas **Iceberg REST
catalog** (`lakehouse`), in a **medallion** layout: `landing` (raw) → `bronze` → `silver` → `gold`.

## Namespaces
`scripts/register_iceberg.py` creates the `bronze`/`silver`/`gold` namespaces (idempotent). It runs
automatically during `make up`, or standalone: `uv run python scripts/register_iceberg.py`.

## Integration matrix (preflight Layer 2)
`make preflight` runs Layer 1 (service existence/init) then Layer 2 (real service↔service round-trips:
Spark↔MinIO↔Iceberg, Jupyter↔PyIceberg, Airflow↔MinIO/Spark, Zeppelin↔Spark). Layer 2 executes probes
inside the containers via `docker exec` (Spark Connect is not host-reachable) and prints a pass/fail
matrix. Edges depending on undelivered Atlas items are `skipped` by manifest until enabled.

## Bronze smoke
`scripts/bronze_smoke.py` loads a landing dataset into `lakehouse.bronze.*` via Spark Connect — the
end-to-end proof that the lakehouse path works.
```

Add a README §4 link to `docs/lakehouse.md`.

- [ ] **Step 5: Run tests + lint + verifier**

Run: `uv run pytest -m "not infra" -q` → green. `uv run ruff check .` → clean. `uv run python scripts/verify_repo.py --root .` → exit 0.

- [ ] **Step 6: Commit**

```bash
git add scripts/start-all.sh Makefile docs/lakehouse.md README.md tests/scripts/test_start_all_smoke.py
git commit -m "feat(lakehouse): run Layer 2 in the readiness gate + docs"
```

---

## Phase 1b exit criteria

- [ ] `uv run pytest -m "not infra" -q` → all pass (register_iceberg, catalog, Layer 2 framework + edge gating, bronze transform helper).
- [ ] `uv run ruff check .` → clean; `uv run python scripts/verify_repo.py --root .` → exit 0.
- [ ] `./scripts/start-all.sh --dry-run` → lists `register_iceberg`, `preflight`, and `layer2`.
- [ ] PR into `main`, `static-and-unit` green, squash-merge.
- [ ] **Once Atlas delivers A1–A6 (live, `RUN_INFRA=1`):** `register_iceberg` creates the 3 namespaces; `make preflight` Layer 2 matrix is all-green; `bronze_smoke` lands `lakehouse.bronze.nyc_taxi_trips`.

## Self-review (against the spec + Phase 1a outline)

**Spec coverage (Phase 1 lakehouse half):** `register_iceberg` namespaces (Task 1 ✓); preflight Layer 2 integration matrix (Tasks 3–4 ✓); bronze smoke (Task 5 ✓); boot wiring + `ICEBERG_REST_ENABLED` (Tasks 2, 6 ✓); docs (Task 6 ✓). Matches the Phase 1a plan's "Phase 1b outline".

**Now-green vs assumed-contract:** Tasks 1–3 + the wiring/unit tests are CI-green immediately (mocks/fakes). Tasks 4–5's live round-trips + Task 6's live gate are `@pytest.mark.infra`, authored against the assumed A1–A6 contract, and go green when Atlas delivers — the probe client APIs are the explicit tuning point at that time.

**Placeholder scan:** no TBD/TODO; every step has complete, runnable content. The "assumed contract" caveat on the probes is a validation note, not a placeholder — the code is real.

**Type/name consistency:** `_catalog_config`/`rest_catalog_from_env`/`ensure_namespaces`, `register_iceberg.run`, `Edge`/`run_layer2`/`render_matrix`/`default_exec`/`EDGES`, `bronze_table`/`build_bronze` are used identically across defining and consuming steps. `Result` is reused from Layer 1.
