# data-eng-lab — Phase 1a: Data Acquisition — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Give `data-eng-lab` a declarative dataset **registry** and an idempotent **downloader** that fetches/generates a curated set of standard open datasets at a chosen SCALE and lands them into MinIO's `landing` bucket — runnable **today** against the base Atlas `data-eng` track (no Iceberg/A1–A2 dependency).

**Architecture:** A `datasets/` Python package: `registry.yaml` (declarative, schema-validated) → `registry.py` (loader + `Dataset` model + SCALE resolution) → source fetchers (`sources/http.py` for direct downloads + optional unzip; `sources/tpch.py` for DuckDB-generated TPC-H) → `s3.py` (boto3 client built from `infra/.env`, uploading to `s3://landing/<prefix>/…`). A thin CLI `scripts/download_datasets.py` orchestrates. Every unit is tested off-network with mocked HTTP (`responses`) + mocked S3 (`moto`) + tiny committed fixtures; TPC-H generation is tested with a real tiny DuckDB run (no network).

**Tech Stack:** Python 3.11, `uv`, `ruff`, `pytest`; `boto3` + `moto`; `requests` + `responses`; `duckdb`; `PyYAML`; MinIO (already provided by Atlas).

## Global Constraints

- **Never edit anything under `infra/`** (the Atlas submodule). MinIO credentials + published port are read from `infra/.env` at runtime, never hardcoded or committed.
- **Phase 1a needs only the base Atlas `data-eng` track** (MinIO is on by default). No Iceberg / A1–A2 dependency. The downloader runs **from the host** and uploads via MinIO's **published** S3 port (`MINIO_PORT` in `infra/.env`), endpoint `http://localhost:<MINIO_PORT>`, path-style addressing.
- **Target bucket:** `landing` (created by Phase 0's `create_buckets.sh`). Object keys: `<landing_prefix>/<filename>`.
- **SCALE tiers:** `tiny` (CI/smoke), `small` (default dev), `medium` (few-GB). Selected via `--scale`.
- **Idempotent:** re-running skips objects already present in `landing` (unless `--force`).
- **Python 3.11**, ruff `line-length = 120`, `select = ["E","F","W","I"]`. New deps go in `[dependency-groups] dev` (the repo is not a package).
- **Unit tests never hit the network or a real MinIO.** Live end-to-end (`make datasets` against a running stack) is a manual/local verification, not a CI gate.
- **Branch/PR workflow:** `main` is protected; this work lands via a feature branch → PR with the `static-and-unit` check green.

## File Structure

- `datasets/registry.yaml` — the declarative registry (data).
- `datasets/__init__.py` — marks the package.
- `datasets/schema.py` — `REGISTRY_SCHEMA` + `validate_registry(doc) -> list[str]` (returns error messages).
- `datasets/registry.py` — `Dataset` dataclass, `load_registry(path) -> dict[str, Dataset]`, `resolve_scale(ds, scale) -> ScalePlan`.
- `datasets/s3.py` — `s3_client_from_env(infra_dir)`, `object_exists(client, bucket, key)`, `upload_file(client, path, bucket, key)`.
- `datasets/sources/__init__.py`, `datasets/sources/http.py` (`fetch_http(plan, dest) -> list[Path]`), `datasets/sources/tpch.py` (`generate_tpch(sf, dest) -> list[Path]`).
- `scripts/download_datasets.py` — CLI orchestrator.
- `scripts/verify_repo.py` — extend with a registry-consistency check.
- `tests/datasets/…` — unit tests; `tests/fixtures/datasets/…` — tiny committed sample files.
- `Makefile` — `datasets` target; `pyproject.toml` — new dev deps; `docs/datasets.md` — usage.

---

### Task 1: Dataset registry schema + `registry.yaml` + verifier check

**Files:**
- Create: `datasets/registry.yaml`, `datasets/__init__.py`, `datasets/schema.py`
- Modify: `scripts/verify_repo.py`, `scripts/verify_repo_config.yaml`
- Test: `tests/datasets/__init__.py`, `tests/datasets/test_schema.py`, `tests/test_verify_repo.py` (add a case)

**Interfaces:**
- Produces: `schema.validate_registry(doc: dict) -> list[str]` (empty list = valid). `verify_repo.py` gains `_check_dataset_registry` which loads `datasets/registry.yaml`, validates it, and emits an `error` Finding per problem (check id `dataset.registry`).

- [ ] **Step 1: Write the failing test**

Create `tests/datasets/__init__.py` (empty) and `tests/datasets/test_schema.py`:

```python
import importlib.util
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
_spec = importlib.util.spec_from_file_location("schema", ROOT / "datasets" / "schema.py")
schema = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(schema)


def _valid_doc():
    return {
        "version": 1,
        "datasets": {
            "nyc_taxi": {
                "description": "d", "format": "parquet", "license": "public",
                "landing_prefix": "nyc_taxi", "fetch": {"kind": "http"},
                "scales": {"tiny": {"urls": ["https://x/a.parquet"]}},
            }
        },
    }


def test_valid_registry_has_no_errors():
    assert schema.validate_registry(_valid_doc()) == []


def test_missing_landing_prefix_is_error():
    doc = _valid_doc()
    del doc["datasets"]["nyc_taxi"]["landing_prefix"]
    errs = schema.validate_registry(doc)
    assert any("landing_prefix" in e for e in errs), errs


def test_unknown_fetch_kind_is_error():
    doc = _valid_doc()
    doc["datasets"]["nyc_taxi"]["fetch"]["kind"] = "ftp"
    errs = schema.validate_registry(doc)
    assert any("fetch.kind" in e for e in errs), errs


def test_http_scale_requires_urls():
    doc = _valid_doc()
    doc["datasets"]["nyc_taxi"]["scales"]["tiny"] = {}
    errs = schema.validate_registry(doc)
    assert any("urls" in e for e in errs), errs
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/datasets/test_schema.py -q`
Expected: FAIL — `datasets/schema.py` missing.

- [ ] **Step 3: Implement schema, registry.yaml, and the verifier check**

`datasets/__init__.py`: empty file.

`datasets/schema.py`:

```python
"""Validation for datasets/registry.yaml. Pure functions, no I/O."""
from __future__ import annotations

VALID_KINDS = {"http", "tpch"}


def validate_registry(doc: dict) -> list[str]:
    """Return a list of human-readable error strings; empty means valid."""
    errors: list[str] = []
    if doc.get("version") != 1:
        errors.append("registry: 'version' must be 1")
    datasets = doc.get("datasets")
    if not isinstance(datasets, dict) or not datasets:
        errors.append("registry: 'datasets' must be a non-empty mapping")
        return errors

    for name, ds in datasets.items():
        p = f"datasets.{name}"
        for field in ("description", "format", "license", "landing_prefix", "fetch", "scales"):
            if field not in ds:
                errors.append(f"{p}: missing '{field}'")
        kind = (ds.get("fetch") or {}).get("kind")
        if kind not in VALID_KINDS:
            errors.append(f"{p}: fetch.kind '{kind}' not in {sorted(VALID_KINDS)}")
        scales = ds.get("scales") or {}
        if not scales:
            errors.append(f"{p}: 'scales' must define at least one tier")
        for tier, spec in scales.items():
            if kind == "http" and not spec.get("urls"):
                errors.append(f"{p}.scales.{tier}: http datasets require a non-empty 'urls' list")
            if kind == "tpch" and "sf" not in spec:
                errors.append(f"{p}.scales.{tier}: tpch datasets require 'sf'")
    return errors
```

`datasets/registry.yaml` (v1 core — the generic `http` kind handles any direct-download dataset, so more can be added later as pure data with no code change):

```yaml
version: 1
datasets:
  nyc_taxi:
    description: "NYC TLC yellow taxi trips (columnar analytical, time-partitioned)"
    format: parquet
    license: "NYC TLC — public"
    landing_prefix: nyc_taxi
    fetch: { kind: http }
    scales:
      tiny:
        urls:
          - "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-01.parquet"
      small:
        urls:
          - "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-01.parquet"
          - "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-02.parquet"
          - "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-03.parquet"
      medium:
        urls:
          - "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-01.parquet"
          - "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-02.parquet"
          - "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-03.parquet"
          - "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-04.parquet"
          - "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-05.parquet"
          - "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-06.parquet"
  gh_archive:
    description: "GH Archive hourly GitHub events (semi-structured JSON; also the streaming source)"
    format: json.gz
    license: "GH Archive — public"
    landing_prefix: gh_archive
    fetch: { kind: http }
    scales:
      tiny:
        urls: ["https://data.gharchive.org/2023-01-01-0.json.gz"]
      small:
        urls:
          - "https://data.gharchive.org/2023-01-01-0.json.gz"
          - "https://data.gharchive.org/2023-01-01-1.json.gz"
          - "https://data.gharchive.org/2023-01-01-2.json.gz"
      medium:
        urls:
          - "https://data.gharchive.org/2023-01-01-0.json.gz"
          - "https://data.gharchive.org/2023-01-01-1.json.gz"
          - "https://data.gharchive.org/2023-01-01-2.json.gz"
          - "https://data.gharchive.org/2023-01-01-3.json.gz"
          - "https://data.gharchive.org/2023-01-01-4.json.gz"
          - "https://data.gharchive.org/2023-01-01-5.json.gz"
  movielens:
    description: "MovieLens ratings + movies (joins, feature engineering)"
    format: csv
    license: "GroupLens — research use; downloaded, not redistributed"
    landing_prefix: movielens
    fetch: { kind: http, unzip: true }
    scales:
      tiny:
        urls: ["https://files.grouplens.org/datasets/movielens/ml-latest-small.zip"]
      small:
        urls: ["https://files.grouplens.org/datasets/movielens/ml-latest-small.zip"]
      medium:
        urls: ["https://files.grouplens.org/datasets/movielens/ml-25m.zip"]
  tpch:
    description: "TPC-H benchmark, DuckDB-generated (star schema, joins, SQL)"
    format: parquet
    license: "TPC-H spec; generated data is unrestricted"
    landing_prefix: tpch
    fetch: { kind: tpch }
    scales:
      tiny: { sf: 0.01 }
      small: { sf: 1 }
      medium: { sf: 10 }
```

Extend `scripts/verify_repo.py` — add this check function and register it in `CHECKS`:

```python
def _check_dataset_registry(root: Path, cfg: dict) -> list[Finding]:
    import importlib.util  # noqa: PLC0415

    reg = root / "datasets" / "registry.yaml"
    if not reg.exists():
        return []  # registry is optional until Phase 1a lands
    # Load this repo's schema.py BY PATH relative to verify_repo.py's own location, so it
    # works no matter how the verifier is invoked (sys.path[0] is scripts/, not the repo root).
    schema_path = Path(__file__).resolve().parent.parent / "datasets" / "schema.py"
    spec = importlib.util.spec_from_file_location("_dataset_schema", schema_path)
    schema = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(schema)
    try:
        doc = yaml.safe_load(reg.read_text(encoding="utf-8"))
    except Exception as exc:  # noqa: BLE001
        return [Finding("dataset.registry", "error", f"registry.yaml is not valid YAML: {exc}")]
    return [Finding("dataset.registry", "error", msg) for msg in schema.validate_registry(doc)]
```

Register it: change `CHECKS = [_check_naming, _check_declared_dirs_exist]` to
`CHECKS = [_check_naming, _check_declared_dirs_exist, _check_dataset_registry]`.

The check loads `datasets/schema.py` by file path relative to `verify_repo.py`'s own location
(`Path(__file__).parent.parent`) — so it does NOT depend on `datasets` being an importable package
(when the verifier runs as a script, `sys.path[0]` is `scripts/`, not the repo root). It validates
whichever `registry.yaml` lives under `--root`, against this repo's schema.

Add to `tests/test_verify_repo.py`:

```python
def test_registry_check_flags_invalid_registry(tmp_path: Path):
    # a repo root with a broken registry produces a dataset.registry error.
    # (schema.py is loaded from THIS repo relative to verify_repo.py, so no monkeypatch needed.)
    (tmp_path / "datasets").mkdir()
    (tmp_path / "datasets" / "registry.yaml").write_text("version: 2\ndatasets: {}\n")
    cfg = {"active_scenario_dirs": [], "scenario_name_regex": r"^x$"}
    findings = verify_repo.run_checks(tmp_path, cfg)
    assert any(f.check == "dataset.registry" and f.severity == "error" for f in findings), findings
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/datasets/test_schema.py tests/test_verify_repo.py -q`
Expected: PASS (all).

- [ ] **Step 5: Verify the real registry validates and lint is clean**

Run: `uv run python scripts/verify_repo.py --root .` → `0 error(s)`, exit 0.
Run: `uv run ruff check .` → clean.

- [ ] **Step 6: Commit**

```bash
git add datasets/registry.yaml datasets/__init__.py datasets/schema.py scripts/verify_repo.py scripts/verify_repo_config.yaml tests/datasets/ tests/test_verify_repo.py
git commit -m "feat(datasets): registry.yaml + schema validation wired into the verifier"
```

---

### Task 2: Registry loader (`datasets/registry.py`)

**Files:**
- Create: `datasets/registry.py`
- Test: `tests/datasets/test_registry.py`

**Interfaces:**
- Produces:
  - `@dataclass(frozen=True) class Dataset: name: str; description: str; format: str; license: str; landing_prefix: str; kind: str; unzip: bool; scales: dict`
  - `load_registry(path: Path) -> dict[str, Dataset]` (raises `ValueError` listing schema errors if invalid)
  - `@dataclass(frozen=True) class ScalePlan: dataset: Dataset; scale: str; urls: list[str]; sf: float | None`
  - `resolve_scale(ds: Dataset, scale: str) -> ScalePlan` (raises `KeyError` for an unknown scale)

- [ ] **Step 1: Write the failing test**

Create `tests/datasets/test_registry.py`:

```python
import importlib.util
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
_spec = importlib.util.spec_from_file_location("dataset_registry", ROOT / "datasets" / "registry.py")
reg = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(reg)

REAL = ROOT / "datasets" / "registry.yaml"


def test_load_real_registry_has_core_datasets():
    ds = reg.load_registry(REAL)
    assert {"nyc_taxi", "gh_archive", "movielens", "tpch"} <= set(ds)
    assert ds["nyc_taxi"].kind == "http"
    assert ds["tpch"].kind == "tpch"
    assert ds["movielens"].unzip is True


def test_resolve_http_scale_returns_urls():
    ds = reg.load_registry(REAL)["nyc_taxi"]
    plan = reg.resolve_scale(ds, "tiny")
    assert plan.urls and plan.sf is None
    assert all(u.startswith("http") for u in plan.urls)


def test_resolve_tpch_scale_returns_sf():
    ds = reg.load_registry(REAL)["tpch"]
    plan = reg.resolve_scale(ds, "small")
    assert plan.sf == 1 and plan.urls == []


def test_unknown_scale_raises():
    ds = reg.load_registry(REAL)["nyc_taxi"]
    with pytest.raises(KeyError):
        reg.resolve_scale(ds, "gigantic")


def test_invalid_registry_raises(tmp_path: Path):
    bad = tmp_path / "r.yaml"
    bad.write_text("version: 1\ndatasets: {}\n")
    with pytest.raises(ValueError):
        reg.load_registry(bad)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/datasets/test_registry.py -q`
Expected: FAIL — `datasets/registry.py` missing.

- [ ] **Step 3: Implement `datasets/registry.py`**

```python
"""Load and resolve datasets/registry.yaml."""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import yaml

from datasets.schema import validate_registry


@dataclass(frozen=True)
class Dataset:
    name: str
    description: str
    format: str
    license: str
    landing_prefix: str
    kind: str
    unzip: bool
    scales: dict


@dataclass(frozen=True)
class ScalePlan:
    dataset: Dataset
    scale: str
    urls: list[str]
    sf: float | None


def load_registry(path: Path) -> dict[str, Dataset]:
    doc = yaml.safe_load(Path(path).read_text(encoding="utf-8"))
    errors = validate_registry(doc)
    if errors:
        raise ValueError("invalid registry:\n  - " + "\n  - ".join(errors))
    out: dict[str, Dataset] = {}
    for name, ds in doc["datasets"].items():
        fetch = ds["fetch"]
        out[name] = Dataset(
            name=name,
            description=ds["description"],
            format=ds["format"],
            license=ds["license"],
            landing_prefix=ds["landing_prefix"],
            kind=fetch["kind"],
            unzip=bool(fetch.get("unzip", False)),
            scales=ds["scales"],
        )
    return out


def resolve_scale(ds: Dataset, scale: str) -> ScalePlan:
    if scale not in ds.scales:
        raise KeyError(f"dataset '{ds.name}' has no scale '{scale}' (have: {sorted(ds.scales)})")
    spec = ds.scales[scale]
    return ScalePlan(
        dataset=ds,
        scale=scale,
        urls=list(spec.get("urls", [])),
        sf=spec.get("sf"),
    )
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/datasets/test_registry.py -q`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add datasets/registry.py tests/datasets/test_registry.py
git commit -m "feat(datasets): registry loader with Dataset/ScalePlan models"
```

---

### Task 3: MinIO S3 helper (`datasets/s3.py`)

**Files:**
- Create: `datasets/s3.py`
- Modify: `pyproject.toml` (add `moto` to dev deps)
- Test: `tests/datasets/test_s3.py`

**Interfaces:**
- Produces:
  - `s3_client_from_env(infra_dir: Path)` — builds a boto3 S3 client from `infra/.env` (`MINIO_ROOT_USER`, `MINIO_ROOT_PASSWORD`, `MINIO_PORT`), endpoint `http://localhost:<MINIO_PORT>`, path-style, region `us-east-1`. Raises `RuntimeError` if creds/port absent.
  - `object_exists(client, bucket: str, key: str) -> bool`
  - `upload_file(client, path: Path, bucket: str, key: str) -> None`

- [ ] **Step 1: Add the moto dev dependency**

In `pyproject.toml`, add to `[dependency-groups] dev`: `"moto[s3]>=5"`. Then `uv sync` so it's available.

- [ ] **Step 2: Write the failing test**

Create `tests/datasets/test_s3.py`:

```python
import importlib.util
from pathlib import Path

import boto3
import pytest
from moto import mock_aws

ROOT = Path(__file__).resolve().parents[2]
_spec = importlib.util.spec_from_file_location("dataset_s3", ROOT / "datasets" / "s3.py")
s3mod = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(s3mod)


@mock_aws
def test_upload_and_exists_roundtrip(tmp_path: Path):
    client = boto3.client("s3", region_name="us-east-1")
    client.create_bucket(Bucket="landing")
    f = tmp_path / "a.txt"
    f.write_text("hello")

    assert s3mod.object_exists(client, "landing", "nyc_taxi/a.txt") is False
    s3mod.upload_file(client, f, "landing", "nyc_taxi/a.txt")
    assert s3mod.object_exists(client, "landing", "nyc_taxi/a.txt") is True


def test_client_from_env_reads_infra_env(tmp_path: Path):
    infra = tmp_path / "infra"
    infra.mkdir()
    (infra / ".env").write_text(
        "MINIO_ROOT_USER=minioadmin\nMINIO_ROOT_PASSWORD=secret\nMINIO_PORT=64093\n"
    )
    client = s3mod.s3_client_from_env(infra)
    assert client.meta.endpoint_url == "http://localhost:64093"


def test_client_from_env_missing_creds_raises(tmp_path: Path):
    infra = tmp_path / "infra"
    infra.mkdir()
    (infra / ".env").write_text("MINIO_PORT=64093\n")
    with pytest.raises(RuntimeError):
        s3mod.s3_client_from_env(infra)
```

- [ ] **Step 3: Run test to verify it fails**

Run: `uv run pytest tests/datasets/test_s3.py -q`
Expected: FAIL — `datasets/s3.py` missing.

- [ ] **Step 4: Implement `datasets/s3.py`**

```python
"""Thin boto3 helper for landing objects into MinIO, configured from infra/.env."""
from __future__ import annotations

from pathlib import Path

import boto3
from botocore.client import Config


def _envval(key: str, env_file: Path) -> str:
    if not env_file.exists():
        return ""
    val = ""
    for line in env_file.read_text(encoding="utf-8").splitlines():
        if line.startswith(f"{key}="):
            val = line.split("=", 1)[1].strip()  # last wins
    return val


def s3_client_from_env(infra_dir: Path):
    env_file = Path(infra_dir) / ".env"
    user = _envval("MINIO_ROOT_USER", env_file)
    password = _envval("MINIO_ROOT_PASSWORD", env_file)
    port = _envval("MINIO_PORT", env_file)
    if not (user and password and port):
        raise RuntimeError(
            f"MinIO creds/port missing in {env_file} — start the stack (make up) first "
            "so Atlas generates them."
        )
    return boto3.client(
        "s3",
        endpoint_url=f"http://localhost:{port}",
        aws_access_key_id=user,
        aws_secret_access_key=password,
        region_name="us-east-1",
        config=Config(s3={"addressing_style": "path"}),
    )


def object_exists(client, bucket: str, key: str) -> bool:
    from botocore.exceptions import ClientError

    try:
        client.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError as exc:
        if exc.response["Error"]["Code"] in ("404", "NoSuchKey", "NotFound"):
            return False
        raise


def upload_file(client, path: Path, bucket: str, key: str) -> None:
    client.upload_file(str(path), bucket, key)
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `uv run pytest tests/datasets/test_s3.py -q`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add datasets/s3.py pyproject.toml uv.lock tests/datasets/test_s3.py
git commit -m "feat(datasets): MinIO S3 helper (client from infra/.env, upload/exists) + moto tests"
```

---

### Task 4: HTTP source fetcher (`datasets/sources/http.py`)

**Files:**
- Create: `datasets/sources/__init__.py`, `datasets/sources/http.py`
- Modify: `pyproject.toml` (add `responses` to dev deps)
- Test: `tests/datasets/test_http.py`

**Interfaces:**
- Consumes: `ScalePlan` (from Task 2).
- Produces: `fetch_http(plan: ScalePlan, dest: Path) -> list[Path]` — downloads each URL in `plan.urls` into `dest`; if `plan.dataset.unzip`, extracts each `.zip` and returns the extracted files instead of the zip. Returns the list of local files to upload.

- [ ] **Step 1: Add the responses dev dependency**

In `pyproject.toml` `[dependency-groups] dev`, add `"responses>=0.25"`. Then `uv sync`.

- [ ] **Step 2: Write the failing test**

Create `tests/datasets/test_http.py`:

```python
import importlib.util
import io
import zipfile
from pathlib import Path

import responses

ROOT = Path(__file__).resolve().parents[2]


def _load(name, rel):
    spec = importlib.util.spec_from_file_location(name, ROOT / rel)
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    return m


reg = _load("dataset_registry", "datasets/registry.py")
http = _load("dataset_http", "datasets/sources/http.py")


def _plan(urls, unzip=False):
    ds = reg.Dataset("d", "d", "csv", "l", "d", "http", unzip, {"tiny": {"urls": urls}})
    return reg.resolve_scale(ds, "tiny")


@responses.activate
def test_fetch_downloads_each_url(tmp_path: Path):
    responses.add(responses.GET, "https://x/a.parquet", body=b"AAA", status=200)
    responses.add(responses.GET, "https://x/b.parquet", body=b"BBB", status=200)
    files = http.fetch_http(_plan(["https://x/a.parquet", "https://x/b.parquet"]), tmp_path)
    assert sorted(f.name for f in files) == ["a.parquet", "b.parquet"]
    assert (tmp_path / "a.parquet").read_bytes() == b"AAA"


@responses.activate
def test_fetch_unzips_when_flagged(tmp_path: Path):
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as z:
        z.writestr("ml/ratings.csv", "u,m,r\n1,2,3\n")
    responses.add(responses.GET, "https://x/ml.zip", body=buf.getvalue(), status=200)
    files = http.fetch_http(_plan(["https://x/ml.zip"], unzip=True), tmp_path)
    names = [f.name for f in files]
    assert "ratings.csv" in names
    assert not any(f.suffix == ".zip" for f in files)
```

- [ ] **Step 3: Run test to verify it fails**

Run: `uv run pytest tests/datasets/test_http.py -q`
Expected: FAIL — module missing.

- [ ] **Step 4: Implement the fetcher**

`datasets/sources/__init__.py`: empty file.

`datasets/sources/http.py`:

```python
"""HTTP source fetcher: download raw dataset files (with optional unzip) to a local dir."""
from __future__ import annotations

import zipfile
from pathlib import Path
from urllib.parse import urlparse

import requests


def _download(url: str, dest: Path) -> Path:
    name = Path(urlparse(url).path).name
    out = dest / name
    with requests.get(url, stream=True, timeout=120) as resp:
        resp.raise_for_status()
        with open(out, "wb") as fh:
            for chunk in resp.iter_content(chunk_size=1 << 20):
                fh.write(chunk)
    return out


def _unzip(path: Path, dest: Path) -> list[Path]:
    extracted: list[Path] = []
    with zipfile.ZipFile(path) as z:
        for member in z.namelist():
            if member.endswith("/"):
                continue
            target = dest / Path(member).name  # flatten
            with z.open(member) as src, open(target, "wb") as out:
                out.write(src.read())
            extracted.append(target)
    path.unlink()  # drop the zip once extracted
    return extracted


def fetch_http(plan, dest: Path) -> list[Path]:
    dest.mkdir(parents=True, exist_ok=True)
    files: list[Path] = []
    for url in plan.urls:
        downloaded = _download(url, dest)
        if plan.dataset.unzip and downloaded.suffix == ".zip":
            files.extend(_unzip(downloaded, dest))
        else:
            files.append(downloaded)
    return files
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `uv run pytest tests/datasets/test_http.py -q`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add datasets/sources/__init__.py datasets/sources/http.py pyproject.toml uv.lock tests/datasets/test_http.py
git commit -m "feat(datasets): HTTP fetcher with optional unzip + responses tests"
```

---

### Task 5: TPC-H generator (`datasets/sources/tpch.py`)

**Files:**
- Create: `datasets/sources/tpch.py`
- Modify: `pyproject.toml` (add `duckdb` to dev deps)
- Test: `tests/datasets/test_tpch.py`

**Interfaces:**
- Produces: `generate_tpch(sf: float, dest: Path) -> list[Path]` — uses DuckDB's `tpch` extension to `dbgen(sf=...)`, exports each TPC-H table to `dest/<table>.parquet`, returns the parquet paths. Deterministic; no network.

- [ ] **Step 1: Add the duckdb dependency**

In `pyproject.toml` `[dependency-groups] dev`, add `"duckdb>=1.0"`. Then `uv sync`.

- [ ] **Step 2: Write the failing test**

Create `tests/datasets/test_tpch.py`:

```python
import importlib.util
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
_spec = importlib.util.spec_from_file_location("dataset_tpch", ROOT / "datasets" / "sources" / "tpch.py")
tpch = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(tpch)


def test_generate_tiny_tpch(tmp_path: Path):
    files = tpch.generate_tpch(0.01, tmp_path)
    names = {f.stem for f in files}
    # TPC-H has 8 tables
    assert {"lineitem", "orders", "customer", "nation", "region", "part", "supplier", "partsupp"} <= names
    assert all(f.suffix == ".parquet" and f.stat().st_size > 0 for f in files)
```

- [ ] **Step 3: Run test to verify it fails**

Run: `uv run pytest tests/datasets/test_tpch.py -q`
Expected: FAIL — module missing.

- [ ] **Step 4: Implement the generator**

`datasets/sources/tpch.py`:

```python
"""Generate TPC-H data locally via DuckDB's tpch extension and export to Parquet."""
from __future__ import annotations

from pathlib import Path

import duckdb

TPCH_TABLES = [
    "customer", "lineitem", "nation", "orders",
    "part", "partsupp", "region", "supplier",
]


def generate_tpch(sf: float, dest: Path) -> list[Path]:
    dest.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    con.execute("INSTALL tpch; LOAD tpch;")
    con.execute(f"CALL dbgen(sf={sf})")
    out: list[Path] = []
    for table in TPCH_TABLES:
        target = dest / f"{table}.parquet"
        con.execute(f"COPY {table} TO '{target.as_posix()}' (FORMAT PARQUET)")
        out.append(target)
    con.close()
    return out
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `uv run pytest tests/datasets/test_tpch.py -q`
Expected: PASS (8 parquet files generated at SF 0.01).

- [ ] **Step 6: Commit**

```bash
git add datasets/sources/tpch.py pyproject.toml uv.lock tests/datasets/test_tpch.py
git commit -m "feat(datasets): DuckDB TPC-H generator + test"
```

---

### Task 6: Downloader CLI (`scripts/download_datasets.py`)

**Files:**
- Create: `scripts/download_datasets.py`
- Test: `tests/datasets/test_download_cli.py`

**Interfaces:**
- Consumes: `load_registry`, `resolve_scale`, `fetch_http`, `generate_tpch`, `s3_client_from_env`, `object_exists`, `upload_file`.
- Produces:
  - `plan_uploads(datasets: dict, scale: str, only: list[str] | None) -> list[tuple[str, str]]` — returns `(dataset_name, scale)` pairs to process (pure; used for `--dry-run`).
  - `run(registry_path, infra_dir, scale, only, force, dry_run, client=None) -> int` — orchestrates fetch→upload into `landing`; returns count of objects uploaded (0 on dry-run). `client` is injectable for tests.
  - CLI: `--scale {tiny,small,medium}` (default `small`), `--only NAME` (repeatable), `--force`, `--dry-run`.

- [ ] **Step 1: Write the failing test**

Create `tests/datasets/test_download_cli.py`:

```python
import importlib.util
from pathlib import Path

import boto3
from moto import mock_aws

ROOT = Path(__file__).resolve().parents[2]
_spec = importlib.util.spec_from_file_location("download_datasets", ROOT / "scripts" / "download_datasets.py")
cli = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(cli)

REG = ROOT / "datasets" / "registry.yaml"


def test_dry_run_plans_selected_datasets():
    pairs = cli.plan_uploads(cli.load_registry(REG), "tiny", only=["nyc_taxi"])
    assert pairs == [("nyc_taxi", "tiny")]


@mock_aws
def test_run_uploads_http_dataset(tmp_path: Path, monkeypatch):
    client = boto3.client("s3", region_name="us-east-1")
    client.create_bucket(Bucket="landing")

    # stub the http fetcher to avoid the network: land two fake files
    def fake_fetch(plan, dest):
        dest.mkdir(parents=True, exist_ok=True)
        a = dest / "part-0.parquet"
        a.write_bytes(b"X")
        return [a]

    monkeypatch.setattr(cli, "fetch_http", fake_fetch)

    n = cli.run(REG, infra_dir=tmp_path, scale="tiny", only=["nyc_taxi"],
                force=False, dry_run=False, client=client)
    assert n == 1
    assert cli.object_exists(client, "landing", "nyc_taxi/part-0.parquet")

    # idempotent: second run skips the existing object
    n2 = cli.run(REG, infra_dir=tmp_path, scale="tiny", only=["nyc_taxi"],
                 force=False, dry_run=False, client=client)
    assert n2 == 0
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/datasets/test_download_cli.py -q`
Expected: FAIL — script missing.

- [ ] **Step 3: Implement the CLI**

`scripts/download_datasets.py`:

```python
#!/usr/bin/env python3
"""Download/generate datasets at a chosen SCALE and land them into MinIO's 'landing' bucket."""
from __future__ import annotations

import argparse
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from datasets.registry import load_registry, resolve_scale  # noqa: E402
from datasets.s3 import object_exists, s3_client_from_env, upload_file  # noqa: E402
from datasets.sources.http import fetch_http  # noqa: E402
from datasets.sources.tpch import generate_tpch  # noqa: E402

BUCKET = "landing"


def plan_uploads(datasets: dict, scale: str, only: list[str] | None) -> list[tuple[str, str]]:
    names = [n for n in datasets if (not only or n in only)]
    return [(n, scale) for n in names]


def _fetch_files(plan, dest: Path) -> list[Path]:
    if plan.dataset.kind == "http":
        return fetch_http(plan, dest)
    if plan.dataset.kind == "tpch":
        return generate_tpch(plan.sf, dest)
    raise ValueError(f"unknown fetch kind: {plan.dataset.kind}")


def run(registry_path, infra_dir, scale, only, force, dry_run, client=None) -> int:
    datasets = load_registry(Path(registry_path))
    pairs = plan_uploads(datasets, scale, only)
    if dry_run:
        for name, sc in pairs:
            print(f"+ would land {name} @ {sc} -> s3://{BUCKET}/{datasets[name].landing_prefix}/")
        return 0

    if client is None:
        client = s3_client_from_env(Path(infra_dir))

    uploaded = 0
    for name, sc in pairs:
        ds = datasets[name]
        plan = resolve_scale(ds, sc)
        with tempfile.TemporaryDirectory() as tmp:
            files = _fetch_files(plan, Path(tmp))
            for f in files:
                key = f"{ds.landing_prefix}/{f.name}"
                if not force and object_exists(client, BUCKET, key):
                    print(f"= skip existing s3://{BUCKET}/{key}")
                    continue
                upload_file(client, f, BUCKET, key)
                uploaded += 1
                print(f"↑ s3://{BUCKET}/{key}")
    return uploaded


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="Land datasets into MinIO.")
    ap.add_argument("--scale", choices=["tiny", "small", "medium"], default="small")
    ap.add_argument("--only", action="append", help="dataset name (repeatable)")
    ap.add_argument("--force", action="store_true", help="re-upload even if present")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--registry", default=str(ROOT / "datasets" / "registry.yaml"))
    ap.add_argument("--infra-dir", default=str(ROOT / "infra"))
    args = ap.parse_args(argv)
    n = run(args.registry, args.infra_dir, args.scale, args.only, args.force, args.dry_run)
    print(f"\nlanded {n} object(s) into s3://{BUCKET}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/datasets/test_download_cli.py -q`
Expected: PASS (dry-run plan + upload + idempotent skip).

- [ ] **Step 5: Verify the whole non-infra suite + lint**

Run: `uv run pytest -m "not infra" -q` → all green.
Run: `uv run ruff check .` → clean.

- [ ] **Step 6: Commit**

```bash
git add scripts/download_datasets.py tests/datasets/test_download_cli.py
git commit -m "feat(datasets): idempotent downloader CLI (fetch/generate -> MinIO landing)"
```

---

### Task 7: `make datasets`, docs, and CI

**Files:**
- Modify: `Makefile`, `docs/…` (create `docs/datasets.md`), `README.md` (link it)
- Test: `tests/test_makefile.py` (already asserts targets; extend the target list check)

**Interfaces:**
- Produces: `make datasets` runs the downloader (default `small`, overridable via `SCALE=`); documentation of the dataset flow.

- [ ] **Step 1: Update the Makefile-target test**

In `tests/test_makefile.py`, the `TARGETS` list already contains `datasets`. Change the `datasets` recipe expectation by asserting it now invokes the downloader — add this test:

```python
def test_datasets_target_runs_downloader():
    text = subprocess.run(["make", "-npq"], cwd=ROOT, capture_output=True, text=True).stdout
    assert "download_datasets.py" in text, "make datasets should call the downloader"
```

- [ ] **Step 2: Run it to verify it fails**

Run: `uv run pytest tests/test_makefile.py -q`
Expected: FAIL — the stub `datasets` target doesn't call the downloader yet.

- [ ] **Step 3: Update the Makefile `datasets` target**

Replace the stub recipe:

```makefile
datasets: ## Download datasets into MinIO (Phase 1)
	@echo "datasets: implemented in Phase 1"
```

with:

```makefile
datasets: ## Download datasets into MinIO landing bucket (override tier with SCALE=tiny|small|medium)
	uv run python scripts/download_datasets.py --scale $(if $(SCALE),$(SCALE),small)
```

- [ ] **Step 4: Create `docs/datasets.md`**

```markdown
# Datasets

`data-eng-lab` lands a curated set of standard open datasets into MinIO's `landing` bucket, driven by
a declarative registry.

## Registry
`datasets/registry.yaml` declares each dataset: `format`, `license`, `landing_prefix`, a `fetch.kind`
(`http` for direct downloads with optional `unzip`, or `tpch` for DuckDB-generated TPC-H), and per-SCALE
parameters (`tiny` / `small` / `medium`). It is schema-validated by the repo verifier.

## Usage
```bash
make up                 # boot the Atlas data-eng track (MinIO must be running)
make datasets            # land the 'small' tier
make datasets SCALE=tiny # CI-sized subset
uv run python scripts/download_datasets.py --scale medium --only nyc_taxi
uv run python scripts/download_datasets.py --dry-run   # show what would be landed
```
The downloader reads MinIO credentials + the published S3 port from `infra/.env` and is idempotent
(existing objects are skipped unless `--force`).

## Current datasets
| Dataset | Shape | Fetch |
|---|---|---|
| `nyc_taxi` | Columnar analytical (Parquet) | http |
| `gh_archive` | Semi-structured JSON events / streaming source | http |
| `movielens` | Ratings + joins | http (unzip) |
| `tpch` | Benchmark star-schema | tpch (DuckDB) |

More datasets (e.g. `online_retail`, `noaa`) are added as pure registry entries (`http` kind) when
their Phase 2 scenarios need them — no code change required.
```

Add a line under README §3 (Quick start) pointing to `docs/datasets.md`.

- [ ] **Step 5: Run tests + lint + verifier**

Run: `uv run pytest -m "not infra" -q` → all green.
Run: `uv run ruff check .` → clean.
Run: `uv run python scripts/verify_repo.py --root .` → exit 0.

- [ ] **Step 6: Commit**

```bash
git add Makefile docs/datasets.md README.md tests/test_makefile.py
git commit -m "feat(datasets): wire 'make datasets' + docs"
```

---

## Phase 1a exit criteria (verify all)

- [ ] `uv run pytest -m "not infra" -q` → all pass (registry, loader, s3/moto, http/responses, tpch, CLI).
- [ ] `uv run ruff check .` → clean; `uv run python scripts/verify_repo.py --root .` → exit 0 (registry validated).
- [ ] `make datasets SCALE=tiny --dry-run`-equivalent (`uv run python scripts/download_datasets.py --dry-run --scale tiny`) prints the plan.
- [ ] **Local integration (manual, needs the stack):** `make up` → `make datasets SCALE=tiny` lands objects under `s3://landing/{nyc_taxi,gh_archive,movielens,tpch}/` (verify in the MinIO console); a second run skips them (idempotent).
- [ ] PR into `main` with `static-and-unit` green; squash-merge.

## Self-review (against the spec)

**Spec coverage (Phase 1 "data" half):** dataset registry (Task 1 ✓), downloader + SCALE knobs (Tasks 4–6 ✓), TPC-H via DuckDB (Task 5 ✓), fixtures/mocked tests so CI needs no network (Tasks 1–6 ✓), `make datasets` (Task 7 ✓), lands into MinIO `landing` (Task 3/6 ✓). The lakehouse/Iceberg + preflight-Layer-2 half is **Phase 1b** (below), gated on Atlas A1/A2.

**Placeholder scan:** no TBD/TODO; every code step is complete and runnable. The registry ships 4 datasets; adding more is pure data (the `http` fetcher is generic) — explicitly noted, not a gap.

**Type/name consistency:** `Dataset`, `ScalePlan`, `resolve_scale`, `fetch_http(plan, dest)`, `generate_tpch(sf, dest)`, `s3_client_from_env`, `object_exists`, `upload_file`, `run(...)`, `plan_uploads(...)` are used identically across defining and consuming tasks.

---

## Phase 1b — Lakehouse + Integration Matrix (OUTLINE — gated on Atlas A1/A2/A3/A4/A6)

Written as a detailed plan **once Atlas delivers** the Iceberg REST catalog (A1), Iceberg Spark-runtime jar (A2), Zeppelin interpreter seeding (A3), Jupyter data libs (A4), and the Airflow spark-submit client (A6). Anticipated tasks:

1. **`scripts/register_iceberg.py`** — create `bronze`/`silver`/`gold` namespaces in the REST catalog via PyIceberg (`load_catalog("rest", uri=ICEBERG_REST_URI)`); idempotent; unit-tested with a mocked catalog, live-gated integration test creates them for real.
2. **Preflight Layer 2 — integration matrix** — extend `tests/infra/` with an edge registry + `run_layer2` + matrix render, executing real round-trips **inside the network** (via `docker exec` into jupyter/airflow, and Zeppelin's REST API on its published port): Spark→MinIO (s3a r/w), Spark→Iceberg (create/insert/select/drop), Zeppelin→Spark (`%spark` paragraph), Jupyter→Spark Connect / MinIO / PyIceberg, Airflow→MinIO (`S3Hook`) / Spark (`spark_default`), Iceberg-REST→Postgres/MinIO. Unit-tested with injected fakes now; live bodies finalized against the delivered stack.
3. **Bronze smoke** — a minimal `landing`→Iceberg `bronze` load (one dataset) proving the end-to-end lakehouse path, run via Spark Connect.
4. **Wire into `start-all.sh`** — run `register_iceberg` after bucket creation and preflight Layer 2 as the extended readiness gate; enable the `ICEBERG_REST_ENABLED` manifest flag.
5. **Docs** — `docs/lakehouse.md` (catalog usage, medallion namespaces).

**Exit:** preflight Layer 2 all-green against enhanced Atlas; a dataset visible as an Iceberg `bronze` table queryable from Spark, PySpark, and PyIceberg.
