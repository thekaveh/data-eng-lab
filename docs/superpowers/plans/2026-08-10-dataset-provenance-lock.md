# Dataset Provenance Lock Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the version-1 dataset source catalog with a populated, validated version-2 provenance lock for every HTTP and TPC-H dataset scale without yet changing downloader enforcement.

**Architecture:** `datasets/registry.yaml` remains the single source of truth. Pure helpers canonicalize schemas and validate lock primitives; a version-2 schema validator and typed registry parser expose normalized HTTP artifacts and deterministic generator outputs. Read-only audit tooling gathers candidate metadata, while production download/upload/reuse enforcement remains isolated in child issue #81.

**Tech Stack:** Python 3.11, PyYAML, Requests, DuckDB 1.5.4, Docker/OCI, pytest, Moto, Ruff, MkDocs Material.

## Global Constraints

- Work only on `codex/80-dataset-provenance-lock`, created from current `origin/develop`.
- Never modify files inside `infra`; its gitlink remains `c6cf73d7168db1a7840fc45c9ed3e385071996d8`.
- Never touch or stage `docs/superpowers/plans/2026-07-21-atlas-submodule-modernization.md`; its SHA-256 remains `f7eb55036e3020f419d9acb42bbc502bfe0528219a980a6973ed083591d2ba66`.
- Registry version 2 is the only accepted production schema when this plan finishes.
- Every accepted artifact has one exact positive byte size and lowercase 64-character SHA-256 digest.
- Source and object drift fail closed; there are no alternate-digest or tolerance lists.
- HTTP scales reference normalized artifact IDs; repeated metadata is forbidden.
- Archives lock the raw archive and every extracted landing output.
- TPC-H locks all eight outputs for `tiny`, `small`, and `medium` under the canonical `linux/amd64` environment.
- Canonical base image: `python:3.11.13-slim-bookworm` `linux/amd64` manifest `sha256:cec9aa7aa96eea4fa036e9b82be1e6b325f2e3707f462d885868df51ec0a4b47`.
- Canonical DuckDB version: `1.5.4`; Linux AMD64 wheel SHA-256: `ccc7f2694d02b4763fee61021d45e12f7bc5743993686563957df0cef799fbae`.
- Canonical installed TPC-H extension SHA-256: `a6516e487106b4f95bd6d85da4364debdcb2db536d015784bc43209af6ed0125`; runtime generation is network-disabled and never installs extensions.
- Approved `uv.lock` baseline SHA-256: `a376ce1b5bd5621290aaded68c22572690395419876da41814e28469bb4186b1`.
- Canonical TPC-H export: UTC, `C.UTF-8`, one thread, preserved insertion order, Zstandard compression, 100,000-row groups, and the table key ordering in the approved design.
- Issue #80 may parse and audit the contract but must not change download, extraction, generation, upload, or existing-object reuse behavior in `scripts/download_datasets.py`, `datasets/sources/http.py`, `datasets/sources/tpch.py`, or `datasets/s3.py`.
- Use TDD for each behavior change; record the intended RED before implementation and GREEN afterward.
- Use `apply_patch` for repository edits and exact-path staging before every commit.

---

## File and responsibility map

| Path | Responsibility |
|---|---|
| `datasets/locking.py` | Canonical JSON, schema fingerprints, file hashes, lock scalar/path validation |
| `datasets/schema.py` | Pure version-2 document validation and aggregated path-qualified errors |
| `datasets/registry.py` | Frozen typed contracts, version-2 parsing, scale resolution, legacy downloader views |
| `datasets/registry.yaml` | Populated single-source provenance lock |
| `scripts/audit_dataset_lock.py` | Read-only HTTP/archive candidate metadata and comparison CLI |
| `datasets/tpch-lock-requirements.txt` | Exact DuckDB wheel requirement and hash for canonical generation |
| `datasets/tpch_lock_export.py` | Deterministic TPC-H generation and output metadata inside canonical container |
| `datasets/tpch-lock.Dockerfile` | Immutable `linux/amd64` reference environment |
| `tests/datasets/fixtures/registry-v2-minimal.yaml` | Minimal direct, archive, and generator contract fixture |
| `tests/datasets/test_locking.py` | Lock primitive/canonicalization tests |
| `tests/datasets/test_schema.py` | Version-2 validation tests |
| `tests/datasets/test_registry.py` | Typed parsing/resolution and real-registry tests |
| `tests/datasets/test_http.py` | Legacy HTTP fetcher compatibility with typed scale plans |
| `tests/datasets/test_audit_dataset_lock.py` | Offline audit CLI tests using mocked HTTP/archives |
| `tests/datasets/test_tpch_lock_export.py` | Deterministic export query/metadata tests |
| `tests/test_docs_content_contract.py` | Public provenance/documentation contract |
| `docs/datasets.md` | Canonical public lock, attribution, and update runbook |
| `docs/CHANGELOG.md` | Unreleased issue #80 delivery record |

---

## Normative registry-v2 contract

The validator, fixture, parser, production registry, tests, and documentation
must use this exact field tree. Every mapping rejects unknown keys.

### Root and identifiers

- Root required fields: `version`, `lock`, `datasets`; `version` is integer `2`.
- `lock` required fields and values:
  `algorithm: sha256`, `source_drift: fail`, `object_drift: fail`,
  `schema_fingerprint: sha256-canonical-json`, and
  `update_policy: reviewed-lock-update`.
- Dataset, artifact, and schema identifiers match
  `^[a-z0-9][a-z0-9_-]*$`; scale identifiers are exactly `tiny`, `small`, and
  `medium` in the production registry.
- Dataset shared required fields: `description`, `format`, `license`,
  `landing_prefix`, `fetch`, `provenance`, and `schemas`.
- An HTTP dataset additionally requires `artifacts` and `scales` and forbids
  `generator`. A TPC-H dataset requires `generator` and forbids `artifacts`
  and top-level `scales`.
- `fetch` is `{kind: http, unzip: true-or-false}` for HTTP or `{kind: tpch}` for
  TPC-H. The `unzip` key is required for HTTP so archive behavior is explicit.

### Provenance, URLs, and source versions

`provenance` requires exactly `publisher`, `homepage`, `license_name`,
`license_url`, `attribution`, `source_stability`, and `update_policy`.
Human-readable strings must be non-empty. `source_stability` is `mutable` or
`immutable`; `update_policy` is `reviewed-lock-update`.
Provenance free text rejects non-global IP addresses,
user-home or temporary paths, host ports, MinIO endpoints, URI credentials,
and credential key/value forms.
Provenance free text rejects unambiguous endpoint-shaped machine-local values;
it preserves arbitrary semantic `Label:number` references.
An HTTP artifact may optionally include one complete `provenance` override with
the same exact field set and validation. When present, that artifact-level
mapping governs the selected release; dataset provenance is the conservative
default.

Authoritative URLs must use HTTPS and a public DNS host. Reject credentials,
explicit ports, `localhost`, `.localhost`, `.local`, loopback/link-local/private
IP literals, fragments, and empty hosts. This rule applies to provenance,
license, source, and extension-repository URLs.

Each HTTP artifact requires exactly `url`, `version`, `stability`, `evidence`,
`raw`, and `outputs`, and optionally accepts `provenance`:

- `version` requires `kind` (`revision` or `publication-date`) and a non-empty
  `value`; publication dates are strict ISO `YYYY-MM-DD` values. A mutable URL
  alias such as `ml-latest-small.zip` must use the authoritative release date
  or revision of the bytes acquired, never the alias text or retrieval time.
  If authoritative release identity cannot be established, acquisition is
  blocked and the artifact is not committed.
- `stability` is `mutable` or `immutable` and must exactly match its effective provenance:
  the artifact-level override when present, otherwise the dataset provenance.
- `evidence` allows only optional non-empty `etag`, `last_modified`, and
  `observed_at` strings. `observed_at` is an ISO-8601 UTC timestamp and is
  supporting evidence, not source identity.
- `raw` requires exactly `name`, positive integer `size_bytes`, and lowercase
  64-character `sha256`.
- Every `outputs` entry requires `object_name`, positive `size_bytes`,
  lowercase `sha256`, and a known `schema`. A direct artifact additionally
  requires `raw_identity: true`, forbids `member_path`, and exactly matches raw
  name, size, and digest. An archive output requires `member_path`, forbids
  `raw_identity`, and preserves the exact safe member path. Object names are
  unique within each artifact and across artifacts selected by the same scale.
  Mutually exclusive release artifacts may deliberately reuse the same
  flattened names because their landing identity is scale-local.
- Each HTTP scale contains exactly `artifacts`, a non-empty ordered list of
  known artifact IDs with no duplicates.

### Schema vocabulary and format options

Each schema requires exactly `format`, `mode`, `fields`, `options`, and
`fingerprint`. `mode` is `exact` or `minimum`. Each ordered field contains
exactly `name`, `logical_type`, and boolean `nullable`; names are non-empty and
unique. Logical types are `boolean`, `int8`, `int16`, `int32`, `int64`,
`uint8`, `uint16`, `uint32`, `uint64`, `float32`, `float64`, `date`,
`timestamp`, `timestamp-tz`, `string`, `binary`, `json`, or
`decimal(P,S)` where `1 <= P <= 38` and `0 <= S <= P`.

Allowed schema formats and exact `options` shapes are:

| Format | Required options | Field rule |
|---|---|---|
| `parquet` | empty mapping | one or more fields |
| `csv` | boolean `header`, one-character `delimiter`, and `encoding: utf-8` | one or more fields |
| `jsonl-gzip` | `record_shape: object`, `compression: gzip`, `encoding: utf-8` | one or more fields; `minimum` permitted |
| `xlsx` | non-empty unique string list `sheets` and positive integer `header_row` | one or more fields shared by the declared sheets |
| `text` | `encoding: utf-8` | fields must be empty |

The fingerprint is SHA-256 of canonical JSON for the entire schema entry after
removing `fingerprint`. Non-tabular archive members such as MovieLens README
files use `text`; they are never omitted from the landing-output lock.

### TPC-H generator

The generator requires exactly `engine`, `extension`, `environment`,
`command`, `export`, and `scales`:

- `engine`: `name: duckdb`, `version: 1.5.4`, and
  `wheel_sha256: ccc7f2694d02b4763fee61021d45e12f7bc5743993686563957df0cef799fbae`.
- `extension`: `name: tpch`, `version_relation: engine-version`,
  `repository_url: https://extensions.duckdb.org/v1.5.4/linux_amd64/tpch.duckdb_extension.gz`,
  and installed artifact
  `sha256: a6516e487106b4f95bd6d85da4364debdcb2db536d015784bc43209af6ed0125`.
- `environment`: exact base `image`, immutable `image_digest`,
  `platform: linux/amd64`, exact `uv_lock_sha256`, `locale: C.UTF-8`,
  `timezone: UTC`, `threads: 1`, and `preserve_insertion_order: true`.
- `command`: `procedure: dbgen` and `scale_parameter: sf`.
- `export`: `format: parquet`, `compression: zstd`,
  `row_group_size: 100000`, plus the exact eight-table `order_by` mapping from
  the approved design.
- `scales` contains exactly `tiny`, `small`, and `medium`, with scale factors
  `0.01`, `1`, and `10`. Each scale has exactly eight outputs in canonical table
  order. Every output requires `table`, safe `object_name`, positive
  `size_bytes`, lowercase `sha256`, and a known `schema`; object names are
  the table name plus `.parquet`; table/schema references are unique and complete.

All nested required-field, type, value, reference, uniqueness, URL, and unknown
field rules above are acceptance requirements, not implementation suggestions.

---

### Task 1: Add lock canonicalization and scalar validation primitives

**Files:**
- Create: `datasets/locking.py`
- Create: `tests/datasets/test_locking.py`

**Interfaces:**
- Produces: `canonical_json(value: Mapping[str, object]) -> bytes`
- Produces: `schema_fingerprint(schema: Mapping[str, object]) -> str`
- Produces: `file_metadata(path: Path) -> tuple[int, str]`
- Produces: `validate_sha256(value: object, path: str) -> list[str]`
- Produces: `validate_size(value: object, path: str) -> list[str]`
- Produces: `validate_relative_path(value: object, path: str) -> list[str]`
- Consumes: only Python standard library

- [ ] **Step 1: Write failing canonicalization and validation tests**

```python
from pathlib import Path

from datasets.locking import (
    canonical_json,
    file_metadata,
    schema_fingerprint,
    validate_relative_path,
    validate_sha256,
    validate_size,
)


def test_canonical_json_sorts_mapping_keys_and_preserves_field_order():
    left = {"mode": "exact", "fields": [{"name": "b"}, {"name": "a"}], "format": "csv"}
    right = {"format": "csv", "fields": [{"name": "b"}, {"name": "a"}], "mode": "exact"}
    assert canonical_json(left) == canonical_json(right)
    assert canonical_json(left).decode() == (
        '{"fields":[{"name":"b"},{"name":"a"}],"format":"csv","mode":"exact"}'
    )


def test_schema_fingerprint_ignores_existing_fingerprint():
    schema = {"format": "csv", "mode": "exact", "fields": [], "fingerprint": "0" * 64}
    assert schema_fingerprint(schema) == schema_fingerprint({"format": "csv", "mode": "exact", "fields": []})


def test_file_metadata_returns_positive_size_and_sha256(tmp_path: Path):
    artifact = tmp_path / "artifact.bin"
    artifact.write_bytes(b"locked-bytes")
    assert file_metadata(artifact) == (
        12,
        "60170e42b944363f7cc231ceb230fea6f13f7691b66976c933f343042f9b39ff",
    )


def test_lock_scalar_validators_reject_malformed_values():
    assert validate_sha256("A" * 64, "x.sha256") == ["x.sha256: must be 64 lowercase hexadecimal characters"]
    assert validate_size(True, "x.size_bytes") == ["x.size_bytes: must be a positive integer"]
    assert validate_size(0, "x.size_bytes") == ["x.size_bytes: must be a positive integer"]
    assert validate_relative_path("../escape.csv", "x.object_name") == [
        "x.object_name: must be a safe relative POSIX path"
    ]
    assert validate_relative_path("/absolute.csv", "x.object_name") == [
        "x.object_name: must be a safe relative POSIX path"
    ]
```

- [ ] **Step 2: Run the focused tests and capture RED**

Run: `uv run pytest tests/datasets/test_locking.py -q`

Expected: collection fails because `datasets.locking` does not exist.

- [ ] **Step 3: Implement the complete primitive module**

```python
"""Pure canonicalization and scalar validation for dataset provenance locks."""
from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping
from pathlib import Path, PurePosixPath

SHA256_RE = re.compile(r"^[0-9a-f]{64}$")


def canonical_json(value: Mapping[str, object]) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


def schema_fingerprint(schema: Mapping[str, object]) -> str:
    payload = {key: value for key, value in schema.items() if key != "fingerprint"}
    return hashlib.sha256(canonical_json(payload)).hexdigest()


def file_metadata(path: Path) -> tuple[int, str]:
    digest = hashlib.sha256()
    size = 0
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1 << 20), b""):
            size += len(chunk)
            digest.update(chunk)
    return size, digest.hexdigest()


def validate_sha256(value: object, path: str) -> list[str]:
    if not isinstance(value, str) or SHA256_RE.fullmatch(value) is None:
        return [f"{path}: must be 64 lowercase hexadecimal characters"]
    return []


def validate_size(value: object, path: str) -> list[str]:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        return [f"{path}: must be a positive integer"]
    return []


def validate_relative_path(value: object, path: str) -> list[str]:
    if not isinstance(value, str) or not value:
        return [f"{path}: must be a safe relative POSIX path"]
    candidate = PurePosixPath(value)
    if (
        candidate == PurePosixPath(".")
        or candidate.is_absolute()
        or ".." in candidate.parts
        or "\\" in value
        or value.endswith("/")
    ):
        return [f"{path}: must be a safe relative POSIX path"]
    return []
```

- [ ] **Step 4: Run GREEN and lint**

Run: `uv run pytest tests/datasets/test_locking.py -q`

Expected: all tests pass.

Run: `uv run ruff check datasets/locking.py tests/datasets/test_locking.py`

Expected: `All checks passed!`

- [ ] **Step 5: Verify and commit exact paths**

Run: `git diff --check && shasum -a 256 docs/superpowers/plans/2026-07-21-atlas-submodule-modernization.md`

Expected checksum: `f7eb55036e3020f419d9acb42bbc502bfe0528219a980a6973ed083591d2ba66`.

```bash
git add datasets/locking.py tests/datasets/test_locking.py
git diff --cached --check
git commit -m "feat(datasets): add provenance lock primitives (#80)"
```

---

### Task 2: Define and validate the version-2 registry schema

**Files:**
- Create: `tests/datasets/fixtures/registry-v2-minimal.yaml`
- Modify: `datasets/schema.py`
- Modify: `tests/datasets/test_schema.py`

**Interfaces:**
- Consumes: `schema_fingerprint`, `validate_sha256`, `validate_size`, `validate_relative_path`
- Produces: `validate_registry_v2(doc: object) -> list[str]`
- Preserves temporarily: `validate_registry(doc: dict) -> list[str]` continues validating the committed version-1 registry until Task 7 atomically migrates it

- [ ] **Step 1: Add a complete minimal version-2 fixture**

The fixture contains exactly three datasets:

1. `direct`: one HTTPS artifact whose raw and landing size/hash agree, one exact schema, and one tier referencing the artifact;
2. `archive`: one HTTPS ZIP with one member/output and one exact CSV schema;
3. `generated`: one DuckDB generator, all three required tiers, and all 24
   TPC-H outputs referencing the eight table schemas.

Use real SHA-256 values derived from `direct-raw`, `archive-raw`,
`archive-ratings`, and the exact Cartesian labels
`tpch-{tiny|small|medium}-{customer|lineitem|nation|orders|part|partsupp|region|supplier}`.
Generate the values with:

```bash
uv run python -c 'import hashlib; tables=("customer","lineitem","nation","orders","part","partsupp","region","supplier"); labels=("direct-raw","archive-raw","archive-ratings")+tuple(f"tpch-{scale}-{table}" for scale in ("tiny","small","medium") for table in tables); print("\n".join(f"{label} {hashlib.sha256(label.encode()).hexdigest()}" for label in labels))'
```

Use the UTF-8 byte length of each label as its positive fixture size. Do not use
repeated-character sentinel hashes.

- [ ] **Step 2: Write failing validation tests**

```python
from copy import deepcopy
from pathlib import Path

import pytest
import yaml

from datasets.schema import validate_registry_v2

FIXTURE = Path(__file__).parent / "fixtures" / "registry-v2-minimal.yaml"


def _v2() -> dict:
    return yaml.safe_load(FIXTURE.read_text(encoding="utf-8"))


def test_minimal_v2_registry_is_valid():
    assert validate_registry_v2(_v2()) == []


def test_v2_rejects_missing_provenance_field():
    doc = _v2()
    del doc["datasets"]["direct"]["provenance"]["publisher"]
    assert "datasets.direct.provenance: missing 'publisher'" in validate_registry_v2(doc)


def test_v2_rejects_malformed_artifact_lock():
    doc = _v2()
    artifact = doc["datasets"]["direct"]["artifacts"]["sample"]
    artifact["raw"]["size_bytes"] = 0
    artifact["raw"]["sha256"] = "ABC"
    errors = validate_registry_v2(doc)
    assert "datasets.direct.artifacts.sample.raw.size_bytes: must be a positive integer" in errors
    assert "datasets.direct.artifacts.sample.raw.sha256: must be 64 lowercase hexadecimal characters" in errors


def test_v2_rejects_unknown_scale_artifact_reference():
    doc = _v2()
    doc["datasets"]["direct"]["scales"]["tiny"]["artifacts"] = ["missing"]
    assert "datasets.direct.scales.tiny.artifacts[0]: unknown artifact 'missing'" in validate_registry_v2(doc)


def test_v2_rejects_schema_fingerprint_mismatch():
    doc = _v2()
    doc["datasets"]["direct"]["schemas"]["sample"]["fingerprint"] = "0" * 64
    assert "datasets.direct.schemas.sample.fingerprint: does not match canonical schema" in validate_registry_v2(doc)


def test_v2_rejects_active_machine_local_values():
    doc = _v2()
    doc["datasets"]["direct"]["provenance"]["homepage"] = "http://localhost:9000/source"
    assert "datasets.direct.provenance.homepage: must be an authoritative HTTPS URL" in validate_registry_v2(doc)


@pytest.mark.parametrize("root", [None, [], "registry", 2])
def test_v2_rejects_non_mapping_roots(root: object):
    assert validate_registry_v2(root) == ["registry: must be a mapping"]


def test_v2_rejects_https_loopback_even_with_valid_scheme():
    doc = _v2()
    doc["datasets"]["direct"]["provenance"]["homepage"] = "https://127.0.0.1/source"
    assert "datasets.direct.provenance.homepage: must be an authoritative HTTPS URL" in validate_registry_v2(doc)
```

Add table-driven deletion, wrong-type, invalid-value, unknown-field, reference,
and uniqueness mutations for every required and allowed field in the normative
contract above. The matrix must explicitly cover: root/global lock policy;
identifier syntax; every schema format/mode/logical type/nullability/options
shape; artifact/source version/stability/evidence/raw/output fields; source
stability inheritance; non-HTTPS, `https://localhost`, loopback, private,
credential-bearing, and explicit-port URLs; unsafe paths; duplicate objects;
direct raw/output disagreement; archive output without `member_path`; unknown
artifact/schema references; and every TPC-H engine, extension, environment,
command, export, scale, and output field. Assert every error is path-qualified.

- [ ] **Step 3: Run RED**

Run: `uv run pytest tests/datasets/test_schema.py -q`

Expected: import or assertion failure because `validate_registry_v2` is absent.

- [ ] **Step 4: Implement `validate_registry_v2` as pure composed validators**

Keep `validate_registry` unchanged in this task. Add these exact private
boundaries in `datasets/schema.py`:

- `_required(mapping: object, path: str, fields: tuple[str, ...]) -> tuple[dict[str, object], list[str]]` validates mapping type and required keys;
- `_unknown(mapping: dict[str, object], path: str, allowed: frozenset[str]) -> list[str]` rejects every undeclared key;
- `_https(value: object, path: str) -> list[str]` accepts only authoritative HTTPS URLs;
- `_validate_provenance(value: object, path: str) -> list[str]` validates the complete shared provenance mapping;
- `_validate_schemas(value: object, path: str) -> list[str]` validates schema entries and recomputed fingerprints;
- `_validate_http_dataset(dataset: dict[str, object], path: str) -> list[str]` validates normalized artifacts, outputs, and scale references;
- `_validate_tpch_dataset(dataset: dict[str, object], path: str) -> list[str]` validates the complete generator environment and all scale outputs;
- `validate_registry_v2(doc: object) -> list[str]` validates the root type before accessing keys, composes the validators, and returns all errors.

Each helper returns errors instead of raising. `validate_registry_v2` accumulates all errors in deterministic document order. Use the exact allowed-field sets from the approved design and fixture. Calculate schema fingerprints with `schema_fingerprint`; never trust a stored fingerprint without recomputing it.

- [ ] **Step 5: Run GREEN and focused regression**

Run: `uv run pytest tests/datasets/test_schema.py tests/datasets/test_locking.py -q`

Expected: all tests pass, including the existing version-1 tests through `validate_registry`.

- [ ] **Step 6: Exact-stage and commit**

```bash
git add datasets/schema.py tests/datasets/test_schema.py tests/datasets/fixtures/registry-v2-minimal.yaml
git diff --cached --check
git commit -m "feat(datasets): validate registry v2 locks (#80)"
```

---

### Task 3: Add typed version-2 parsing and scale resolution

**Files:**
- Modify: `datasets/registry.py`
- Modify: `tests/datasets/test_registry.py`

**Interfaces:**
- Consumes: `validate_registry_v2(doc)`
- Produces frozen dataclasses: `Provenance`, `SchemaField`, `SchemaContract`, `SourceVersion`, `RawArtifact`, `LandingObject`, `HttpArtifact`, `GeneratorOutput`, `GeneratorScale`, `GeneratorContract`
- Produces: `load_registry_v2(path: Path) -> dict[str, Dataset]`
- Produces: `_parse_v2_datasets(raw: Mapping[str, object]) -> dict[str, Dataset]`
- Produces: `resolve_scale(dataset: Dataset, scale: str) -> ScalePlan`
- Preserves temporarily: current `load_registry` continues reading version 1 until Task 7

Use these complete frozen model fields; wrap every stored mapping in
`MappingProxyType` and every sequence in a tuple so the models are deeply
read-only at the contract boundary:

| Model | Fields |
|---|---|
| `Provenance` | `publisher`, `homepage`, `license_name`, `license_url`, `attribution`, `source_stability`, `update_policy` |
| `SchemaField` | `name`, `logical_type`, `nullable` |
| `SchemaContract` | `id`, `format`, `mode`, `fields: tuple[SchemaField, ...]`, `options: Mapping[str, object]`, `fingerprint` |
| `SourceVersion` | `kind`, `value` |
| `RawArtifact` | `name`, `size_bytes`, `sha256` |
| `LandingObject` | `object_name`, `size_bytes`, `sha256`, `schema_id`, `member_path: str | None`, `raw_identity: bool` |
| `HttpArtifact` | `id`, `url`, `version`, `stability`, `evidence: Mapping[str, str]`, `raw`, `outputs: tuple[LandingObject, ...]`, `provenance: Provenance | None` |
| `GeneratorEnvironment` | `image`, `image_digest`, `platform`, `uv_lock_sha256`, `locale`, `timezone`, `threads`, `preserve_insertion_order` |
| `GeneratorOutput` | `table`, `object_name`, `size_bytes`, `sha256`, `schema_id` |
| `GeneratorScale` | `name`, `scale_factor`, `outputs: tuple[GeneratorOutput, ...]` |
| `GeneratorContract` | `engine_name`, `engine_version`, `engine_wheel_sha256`, `extension_name`, `extension_version_relation`, `extension_repository_url`, `extension_sha256`, `environment`, `procedure`, `scale_parameter`, `export_format`, `compression`, `row_group_size`, `order_by: Mapping[str, tuple[str, ...]]`, `scales: Mapping[str, GeneratorScale]` |
| `Dataset` | existing `name`, `description`, `format`, `license`, `landing_prefix`, `kind`, `unzip`, `scales: Mapping[str, tuple[str, ...] | GeneratorScale]`; plus temporary compatibility fields `provenance: Provenance | None = None`, empty read-only `schemas`, empty read-only `artifacts`, and `generator: GeneratorContract | None = None` |

`ScalePlan` has exactly `dataset`, `scale`, `urls: tuple[str, ...]`,
`sf: float | None`, `artifacts: tuple[HttpArtifact, ...]`, and
`generator_scale: GeneratorScale | None`.

The optional/defaulted `Dataset` fields exist only so the version-1 production
loader and existing isolated constructors remain callable through Task 6.
`load_registry_v2` always populates provenance and the complete typed contract;
Task 7 removes the version-1 loader branch, after which no production-loaded
dataset can contain those compatibility defaults.

- [ ] **Step 1: Write failing parser tests against the minimal fixture**

```python
def test_load_v2_resolves_shared_http_artifacts_without_duplication():
    datasets = reg.load_registry_v2(V2_FIXTURE)
    direct = datasets["direct"]
    tiny = reg.resolve_scale(direct, "tiny")
    assert [artifact.id for artifact in tiny.artifacts] == ["sample"]
    assert tiny.urls == ("https://example.invalid/sample.csv",)
    assert tiny.sf is None
    assert tiny.artifacts[0] is direct.artifacts["sample"]


def test_load_v2_resolves_archive_member_and_schema():
    archive = reg.load_registry_v2(V2_FIXTURE)["archive"]
    output = reg.resolve_scale(archive, "tiny").artifacts[0].outputs[0]
    assert output.member_path == "archive/ratings.csv"
    assert output.schema_id == "ratings"


def test_load_v2_resolves_generator_outputs_and_legacy_sf_view():
    generated = reg.load_registry_v2(V2_FIXTURE)["generated"]
    plan = reg.resolve_scale(generated, "tiny")
    assert plan.sf == 0.01
    assert plan.urls == ()
    assert len(plan.generator_scale.outputs) == 8


@pytest.mark.parametrize("content", ["null\n", "[]\n", "registry\n", "2\n"])
def test_load_v2_reports_non_mapping_roots(tmp_path: Path, content: str):
    candidate = tmp_path / "registry.yaml"
    candidate.write_text(content, encoding="utf-8")
    with pytest.raises(ValueError, match="registry: must be a mapping"):
        reg.load_registry_v2(candidate)
```

- [ ] **Step 2: Run RED**

Run: `uv run pytest tests/datasets/test_registry.py -q`

Expected: failures because `load_registry_v2` and the typed lock fields do not exist.

- [ ] **Step 3: Implement frozen models and parser**

Use tuples for ordered immutable collections and mappings copied into read-only values. Extend the existing structures with these signatures:

```python
@dataclass(frozen=True)
class ScalePlan:
    dataset: Dataset
    scale: str
    urls: tuple[str, ...]
    sf: float | None
    artifacts: tuple[HttpArtifact, ...] = ()
    generator_scale: GeneratorScale | None = None


def load_registry_v2(path: Path) -> dict[str, Dataset]:
    doc = yaml.safe_load(path.read_text(encoding="utf-8"))
    errors = validate_registry_v2(doc)
    if errors:
        raise ValueError("invalid registry:\n  - " + "\n  - ".join(errors))
    return _parse_v2_datasets(doc["datasets"])
```

`resolve_scale` dispatches on the already parsed scale entry and reconstructs `urls` from referenced HTTP artifacts. It never refetches or re-reads YAML. Keep URL order identical to the scale's artifact-reference order.

- [ ] **Step 4: Run GREEN and existing download tests**

Run: `uv run pytest tests/datasets/test_registry.py tests/datasets/test_download_cli.py -q`

Expected: all tests pass; the version-1 production path remains behaviorally unchanged.

- [ ] **Step 5: Exact-stage and commit**

```bash
git add datasets/registry.py tests/datasets/test_registry.py
git diff --cached --check
git commit -m "feat(datasets): parse typed provenance locks (#80)"
```

---

### Task 4: Build the read-only HTTP and archive audit tool

**Files:**
- Create: `scripts/audit_dataset_lock.py`
- Create: `tests/datasets/test_audit_dataset_lock.py`

**Interfaces:**
- Consumes: `file_metadata(path)`
- Produces: `audit_http(url: str, *, archive: bool) -> dict[str, object]`
- Produces CLI: `uv run python scripts/audit_dataset_lock.py http --url URL [--archive] --output FILE`
- Never writes `datasets/registry.yaml` and never accesses MinIO

- [ ] **Step 1: Write mocked direct/archive audit tests**

```python
@responses.activate
def test_audit_http_emits_raw_and_direct_output():
    responses.add(
        responses.GET,
        "https://source.invalid/data.csv",
        body=b"id,name\n1,Ada\n",
        status=200,
        headers={"ETag": '"locked"', "Last-Modified": "Mon, 01 Jan 2024 00:00:00 GMT"},
    )
    result = audit.audit_http("https://source.invalid/data.csv", archive=False)
    assert result["raw"]["name"] == "data.csv"
    assert result["raw"]["size_bytes"] == 14
    assert result["outputs"][0]["object_name"] == "data.csv"
    assert result["outputs"][0]["sha256"] == result["raw"]["sha256"]


@responses.activate
def test_audit_zip_preserves_member_paths_and_rejects_flatten_collision():
    payload = zip_bytes({"a/data.csv": b"a", "b/data.csv": b"b"})
    responses.add(responses.GET, "https://source.invalid/data.zip", body=payload, status=200)
    with pytest.raises(ValueError, match="flatten to duplicate object name data.csv"):
        audit.audit_http("https://source.invalid/data.zip", archive=True)


def test_cli_requires_explicit_output_and_never_changes_registry(tmp_path: Path):
    registry = ROOT / "datasets" / "registry.yaml"
    before = registry.read_bytes()
    assert audit.main(["http", "--url", "https://source.invalid/data.csv"]) == 2
    assert registry.read_bytes() == before


@responses.activate
def test_cli_keeps_only_requested_metadata_output(tmp_path: Path, monkeypatch):
    responses.add(responses.GET, "https://source.invalid/data.csv", body=b"locked", status=200)
    output = tmp_path / "candidate.yaml"
    assert audit.main(["http", "--url", "https://source.invalid/data.csv", "--output", str(output)]) == 0
    assert [path.name for path in tmp_path.iterdir()] == ["candidate.yaml"]
```

- [ ] **Step 2: Run RED**

Run: `uv run pytest tests/datasets/test_audit_dataset_lock.py -q`

Expected: collection fails because the audit module does not exist.

- [ ] **Step 3: Implement safe read-only auditing**

The tool must:

- stream downloads with a 120-second timeout;
- reject URLs that fail the normative authoritative-HTTPS policy;
- calculate hashes from saved bytes;
- capture ETag/Last-Modified as non-authoritative evidence;
- open ZIPs only after download;
- reject absolute members, `..`, symlinks, non-files, and flattened-name collisions;
- calculate every member/output size and digest;
- own an internal `TemporaryDirectory` for every download/extraction and return
  only calculated metadata after that directory has been deleted;
- serialize deterministic YAML to the explicitly supplied CLI output path;
- refuse an output path equal to the registry path;
- leave no raw or extracted bytes beside the requested metadata output.

Use `argparse` subparsers and return exit code 2 for usage errors. Do not import or call `datasets.sources.http.fetch_http`, because that is the production path intentionally deferred to #81.

- [ ] **Step 4: Run GREEN and lint**

Run: `uv run pytest tests/datasets/test_audit_dataset_lock.py tests/datasets/test_locking.py -q`

Expected: all tests pass.

Run: `uv run ruff check scripts/audit_dataset_lock.py tests/datasets/test_audit_dataset_lock.py`

Expected: `All checks passed!`

- [ ] **Step 5: Commit**

```bash
git add scripts/audit_dataset_lock.py tests/datasets/test_audit_dataset_lock.py
git diff --cached --check
git commit -m "feat(datasets): add read-only artifact audit (#80)"
```

---

### Task 5: Add the canonical TPC-H reference exporter

**Files:**
- Create: `datasets/tpch-lock-requirements.txt`
- Create: `datasets/tpch-lock.Dockerfile`
- Create: `datasets/tpch_lock_export.py`
- Create: `tests/datasets/test_tpch_lock_export.py`

**Interfaces:**
- Produces CLI: `python -m datasets.tpch_lock_export --scale {0.01,1,10} --output-dir PATH --metadata FILE`
- Produces one deterministic Parquet file per table plus deterministic YAML metadata
- Produces: `verify_runtime_inputs(uv_lock: Path, extension: Path) -> None`
- Produces: `session_statements() -> tuple[str, str]`
- Does not modify `datasets/sources/tpch.py`

- [ ] **Step 1: Add exact canonical environment files**

`datasets/tpch-lock-requirements.txt` contains:

```text
duckdb==1.5.4 --hash=sha256:ccc7f2694d02b4763fee61021d45e12f7bc5743993686563957df0cef799fbae
PyYAML==6.0.3 --hash=sha256:b8bb0864c5a28024fac8a632c443c87c5aa6f215c0b126c449ae1a150412f31d
```

The PyYAML value is the CPython 3.11 manylinux x86_64 wheel hash recorded in
the approved `uv.lock` baseline. Before committing, verify both requirement
hashes still match that unchanged lockfile. A mismatch blocks the task and
requires a plan/design review; do not substitute or fetch an unpinned
dependency.

`datasets/tpch-lock.Dockerfile` starts with:

```dockerfile
FROM --platform=linux/amd64 python@sha256:cec9aa7aa96eea4fa036e9b82be1e6b325f2e3707f462d885868df51ec0a4b47
ENV LANG=C.UTF-8 LC_ALL=C.UTF-8 TZ=UTC PYTHONHASHSEED=0
COPY datasets/tpch-lock-requirements.txt /tmp/requirements.txt
RUN python -m pip install --no-cache-dir --require-hashes -r /tmp/requirements.txt
COPY uv.lock /workspace/uv.lock
RUN echo "a376ce1b5bd5621290aaded68c22572690395419876da41814e28469bb4186b1  /workspace/uv.lock" | sha256sum -c -
RUN python -c "import duckdb; duckdb.connect().execute('INSTALL tpch')" && \
    echo "a6516e487106b4f95bd6d85da4364debdcb2db536d015784bc43209af6ed0125  /root/.duckdb/extensions/v1.5.4/linux_amd64/tpch.duckdb_extension" | sha256sum -c -
WORKDIR /workspace
COPY datasets /workspace/datasets
ENTRYPOINT ["python", "-m", "datasets.tpch_lock_export"]
```

The build is the only networked extension acquisition step. It verifies the
installed DuckDB TPC-H extension bytes against the exact reviewed digest before
forming the image. Runtime generation uses `LOAD tpch`, never `INSTALL`, and is
always invoked with Docker `--network=none`.

- [ ] **Step 2: Write failing deterministic query and metadata tests**

```python
@pytest.mark.parametrize(
    ("table", "order_by"),
    [
        ("customer", "c_custkey"),
        ("lineitem", "l_orderkey, l_linenumber"),
        ("nation", "n_nationkey"),
        ("orders", "o_orderkey"),
        ("part", "p_partkey"),
        ("partsupp", "ps_partkey, ps_suppkey"),
        ("region", "r_regionkey"),
        ("supplier", "s_suppkey"),
    ],
)
def test_copy_query_is_fully_deterministic(table: str, order_by: str):
    query = exporter.copy_query(table, Path(f"/{table}.parquet"))
    assert f"SELECT * FROM {table} ORDER BY {order_by}" in query
    assert "COMPRESSION ZSTD" in query
    assert "ROW_GROUP_SIZE 100000" in query


def test_metadata_contains_all_locked_environment_inputs(monkeypatch):
    monkeypatch.setattr(exporter, "DUCKDB_VERSION", "1.5.4")
    metadata = exporter.environment_metadata()
    assert metadata["platform"] == "linux/amd64"
    assert metadata["base_image_digest"] == (
        "sha256:cec9aa7aa96eea4fa036e9b82be1e6b325f2e3707f462d885868df51ec0a4b47"
    )
    assert metadata["duckdb_version"] == "1.5.4"
    assert metadata["locale"] == "C.UTF-8"
    assert metadata["timezone"] == "UTC"
    assert metadata["threads"] == 1
    assert metadata["preserve_insertion_order"] is True


def test_session_settings_force_single_threaded_ordered_export():
    assert exporter.session_statements() == (
        "SET threads=1",
        "SET preserve_insertion_order=true",
    )


def test_runtime_inputs_fail_closed_on_lock_or_extension_drift(tmp_path: Path):
    uv_lock = tmp_path / "uv.lock"
    extension = tmp_path / "tpch.duckdb_extension"
    uv_lock.write_bytes(b"drifted-lock")
    extension.write_bytes(b"drifted-extension")
    with pytest.raises(ValueError, match="uv.lock SHA-256 mismatch"):
        exporter.verify_runtime_inputs(uv_lock, extension)


def test_dockerfile_pins_and_verifies_offline_runtime_inputs():
    text = (ROOT / "datasets" / "tpch-lock.Dockerfile").read_text(encoding="utf-8")
    assert "a376ce1b5bd5621290aaded68c22572690395419876da41814e28469bb4186b1" in text
    assert "a6516e487106b4f95bd6d85da4364debdcb2db536d015784bc43209af6ed0125" in text
    assert "INSTALL tpch" in text
```

- [ ] **Step 3: Run RED**

Run: `uv run pytest tests/datasets/test_tpch_lock_export.py -q`

Expected: import failure because `datasets.tpch_lock_export` does not exist.

- [ ] **Step 4: Implement deterministic generation**

`copy_query(table, target)` returns the exact structure
`COPY (SELECT * FROM {table} ORDER BY {order_by}) TO '{target}' (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 100000)` using the fixed order-key mapping and a safely quoted target. Reject unknown tables and scale factors outside `{0.01, 1.0, 10.0}`.

`main` must:

1. verify `duckdb.__version__ == "1.5.4"`;
2. verify UTC and `C.UTF-8`;
3. hash `/workspace/uv.lock` and the installed extension and fail unless both
   match their exact constants;
4. execute `SET threads=1` and `SET preserve_insertion_order=true`, then
   `LOAD tpch` without any `INSTALL` statement;
5. run exactly one of `CALL dbgen(sf=0.01)`, `CALL dbgen(sf=1)`, or
   `CALL dbgen(sf=10)` according to the validated scale argument;
6. export all eight ordered tables;
7. calculate size/SHA with `file_metadata`;
8. write deterministic YAML metadata only after all outputs succeed.

- [ ] **Step 5: Run focused GREEN**

Run: `uv run pytest tests/datasets/test_tpch_lock_export.py -q`

Expected: all offline tests pass.

Build smoke:

```bash
docker build --platform linux/amd64 -f datasets/tpch-lock.Dockerfile -t data-eng-lab-tpch-lock:1.5.4 .
```

Expected: image builds from the exact manifest and hash-locked requirements.

- [ ] **Step 6: Commit**

```bash
git add datasets/tpch-lock-requirements.txt datasets/tpch-lock.Dockerfile datasets/tpch_lock_export.py tests/datasets/test_tpch_lock_export.py
git diff --cached --check
git commit -m "feat(datasets): add canonical TPC-H lock exporter (#80)"
```

---

### Task 6: Acquire and review authoritative candidate metadata

**Files:**
- No committed raw/generated data
- Output evidence: `/private/tmp/data-eng-lab-dataset-lock-80/`
- Append report: `/private/tmp/data-eng-lab-goal/task-80-report.md`

**Interfaces:**
- Consumes: `scripts/audit_dataset_lock.py`, canonical TPC-H container
- Produces: reviewed candidate YAML for 15 unique HTTP artifacts and 24 TPC-H outputs

- [ ] **Step 1: Create an explicit temporary evidence directory**

Run: `mkdir -p /private/tmp/data-eng-lab-dataset-lock-80/http /private/tmp/data-eng-lab-dataset-lock-80/tpch`

Expected: only `/private/tmp` is written; no repository data directory appears.

- [ ] **Step 2: Audit all 15 unique authoritative HTTP URLs**

Use the committed current URL inventory: six NYC Taxi months, six GH Archive hours, MovieLens latest-small and 25m archives, and UCI Online Retail II. Invoke the audit CLI once per unique URL, marking the three ZIP sources with `--archive`, and write one deterministic candidate file per source beneath the temporary HTTP directory.

Expected evidence:

- exactly 15 raw source records;
- raw names match authoritative URL basenames;
- direct outputs equal their raw size/digest;
- archive members retain exact member paths;
- no flatten collision or unsafe member;
- response ETag/Last-Modified recorded when supplied;
- no registry or MinIO write.

- [ ] **Step 3: Review source identity, publication/revision, licenses, and attribution**

For each dataset, record the authoritative publisher/homepage/license URL in
the report. Classify sources as mutable unless the publisher explicitly
promises immutable bytes. URL date/month/hour segments and authoritative
published release metadata can supply source-version values; transport headers
remain supporting evidence only. In particular, `ml-latest-small.zip` is a
mutable alias and `latest-small` is not a revision. Read its authoritative
release date/revision from publisher metadata or the acquired release README.
If the bytes have no authoritative release identity, stop and report the
blocked artifact rather than substituting retrieval time.

- [ ] **Step 4: Generate canonical TPC-H outputs for every scale**

For each scale `0.01`, `1`, and `10`, run the canonical container with:

```bash
docker run --rm --network=none --platform linux/amd64 \
  -v /private/tmp/data-eng-lab-dataset-lock-80/tpch:/out \
  data-eng-lab-tpch-lock:1.5.4 \
  --scale 0.01 \
  --output-dir /out/tiny \
  --metadata /out/tiny.yaml
```

Repeat with `--scale 1 --output-dir /out/small --metadata /out/small.yaml` and `--scale 10 --output-dir /out/medium --metadata /out/medium.yaml`.

Expected: 24 non-empty Parquet outputs, three metadata files, identical environment blocks, and no repository output.

- [ ] **Step 5: Re-run every scale and prove byte determinism**

Generate `0.01`, `1`, and `10` again under `--network=none` into
`/out/tiny-repeat`, `/out/small-repeat`, and `/out/medium-repeat`. Compare every
size and SHA-256 with the first three metadata files.

Expected: all 24 repeated records are byte-identical. Any mismatch blocks
registry population and must be diagnosed before continuing.

- [ ] **Step 6: Derive reviewed schemas**

Create schema candidates for:

- each distinct NYC Taxi Parquet physical schema, preserving January's reviewed
  `passenger_count` and identifier-width distinction from February through June;
- one minimum GH Archive JSON contract covering fields actually consumed by repository scenarios;
- each MovieLens extracted CSV shape in each release and a `text` contract for
  every README/license/non-tabular member that the current fetcher lands;
- each Online Retail II extracted object shape, including XLSX sheet/header
  structure when the authoritative archive is XLSX rather than CSV;
- all eight TPC-H tables using the TPC-H specification and generated Parquet schemas.

Compute every schema fingerprint with `schema_fingerprint`. Record source evidence and any unexpected archive member or format mismatch in the report; do not conceal it by changing the declared format silently.

- [ ] **Step 7: Confirm candidate counts and append evidence report**

Run a read-only script or YAML query that proves 15 unique HTTP raw artifacts and 24 generated outputs. Record commands, timestamps, sizes, hashes, schema IDs, source headers, licenses, and any upstream mutability in `/private/tmp/data-eng-lab-goal/task-80-report.md`.

---

### Task 7: Populate registry v2 and activate the typed production parser

**Files:**
- Modify: `datasets/registry.yaml`
- Modify: `datasets/schema.py`
- Modify: `datasets/registry.py`
- Modify: `tests/datasets/test_schema.py`
- Modify: `tests/datasets/test_registry.py`
- Modify: `tests/datasets/test_download_cli.py`
- Modify: `tests/datasets/test_http.py`
- Modify: `tests/scenarios/test_batch_ingest_taxi_schema.py`

**Interfaces:**
- Consumes: reviewed Task 6 candidates
- Makes production: `validate_registry` delegates only to `validate_registry_v2`
- Makes production: `load_registry` delegates only to version-2 parsing
- Preserves: `ScalePlan.urls` and `ScalePlan.sf` consumed by the current downloader

- [ ] **Step 1: Add failing real-registry completeness tests**

```python
def test_real_registry_is_version_2_and_fully_locked():
    doc = yaml.safe_load(REAL.read_text(encoding="utf-8"))
    assert doc["version"] == 2
    assert validate_registry(doc) == []
    http_artifacts = sum(
        len(dataset.get("artifacts", {}))
        for dataset in doc["datasets"].values()
        if dataset["fetch"]["kind"] == "http"
    )
    generated_outputs = sum(
        len(scale["outputs"])
        for dataset in doc["datasets"].values()
        if dataset["fetch"]["kind"] == "tpch"
        for scale in dataset["generator"]["scales"].values()
    )
    assert http_artifacts == 15
    assert generated_outputs == 24


def test_real_registry_has_no_sentinel_or_unreviewed_lock_values():
    text = REAL.read_text(encoding="utf-8")
    for forbidden in ("TBD", "TODO", "unknown", "placeholder", "0" * 64, "f" * 64):
        assert forbidden not in text


def test_version_1_registry_is_rejected():
    assert validate_registry({"version": 1, "datasets": {}}) == [
        "registry: 'version' must be 2"
    ]
```

- [ ] **Step 2: Run RED**

Run: `uv run pytest tests/datasets/test_schema.py tests/datasets/test_registry.py -q`

Expected: real registry/version assertions fail while version 1 remains active.

- [ ] **Step 3: Replace the registry with exact reviewed values**

For each HTTP dataset:

- add complete provenance and schema maps;
- define each of the 15 unique artifacts once;
- paste exact Task 6 raw/output names, sizes, and SHA-256 values;
- make `tiny`, `small`, and `medium` reference ordered artifact IDs;
- preserve the same source selection currently represented by the URL lists.

For TPC-H:

- record the exact base image, platform, DuckDB version/wheel hash, `uv.lock`
  hash, UTC/locale, reviewed installed extension hash/repository, single-thread
  and insertion-order settings, command, ordering, compression, and row-group
  settings;
- add all 24 exact generated output records and eight schema contracts;
- preserve scale factors `0.01`, `1`, and `10`.

Use exact authoritative licenses and attribution. If Task 6 discovers that the current `format` or archive outputs are inaccurate, update the registry description/format to the observed authoritative shape and record the correction in the report and changelog; do not alter production extraction behavior in this issue.

For mutually exclusive releases that share flattened names under one landing
prefix, record complete artifact-level provenance. Issue #81 must consume the
selected release as one atomic identity, replace it atomically, and prevent
mixed stale-release objects; issue #80 does not change downloader behavior.

- [ ] **Step 4: Atomically switch production validation/parsing to v2**

```python
def validate_registry(doc: dict) -> list[str]:
    if not isinstance(doc, dict) or doc.get("version") != 2:
        return ["registry: 'version' must be 2"]
    return validate_registry_v2(doc)


def load_registry(path: Path) -> dict[str, Dataset]:
    return load_registry_v2(path)
```

Remove the version-1 parsing branch and obsolete version-1-only validation code. Update fixtures/tests that directly construct `Dataset` or inspect raw `urls` so they use the new typed constructors or version-2 registry shape. Do not change the production downloader/source/S3 modules.

- [ ] **Step 5: Run focused GREEN**

Run:

```bash
uv run pytest \
  tests/datasets/test_locking.py \
  tests/datasets/test_schema.py \
  tests/datasets/test_registry.py \
  tests/datasets/test_download_cli.py \
  tests/datasets/test_http.py \
  tests/scenarios/test_batch_ingest_taxi_schema.py -q
```

Expected: all focused tests pass, current URL/scale selections remain unchanged, and no network is used.

- [ ] **Step 6: Run verifier and confirm production paths are untouched**

Run: `make verify`

Expected: `0 finding(s), 0 error(s)`.

Run:

```bash
git diff origin/develop...HEAD -- \
  scripts/download_datasets.py datasets/sources/http.py datasets/sources/tpch.py datasets/s3.py
```

Expected: no output.

- [ ] **Step 7: Exact-stage and commit**

```bash
git add \
  datasets/registry.yaml datasets/schema.py datasets/registry.py \
  tests/datasets/test_schema.py tests/datasets/test_registry.py \
  tests/datasets/test_download_cli.py tests/datasets/test_http.py \
  tests/scenarios/test_batch_ingest_taxi_schema.py
git diff --cached --check
git commit -m "feat(datasets): populate versioned provenance lock (#80)"
```

---

### Task 8: Document provenance, attribution, and reviewed updates on all surfaces

**Files:**
- Modify: `docs/datasets.md`
- Modify: `docs/CHANGELOG.md`
- Modify: `tests/test_docs_content_contract.py`

**Interfaces:**
- Consumes: registry v2 facts
- Produces: canonical public contract projected to MkDocs and wiki

- [ ] **Step 1: Write failing documentation contract tests**

```python
def test_dataset_docs_describe_versioned_fail_closed_provenance_contract():
    text = (ROOT / "docs" / "datasets.md").read_text(encoding="utf-8")
    for phrase in (
        "registry version 2",
        "strict fail-on-drift",
        "raw archive",
        "extracted landing object",
        "DuckDB 1.5.4",
        "SHA-256",
        "schema fingerprint",
        "reviewed lock update",
        "runtime enforcement is tracked in issue #81",
    ):
        assert phrase in text


def test_dataset_docs_link_authoritative_sources_and_licenses():
    text = (ROOT / "docs" / "datasets.md").read_text(encoding="utf-8")
    for dataset in ("NYC Taxi", "GH Archive", "MovieLens", "Online Retail II", "TPC-H"):
        assert dataset in text
    registry = yaml.safe_load((ROOT / "datasets" / "registry.yaml").read_text())
    for dataset in registry["datasets"].values():
        assert dataset["provenance"]["homepage"] in text
        assert dataset["provenance"]["license_url"] in text
```

- [ ] **Step 2: Run RED**

Run: `uv run pytest tests/test_docs_content_contract.py -q`

Expected: the new provenance-contract assertions fail.

- [ ] **Step 3: Update canonical documentation and changelog**

In `docs/datasets.md`:

- correct the old `fetch.scale_params` description to the actual v2 schema;
- explain normalized artifacts, raw/archive/output locks, generator environment, schemas, and tier references;
- state strict fail-on-drift and the issue #80/#81 boundary exactly;
- add a source/license/attribution table driven by the same authoritative values in the registry;
- document the audit/update sequence and commands;
- correct the claim that existing objects are currently verified: until #81, say the lock is defined and parsed but runtime enforcement remains pending.

In `docs/CHANGELOG.md`, add one Unreleased `Changed` entry for registry v2, all source/object/generator/schema locks, and the explicit #81 enforcement boundary.

- [ ] **Step 4: Run GREEN and three-surface gates**

Run: `uv run pytest tests/test_docs_content_contract.py -q`

Expected: all documentation contract tests pass.

Run: `make docs-check && make docs-wiki`

Expected: strict MkDocs build and wiki projection checks pass without warnings or generated-source edits.

- [ ] **Step 5: Commit**

```bash
git add docs/datasets.md docs/CHANGELOG.md tests/test_docs_content_contract.py
git diff --cached --check
git commit -m "docs: explain dataset provenance review contract (#80)"
```

---

### Task 9: Run final verification and freeze the issue #80 review package

**Files:**
- Append: `/private/tmp/data-eng-lab-goal/task-80-report.md`
- No additional repository files unless a gate exposes an issue #80 defect

**Interfaces:**
- Consumes: complete issue #80 branch
- Produces: evidence for independent task/spec and whole-branch reviews

- [ ] **Step 1: Run focused dataset and docs suites**

```bash
uv run pytest -m "not infra and not network" \
  tests/datasets tests/scenarios/test_batch_ingest_taxi_schema.py \
  tests/test_docs_content_contract.py -q
```

Expected: all pass, with network and live-infrastructure tests explicitly
deselected.

- [ ] **Step 2: Run the complete offline matrix**

```bash
make lint
make test
make verify
make docs-check
make docs-wiki
git diff --check
```

Expected:

- Ruff clean;
- all non-infra/non-network tests pass;
- verifier reports `0 finding(s), 0 error(s)`;
- strict MkDocs and wiki parity pass;
- no whitespace errors.

- [ ] **Step 3: Verify safety, scope, and lock evidence**

```bash
git -C infra status --short
git submodule status
shasum -a 256 docs/superpowers/plans/2026-07-21-atlas-submodule-modernization.md
git status --short --branch
git diff --name-only origin/develop...HEAD
```

Expected:

- Atlas interior clean at `c6cf73d7168db1a7840fc45c9ed3e385071996d8`;
- protected plan is the only unrelated untracked path and has the exact protected checksum;
- no downloader/source/S3 enforcement files changed;
- only issue #80 design, plan, lock/model/audit/test/docs files appear.

- [ ] **Step 4: Append the final report**

Record:

- all commit SHAs and exact paths;
- RED/GREEN outputs for each task;
- HTTP acquisition timestamps, authoritative URLs, headers, sizes, hashes, archive outputs, licenses, and mutability classifications;
- TPC-H base manifest, package/wheel/uv-lock hashes, repeat-generation proof, all 24 output locks, and schema evidence;
- focused and full gate output;
- explicitly deferred #81 runtime enforcement;
- protected-plan and Atlas invariants;
- any environmental warnings separately from repository defects.

- [ ] **Step 5: Request two independent read-only reviews**

Review 1 checks the complete branch against issue #80 and the approved design. Review 2 checks correctness, security, provenance accuracy, schema/test quality, docs parity, and issue #81 boundary. Fix every substantiated Critical, Important, or Minor finding with TDD, rerun affected and full gates, and repeat reviews until both report zero findings and Ready to merge.

---

### Task 10: Feature PR, promotion, issue closeout, and cleanup

**Files:**
- No new implementation files
- GitHub issue #80, Project #7, PR metadata, and execution ledger

**Interfaces:**
- Consumes: independently approved, fully green issue #80 branch
- Produces: merged `develop` and `main`, reconciled Gitflow ancestry, closed issue/project item, cleaned feature refs

- [ ] **Step 1: Push and open the ready feature PR to `develop`**

The PR body includes issue link, normalized-v2 rationale, exact source/generator environment, candidate acquisition evidence, all tests, docs effects, #81 deferral, rollback (revert the feature PR), and an acceptance checklist.

- [ ] **Step 2: Wait for every protected check and review**

Do not merge while any current check is pending/failing or merge state is not clean. Address all actionable feedback, rerun local gates, push corrections, and resolve review threads.

- [ ] **Step 3: Merge using the established merge-commit convention**

Delete the remote feature branch only after the merge succeeds. Fetch/prune, switch to `develop`, fast-forward, and safely delete the local feature branch after its commits are reachable.

- [ ] **Step 4: Open and merge the succeeding `develop` to `main` promotion PR**

Verify the promotion contains only reviewed issue #80 paths, wait for all checks, and merge only when clean.

- [ ] **Step 5: Reconcile `main` back into `develop` if required**

If the promotion merge commit is not an ancestor of `develop`, open a reviewed ancestry-only `main` to `develop` PR. Verify the trees are identical before merging it and wait for every check.

- [ ] **Step 6: Close issue #80 and update Project #7**

Comment with feature/promotion/reconciliation PRs, commit/merge SHAs, exact acquisition/generator evidence, tests, docs, #81 deferral, Atlas/protected-plan invariants, and main/develop tree identity. Close #80 only after `main` contains the accepted result and mark its Project item Done. Keep parent #79 In Progress until #81 is complete.

- [ ] **Step 7: Final cleanup before #81**

Prove no issue #80 PR remains open, its feature branch is absent locally/remotely, `develop` is checked out/current, `main` and `develop` have identical intended trees with correct ancestry, Atlas is clean, and the protected plan checksum is exact. Update `.superpowers/sdd/backlog-goal-2026-08-10.md`, then begin #81 from the refreshed `origin/develop`.
