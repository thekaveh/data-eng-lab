# Dataset Lock Enforcement Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Enforce the committed dataset provenance locks through secure acquisition, deterministic generation, immutable MinIO publication, verified reuse, atomic manifest resolution, and every runtime consumer.

**Architecture:** The version-2 registry remains the byte-and-schema authority. Acquisition produces typed verified files; publication writes a transaction-unique immutable generation and content-addressed manifest before conditionally switching one active pointer. A pinned internal resolver service verifies the selected generation and returns ordered immutable URIs to Python, Scala, Airflow, Trino, and notebook consumers.

**Tech Stack:** Python 3.11, boto3/botocore, Requests/urllib3, DuckDB 1.5.4, standard-library ZIP/XML/CSV/JSON/GZIP, Docker linux/amd64, MinIO S3 API, Scala/Spark, Airflow, pytest, Moto/Stubber, Ruff, MkDocs Material.

## Global Constraints

- Work only on `codex/81-dataset-lock-enforcement`, based on `origin/develop` commit `6d6ae3c3daf8107b50010f7092fbc419b3e2af3b` and design commit `1cee879`.
- Follow strict RED then GREEN TDD for every behavior change and record both commands/results in `.superpowers/sdd/issue-81-task-<N>-report.md` without staging those scratch reports.
- Use `apply_patch` for repository edits and exact-path `git add` before each commit.
- Never modify Atlas source inside `infra`; its gitlink remains `c6cf73d7168db1a7840fc45c9ed3e385071996d8` and `git -C infra status --short` remains empty.
- Never touch or stage `docs/superpowers/plans/2026-07-21-atlas-submodule-modernization.md`; its SHA-256 remains `f7eb55036e3020f419d9acb42bbc502bfe0528219a980a6973ed083591d2ba66`.
- Do not change root `uv.lock`; its SHA-256 remains `a376ce1b5bd5621290aaded68c22572690395419876da41814e28469bb4186b1`, which is locked into the canonical TPC-H environment.
- The registry is read-only at runtime. No mismatch, refresh, rollback, or recovery path may rewrite `datasets/registry.yaml` or accept alternate bytes.
- `plan_id` equals the selected-plan SHA-256; `publication_id` is `uuid.uuid4().hex`; every physical object key is `<landing_prefix>/_generations/<plan_id>/<publication_id>/<object_name>`.
- Generation objects and immutable manifests use `If-None-Match: *`; active pointer replacement uses the previously observed opaque ETag with `If-Match`.
- ETag, `HEAD`, and user metadata never prove content. Every local, staged, uploaded, reused, rolled-back, and consumed object is streamed for exact size/SHA-256 and physical-schema verification.
- Each consumer supplies an expected `tiny`, `small`, or `medium` scale. No consumer accepts whichever scale happens to be active.
- Publication is atomic per dataset, not across a multi-dataset CLI invocation.
- General `make up` remains service-only. Dataset acquisition is not folded into stack startup.
- Full consumer migration is in scope; no production code may retain a flat `s3a://landing/<dataset>/...` read path.
- Complete every task with a fresh independent spec review and quality review before starting the next task.

---

## File and responsibility map

| Path | Responsibility |
|---|---|
| `datasets/verification.py` | Typed contexts, mismatch diagnostics, streaming size/SHA checks, exact name sets |
| `datasets/schema_inspection.py` | Offline Parquet, CSV, JSONL-GZIP, XLSX, and text physical-schema enforcement |
| `datasets/acquisition.py` | Shared secure HTTPS transport, ZIP preflight, member policy, bounded extraction |
| `datasets/sources/http.py` | Typed direct/archive artifact acquisition into verified files |
| `datasets/sources/tpch.py` | Canonical container build/run and eight-output verification |
| `datasets/s3.py` | Streamed S3 reads, immutable writes, conditional control writes, ETag reconciliation, leases |
| `datasets/publication.py` | Plan IDs, immutable manifests, active pointers, resolution, rollback, publication state machine |
| `datasets/resolver_service.py` | Internal read-only HTTP resolver API |
| `datasets/resolver.Dockerfile` | Pinned resolver runtime built from unchanged root lock |
| `scripts/download_datasets.py` | Default/verify/refresh/rollback/dry-run orchestration |
| `scripts/resolve_dataset.py` | Host canonical-JSON resolver CLI |
| `compose/data-eng-lab.yml` | Consumer-owned resolver service and internal endpoint wiring |
| `spark-apps/*` and `scenarios/*` | Expected-scale propagation and immutable URI consumption |
| `tests/datasets/*` | Focused lock, acquisition, schema, S3, publication, CLI, and resolver contracts |
| `tests/infra/*` | Live MinIO CAS/resolution and network-disabled TPC-H acceptance |
| `docs/datasets.md` | Canonical public verification/publication/recovery runbook |
| `README.md`, `docs/getting-started.md`, `docs/go-live.md`, `docs/CHANGELOG.md` | Synced entry, operations, evidence, and release surfaces |

## Design coverage map

| Approved design requirement | Implemented and proved by |
|---|---|
| Byte locks, exact names, diagnostics | Task 1 |
| Physical schemas and bounded offline parsing | Task 2 |
| Hardened HTTPS/ZIP shared by audit and production | Tasks 3-4 |
| Canonical network-disabled TPC-H | Task 5 |
| Conditional writes, remote hashing, leases, ambiguous outcomes | Task 6 |
| Plan/publication IDs, immutable history, pointers, rollback, reader verification | Task 7 |
| Default/verify/refresh/rollback/legacy/crash transaction semantics | Task 8 |
| Deployable cross-language resolver and endpoint boundaries | Task 9 |
| Expected-scale propagation and removal of flat runtime reads | Task 10 |
| Canonical/MkDocs/wiki runbooks and lock-update separation | Task 11 |
| Full offline review and invariant proof | Task 12 |
| Real MinIO, resolver, HTTP, TPC-H, consumer, and recovery proof | Task 13 |
| Feature/develop/main/back-sync promotion and board/branch cleanup | Task 14 |

---

### Task 1: Add typed byte verification and exact-set contracts

**Files:**
- Create: `datasets/verification.py`
- Create: `tests/datasets/test_verification.py`

**Interfaces:**
- Consumes: `datasets.locking.file_metadata`
- Produces: `VerificationContext`, `ExpectedObject`, `VerifiedFile`, `LockMismatch`, `verify_stream`, `verify_file`, `require_exact_names`

- [ ] **Step 1: Write the failing verification tests**

```python
def test_verify_stream_rejects_truncation_with_structured_context():
    context = VerificationContext("movielens", "small", "download", "latest_small", "ratings.csv")
    with pytest.raises(LockMismatch) as caught:
        verify_stream(io.BytesIO(b"abc"), 4, hashlib.sha256(b"abcd").hexdigest(), context)
    assert caught.value.field == "size_bytes"
    assert caught.value.expected == 4
    assert caught.value.actual == 3


def test_require_exact_names_reports_missing_and_extra_together():
    with pytest.raises(LockMismatch) as caught:
        require_exact_names(("a.csv", "b.csv"), ("b.csv", "c.csv"), CONTEXT)
    assert caught.value.expected == ("a.csv", "b.csv")
    assert caught.value.actual == ("b.csv", "c.csv")
```

Cover empty, truncated, oversized, wrong SHA, exact success, non-seekable streams, read failures, duplicate expected/actual names, path identity, and redacted `str(error)` output.

- [ ] **Step 2: Run the focused RED**

Run: `uv run pytest tests/datasets/test_verification.py -q`

Expected: collection fails because `datasets.verification` does not exist.

- [ ] **Step 3: Implement the minimal typed verifier**

```python
@dataclass(frozen=True)
class VerificationContext:
    dataset: str
    scale: str
    stage: str
    artifact: str | None = None
    object_name: str | None = None


@dataclass(frozen=True)
class ExpectedObject:
    object_name: str
    size_bytes: int
    sha256: str
    schema_id: str


@dataclass(frozen=True)
class VerifiedFile:
    path: Path
    expected: ExpectedObject


class LockMismatch(ValueError):
    def __init__(self, context: VerificationContext, field: str, expected: object, actual: object):
        self.context = context
        self.field = field
        self.expected = expected
        self.actual = actual
        super().__init__(f"{context.dataset}/{context.scale} {context.stage} {field} mismatch")


def verify_stream(stream: BinaryIO, expected_size: int, expected_sha256: str,
                  context: VerificationContext) -> tuple[int, str]:
    digest = hashlib.sha256()
    size = 0
    while chunk := stream.read(min(1 << 20, expected_size - size + 1)):
        size += len(chunk)
        if size > expected_size:
            raise LockMismatch(context, "size_bytes", expected_size, size)
        digest.update(chunk)
    if size != expected_size:
        raise LockMismatch(context, "size_bytes", expected_size, size)
    actual_sha256 = digest.hexdigest()
    if actual_sha256 != expected_sha256:
        raise LockMismatch(context, "sha256", expected_sha256, actual_sha256)
    return size, actual_sha256


def verify_file(path: Path, expected: ExpectedObject,
                context: VerificationContext) -> VerifiedFile:
    with path.open("rb") as stream:
        verify_stream(stream, expected.size_bytes, expected.sha256, context)
    return VerifiedFile(path.resolve(strict=True), expected)


def require_exact_names(expected: Sequence[str], actual: Sequence[str],
                        context: VerificationContext) -> None:
    expected_names = tuple(expected)
    actual_names = tuple(actual)
    if actual_names != expected_names:
        raise LockMismatch(context, "object_names", expected_names, actual_names)
```

`verify_stream` reads at most the expected size plus one byte in bounded chunks, distinguishes truncation/oversize/digest mismatch, and never includes secret URLs or local temporary roots in diagnostics.

- [ ] **Step 4: Run GREEN and lint**

Run: `uv run pytest tests/datasets/test_verification.py tests/datasets/test_locking.py -q`

Expected: all tests pass.

Run: `uv run ruff check datasets/verification.py tests/datasets/test_verification.py`

Expected: `All checks passed!`

- [ ] **Step 5: Review, stage exact files, and commit**

```bash
git add datasets/verification.py tests/datasets/test_verification.py
git commit -m "feat(datasets): add typed lock verification (#81)"
```

---

### Task 2: Enforce deterministic physical schemas offline

**Files:**
- Create: `datasets/schema_inspection.py`
- Create: `tests/datasets/test_schema_inspection.py`
- Modify: `tests/datasets/fixtures/registry-v2-minimal.yaml`

**Interfaces:**
- Consumes: `SchemaContract`, `SchemaField`, `VerifiedFile`, `LockMismatch`, `VerificationContext`
- Produces: `ObservedField`, `ObservedSchema`, `normalize_parquet_schema`, `verify_physical_schema`

- [ ] **Step 1: Write table-driven RED tests for all five formats**

```python
@pytest.mark.parametrize("logical, duckdb_type", [
    ("boolean", "BOOLEAN"), ("int8", "TINYINT"), ("int64", "BIGINT"),
    ("float32", "FLOAT"), ("float64", "DOUBLE"), ("date", "DATE"),
    ("timestamp", "TIMESTAMP_NS"), ("timestamp-tz", "TIMESTAMP WITH TIME ZONE"),
    ("string", "VARCHAR"), ("binary", "BLOB"), ("decimal(12,2)", "DECIMAL(12,2)"),
])
def test_parquet_normalization_is_frozen(logical, duckdb_type):
    assert normalize_parquet_schema([parquet_row("value", duckdb_type, "OPTIONAL")]) == (
        ObservedField("value", logical, True),
    )


def test_xlsx_rejects_formula_cells_before_value_validation(tmp_path):
    path = xlsx_fixture(tmp_path, cell_xml='<c r="A2"><f>1+1</f><v>2</v></c>')
    with pytest.raises(LockMismatch, match="formula"):
        verify_physical_schema(path, XLSX_CONTRACT, CONTEXT)
```

Add fixtures/tests for Parquet order/types/nullability/unsupported nesting; CSV BOM, quoting, header, width, null, numeric range and mixed types; JSON gzip integrity, nested dotted fields, minimum extras, missing/non-nullable fields, depth/string/record/expanded bounds; XLSX DTD/entity/formula/shared-string/style/date/sheet/header/type/expansion handling; and UTF-8 text.

- [ ] **Step 2: Run the focused RED**

Run: `uv run pytest tests/datasets/test_schema_inspection.py -q`

Expected: collection fails because `datasets.schema_inspection` does not exist.

- [ ] **Step 3: Implement the authoritative inspectors**

```python
@dataclass(frozen=True)
class ObservedField:
    name: str
    logical_type: str
    nullable: bool


@dataclass(frozen=True)
class ObservedSchema:
    fields: tuple[ObservedField, ...]


def verify_physical_schema(path: Path, contract: SchemaContract,
                           context: VerificationContext) -> ObservedSchema:
    inspectors = {
        "parquet": inspect_parquet,
        "csv": inspect_csv,
        "jsonl-gzip": inspect_jsonl_gzip,
        "xlsx": inspect_xlsx,
        "text": inspect_text,
    }
    observed = inspectors[contract.format](path, contract, context)
    compare_observed_schema(observed, contract, context)
    return observed
```

Pin the implementation check to `duckdb.__version__ == "1.5.4"`. Implement the exact type/null/order rules in the approved design. Use streaming standard-library parsers for CSV/JSON/GZIP/XML, reject XLSX formulas and DTD/entity declarations, and cap expanded bytes at `min(max(64 MiB, 200 * locked_size), 8 GiB)` with JSON depth 64 and record/string size 16 MiB.

- [ ] **Step 4: Run GREEN against production schemas**

Run: `uv run pytest tests/datasets/test_schema_inspection.py tests/datasets/test_registry.py tests/datasets/test_schema.py -q`

Expected: all tests pass and every committed production schema has a frozen mapping case.

Run: `uv run ruff check datasets/schema_inspection.py tests/datasets/test_schema_inspection.py`

Expected: `All checks passed!`

- [ ] **Step 5: Commit the schema boundary**

```bash
git add datasets/schema_inspection.py tests/datasets/test_schema_inspection.py tests/datasets/fixtures/registry-v2-minimal.yaml
git commit -m "feat(datasets): verify physical lock schemas (#81)"
```

---

### Task 3: Extract one shared hardened HTTP and ZIP security boundary

**Files:**
- Create: `datasets/acquisition.py`
- Create: `tests/datasets/test_acquisition.py`
- Modify: `scripts/audit_dataset_lock.py`
- Modify: `tests/datasets/test_audit_dataset_lock.py`

**Interfaces:**
- Consumes: `validate_relative_path`, authoritative URL/IP policy from `datasets.schema`
- Produces: `ResponseEvidence`, `DownloadedFile`, `ArchiveEntry`, `download_bounded`, `preflight_zip`, `validated_zip_members`, `extract_members`

- [ ] **Step 1: Write RED compatibility and security tests**

```python
def test_audit_and_production_share_the_same_zip_policy(monkeypatch, archive):
    monkeypatch.setattr(audit_dataset_lock, "validated_zip_members", sentinel_policy)
    audit_dataset_lock._archive_outputs(archive, archive.parent)
    assert sentinel_policy.called_once


def test_download_pins_public_dns_and_rejects_private_peer(fake_dns, fake_transport):
    fake_dns.answer("example.test", "203.0.113.10")
    fake_transport.peer = "127.0.0.1"
    with pytest.raises(ValueError, match="connected peer"):
        download_bounded("https://example.test/file", TARGET, MAX_BYTES)
```

Port all existing audit adversarial ZIP cases into shared-API assertions. Add proxy disablement, A/AAAA all-public policy, pinned address with original TLS SNI/Host, peer equality, redirect revalidation/limit, query redaction, deadline/socket rebinding, identity encoding, exclusive path creation, and cleanup ownership cases.

- [ ] **Step 2: Run RED before moving implementation**

Run: `uv run pytest tests/datasets/test_acquisition.py tests/datasets/test_audit_dataset_lock.py -q`

Expected: new tests fail because shared APIs do not exist and the audit owns private copies.

- [ ] **Step 3: Move the proven algorithms without weakening them**

```python
@dataclass(frozen=True)
class DownloadedFile:
    path: Path
    evidence: ResponseEvidence


@dataclass(frozen=True)
class ArchiveEntry:
    member_path: str
    object_name: str
    size_bytes: int


@dataclass(frozen=True)
class ZipLimits:
    max_entries: int = 10_000
    max_central_directory_bytes: int = 64 * 1024 * 1024
    max_total_expanded_bytes: int = 8 * 1024 * 1024 * 1024
    max_compression_ratio: int = 200
```

Implement `download_bounded(url, destination, max_bytes, deadline_seconds=120.0, transport=None)`, `preflight_zip(path, limits)`, `validated_zip_members(path, limits)`, and `extract_members(path, entries, destination)` with the exact signatures in the Interfaces block. Move EOCD/ZIP64 selection, central-directory streaming, structural-directory rules, normalized namespace checks, encryption/compression policy, limits, and owned extraction from the audit module. Keep the audit CLI as a thin consumer that still emits identical evidence YAML.

- [ ] **Step 4: Run the complete shared/audit GREEN**

Run: `uv run pytest tests/datasets/test_acquisition.py tests/datasets/test_audit_dataset_lock.py -q`

Expected: all old audit tests and new shared-policy tests pass.

Run: `uv run ruff check datasets/acquisition.py scripts/audit_dataset_lock.py tests/datasets/test_acquisition.py tests/datasets/test_audit_dataset_lock.py`

Expected: `All checks passed!`

- [ ] **Step 5: Commit the shared security boundary**

```bash
git add datasets/acquisition.py scripts/audit_dataset_lock.py tests/datasets/test_acquisition.py tests/datasets/test_audit_dataset_lock.py
git commit -m "refactor(datasets): share hardened acquisition policy (#81)"
```

---

### Task 4: Enforce typed HTTP artifact locks before extraction or return

**Files:**
- Modify: `datasets/sources/http.py`
- Modify: `tests/datasets/test_http.py`

**Interfaces:**
- Consumes: `ScalePlan.artifacts`, `download_bounded`, `validated_zip_members`, `extract_members`, `verify_file`, `verify_physical_schema`
- Produces: `fetch_http(plan: ScalePlan, dest: Path) -> tuple[VerifiedFile, ...]`

- [ ] **Step 1: Replace legacy fixture tests with lock-grade RED cases**

```python
def test_fetch_rejects_raw_digest_before_archive_open(tmp_path, responses):
    plan = archive_plan(raw_size=4, raw_sha256=sha256(b"good"))
    responses.get(plan.artifacts[0].url, body=b"evil")
    with pytest.raises(LockMismatch, match="sha256"):
        fetch_http(plan, tmp_path)
    assert list(tmp_path.iterdir()) == []


def test_fetch_returns_registry_ordered_verified_outputs(tmp_path, responses):
    plan, archive = exact_archive_plan()
    responses.get(plan.artifacts[0].url, body=archive)
    result = fetch_http(plan, tmp_path)
    assert tuple(item.expected.object_name for item in result) == plan_object_names(plan)
```

Cover direct raw identity, raw size/SHA, missing/extra/duplicate member, wrong member path, wrong flattened name, output size/SHA/schema, multi-artifact ordering, no partial return, and cleanup after every failure.

- [ ] **Step 2: Run RED**

Run: `uv run pytest tests/datasets/test_http.py -q`

Expected: failures show the legacy URL/unzip implementation ignores typed locks.

- [ ] **Step 3: Implement lock-owned acquisition**

```python
def fetch_http(plan: ScalePlan, dest: Path) -> tuple[VerifiedFile, ...]:
    results: list[VerifiedFile] = []
    with owned_directory(dest) as transaction_root:
        for artifact in plan.artifacts:
            results.extend(fetch_artifact(plan, artifact, transaction_root))
        expected_names = tuple(output.object_name for artifact in plan.artifacts for output in artifact.outputs)
        context = VerificationContext(plan.dataset.name, plan.scale, "result")
        require_exact_names(expected_names, tuple(item.expected.object_name for item in results), context)
        published = publish_verified_files(results, transaction_root, dest)
    return tuple(published)
```

Never reconstruct a name from URL or `Path.name` after the registry is loaded. Keep raw and extracted paths transaction-owned and remove them on failure.

- [ ] **Step 4: Run GREEN and compatibility suites**

Run: `uv run pytest tests/datasets/test_http.py tests/datasets/test_acquisition.py tests/datasets/test_registry.py -q`

Expected: all pass.

- [ ] **Step 5: Commit HTTP enforcement**

```bash
git add datasets/sources/http.py tests/datasets/test_http.py
git commit -m "feat(datasets): enforce HTTP artifact locks (#81)"
```

---

### Task 5: Replace host TPC-H generation with the canonical offline container

**Files:**
- Modify: `datasets/sources/tpch.py`
- Modify: `tests/datasets/test_tpch.py`
- Modify: `tests/datasets/test_tpch_lock_export.py`

**Interfaces:**
- Consumes: `ScalePlan.dataset.generator`, `ScalePlan.generator_scale`, `VerifiedFile`, canonical `datasets/tpch-lock.Dockerfile` and exporter
- Produces: `generate_tpch(plan: ScalePlan, dest: Path, runner: ContainerRunner | None = None) -> tuple[VerifiedFile, ...]`

- [ ] **Step 1: Write RED container-contract tests**

```python
def test_generate_tpch_uses_network_none_and_exact_platform(tmp_path, fake_runner, tpch_plan):
    generate_tpch(tpch_plan, tmp_path, runner=fake_runner)
    assert fake_runner.run_args.platform == "linux/amd64"
    assert fake_runner.run_args.network == "none"


def test_generate_tpch_rejects_metadata_environment_drift(tmp_path, fake_runner, tpch_plan):
    fake_runner.metadata["duckdb_version"] = "1.5.3"
    with pytest.raises(LockMismatch, match="duckdb_version"):
        generate_tpch(tpch_plan, tmp_path, runner=fake_runner)
```

Cover pinned base manifest/platform, canonical entrypoint/env labels, root `uv.lock` digest, build-time wheel hash proof, runtime DuckDB version/extension hash, locale/timezone/threads/export settings, scale factor, eight exact names, metadata, size/SHA/schema, no runtime install/network, and no host fallback.

- [ ] **Step 2: Run RED**

Run: `uv run pytest tests/datasets/test_tpch.py tests/datasets/test_tpch_lock_export.py -q`

Expected: legacy `generate_tpch(sf, dest)` tests fail because it uses host DuckDB and runtime `INSTALL`.

- [ ] **Step 3: Implement the container runner boundary**

```python
def generate_tpch(plan: ScalePlan, dest: Path,
                  runner: ContainerRunner | None = None) -> tuple[VerifiedFile, ...]:
    active_runner = runner or DockerContainerRunner()
    contract = require_generator_contract(plan.dataset.generator)
    scale = require_generator_scale(plan.generator_scale)
    context = VerificationContext(plan.dataset.name, plan.scale, "image")
    evidence = active_runner.ensure_image(contract)
    verify_image_evidence(evidence, contract, context)
    with owned_directory(dest) as output_root:
        metadata_path = output_root / "metadata.json"
        active_runner.run(contract, scale, output_root, metadata_path)
        verified = verify_tpch_outputs(plan, output_root, metadata_path)
        published = publish_verified_files(verified, output_root, dest)
    return tuple(published)
```

Build the image only when absent or label-mismatched, inspect base/platform/entrypoint/env, run with `--network=none`, parse the atomic exporter metadata, require exactly eight outputs in registry order, then verify bytes and schemas.

- [ ] **Step 4: Run GREEN and a mocked no-network proof**

Run: `uv run pytest tests/datasets/test_tpch.py tests/datasets/test_tpch_lock_export.py -q`

Expected: all pass; the obsolete test allowing network installation is removed or replaced by a strict prohibition test.

- [ ] **Step 5: Commit canonical generation**

```bash
git add datasets/sources/tpch.py tests/datasets/test_tpch.py tests/datasets/test_tpch_lock_export.py
git commit -m "feat(datasets): enforce canonical TPC-H generation (#81)"
```

---

### Task 6: Add conditional S3 writes, remote verification, and ABA-safe leases

**Files:**
- Modify: `datasets/s3.py`
- Modify: `tests/datasets/test_s3.py`

**Interfaces:**
- Consumes: `VerificationContext`, `ExpectedObject`, `verify_stream`, boto3 S3 client
- Produces: `ObjectSnapshot`, `ConditionalConflict`, `AmbiguousWrite`, `stream_verify_object`, `put_immutable_object`, `put_control_object`, `read_control_object`, `Lease`, `acquire_lease`, `renew_lease`, `release_lease`

- [ ] **Step 1: Write RED adapter and lease tests**

```python
def test_head_metadata_never_substitutes_for_get_hash(client, locked_object):
    client.put_object(Bucket="landing", Key=locked_object.key, Body=b"evil", Metadata=locked_object.metadata)
    with pytest.raises(LockMismatch, match="sha256"):
        stream_verify_object(client, "landing", locked_object.key, locked_object.expected, CONTEXT)


def test_stale_lease_owner_cannot_release_successor(fake_s3):
    first = acquire_lease(fake_s3, "nyc_taxi", PUBLICATION_A, NONCE_A)
    successor = fake_s3.expire_and_take_over(first, PUBLICATION_B, NONCE_B)
    with pytest.raises(ConditionalConflict):
        release_lease(fake_s3, first)
    assert fake_s3.current_lease == successor
```

Cover opaque quoted ETags; If-None/If-Match request parameters; 404/409/412 mapping; lost-response reconciliation for exact versus competing bytes; post-upload GET/hash; metadata mismatch; server `Date` missing or over 300 seconds skewed; five-second proposal window; missing/released/active/expired acquire; renewal; lease loss; and release-as-conditional-PUT.

- [ ] **Step 2: Run RED**

Run: `uv run pytest tests/datasets/test_s3.py -q`

Expected: failures show only existence and unconditional upload APIs exist.

- [ ] **Step 3: Implement exact S3 adapter semantics**

```python
@dataclass(frozen=True)
class ObjectSnapshot:
    etag: str
    metadata: Mapping[str, str]
    server_date: datetime
    size_bytes: int
    sha256: str


@dataclass(frozen=True)
class ControlSnapshot:
    body: bytes
    etag: str
    server_date: datetime


def put_immutable_object(client, bucket: str, key: str, path: Path,
                         expected: ExpectedObject, metadata: Mapping[str, str]) -> ObjectSnapshot:
    with path.open("rb") as body:
        try:
            client.put_object(Bucket=bucket, Key=key, Body=body, Metadata=dict(metadata), IfNoneMatch="*")
        except ClientError as error:
            reconcile_immutable_write(error, client, bucket, key, path, expected, metadata)
    return read_and_verify_object(client, bucket, key, expected, metadata)


def put_control_object(client, bucket: str, key: str, body: bytes,
                       *, if_match: str | None = None,
                       if_none_match: bool = False) -> ControlSnapshot:
    request = {"Bucket": bucket, "Key": key, "Body": body}
    if if_match is not None:
        request["IfMatch"] = if_match
    if if_none_match:
        request["IfNoneMatch"] = "*"
    client.put_object(**request)
    return read_control_object(client, bucket, key)
```

Use `PutObject(IfNoneMatch="*")` or `PutObject(IfMatch=etag)` directly, preserve ETags as opaque values, and reconcile ambiguous outcomes only by exact GET comparison. Never retry pointer CAS with a stale ETag.

- [ ] **Step 4: Run GREEN with Moto plus Stubber/fake semantics**

Run: `uv run pytest tests/datasets/test_s3.py tests/datasets/test_verification.py -q`

Expected: all pass. The report states that Moto is not evidence of MinIO conditional behavior.

- [ ] **Step 5: Commit the object-store boundary**

```bash
git add datasets/s3.py tests/datasets/test_s3.py
git commit -m "feat(datasets): add conditional verified S3 writes (#81)"
```

---

### Task 7: Define plan identities, immutable manifests, pointers, and pure resolution

**Files:**
- Create: `datasets/publication.py`
- Create: `tests/datasets/test_publication.py`

**Interfaces:**
- Consumes: `canonical_json`, typed registry contracts, S3 adapter, physical-schema verifier
- Produces: `ResolvedObject`, `ImmutableManifest`, `ActivePointer`, `ResolvedDataset`, `selected_plan_document`, `plan_id`, `publication_prefix`, `resolve_active_dataset`, `rollback_manifest`

- [ ] **Step 1: Write RED canonical-model tests**

```python
def test_unrelated_registry_edit_does_not_change_selected_plan_id(registry_document):
    before = plan_id(resolve(registry_document, "movielens", "small"))
    registry_document["datasets"]["nyc_taxi"]["description"] += " updated"
    after = plan_id(resolve(registry_document, "movielens", "small"))
    assert after == before


def test_pointer_resolves_only_expected_scale(fake_s3, published_small):
    with pytest.raises(LockMismatch, match="scale"):
        resolve_active_dataset(fake_s3, REGISTRY, "movielens", "medium")
```

Cover exact canonical JSON bytes/no newline, full 64-character plan ID, 32-character publication ID, safe physical keys, immutable manifest digest/key, minimal pointer, unknown-field rejection, raw registry hash audit-only behavior, complete remote verification, registry order, wrong expected scale, corrupt pointer/manifest, missing object, rollback after scale/unrelated/selected-plan changes, and concurrent pointer conflict.

- [ ] **Step 2: Run RED**

Run: `uv run pytest tests/datasets/test_publication.py -q`

Expected: collection fails because `datasets.publication` does not exist.

- [ ] **Step 3: Implement pure canonical control models and resolver**

```python
@dataclass(frozen=True)
class ResolvedObject:
    object_name: str
    uri: str
    size_bytes: int
    sha256: str
    schema_id: str


@dataclass(frozen=True)
class ResolvedDataset:
    dataset: str
    scale: str
    plan_id: str
    manifest_sha256: str
    publication_id: str
    objects: tuple[ResolvedObject, ...]


def resolve_active_dataset(client, registry: Mapping[str, Dataset],
                           dataset_id: str, expected_scale: str) -> ResolvedDataset:
    plan = resolve_scale(registry[dataset_id], expected_scale)
    pointer = read_active_pointer(client, dataset_id)
    manifest = read_immutable_manifest(client, pointer)
    validate_manifest_for_plan(manifest, plan)
    objects = tuple(verify_resolved_object(client, plan, item) for item in manifest.objects)
    return ResolvedDataset(
        dataset=dataset_id,
        scale=expected_scale,
        plan_id=manifest.plan_id,
        manifest_sha256=pointer.manifest_sha256,
        publication_id=manifest.publication_id,
        objects=objects,
    )
```

The resolver reads pointer then content-addressed manifest, verifies canonical bytes/digests/plan, streams and schema-checks every object, and returns no partial result.

- [ ] **Step 4: Run GREEN**

Run: `uv run pytest tests/datasets/test_publication.py tests/datasets/test_s3.py tests/datasets/test_schema_inspection.py -q`

Expected: all pass.

- [ ] **Step 5: Commit publication models**

```bash
git add datasets/publication.py tests/datasets/test_publication.py
git commit -m "feat(datasets): model immutable dataset publications (#81)"
```

---

### Task 8: Implement full publication transactions and CLI modes

**Files:**
- Modify: `datasets/publication.py`
- Modify: `scripts/download_datasets.py`
- Modify: `tests/datasets/test_publication.py`
- Modify: `tests/datasets/test_download_cli.py`

**Interfaces:**
- Consumes: verified HTTP/TPC-H producers, leases, S3 adapter, canonical models
- Produces: `PublishMode`, `PublishResult`, `publish_dataset`, updated `run` and CLI flags

- [ ] **Step 1: Write RED state-machine and CLI tests**

```python
def test_exact_second_run_reads_but_does_not_acquire_or_mutate(published, spies):
    result = publish_dataset(published.plan, mode=PublishMode.DEFAULT, services=spies)
    assert result.status == "verified-existing"
    assert spies.source_requests == spies.puts == spies.deletes == 0
    assert spies.gets == len(published.plan.objects) + 2


def test_lost_pointer_response_reconciles_exact_self_commit(transaction):
    transaction.s3.lose_response_after_pointer_commit = True
    result = transaction.publish()
    assert result.status == "published-reconciled"
    assert transaction.s3.pointer.manifest_sha256 == result.manifest_sha256
```

Cover first publication, exact reuse, verified legacy top-level migration, legacy extra/missing/corrupt behavior, refresh, force alias, verify-only, rollback, dry-run, scale transition, orphan isolation, corrupt pointer recovery by ETag, acquire failure, lease expiry mid-upload, lost object/manifest/pointer responses, two publishers, interruption at every state, per-dataset atomicity, multi-dataset partial result/nonzero exit, and all invalid flag combinations.

- [ ] **Step 2: Run RED**

Run: `uv run pytest tests/datasets/test_publication.py tests/datasets/test_download_cli.py -q`

Expected: failures show the legacy CLI skips existence and uploads one file at a time.

- [ ] **Step 3: Implement the exact transaction state machine**

```python
class PublishMode(Enum):
    DEFAULT = "default"
    VERIFY_ONLY = "verify-only"
    REFRESH = "refresh"
    ROLLBACK = "rollback"


def publish_dataset(plan: ScalePlan, *, mode: PublishMode, client,
                    fetcher: Fetcher, rollback_sha256: str | None = None,
                    dry_run: bool = False) -> PublishResult:
    active = inspect_active_state(client, plan)
    if dry_run:
        return describe_publication(plan, active, mode, rollback_sha256)
    if mode in {PublishMode.DEFAULT, PublishMode.VERIFY_ONLY}:
        return finish_verified_or_initial(plan, active, mode, client, fetcher)
    if mode is PublishMode.ROLLBACK:
        return publish_rollback(plan, active, require_digest(rollback_sha256), client)
    return publish_refresh(plan, active, client, fetcher)
```

Generate a new publication ID for each attempted upload, upload all immutable objects with If-None, re-read/verify them, write/re-read the immutable manifest, then CAS the pointer. List legacy keys with pagination and compare only direct top-level keys while excluding `_generations/`. Preserve all prior data/history and perform no automatic deletion.

- [ ] **Step 4: Run GREEN and argument-contract checks**

Run: `uv run pytest tests/datasets/test_publication.py tests/datasets/test_download_cli.py tests/datasets/test_http.py tests/datasets/test_tpch.py -q`

Expected: all pass.

Run: `uv run ruff check datasets/publication.py scripts/download_datasets.py tests/datasets/test_publication.py tests/datasets/test_download_cli.py`

Expected: `All checks passed!`

- [ ] **Step 5: Commit orchestration**

```bash
git add datasets/publication.py scripts/download_datasets.py tests/datasets/test_publication.py tests/datasets/test_download_cli.py
git commit -m "feat(datasets): publish verified immutable generations (#81)"
```

---

### Task 9: Add the pinned internal resolver service and host CLI

**Files:**
- Create: `datasets/resolver_service.py`
- Create: `datasets/resolver.Dockerfile`
- Create: `scripts/resolve_dataset.py`
- Create: `tests/datasets/test_resolver_service.py`
- Create: `tests/datasets/test_resolve_cli.py`
- Modify: `compose/data-eng-lab.yml`
- Modify: `atlas.consumer.yml`
- Modify: `tests/test_atlas_usage_contract.py`

**Interfaces:**
- Consumes: `resolve_active_dataset`, `DATASET_SCALE`, host/container MinIO endpoint resolution
- Produces: `POST /v1/resolve`, `GET /healthz`, canonical resolution JSON, `DATASET_RESOLVER_URI`

- [ ] **Step 1: Write RED API, container, and overlay contracts**

```python
def test_resolve_endpoint_requires_dataset_and_expected_scale(client, fake_resolver):
    response = client.post("/v1/resolve", json={"dataset": "movielens"})
    assert response.status_code == 400
    assert response.json()["error"] == "expected_scale is required"


def test_airflow_dag_import_performs_no_resolver_or_s3_access(monkeypatch):
    monkeypatch.setattr(socket, "create_connection", fail_on_call)
    import_all_data_eng_lab_dags()
```

Cover exact request/response fields, scale precedence, no partial result, no secrets/endpoints in JSON/logs, health without S3 mutation, unchanged root lock digest, pinned base/platform, internal-only port, endpoint choice, and compose dependency/health wiring.

- [ ] **Step 2: Run RED**

Run: `uv run pytest tests/datasets/test_resolver_service.py tests/datasets/test_resolve_cli.py tests/test_atlas_usage_contract.py -q`

Expected: missing module/CLI/service/compose contract failures.

- [ ] **Step 3: Implement the shared service and CLI**

```python
def resolve_request(document: Mapping[str, object], services: ResolverServices) -> bytes:
    dataset = require_identifier(document, "dataset")
    scale = require_scale(document, "expected_scale")
    return canonical_json(asdict(resolve_active_dataset(services.client, services.registry, dataset, scale)))
```

Use a small standard-library HTTP server bound only inside the compose network. Build from the pinned Python base with dependencies already in the unchanged root lock. Configure the service with container MinIO endpoint and generated credentials; configure the host CLI through existing host endpoint resolution.

- [ ] **Step 4: Run GREEN, build, and inspect the resolver image**

Run: `uv run pytest tests/datasets/test_resolver_service.py tests/datasets/test_resolve_cli.py tests/test_atlas_usage_contract.py -q`

Expected: all pass.

Run: `docker build --platform linux/amd64 -f datasets/resolver.Dockerfile -t data-eng-lab-dataset-resolver:test .`

Expected: build succeeds and `docker image inspect` shows no host port, canonical entrypoint, linux/amd64, and the expected root-lock label.

- [ ] **Step 5: Commit resolver delivery**

```bash
git add datasets/resolver_service.py datasets/resolver.Dockerfile scripts/resolve_dataset.py tests/datasets/test_resolver_service.py tests/datasets/test_resolve_cli.py compose/data-eng-lab.yml atlas.consumer.yml tests/test_atlas_usage_contract.py
git commit -m "feat(datasets): add verified resolver service (#81)"
```

---

### Task 10: Migrate every runtime consumer to expected-scale immutable URIs

**Files:**
- Modify: `spark-apps/nyc-taxi-etl/dag.py`
- Modify: `spark-apps/nyc-taxi-medallion/dag.py`
- Modify: `spark-apps/nyc-taxi-etl/src/main/scala/com/thekaveh/dataeng/nyctaxi/NycTaxiEtl.scala`
- Modify: `spark-apps/nyc-taxi-medallion/src/main/scala/com/thekaveh/dataeng/medallion/NycTaxiMedallion.scala`
- Modify: `scripts/bronze_smoke.py`
- Modify: `scripts/new_scenario.py`
- Modify: every `scenarios/*/jupyter/notebook.ipynb` and `scenarios/*/zeppelin/notebook.zpln` containing a flat landing path
- Modify: matching Spark/DAG/scenario/reproducibility tests
- Create: `tests/datasets/test_consumer_resolution_inventory.py`

**Interfaces:**
- Consumes: `POST /v1/resolve` canonical JSON and `DATASET_SCALE`
- Produces: explicit ordered immutable URI arguments/variables retained for one run

- [ ] **Step 1: Write a RED exact-inventory guard**

```python
def test_runtime_consumers_do_not_construct_flat_landing_paths():
    offenders = scan_runtime_sources(LEGACY_LANDING_PATTERN)
    assert offenders == []


@pytest.mark.parametrize("scale", ["tiny", "small", "medium"])
def test_each_consumer_requests_its_effective_scale(scale, consumer_fixture):
    result = consumer_fixture.bootstrap(explicit_scale=scale)
    assert result.request == {"dataset": consumer_fixture.dataset, "expected_scale": scale}
```

Freeze the current runtime inventory, including all Jupyter and Zeppelin notebooks, both DAGs/apps, smoke code, templates, and notebook reproducibility harness. Exclude docs/diagrams only when they describe logical—not physical—paths.

- [ ] **Step 2: Run RED**

Run: `uv run pytest tests/datasets/test_consumer_resolution_inventory.py tests/test_dag_catalog_conf.py tests/scenarios -q`

Expected: failures list every current flat-path runtime consumer.

- [ ] **Step 3: Migrate consumers without changing business logic**

Use one bootstrap paragraph/call per notebook to request dataset plus effective scale and retain the ordered immutable URI tuple. Airflow resolves only during task execution and passes URIs to Spark arguments. Scala applications require arguments and remove flat defaults. Trino DDL and streaming readers use the retained immutable prefix/object list. TPC-H consumers use `.parquet` names from the result. Preserve transformations, assertions, visualization, and scenario output behavior.

```python
scale = os.environ.get("DATASET_SCALE", "small")
resolved = resolve_dataset("nyc_taxi", scale)
taxi_paths = [item["uri"] for item in resolved["objects"]]
raw = spark.read.parquet(*taxi_paths)
```

```scala
require(args.nonEmpty, "verified immutable NYC Taxi URI arguments are required")
val raw = spark.read.parquet(args: _*)
```

- [ ] **Step 4: Run GREEN across Python, Scala, DAG, and notebook contracts**

Run: `uv run pytest tests/datasets/test_consumer_resolution_inventory.py tests/test_dag_catalog_conf.py tests/scenarios tests/lakehouse -q`

Expected: all pass and inventory is empty.

Run: `make spark-apps-test`

Expected: both Maven projects pass.

- [ ] **Step 5: Commit consumer migration**

Stage only the enumerated runtime and test paths, inspect `git diff --cached --name-only`, then:

```bash
git commit -m "feat(datasets): resolve verified consumer generations (#81)"
```

---

### Task 11: Synchronize canonical, site, wiki, and operational documentation

**Files:**
- Modify: `docs/datasets.md`
- Modify: `README.md`
- Modify: `docs/getting-started.md`
- Modify: `docs/go-live.md`
- Modify: `docs/CHANGELOG.md`
- Modify: affected scenario/app README and canonical notebook documentation sources
- Modify: `tests/test_docs_content_contract.py`
- Modify: `scripts/docs/test_check_docs.py`

**Interfaces:**
- Consumes: final CLI modes, pointer/manifests, resolver service, live command sequence
- Produces: one canonical lock-enforcement/runbook contract projected to repo/MkDocs/wiki

- [ ] **Step 1: Write RED docs contracts**

```python
def test_dataset_docs_describe_verified_publication_and_recovery():
    text = DATASETS_DOC.read_text()
    for phrase in (
        "immutable generation", "active pointer", "--verify-only", "--refresh",
        "--rollback-manifest", "DATASET_SCALE", "runtime mismatch never updates the registry",
    ):
        assert phrase in text
    assert "enforcement is pending" not in text
```

Assert current commands, default behavior, refresh/force alias, rollback restrictions, legacy migration, retained history/no automatic GC, corruption/concurrency diagnostics, expected scale, resolver service, trust boundary, intentional #80 lock refresh, and no `make up` acquisition.

- [ ] **Step 2: Run RED**

Run: `uv run pytest tests/test_docs_content_contract.py scripts/docs/test_check_docs.py -q`

Expected: failures identify stale pending/existence-only/flat-path claims.

- [ ] **Step 3: Update only canonical sources and changelog**

Document the exact verified workflow:

```bash
make datasets SCALE=small
uv run python scripts/download_datasets.py --scale small --verify-only
uv run python scripts/download_datasets.py --scale small --only movielens --refresh
uv run python scripts/download_datasets.py --scale small --only movielens --rollback-manifest <64-hex-digest>
```

State that rollback requires a digest produced by `--dry-run`/history reporting and a current matching selected plan. Keep runtime mismatch separate from the reviewed #80 audit/edit workflow.

- [ ] **Step 4: Run all three-surface gates**

Run: `uv run pytest tests/test_docs_content_contract.py scripts/docs/test_check_docs.py -q`

Expected: all pass.

Run: `make docs-check && make docs-wiki`

Expected: strict MkDocs and wiki synchronization pass with no drift.

- [ ] **Step 5: Commit documentation**

Stage exact canonical/test files and commit:

```bash
git commit -m "docs: explain verified dataset publication (#81)"
```

---

### Task 12: Run the complete offline matrix and two-stage final review

**Files:**
- Modify only files required by confirmed review findings
- Create scratch only: `.superpowers/sdd/issue-81-final-report.md`

**Interfaces:**
- Consumes: Tasks 1-11 complete branch
- Produces: clean offline verification evidence and reviewer-ready commit range

- [ ] **Step 1: Run focused and complete tests**

Run: `uv run pytest tests/datasets tests/test_dag_catalog_conf.py tests/scenarios tests/lakehouse tests/test_docs_content_contract.py -q`

Expected: all focused tests pass.

Run: `make lint && make test && make verify && make docs-check && make docs-wiki && git diff --check`

Expected: every command exits 0.

- [ ] **Step 2: Verify immutable boundaries**

Run exact SHA, gitlink, forbidden-path, root-lock, registry/evidence-parity, legacy-path inventory, and branch-scope checks. Expected values are the Global Constraints above; `git diff origin/develop...HEAD -- infra datasets/registry.yaml uv.lock` must show no unintended change.

- [ ] **Step 3: Dispatch independent spec-compliance review**

Reviewer compares every #81 acceptance criterion and every section of the approved design against the full branch. Any Critical/Important/Minor finding returns to the owning task with a fresh RED/GREEN commit.

- [ ] **Step 4: Dispatch independent quality/security review**

Reviewer adversarially checks acquisition SSRF/ZIP policy, schema bounds, S3 CAS/ambiguous writes, lease ABA, crash recovery, pointer/manifest integrity, consumer scale propagation, secrets, idempotence, and Atlas/protected boundaries. Fix every confirmed finding under TDD and repeat both reviews until both say Ready with zero findings.

- [ ] **Step 5: Record the clean offline checkpoint**

Append exact commands, counts, commit range, reviewer verdicts, and invariant hashes to the scratch report. Do not claim live completion yet.

---

### Task 13: Prove MinIO, resolver, acquisition, and TPC-H behavior live

**Files:**
- Create or modify: `tests/infra/test_dataset_lock_enforcement_live.py`
- Modify: `tests/infra/test_dataset_download_live.py`
- Modify: `docs/go-live.md` only if evidence identifiers must be recorded

**Interfaces:**
- Consumes: real Atlas MinIO, consumer resolver container, authoritative tiny HTTP artifacts, canonical TPC-H image
- Produces: durable live evidence for #81 closeout

- [ ] **Step 1: Write the live acceptance test cases**

```python
@pytest.mark.infra
def test_minio_pointer_cas_and_verified_idempotence(live_services):
    first = live_services.publish("movielens", "tiny")
    second = live_services.publish("movielens", "tiny")
    assert first.manifest_sha256 == second.manifest_sha256
    assert second.source_requests == second.puts == second.deletes == 0
    live_services.assert_pointer_cas_conflict(first.pointer_etag)


@pytest.mark.infra
def test_canonical_tiny_tpch_runs_without_network(live_services):
    result = live_services.publish("tpch", "tiny")
    assert len(result.objects) == 8
    assert all(item.verified for item in result.objects)
```

Add real corruption, refresh, rollback, stale-reader, lost-response, lease-ABA, container-resolution, and flat-path-negative cases using uniquely named publication IDs and cleanup limited to test-owned inactive objects.

- [ ] **Step 2: Validate environment without mutation**

Run Atlas env backfill, consumer compose validation, consumer doctor, stack start, and preflight using the current pin. Expected: all gates pass and resolver service is healthy from Airflow, JupyterHub, and Zeppelin containers.

- [ ] **Step 3: Prove real MinIO conditional semantics**

Run live tests for first `If-None-Match`, replacement `If-Match`, quoted opaque ETag propagation, 409/412 conflicts, lost-response reconciliation, lease create/takeover/renew/release, stale-owner failure, and active pointer exact-self reconciliation. Expected: MinIO matches the adapter contract; otherwise stop as unsupported without fallback.

- [ ] **Step 4: Publish and verify bounded HTTP datasets**

Publish selected tiny artifacts, verify raw/output/schema locks, run verify-only, corrupt one inactive candidate and prove isolation, corrupt one active object and prove default fail/no mutation, refresh to a new publication, and prove the previous manifest still resolves. Expected: all outcomes match the state machine and no registry write occurs.

- [ ] **Step 5: Generate canonical tiny TPC-H offline**

Build/inspect linux/amd64 image, run with `--network=none`, verify eight exact outputs/metadata/schemas, publish, resolve, and repeat. Expected: second run performs no source/generation/upload/delete; all hashes match registry.

- [ ] **Step 6: Prove consumers and rollback**

Resolve from each service container, run bounded notebook/app smoke paths against immutable URIs, switch a dataset scale, prove an already-resolved reader retains its old generation, then rollback to a still-current-plan historical manifest. Expected: no mixed generation and no flat physical path.

- [ ] **Step 7: Teardown safely and record evidence**

Stop project containers while preserving volumes unless the runbook explicitly requests cold teardown. Record UTC timestamps, publication/manifest digests, pointer ETags, driver/run identifiers where applicable, test outputs, and negative error scans. Re-run offline gates and invariants after teardown.

- [ ] **Step 8: Commit live tests/evidence docs**

Stage only live tests and required canonical evidence prose, then:

```bash
git commit -m "test(datasets): prove live lock enforcement (#81)"
```

---

### Task 14: Promote through Gitflow, close issues, and clean branches

**Files:**
- No product-file changes unless a final CI finding requires TDD
- Update ignored scratch ledger `.superpowers/sdd/backlog-goal-2026-08-10.md`

**Interfaces:**
- Consumes: final clean branch, live evidence, Project #7 access
- Produces: merged feature/develop/main/back-sync PRs, closed #81 and eligible parent #79, clean branch/PR state

- [ ] **Step 1: Refresh GitHub/project authorization and issue state**

Run `gh auth status`; require `repo`, `workflow`, `read:project`, and `project` scopes before board mutation. Confirm #81 is on Project #7 or add it, set In Progress during promotion, and verify no conflicting open PR.

- [ ] **Step 2: Push feature branch and create PR to `develop`**

Push `codex/81-dataset-lock-enforcement`, create a ready PR with issue link, design/plan, commit summary, offline/live evidence, risks, and `Closes #81` only if repository policy closes on develop; otherwise reserve closure for main promotion. Wait for every required check, inspect failures, fix under TDD, and merge only when green.

- [ ] **Step 3: Promote `develop` to `main`**

Create the Gitflow promotion PR, require all duplicate CI runs to pass, inspect exact commit/tree scope, and merge. Record feature/develop/main commit SHAs and PR URLs.

- [ ] **Step 4: Back-sync main ancestry to develop when required**

If the main merge commit is not an ancestor of `develop`, create `main -> develop` back-sync PR. Require `files: []`, green checks, and merge. Verify `origin/main` is an ancestor of `origin/develop` and both trees match.

- [ ] **Step 5: Close issue/parent and update Project #7**

Post #81 closeout with exact PRs, commits, live evidence, test counts, manifest/publication identifiers, and invariant proof; close #81 and set Done. Re-read parent #79 and all children. Close/set Done only when no child or parent acceptance criterion remains; otherwise comment with the exact remaining child.

- [ ] **Step 6: Clean feature branches and dangling PRs**

Delete merged local/remote feature branches, close only superseded PRs created by this workflow, prune, and verify open PRs are intentional. Never delete unrelated user branches.

- [ ] **Step 7: Return local workspace to synchronized `develop`**

Expected final state:

```text
develop...origin/develop
?? docs/superpowers/plans/2026-07-21-atlas-submodule-modernization.md
```

Verify protected hash, root lock hash, clean Atlas gitlink, no open workflow PRs, and exact main/develop ancestry before marking the goal task complete.
