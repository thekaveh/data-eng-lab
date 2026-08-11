import hashlib
import importlib.util
from copy import deepcopy
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[2]
_spec = importlib.util.spec_from_file_location("schema", ROOT / "datasets" / "schema.py")
schema = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(schema)

FIXTURE = Path(__file__).parent / "fixtures" / "registry-v2-minimal.yaml"


def _valid_doc():
    return {
        "version": 1,
        "datasets": {
            "nyc_taxi": {
                "description": "d",
                "format": "parquet",
                "license": "public",
                "landing_prefix": "nyc_taxi",
                "fetch": {"kind": "http"},
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


def _v2() -> dict:
    return yaml.safe_load(FIXTURE.read_text(encoding="utf-8"))


def _get(doc: object, path: tuple[object, ...]) -> object:
    value = doc
    for part in path:
        value = value[part]  # type: ignore[index]
    return value


def _set(doc: object, path: tuple[object, ...], value: object) -> None:
    parent = _get(doc, path[:-1])
    parent[path[-1]] = value  # type: ignore[index]


def _delete(doc: object, path: tuple[object, ...]) -> None:
    parent = _get(doc, path[:-1])
    del parent[path[-1]]  # type: ignore[index]


def _refresh_fingerprint(doc: dict, dataset: str, schema_id: str) -> None:
    entry = doc["datasets"][dataset]["schemas"][schema_id]
    entry["fingerprint"] = schema.schema_fingerprint(entry)


def test_minimal_v2_registry_is_valid():
    assert schema.validate_registry_v2(_v2()) == []


def test_v2_fixture_contains_exact_label_derived_locks():
    doc = _v2()
    direct = doc["datasets"]["direct"]["artifacts"]["sample"]
    archive = doc["datasets"]["archive"]["artifacts"]["release"]
    locks = [
        ("direct-raw", direct["raw"]),
        ("archive-raw", archive["raw"]),
        ("archive-ratings", archive["outputs"][0]),
    ]
    scales = doc["datasets"]["generated"]["generator"]["scales"]
    for scale in ("tiny", "small", "medium"):
        outputs = scales[scale]["outputs"]
        assert [output["table"] for output in outputs] == [
            "customer",
            "lineitem",
            "nation",
            "orders",
            "part",
            "partsupp",
            "region",
            "supplier",
        ]
        locks.extend((f"tpch-{scale}-{output['table']}", output) for output in outputs)
    assert len(locks) == 27
    for label, lock in locks:
        assert lock["size_bytes"] == len(label.encode())
        assert lock["sha256"] == hashlib.sha256(label.encode()).hexdigest()


def test_v2_rejects_missing_provenance_field():
    doc = _v2()
    del doc["datasets"]["direct"]["provenance"]["publisher"]
    assert "datasets.direct.provenance: missing 'publisher'" in schema.validate_registry_v2(doc)


def test_v2_rejects_malformed_artifact_lock():
    doc = _v2()
    artifact = doc["datasets"]["direct"]["artifacts"]["sample"]
    artifact["raw"]["size_bytes"] = 0
    artifact["raw"]["sha256"] = "ABC"
    errors = schema.validate_registry_v2(doc)
    assert "datasets.direct.artifacts.sample.raw.size_bytes: must be a positive integer" in errors
    assert "datasets.direct.artifacts.sample.raw.sha256: must be 64 lowercase hexadecimal characters" in errors


def test_v2_rejects_unknown_scale_artifact_reference():
    doc = _v2()
    doc["datasets"]["direct"]["scales"]["tiny"]["artifacts"] = ["missing"]
    assert "datasets.direct.scales.tiny.artifacts[0]: unknown artifact 'missing'" in schema.validate_registry_v2(doc)


def test_v2_rejects_schema_fingerprint_mismatch():
    doc = _v2()
    doc["datasets"]["direct"]["schemas"]["sample"]["fingerprint"] = "0" * 64
    assert "datasets.direct.schemas.sample.fingerprint: does not match canonical schema" in schema.validate_registry_v2(
        doc
    )


def test_v2_rejects_active_machine_local_values():
    doc = _v2()
    doc["datasets"]["direct"]["provenance"]["homepage"] = "http://localhost:9000/source"
    assert "datasets.direct.provenance.homepage: must be an authoritative HTTPS URL" in schema.validate_registry_v2(doc)


@pytest.mark.parametrize("root", [None, [], "registry", 2])
def test_v2_rejects_non_mapping_roots(root: object):
    assert schema.validate_registry_v2(root) == ["registry: must be a mapping"]


def test_v2_rejects_https_loopback_even_with_valid_scheme():
    doc = _v2()
    doc["datasets"]["direct"]["provenance"]["homepage"] = "https://127.0.0.1/source"
    assert "datasets.direct.provenance.homepage: must be an authoritative HTTPS URL" in schema.validate_registry_v2(doc)


@pytest.mark.parametrize(
    ("path", "error_path", "field"),
    [
        ((), "registry", "version"),
        ((), "registry", "lock"),
        ((), "registry", "datasets"),
        (("lock",), "registry.lock", "algorithm"),
        (("lock",), "registry.lock", "source_drift"),
        (("lock",), "registry.lock", "object_drift"),
        (("lock",), "registry.lock", "schema_fingerprint"),
        (("lock",), "registry.lock", "update_policy"),
        (("datasets", "direct"), "datasets.direct", "description"),
        (("datasets", "direct"), "datasets.direct", "format"),
        (("datasets", "direct"), "datasets.direct", "license"),
        (("datasets", "direct"), "datasets.direct", "landing_prefix"),
        (("datasets", "direct"), "datasets.direct", "fetch"),
        (("datasets", "direct"), "datasets.direct", "provenance"),
        (("datasets", "direct"), "datasets.direct", "schemas"),
        (("datasets", "direct"), "datasets.direct", "artifacts"),
        (("datasets", "direct"), "datasets.direct", "scales"),
        (("datasets", "generated"), "datasets.generated", "generator"),
        (("datasets", "direct", "fetch"), "datasets.direct.fetch", "kind"),
        (("datasets", "direct", "fetch"), "datasets.direct.fetch", "unzip"),
        (("datasets", "direct", "provenance"), "datasets.direct.provenance", "publisher"),
        (("datasets", "direct", "provenance"), "datasets.direct.provenance", "homepage"),
        (("datasets", "direct", "provenance"), "datasets.direct.provenance", "license_name"),
        (("datasets", "direct", "provenance"), "datasets.direct.provenance", "license_url"),
        (("datasets", "direct", "provenance"), "datasets.direct.provenance", "attribution"),
        (("datasets", "direct", "provenance"), "datasets.direct.provenance", "source_stability"),
        (("datasets", "direct", "provenance"), "datasets.direct.provenance", "update_policy"),
        (("datasets", "direct", "schemas", "sample"), "datasets.direct.schemas.sample", "format"),
        (("datasets", "direct", "schemas", "sample"), "datasets.direct.schemas.sample", "mode"),
        (("datasets", "direct", "schemas", "sample"), "datasets.direct.schemas.sample", "fields"),
        (("datasets", "direct", "schemas", "sample"), "datasets.direct.schemas.sample", "options"),
        (("datasets", "direct", "schemas", "sample"), "datasets.direct.schemas.sample", "fingerprint"),
        (("datasets", "direct", "schemas", "sample", "fields", 0), "datasets.direct.schemas.sample.fields[0]", "name"),
        (
            ("datasets", "direct", "schemas", "sample", "fields", 0),
            "datasets.direct.schemas.sample.fields[0]",
            "logical_type",
        ),
        (
            ("datasets", "direct", "schemas", "sample", "fields", 0),
            "datasets.direct.schemas.sample.fields[0]",
            "nullable",
        ),
        (("datasets", "direct", "artifacts", "sample"), "datasets.direct.artifacts.sample", "url"),
        (("datasets", "direct", "artifacts", "sample"), "datasets.direct.artifacts.sample", "version"),
        (("datasets", "direct", "artifacts", "sample"), "datasets.direct.artifacts.sample", "stability"),
        (("datasets", "direct", "artifacts", "sample"), "datasets.direct.artifacts.sample", "evidence"),
        (("datasets", "direct", "artifacts", "sample"), "datasets.direct.artifacts.sample", "raw"),
        (("datasets", "direct", "artifacts", "sample"), "datasets.direct.artifacts.sample", "outputs"),
        (("datasets", "direct", "artifacts", "sample", "version"), "datasets.direct.artifacts.sample.version", "kind"),
        (("datasets", "direct", "artifacts", "sample", "version"), "datasets.direct.artifacts.sample.version", "value"),
        (("datasets", "direct", "artifacts", "sample", "raw"), "datasets.direct.artifacts.sample.raw", "name"),
        (("datasets", "direct", "artifacts", "sample", "raw"), "datasets.direct.artifacts.sample.raw", "size_bytes"),
        (("datasets", "direct", "artifacts", "sample", "raw"), "datasets.direct.artifacts.sample.raw", "sha256"),
        (
            ("datasets", "direct", "artifacts", "sample", "outputs", 0),
            "datasets.direct.artifacts.sample.outputs[0]",
            "object_name",
        ),
        (
            ("datasets", "direct", "artifacts", "sample", "outputs", 0),
            "datasets.direct.artifacts.sample.outputs[0]",
            "size_bytes",
        ),
        (
            ("datasets", "direct", "artifacts", "sample", "outputs", 0),
            "datasets.direct.artifacts.sample.outputs[0]",
            "sha256",
        ),
        (
            ("datasets", "direct", "artifacts", "sample", "outputs", 0),
            "datasets.direct.artifacts.sample.outputs[0]",
            "schema",
        ),
        (
            ("datasets", "direct", "artifacts", "sample", "outputs", 0),
            "datasets.direct.artifacts.sample.outputs[0]",
            "raw_identity",
        ),
        (
            ("datasets", "archive", "artifacts", "release", "outputs", 0),
            "datasets.archive.artifacts.release.outputs[0]",
            "member_path",
        ),
        (("datasets", "direct", "scales", "tiny"), "datasets.direct.scales.tiny", "artifacts"),
        (("datasets", "generated", "generator"), "datasets.generated.generator", "engine"),
        (("datasets", "generated", "generator"), "datasets.generated.generator", "extension"),
        (("datasets", "generated", "generator"), "datasets.generated.generator", "environment"),
        (("datasets", "generated", "generator"), "datasets.generated.generator", "command"),
        (("datasets", "generated", "generator"), "datasets.generated.generator", "export"),
        (("datasets", "generated", "generator"), "datasets.generated.generator", "scales"),
    ],
)
def test_v2_rejects_missing_required_fields(path, error_path, field):
    doc = _v2()
    del _get(doc, path)[field]
    assert f"{error_path}: missing '{field}'" in schema.validate_registry_v2(doc)


@pytest.mark.parametrize(
    ("path", "field"),
    [
        ((), "root_extra"),
        (("lock",), "extra"),
        (("datasets", "direct"), "extra"),
        (("datasets", "direct", "fetch"), "extra"),
        (("datasets", "direct", "provenance"), "extra"),
        (("datasets", "direct", "schemas", "sample"), "extra"),
        (("datasets", "direct", "schemas", "sample", "fields", 0), "extra"),
        (("datasets", "direct", "schemas", "sample", "options"), "extra"),
        (("datasets", "direct", "artifacts", "sample"), "extra"),
        (("datasets", "direct", "artifacts", "sample", "version"), "extra"),
        (("datasets", "archive", "artifacts", "release", "evidence"), "extra"),
        (("datasets", "direct", "artifacts", "sample", "raw"), "extra"),
        (("datasets", "direct", "artifacts", "sample", "outputs", 0), "member_path"),
        (("datasets", "archive", "artifacts", "release", "outputs", 0), "raw_identity"),
        (("datasets", "direct", "scales", "tiny"), "extra"),
        (("datasets", "generated", "generator"), "extra"),
        (("datasets", "generated", "generator", "engine"), "extra"),
        (("datasets", "generated", "generator", "extension"), "extra"),
        (("datasets", "generated", "generator", "environment"), "extra"),
        (("datasets", "generated", "generator", "command"), "extra"),
        (("datasets", "generated", "generator", "export"), "extra"),
        (("datasets", "generated", "generator", "export", "order_by"), "extra"),
        (("datasets", "generated", "generator", "scales", "tiny"), "extra"),
        (("datasets", "generated", "generator", "scales", "tiny", "outputs", 0), "extra"),
    ],
)
def test_v2_rejects_unknown_fields(path, field):
    doc = _v2()
    mapping = _get(doc, path)
    mapping[field] = "unexpected"
    rendered = "registry" if not path else ".".join(str(part) for part in path)
    if rendered == "lock":
        rendered = "registry.lock"
    rendered = rendered.replace(".0", "[0]")
    assert f"{rendered}: unknown field '{field}'" in schema.validate_registry_v2(doc)


@pytest.mark.parametrize(
    ("path", "field"),
    [
        (("datasets", "generated", "generator", "engine"), "name"),
        (("datasets", "generated", "generator", "engine"), "version"),
        (("datasets", "generated", "generator", "engine"), "wheel_sha256"),
        (("datasets", "generated", "generator", "extension"), "name"),
        (("datasets", "generated", "generator", "extension"), "version_relation"),
        (("datasets", "generated", "generator", "extension"), "repository_url"),
        (("datasets", "generated", "generator", "extension"), "sha256"),
        (("datasets", "generated", "generator", "environment"), "image"),
        (("datasets", "generated", "generator", "environment"), "image_digest"),
        (("datasets", "generated", "generator", "environment"), "platform"),
        (("datasets", "generated", "generator", "environment"), "uv_lock_sha256"),
        (("datasets", "generated", "generator", "environment"), "locale"),
        (("datasets", "generated", "generator", "environment"), "timezone"),
        (("datasets", "generated", "generator", "environment"), "threads"),
        (("datasets", "generated", "generator", "environment"), "preserve_insertion_order"),
        (("datasets", "generated", "generator", "command"), "procedure"),
        (("datasets", "generated", "generator", "command"), "scale_parameter"),
        (("datasets", "generated", "generator", "export"), "format"),
        (("datasets", "generated", "generator", "export"), "compression"),
        (("datasets", "generated", "generator", "export"), "row_group_size"),
        (("datasets", "generated", "generator", "export"), "order_by"),
        (("datasets", "generated", "generator", "scales", "tiny"), "scale_factor"),
        (("datasets", "generated", "generator", "scales", "tiny"), "outputs"),
        (("datasets", "generated", "generator", "scales", "tiny", "outputs", 0), "table"),
        (("datasets", "generated", "generator", "scales", "tiny", "outputs", 0), "object_name"),
        (("datasets", "generated", "generator", "scales", "tiny", "outputs", 0), "size_bytes"),
        (("datasets", "generated", "generator", "scales", "tiny", "outputs", 0), "sha256"),
        (("datasets", "generated", "generator", "scales", "tiny", "outputs", 0), "schema"),
    ],
)
def test_v2_rejects_missing_required_tpch_fields(path, field):
    doc = _v2()
    del _get(doc, path)[field]
    rendered = ".".join(str(part) for part in path).replace(".0", "[0]")
    assert f"{rendered}: missing '{field}'" in schema.validate_registry_v2(doc)


@pytest.mark.parametrize(
    ("path", "value", "error_path"),
    [
        (("lock",), [], "registry.lock"),
        (("datasets",), [], "registry.datasets"),
        (("datasets", "direct"), [], "datasets.direct"),
        (("datasets", "direct", "fetch"), [], "datasets.direct.fetch"),
        (("datasets", "direct", "provenance"), [], "datasets.direct.provenance"),
        (("datasets", "direct", "schemas"), [], "datasets.direct.schemas"),
        (("datasets", "direct", "schemas", "sample"), [], "datasets.direct.schemas.sample"),
        (("datasets", "direct", "schemas", "sample", "fields"), {}, "datasets.direct.schemas.sample.fields"),
        (("datasets", "direct", "schemas", "sample", "fields", 0), [], "datasets.direct.schemas.sample.fields[0]"),
        (("datasets", "direct", "schemas", "sample", "options"), [], "datasets.direct.schemas.sample.options"),
        (("datasets", "direct", "artifacts"), [], "datasets.direct.artifacts"),
        (("datasets", "direct", "artifacts", "sample"), [], "datasets.direct.artifacts.sample"),
        (("datasets", "direct", "artifacts", "sample", "version"), [], "datasets.direct.artifacts.sample.version"),
        (("datasets", "direct", "artifacts", "sample", "evidence"), [], "datasets.direct.artifacts.sample.evidence"),
        (("datasets", "direct", "artifacts", "sample", "raw"), [], "datasets.direct.artifacts.sample.raw"),
        (("datasets", "direct", "artifacts", "sample", "outputs"), {}, "datasets.direct.artifacts.sample.outputs"),
        (
            ("datasets", "direct", "artifacts", "sample", "outputs", 0),
            [],
            "datasets.direct.artifacts.sample.outputs[0]",
        ),
        (("datasets", "direct", "scales"), [], "datasets.direct.scales"),
        (("datasets", "direct", "scales", "tiny"), [], "datasets.direct.scales.tiny"),
        (("datasets", "direct", "scales", "tiny", "artifacts"), {}, "datasets.direct.scales.tiny.artifacts"),
        (("datasets", "generated", "generator"), [], "datasets.generated.generator"),
        (("datasets", "generated", "generator", "engine"), [], "datasets.generated.generator.engine"),
        (("datasets", "generated", "generator", "extension"), [], "datasets.generated.generator.extension"),
        (("datasets", "generated", "generator", "environment"), [], "datasets.generated.generator.environment"),
        (("datasets", "generated", "generator", "command"), [], "datasets.generated.generator.command"),
        (("datasets", "generated", "generator", "export"), [], "datasets.generated.generator.export"),
        (
            ("datasets", "generated", "generator", "export", "order_by"),
            [],
            "datasets.generated.generator.export.order_by",
        ),
        (("datasets", "generated", "generator", "scales"), [], "datasets.generated.generator.scales"),
        (("datasets", "generated", "generator", "scales", "tiny"), [], "datasets.generated.generator.scales.tiny"),
        (
            ("datasets", "generated", "generator", "scales", "tiny", "outputs"),
            {},
            "datasets.generated.generator.scales.tiny.outputs",
        ),
        (
            ("datasets", "generated", "generator", "scales", "tiny", "outputs", 0),
            [],
            "datasets.generated.generator.scales.tiny.outputs[0]",
        ),
    ],
)
def test_v2_rejects_wrong_container_types_without_crashing(path, value, error_path):
    doc = _v2()
    _set(doc, path, value)
    assert any(error.startswith(f"{error_path}:") for error in schema.validate_registry_v2(doc))


@pytest.mark.parametrize(
    ("path", "error_path"),
    [
        (("datasets", "direct", "provenance", "source_stability"), "datasets.direct.provenance.source_stability"),
        (("datasets", "direct", "schemas", "sample", "format"), "datasets.direct.schemas.sample.format"),
        (("datasets", "direct", "schemas", "sample", "mode"), "datasets.direct.schemas.sample.mode"),
        (
            ("datasets", "direct", "artifacts", "sample", "version", "kind"),
            "datasets.direct.artifacts.sample.version.kind",
        ),
        (("datasets", "direct", "artifacts", "sample", "stability"), "datasets.direct.artifacts.sample.stability"),
        (
            ("datasets", "direct", "artifacts", "sample", "outputs", 0, "schema"),
            "datasets.direct.artifacts.sample.outputs[0].schema",
        ),
        (("datasets", "direct", "scales", "tiny", "artifacts", 0), "datasets.direct.scales.tiny.artifacts[0]"),
        (
            ("datasets", "generated", "generator", "scales", "tiny", "outputs", 0, "schema"),
            "datasets.generated.generator.scales.tiny.outputs[0].schema",
        ),
    ],
)
def test_v2_rejects_unhashable_scalar_values_without_crashing(path, error_path):
    doc = _v2()
    _set(doc, path, [])
    assert any(error.startswith(f"{error_path}:") for error in schema.validate_registry_v2(doc))


@pytest.mark.parametrize("field", ["etag", "last_modified", "observed_at"])
def test_v2_rejects_empty_optional_evidence_strings(field):
    doc = _v2()
    doc["datasets"]["archive"]["artifacts"]["release"]["evidence"][field] = ""
    assert any(
        error.startswith(f"datasets.archive.artifacts.release.evidence.{field}:")
        for error in schema.validate_registry_v2(doc)
    )


def test_v2_accepts_stricter_artifact_stability():
    doc = _v2()
    doc["datasets"]["archive"]["artifacts"]["release"]["stability"] = "immutable"
    assert schema.validate_registry_v2(doc) == []


@pytest.mark.parametrize(
    ("url",),
    [
        ("http://example.com/source",),
        ("https://localhost/source",),
        ("https://build.localhost/source",),
        ("https://host.local/source",),
        ("https://127.0.0.1/source",),
        ("https://169.254.1.1/source",),
        ("https://10.0.0.1/source",),
        ("https://172.16.0.1/source",),
        ("https://192.168.1.1/source",),
        ("https://[::1]/source",),
        ("https://user:password@example.com/source",),
        ("https://example.com:443/source",),
        ("https://example.com:/source",),
        ("https://example.com/source#fragment",),
        ("https://bad_host.example/source",),
        ("https://bad..example/source",),
        ("https:///source",),
    ],
)
def test_v2_rejects_non_authoritative_urls(url):
    doc = _v2()
    doc["datasets"]["direct"]["artifacts"]["sample"]["url"] = url
    assert "datasets.direct.artifacts.sample.url: must be an authoritative HTTPS URL" in schema.validate_registry_v2(
        doc
    )


@pytest.mark.parametrize(
    ("path",),
    [
        (("datasets", "direct", "provenance", "homepage"),),
        (("datasets", "direct", "provenance", "license_url"),),
        (("datasets", "direct", "artifacts", "sample", "url"),),
        (("datasets", "generated", "generator", "extension", "repository_url"),),
    ],
)
def test_v2_applies_https_rule_to_every_url_field(path):
    doc = _v2()
    _set(doc, path, "http://example.com/source")
    rendered = ".".join(path)
    assert f"{rendered}: must be an authoritative HTTPS URL" in schema.validate_registry_v2(doc)


@pytest.mark.parametrize(
    ("path", "value", "error_path"),
    [
        (("version",), True, "registry.version"),
        (("version",), 1, "registry.version"),
        (("lock", "algorithm"), "md5", "registry.lock.algorithm"),
        (("lock", "source_drift"), "warn", "registry.lock.source_drift"),
        (("lock", "object_drift"), "warn", "registry.lock.object_drift"),
        (("lock", "schema_fingerprint"), "sha256", "registry.lock.schema_fingerprint"),
        (("lock", "update_policy"), "automatic", "registry.lock.update_policy"),
        (("datasets", "direct", "description"), "", "datasets.direct.description"),
        (("datasets", "direct", "format"), 1, "datasets.direct.format"),
        (("datasets", "direct", "license"), "", "datasets.direct.license"),
        (("datasets", "direct", "landing_prefix"), "../escape", "datasets.direct.landing_prefix"),
        (("datasets", "direct", "fetch", "kind"), "tpch", "datasets.direct.fetch.kind"),
        (("datasets", "direct", "fetch", "unzip"), 1, "datasets.direct.fetch.unzip"),
        (("datasets", "direct", "provenance", "publisher"), "", "datasets.direct.provenance.publisher"),
        (
            ("datasets", "direct", "provenance", "source_stability"),
            "stable",
            "datasets.direct.provenance.source_stability",
        ),
        (
            ("datasets", "direct", "provenance", "update_policy"),
            "automatic",
            "datasets.direct.provenance.update_policy",
        ),
        (("datasets", "direct", "schemas", "sample", "format"), "xml", "datasets.direct.schemas.sample.format"),
        (("datasets", "direct", "schemas", "sample", "mode"), "loose", "datasets.direct.schemas.sample.mode"),
        (
            ("datasets", "direct", "schemas", "sample", "fields", 0, "name"),
            "",
            "datasets.direct.schemas.sample.fields[0].name",
        ),
        (
            ("datasets", "direct", "schemas", "sample", "fields", 0, "logical_type"),
            "decimal(0,0)",
            "datasets.direct.schemas.sample.fields[0].logical_type",
        ),
        (
            ("datasets", "direct", "schemas", "sample", "fields", 0, "nullable"),
            0,
            "datasets.direct.schemas.sample.fields[0].nullable",
        ),
        (
            ("datasets", "direct", "artifacts", "sample", "version", "kind"),
            "tag",
            "datasets.direct.artifacts.sample.version.kind",
        ),
        (
            ("datasets", "direct", "artifacts", "sample", "version", "value"),
            "",
            "datasets.direct.artifacts.sample.version.value",
        ),
        (
            ("datasets", "archive", "artifacts", "release", "version", "value"),
            "2026-02-30",
            "datasets.archive.artifacts.release.version.value",
        ),
        (
            ("datasets", "direct", "artifacts", "sample", "stability"),
            "mutable",
            "datasets.direct.artifacts.sample.stability",
        ),
        (
            ("datasets", "archive", "artifacts", "release", "evidence", "observed_at"),
            "2026-08-10T12:00:00",
            "datasets.archive.artifacts.release.evidence.observed_at",
        ),
        (
            ("datasets", "direct", "artifacts", "sample", "raw", "name"),
            "../raw",
            "datasets.direct.artifacts.sample.raw.name",
        ),
        (
            ("datasets", "direct", "artifacts", "sample", "outputs", 0, "raw_identity"),
            False,
            "datasets.direct.artifacts.sample.outputs[0].raw_identity",
        ),
        (
            ("datasets", "archive", "artifacts", "release", "outputs", 0, "member_path"),
            "../escape.csv",
            "datasets.archive.artifacts.release.outputs[0].member_path",
        ),
        (("datasets", "direct", "scales", "tiny", "artifacts"), [], "datasets.direct.scales.tiny.artifacts"),
    ],
)
def test_v2_rejects_invalid_scalar_values(path, value, error_path):
    doc = _v2()
    _set(doc, path, value)
    assert any(error.startswith(f"{error_path}:") for error in schema.validate_registry_v2(doc))


@pytest.mark.parametrize(
    "identifier",
    ["UPPER", "-leading", "has.dot", "has space", ""],
)
@pytest.mark.parametrize(
    "path",
    [
        ("datasets",),
        ("datasets", "direct", "schemas"),
        ("datasets", "direct", "artifacts"),
    ],
)
def test_v2_rejects_invalid_identifiers(path, identifier):
    doc = _v2()
    mapping = _get(doc, path)
    key = next(iter(mapping))
    mapping[identifier] = mapping.pop(key)
    rendered = ".".join(path)
    assert any(error.startswith(f"{rendered}.{identifier}:") for error in schema.validate_registry_v2(doc))


@pytest.mark.parametrize(
    "logical_type",
    [
        "boolean",
        "int8",
        "int16",
        "int32",
        "int64",
        "uint8",
        "uint16",
        "uint32",
        "uint64",
        "float32",
        "float64",
        "date",
        "timestamp",
        "timestamp-tz",
        "string",
        "binary",
        "json",
        "decimal(1,0)",
        "decimal(38,38)",
    ],
)
def test_v2_accepts_every_logical_type(logical_type):
    doc = _v2()
    doc["datasets"]["direct"]["schemas"]["sample"]["fields"][0]["logical_type"] = logical_type
    _refresh_fingerprint(doc, "direct", "sample")
    assert schema.validate_registry_v2(doc) == []


def test_v2_accepts_boolean_nullable_true():
    doc = _v2()
    doc["datasets"]["direct"]["schemas"]["sample"]["fields"][0]["nullable"] = True
    _refresh_fingerprint(doc, "direct", "sample")
    assert schema.validate_registry_v2(doc) == []


@pytest.mark.parametrize(
    "logical_type",
    ["decimal(39,0)", "decimal(3,4)", "decimal(3,-1)", "decimal(3)", "varchar"],
)
def test_v2_rejects_invalid_logical_types(logical_type):
    doc = _v2()
    doc["datasets"]["direct"]["schemas"]["sample"]["fields"][0]["logical_type"] = logical_type
    _refresh_fingerprint(doc, "direct", "sample")
    assert any(
        error.startswith("datasets.direct.schemas.sample.fields[0].logical_type:")
        for error in schema.validate_registry_v2(doc)
    )


@pytest.mark.parametrize(
    ("format_name", "mode", "fields", "options"),
    [
        ("parquet", "exact", [{"name": "id", "logical_type": "int64", "nullable": False}], {}),
        (
            "csv",
            "exact",
            [{"name": "id", "logical_type": "int64", "nullable": False}],
            {"header": True, "delimiter": "|", "encoding": "utf-8"},
        ),
        (
            "jsonl-gzip",
            "minimum",
            [{"name": "id", "logical_type": "int64", "nullable": False}],
            {"record_shape": "object", "compression": "gzip", "encoding": "utf-8"},
        ),
        (
            "xlsx",
            "exact",
            [{"name": "id", "logical_type": "int64", "nullable": False}],
            {"sheets": ["Sheet1", "Sheet2"], "header_row": 1},
        ),
        ("text", "exact", [], {"encoding": "utf-8"}),
    ],
)
def test_v2_accepts_every_schema_format_options_shape(format_name, mode, fields, options):
    doc = _v2()
    entry = doc["datasets"]["direct"]["schemas"]["sample"]
    entry.update(format=format_name, mode=mode, fields=fields, options=options)
    _refresh_fingerprint(doc, "direct", "sample")
    assert schema.validate_registry_v2(doc) == []


@pytest.mark.parametrize(
    ("format_name", "fields", "options", "error_suffix"),
    [
        ("parquet", [], {}, "fields"),
        ("parquet", [{"name": "id", "logical_type": "int64", "nullable": False}], {"extra": True}, "options"),
        (
            "csv",
            [{"name": "id", "logical_type": "int64", "nullable": False}],
            {"header": 1, "delimiter": ",", "encoding": "utf-8"},
            "options.header",
        ),
        (
            "csv",
            [{"name": "id", "logical_type": "int64", "nullable": False}],
            {"header": True, "delimiter": "::", "encoding": "utf-8"},
            "options.delimiter",
        ),
        (
            "jsonl-gzip",
            [{"name": "id", "logical_type": "int64", "nullable": False}],
            {"record_shape": "array", "compression": "gzip", "encoding": "utf-8"},
            "options.record_shape",
        ),
        (
            "xlsx",
            [{"name": "id", "logical_type": "int64", "nullable": False}],
            {"sheets": ["Sheet1", "Sheet1"], "header_row": 1},
            "options.sheets[1]",
        ),
        (
            "xlsx",
            [{"name": "id", "logical_type": "int64", "nullable": False}],
            {"sheets": ["Sheet1"], "header_row": 0},
            "options.header_row",
        ),
        ("text", [{"name": "id", "logical_type": "int64", "nullable": False}], {"encoding": "utf-8"}, "fields"),
    ],
)
def test_v2_rejects_invalid_schema_options(format_name, fields, options, error_suffix):
    doc = _v2()
    entry = doc["datasets"]["direct"]["schemas"]["sample"]
    entry.update(format=format_name, fields=fields, options=options)
    _refresh_fingerprint(doc, "direct", "sample")
    assert any(
        error.startswith(f"datasets.direct.schemas.sample.{error_suffix}:")
        for error in schema.validate_registry_v2(doc)
    )


def test_v2_rejects_duplicate_schema_field_names():
    doc = _v2()
    fields = doc["datasets"]["direct"]["schemas"]["sample"]["fields"]
    fields.append(deepcopy(fields[0]))
    _refresh_fingerprint(doc, "direct", "sample")
    assert "datasets.direct.schemas.sample.fields[1].name: duplicate field name 'id'" in schema.validate_registry_v2(
        doc
    )


def test_v2_rejects_duplicate_http_objects_and_artifact_references():
    doc = _v2()
    artifact = doc["datasets"]["direct"]["artifacts"]["sample"]
    artifact["outputs"].append(deepcopy(artifact["outputs"][0]))
    doc["datasets"]["direct"]["scales"]["tiny"]["artifacts"].append("sample")
    errors = schema.validate_registry_v2(doc)
    assert "datasets.direct.artifacts.sample.outputs[1].object_name: duplicate object name 'direct.parquet'" in errors
    assert "datasets.direct.scales.tiny.artifacts[1]: duplicate artifact 'sample'" in errors


@pytest.mark.parametrize("field", ["object_name", "size_bytes", "sha256"])
def test_v2_rejects_direct_raw_output_disagreement(field):
    doc = _v2()
    output = doc["datasets"]["direct"]["artifacts"]["sample"]["outputs"][0]
    output[field] = "other.parquet" if field == "object_name" else (11 if field == "size_bytes" else "0" * 64)
    assert any(
        error.startswith(f"datasets.direct.artifacts.sample.outputs[0].{field}:") and "must match raw" in error
        for error in schema.validate_registry_v2(doc)
    )


def test_v2_rejects_archive_output_without_member_path():
    doc = _v2()
    del doc["datasets"]["archive"]["artifacts"]["release"]["outputs"][0]["member_path"]
    assert "datasets.archive.artifacts.release.outputs[0]: missing 'member_path'" in schema.validate_registry_v2(doc)


@pytest.mark.parametrize(
    ("path", "value", "message"),
    [
        (("datasets", "direct", "artifacts", "sample", "outputs", 0, "schema"), "missing", "unknown schema 'missing'"),
        (
            ("datasets", "generated", "generator", "scales", "tiny", "outputs", 0, "schema"),
            "missing",
            "unknown schema 'missing'",
        ),
        (
            ("datasets", "generated", "generator", "scales", "tiny", "outputs", 0, "table"),
            "missing",
            "unknown table 'missing'",
        ),
    ],
)
def test_v2_rejects_unknown_output_references(path, value, message):
    doc = _v2()
    _set(doc, path, value)
    rendered = ".".join(str(part) for part in path).replace(".0", "[0]")
    assert f"{rendered}: {message}" in schema.validate_registry_v2(doc)


@pytest.mark.parametrize(
    ("path", "value"),
    [
        (("datasets", "generated", "generator", "engine", "name"), "sqlite"),
        (("datasets", "generated", "generator", "engine", "version"), "1.5.3"),
        (("datasets", "generated", "generator", "engine", "wheel_sha256"), "0" * 64),
        (("datasets", "generated", "generator", "extension", "name"), "httpfs"),
        (("datasets", "generated", "generator", "extension", "version_relation"), "latest"),
        (("datasets", "generated", "generator", "extension", "repository_url"), "https://example.com/tpch.gz"),
        (("datasets", "generated", "generator", "extension", "sha256"), "0" * 64),
        (("datasets", "generated", "generator", "environment", "image"), "python:latest"),
        (("datasets", "generated", "generator", "environment", "image_digest"), "sha256:" + "0" * 64),
        (("datasets", "generated", "generator", "environment", "platform"), "darwin/arm64"),
        (("datasets", "generated", "generator", "environment", "uv_lock_sha256"), "0" * 64),
        (("datasets", "generated", "generator", "environment", "locale"), "en_US.UTF-8"),
        (("datasets", "generated", "generator", "environment", "timezone"), "America/New_York"),
        (("datasets", "generated", "generator", "environment", "threads"), 2),
        (("datasets", "generated", "generator", "environment", "preserve_insertion_order"), False),
        (("datasets", "generated", "generator", "command", "procedure"), "generate"),
        (("datasets", "generated", "generator", "command", "scale_parameter"), "scale"),
        (("datasets", "generated", "generator", "export", "format"), "csv"),
        (("datasets", "generated", "generator", "export", "compression"), "snappy"),
        (("datasets", "generated", "generator", "export", "row_group_size"), 10),
    ],
)
def test_v2_rejects_tpch_constant_drift(path, value):
    doc = _v2()
    _set(doc, path, value)
    rendered = ".".join(path)
    assert any(error.startswith(f"{rendered}:") for error in schema.validate_registry_v2(doc))


def test_v2_rejects_tpch_scale_set_factor_order_and_completeness_drift():
    doc = _v2()
    scales = doc["datasets"]["generated"]["generator"]["scales"]
    scales["tiny"]["scale_factor"] = 1
    scales["tiny"]["outputs"][0], scales["tiny"]["outputs"][1] = (
        scales["tiny"]["outputs"][1],
        scales["tiny"]["outputs"][0],
    )
    del scales["medium"]
    errors = schema.validate_registry_v2(doc)
    assert any(error.startswith("datasets.generated.generator.scales.tiny.scale_factor:") for error in errors)
    assert any(error.startswith("datasets.generated.generator.scales.tiny.outputs[0].table:") for error in errors)
    assert "datasets.generated.generator.scales: missing 'medium'" in errors


def test_v2_rejects_tpch_duplicate_tables_schemas_and_object_names():
    doc = _v2()
    outputs = doc["datasets"]["generated"]["generator"]["scales"]["tiny"]["outputs"]
    outputs[1]["table"] = outputs[0]["table"]
    outputs[1]["schema"] = outputs[0]["schema"]
    outputs[1]["object_name"] = outputs[0]["object_name"]
    errors = schema.validate_registry_v2(doc)
    assert any("duplicate table" in error for error in errors)
    assert any("duplicate schema" in error for error in errors)
    assert any("duplicate object name" in error for error in errors)


def test_v2_rejects_tpch_order_by_drift_and_missing_table():
    doc = _v2()
    order_by = doc["datasets"]["generated"]["generator"]["export"]["order_by"]
    order_by["customer"] = ["wrong"]
    del order_by["supplier"]
    errors = schema.validate_registry_v2(doc)
    assert any(error.startswith("datasets.generated.generator.export.order_by.customer:") for error in errors)
    assert "datasets.generated.generator.export.order_by: missing 'supplier'" in errors


def test_v2_rejects_extra_or_missing_tpch_table_schemas():
    doc = _v2()
    schemas = doc["datasets"]["generated"]["schemas"]
    schemas["extra"] = deepcopy(schemas["customer"])
    del schemas["supplier"]
    errors = schema.validate_registry_v2(doc)
    assert "datasets.generated.schemas: missing 'supplier'" in errors
    assert "datasets.generated.schemas.extra: not a TPC-H table schema" in errors


def test_v2_rejects_http_tpch_shape_mixing_and_unknown_scales():
    doc = _v2()
    doc["datasets"]["direct"]["generator"] = {}
    doc["datasets"]["generated"]["artifacts"] = {}
    doc["datasets"]["generated"]["scales"] = {}
    doc["datasets"]["direct"]["scales"]["huge"] = {"artifacts": ["sample"]}
    errors = schema.validate_registry_v2(doc)
    assert "datasets.direct: unknown field 'generator'" in errors
    assert "datasets.generated: unknown field 'artifacts'" in errors
    assert "datasets.generated: unknown field 'scales'" in errors
    assert any(error.startswith("datasets.direct.scales.huge:") for error in errors)


def test_v2_aggregates_errors_in_deterministic_document_order():
    doc = _v2()
    doc["version"] = 1
    doc["lock"]["algorithm"] = "md5"
    doc["datasets"]["direct"]["description"] = ""
    errors = schema.validate_registry_v2(doc)
    assert errors == schema.validate_registry_v2(deepcopy(doc))
    assert errors.index("registry.version: must be integer 2") < errors.index(
        "registry.lock.algorithm: must be 'sha256'"
    )
    assert errors.index("registry.lock.algorithm: must be 'sha256'") < next(
        i for i, error in enumerate(errors) if error.startswith("datasets.direct.description:")
    )
