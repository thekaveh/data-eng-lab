"""Validation for datasets/registry.yaml. Pure functions, no I/O."""

from __future__ import annotations

import hashlib
import importlib.util
import ipaddress
import re
from datetime import date, datetime, timedelta
from pathlib import Path, PurePosixPath
from urllib.parse import unquote, urlsplit

if __package__:
    from .locking import (
        schema_fingerprint,
        validate_relative_path,
        validate_sha256,
        validate_size,
    )
else:
    _locking_path = Path(__file__).resolve().with_name("locking.py")
    _locking_module_name = (
        "_data_eng_lab_dataset_locking_" + hashlib.sha256(str(_locking_path).encode()).hexdigest()[:16]
    )
    _locking_spec = importlib.util.spec_from_file_location(_locking_module_name, _locking_path)
    if _locking_spec is None or _locking_spec.loader is None:
        raise ImportError(f"cannot load dataset locking helpers from {_locking_path}")
    _locking = importlib.util.module_from_spec(_locking_spec)
    _locking_spec.loader.exec_module(_locking)
    schema_fingerprint = _locking.schema_fingerprint
    validate_relative_path = _locking.validate_relative_path
    validate_sha256 = _locking.validate_sha256
    validate_size = _locking.validate_size

_IDENTIFIER_RE = re.compile(r"^[a-z0-9][a-z0-9_-]*$")
_DNS_LABEL_RE = re.compile(r"^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$")
_DECIMAL_RE = re.compile(r"^decimal\(([0-9]+),([0-9]+)\)$")
_PROVENANCE_LOCAL_PATTERNS = (
    re.compile(r"(?i)(?<![a-z0-9])(?:localhost|loopback|minio)(?![a-z0-9])"),
    re.compile(r"(?<![0-9])127(?:\.[0-9]{1,3}){3}(?![0-9])"),
    re.compile(r"(?i)(?<![0-9a-f:])::1(?![0-9a-f:])"),
    re.compile(r"(?i)\b[a-z][a-z0-9+.-]*://"),
    re.compile(r"(?i)(?:/private)?/tmp(?:/|\b)|/var/folders(?:/|\b)"),
    re.compile(r"(?i)(?<![\w.-])(?:[a-z0-9](?:[a-z0-9.-]*[a-z0-9])?|\[[0-9a-f:]+\]):[0-9]{1,5}(?![0-9])"),
    re.compile(r"(?i)(?<![\w-])(?:api[_-]?key|access[_-]?key|token|password|secret|key)\s*[:=]\s*\S"),
)
_LOGICAL_TYPES = frozenset(
    {
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
    }
)
_SCALES = ("tiny", "small", "medium")
_TABLES = (
    "customer",
    "lineitem",
    "nation",
    "orders",
    "part",
    "partsupp",
    "region",
    "supplier",
)
_ORDER_BY = {
    "customer": ["c_custkey"],
    "lineitem": ["l_orderkey", "l_linenumber"],
    "nation": ["n_nationkey"],
    "orders": ["o_orderkey"],
    "part": ["p_partkey"],
    "partsupp": ["ps_partkey", "ps_suppkey"],
    "region": ["r_regionkey"],
    "supplier": ["s_suppkey"],
}
_TPCH_CONSTANTS = {
    "engine": {
        "name": "duckdb",
        "version": "1.5.4",
        "wheel_sha256": "ccc7f2694d02b4763fee61021d45e12f7bc5743993686563957df0cef799fbae",
    },
    "extension": {
        "name": "tpch",
        "version_relation": "engine-version",
        "repository_url": ("https://extensions.duckdb.org/v1.5.4/linux_amd64/tpch.duckdb_extension.gz"),
        "sha256": "a6516e487106b4f95bd6d85da4364debdcb2db536d015784bc43209af6ed0125",
    },
    "environment": {
        "image": "python:3.11.13-slim-bookworm",
        "image_digest": ("sha256:cec9aa7aa96eea4fa036e9b82be1e6b325f2e3707f462d885868df51ec0a4b47"),
        "platform": "linux/amd64",
        "uv_lock_sha256": ("a376ce1b5bd5621290aaded68c22572690395419876da41814e28469bb4186b1"),
        "locale": "C.UTF-8",
        "timezone": "UTC",
        "threads": 1,
        "preserve_insertion_order": True,
    },
    "command": {"procedure": "dbgen", "scale_parameter": "sf"},
    "export": {"format": "parquet", "compression": "zstd", "row_group_size": 100000},
}


def _required(mapping: object, path: str, fields: tuple[str, ...]) -> tuple[dict[str, object], list[str]]:
    if not isinstance(mapping, dict):
        return {}, [f"{path}: must be a mapping"]
    return mapping, [f"{path}: missing '{field}'" for field in fields if field not in mapping]


def _unknown(mapping: dict[str, object], path: str, allowed: frozenset[str]) -> list[str]:
    return [f"{path}: unknown field '{field}'" for field in mapping if field not in allowed]


def _https(value: object, path: str) -> list[str]:
    error = [f"{path}: must be an authoritative HTTPS URL"]
    if not isinstance(value, str) or not value:
        return error
    try:
        parsed = urlsplit(value)
        port = parsed.port
    except ValueError:
        return error
    if (
        parsed.scheme != "https"
        or not parsed.hostname
        or parsed.username is not None
        or parsed.password is not None
        or port is not None
        or parsed.fragment
    ):
        return error
    authority = parsed.netloc.rsplit("@", 1)[-1]
    if (authority.startswith("[") and not authority.endswith("]")) or (
        not authority.startswith("[") and ":" in authority
    ):
        return error
    host = parsed.hostname.rstrip(".").lower()
    if host == "localhost" or host.endswith((".localhost", ".local")):
        return error
    try:
        address = ipaddress.ip_address(host)
    except ValueError:
        labels = host.split(".")
        legacy_ipv4 = len(labels) <= 4 and all(
            re.fullmatch(r"(?:[0-9]+|0x[0-9a-f]+)", label) is not None for label in labels
        )
        if (
            legacy_ipv4
            or "." not in host
            or len(host) > 253
            or any(_DNS_LABEL_RE.fullmatch(label) is None for label in labels)
        ):
            return error
    else:
        if not address.is_global:
            return error
    return []


def _authoritative_url_identity(value: object) -> tuple[str, str, int | None, str, str] | None:
    """Return the source identity while preserving server-significant path/query bytes."""
    if not isinstance(value, str) or not value:
        return None
    try:
        parsed = urlsplit(value)
        port = parsed.port
    except ValueError:
        return None
    if parsed.scheme.lower() != "https" or not parsed.hostname:
        return None
    host = parsed.hostname.rstrip(".").lower()
    canonical_port = None if port in (None, 443) else port
    return ("https", host, canonical_port, parsed.path, parsed.query)


def _nonempty_string(value: object, path: str) -> list[str]:
    if not isinstance(value, str) or not value.strip():
        return [f"{path}: must be a non-empty string"]
    return []


def _provenance_text(value: object, path: str) -> list[str]:
    errors = _nonempty_string(value, path)
    if errors or not isinstance(value, str):
        return errors
    if any(pattern.search(value) for pattern in _PROVENANCE_LOCAL_PATTERNS):
        errors.append(f"{path}: must not contain machine-local or credential-like values")
    return errors


def _exact(value: object, expected: object, path: str) -> list[str]:
    if type(value) is not type(expected) or value != expected:
        return [f"{path}: must be {expected!r}"]
    return []


def _identifier(value: object, path: str) -> list[str]:
    if not isinstance(value, str) or _IDENTIFIER_RE.fullmatch(value) is None:
        return [f"{path}: must be a lowercase identifier"]
    return []


def _validate_provenance(value: object, path: str) -> list[str]:
    fields = (
        "publisher",
        "homepage",
        "license_name",
        "license_url",
        "attribution",
        "source_stability",
        "update_policy",
    )
    provenance, errors = _required(value, path, fields)
    if not provenance:
        return errors
    errors += _unknown(provenance, path, frozenset(fields))
    for field in ("publisher", "license_name", "attribution"):
        if field in provenance:
            errors += _provenance_text(provenance[field], f"{path}.{field}")
    for field in ("homepage", "license_url"):
        if field in provenance:
            errors += _https(provenance[field], f"{path}.{field}")
    stability = provenance.get("source_stability")
    if "source_stability" in provenance and (
        not isinstance(stability, str) or stability not in {"mutable", "immutable"}
    ):
        errors.append(f"{path}.source_stability: must be 'mutable' or 'immutable'")
    if "update_policy" in provenance:
        errors += _exact(provenance["update_policy"], "reviewed-lock-update", f"{path}.update_policy")
    return errors


def _valid_logical_type(value: object) -> bool:
    if not isinstance(value, str):
        return False
    if value in _LOGICAL_TYPES:
        return True
    match = _DECIMAL_RE.fullmatch(value)
    if match is None:
        return False
    precision, scale = (int(part) for part in match.groups())
    return 1 <= precision <= 38 and 0 <= scale <= precision


def _validate_schema_options(format_name: object, options: object, path: str) -> list[str]:
    option_map, errors = _required(options, path, ())
    if not isinstance(options, dict):
        return errors
    required_by_format = {
        "parquet": (),
        "csv": ("header", "delimiter", "encoding"),
        "jsonl-gzip": ("record_shape", "compression", "encoding"),
        "xlsx": ("sheets", "header_row"),
        "text": ("encoding",),
    }
    required_fields = required_by_format.get(format_name, ()) if isinstance(format_name, str) else ()
    option_map, required_errors = _required(option_map, path, required_fields)
    errors += required_errors
    errors += _unknown(option_map, path, frozenset(required_fields))
    if format_name == "csv":
        if "header" in option_map and not isinstance(option_map["header"], bool):
            errors.append(f"{path}.header: must be a boolean")
        delimiter = option_map.get("delimiter")
        if "delimiter" in option_map and (not isinstance(delimiter, str) or len(delimiter) != 1):
            errors.append(f"{path}.delimiter: must be one character")
        if "encoding" in option_map:
            errors += _exact(option_map["encoding"], "utf-8", f"{path}.encoding")
    elif format_name == "jsonl-gzip":
        for field, expected in (
            ("record_shape", "object"),
            ("compression", "gzip"),
            ("encoding", "utf-8"),
        ):
            if field in option_map:
                errors += _exact(option_map[field], expected, f"{path}.{field}")
    elif format_name == "xlsx":
        sheets = option_map.get("sheets")
        if "sheets" in option_map:
            if not isinstance(sheets, list) or not sheets:
                errors.append(f"{path}.sheets: must be a non-empty list")
            else:
                seen: set[str] = set()
                for index, sheet in enumerate(sheets):
                    sheet_path = f"{path}.sheets[{index}]"
                    errors += _nonempty_string(sheet, sheet_path)
                    if isinstance(sheet, str) and sheet in seen:
                        errors.append(f"{sheet_path}: duplicate sheet '{sheet}'")
                    elif isinstance(sheet, str):
                        seen.add(sheet)
        if "header_row" in option_map:
            errors += validate_size(option_map["header_row"], f"{path}.header_row")
    elif format_name == "text" and "encoding" in option_map:
        errors += _exact(option_map["encoding"], "utf-8", f"{path}.encoding")
    return errors


def _validate_schemas(value: object, path: str) -> list[str]:
    schemas, errors = _required(value, path, ())
    if not isinstance(value, dict):
        return errors
    if not schemas:
        errors.append(f"{path}: must be a non-empty mapping")
    entry_fields = ("format", "mode", "fields", "options", "fingerprint")
    for schema_id, raw_entry in schemas.items():
        entry_path = f"{path}.{schema_id}"
        errors += _identifier(schema_id, entry_path)
        entry, entry_errors = _required(raw_entry, entry_path, entry_fields)
        errors += entry_errors
        if not isinstance(raw_entry, dict):
            continue
        errors += _unknown(entry, entry_path, frozenset(entry_fields))
        format_name = entry.get("format")
        if "format" in entry and (
            not isinstance(format_name, str) or format_name not in {"parquet", "csv", "jsonl-gzip", "xlsx", "text"}
        ):
            errors.append(f"{entry_path}.format: unsupported schema format")
        mode = entry.get("mode")
        if "mode" in entry and (not isinstance(mode, str) or mode not in {"exact", "minimum"}):
            errors.append(f"{entry_path}.mode: must be 'exact' or 'minimum'")
        fields = entry.get("fields")
        if "fields" in entry:
            if not isinstance(fields, list):
                errors.append(f"{entry_path}.fields: must be a list")
            else:
                if format_name == "text" and fields:
                    errors.append(f"{entry_path}.fields: must be empty for text schemas")
                elif format_name != "text" and not fields:
                    errors.append(f"{entry_path}.fields: must contain at least one field")
                seen_names: set[str] = set()
                field_keys = ("name", "logical_type", "nullable")
                for index, raw_field in enumerate(fields):
                    field_path = f"{entry_path}.fields[{index}]"
                    field, field_errors = _required(raw_field, field_path, field_keys)
                    errors += field_errors
                    if not isinstance(raw_field, dict):
                        continue
                    errors += _unknown(field, field_path, frozenset(field_keys))
                    if "name" in field:
                        errors += _nonempty_string(field["name"], f"{field_path}.name")
                        name = field["name"]
                        if isinstance(name, str) and name in seen_names:
                            errors.append(f"{field_path}.name: duplicate field name '{name}'")
                        elif isinstance(name, str):
                            seen_names.add(name)
                    if "logical_type" in field and not _valid_logical_type(field["logical_type"]):
                        errors.append(f"{field_path}.logical_type: unsupported logical type")
                    if "nullable" in field and not isinstance(field["nullable"], bool):
                        errors.append(f"{field_path}.nullable: must be a boolean")
        if "options" in entry:
            errors += _validate_schema_options(format_name, entry["options"], f"{entry_path}.options")
        if "fingerprint" in entry:
            fingerprint_path = f"{entry_path}.fingerprint"
            errors += validate_sha256(entry["fingerprint"], fingerprint_path)
            try:
                expected = schema_fingerprint(entry)
            except (TypeError, ValueError):
                errors.append(f"{fingerprint_path}: canonical schema is not serializable")
            else:
                if entry["fingerprint"] != expected:
                    errors.append(f"{fingerprint_path}: does not match canonical schema")
    return errors


def _validate_version(value: object, path: str) -> list[str]:
    version, errors = _required(value, path, ("kind", "value"))
    if not isinstance(value, dict):
        return errors
    errors += _unknown(version, path, frozenset({"kind", "value"}))
    kind = version.get("kind")
    if "kind" in version and (not isinstance(kind, str) or kind not in {"revision", "publication-date"}):
        errors.append(f"{path}.kind: must be 'revision' or 'publication-date'")
    if "value" in version:
        errors += _nonempty_string(version["value"], f"{path}.value")
        if kind == "publication-date" and isinstance(version["value"], str):
            published = version["value"]
            try:
                parsed = date.fromisoformat(published)
            except ValueError:
                parsed = None
            if parsed is None or parsed.isoformat() != published:
                errors.append(f"{path}.value: must be a strict ISO YYYY-MM-DD date")
    return errors


def _validate_evidence(value: object, path: str) -> list[str]:
    evidence, errors = _required(value, path, ())
    if not isinstance(value, dict):
        return errors
    errors += _unknown(evidence, path, frozenset({"etag", "last_modified", "observed_at"}))
    for field in ("etag", "last_modified", "observed_at"):
        if field in evidence:
            errors += _nonempty_string(evidence[field], f"{path}.{field}")
    observed = evidence.get("observed_at")
    if isinstance(observed, str) and observed:
        try:
            parsed = datetime.fromisoformat(observed.replace("Z", "+00:00"))
        except ValueError:
            parsed = None
        if parsed is None or "T" not in observed or parsed.tzinfo is None or parsed.utcoffset() != timedelta(0):
            errors.append(f"{path}.observed_at: must be an ISO-8601 UTC timestamp")
    return errors


def _validate_http_dataset(dataset: dict[str, object], path: str) -> list[str]:
    errors: list[str] = []
    fetch, fetch_errors = _required(dataset.get("fetch"), f"{path}.fetch", ("kind", "unzip"))
    errors += fetch_errors
    if isinstance(dataset.get("fetch"), dict):
        errors += _unknown(fetch, f"{path}.fetch", frozenset({"kind", "unzip"}))
        if "kind" in fetch:
            errors += _exact(fetch["kind"], "http", f"{path}.fetch.kind")
        if "unzip" in fetch and not isinstance(fetch["unzip"], bool):
            errors.append(f"{path}.fetch.unzip: must be a boolean")
    is_archive = fetch.get("unzip") is True
    schemas = dataset.get("schemas") if isinstance(dataset.get("schemas"), dict) else {}
    provenance = dataset.get("provenance") if isinstance(dataset.get("provenance"), dict) else {}
    source_stability = provenance.get("source_stability")
    artifacts = dataset.get("artifacts")
    if not isinstance(artifacts, dict):
        if "artifacts" in dataset:
            errors.append(f"{path}.artifacts: must be a mapping")
        artifacts = {}
    elif not artifacts:
        errors.append(f"{path}.artifacts: must be a non-empty mapping")
    artifact_fields = ("url", "version", "stability", "evidence", "raw", "outputs")
    allowed_artifact_fields = frozenset(artifact_fields + ("provenance",))
    for artifact_id, raw_artifact in artifacts.items():
        artifact_path = f"{path}.artifacts.{artifact_id}"
        errors += _identifier(artifact_id, artifact_path)
        artifact, artifact_errors = _required(raw_artifact, artifact_path, artifact_fields)
        errors += artifact_errors
        if not isinstance(raw_artifact, dict):
            continue
        errors += _unknown(artifact, artifact_path, allowed_artifact_fields)
        if "provenance" in artifact:
            errors += _validate_provenance(artifact["provenance"], f"{artifact_path}.provenance")
        authoritative_basename: str | None = None
        if "url" in artifact:
            errors += _https(artifact["url"], f"{artifact_path}.url")
            if isinstance(artifact["url"], str):
                authoritative_basename = unquote(urlsplit(artifact["url"]).path.rsplit("/", 1)[-1])
                if (
                    validate_relative_path(authoritative_basename, f"{artifact_path}.url")
                    or PurePosixPath(authoritative_basename).name != authoritative_basename
                ):
                    errors.append(f"{artifact_path}.url: decoded basename must be a safe artifact name")
        if "version" in artifact:
            errors += _validate_version(artifact["version"], f"{artifact_path}.version")
        stability = artifact.get("stability")
        if "stability" in artifact and (not isinstance(stability, str) or stability not in {"mutable", "immutable"}):
            errors.append(f"{artifact_path}.stability: must be 'mutable' or 'immutable'")
        if source_stability == "immutable" and stability == "mutable":
            errors.append(f"{artifact_path}.stability: cannot be weaker than immutable source stability")
        if "evidence" in artifact:
            errors += _validate_evidence(artifact["evidence"], f"{artifact_path}.evidence")
        raw = artifact.get("raw")
        raw_map, raw_errors = _required(raw, f"{artifact_path}.raw", ("name", "size_bytes", "sha256"))
        if "raw" in artifact:
            errors += raw_errors
        if isinstance(raw, dict):
            errors += _unknown(
                raw_map,
                f"{artifact_path}.raw",
                frozenset({"name", "size_bytes", "sha256"}),
            )
            if "name" in raw_map:
                errors += validate_relative_path(raw_map["name"], f"{artifact_path}.raw.name")
                if authoritative_basename is not None and raw_map["name"] != authoritative_basename:
                    errors.append(
                        f"{artifact_path}.raw.name: must equal decoded authoritative URL basename "
                        f"{authoritative_basename!r}"
                    )
            if "size_bytes" in raw_map:
                errors += validate_size(raw_map["size_bytes"], f"{artifact_path}.raw.size_bytes")
            if "sha256" in raw_map:
                errors += validate_sha256(raw_map["sha256"], f"{artifact_path}.raw.sha256")
        outputs = artifact.get("outputs")
        if "outputs" in artifact and (not isinstance(outputs, list) or not outputs):
            errors.append(f"{artifact_path}.outputs: must be a non-empty list")
        if not isinstance(outputs, list):
            continue
        object_names: set[str] = set()
        for index, raw_output in enumerate(outputs):
            output_path = f"{artifact_path}.outputs[{index}]"
            common_fields = ("object_name", "size_bytes", "sha256", "schema")
            output_fields = common_fields + (("member_path",) if is_archive else ("raw_identity",))
            output, output_errors = _required(raw_output, output_path, output_fields)
            errors += output_errors
            if not isinstance(raw_output, dict):
                continue
            errors += _unknown(output, output_path, frozenset(output_fields))
            object_name = output.get("object_name")
            if "object_name" in output:
                errors += validate_relative_path(object_name, f"{output_path}.object_name")
                if isinstance(object_name, str) and object_name in object_names:
                    errors.append(f"{output_path}.object_name: duplicate object name '{object_name}'")
                elif isinstance(object_name, str):
                    object_names.add(object_name)
            if "size_bytes" in output:
                errors += validate_size(output["size_bytes"], f"{output_path}.size_bytes")
            if "sha256" in output:
                errors += validate_sha256(output["sha256"], f"{output_path}.sha256")
            if "schema" in output:
                schema_id = output["schema"]
                if not isinstance(schema_id, str) or schema_id not in schemas:
                    errors.append(f"{output_path}.schema: unknown schema '{schema_id}'")
            if is_archive:
                if "member_path" in output:
                    errors += validate_relative_path(output["member_path"], f"{output_path}.member_path")
                    member_path = output["member_path"]
                    if isinstance(member_path, str) and isinstance(object_name, str):
                        member_basename = PurePosixPath(member_path).name
                        if object_name != member_basename:
                            errors.append(
                                f"{output_path}.object_name: must equal archive member basename '{member_basename}'"
                            )
            else:
                if "raw_identity" in output:
                    errors += _exact(output["raw_identity"], True, f"{output_path}.raw_identity")
                if isinstance(raw, dict):
                    for output_field, raw_field in (
                        ("object_name", "name"),
                        ("size_bytes", "size_bytes"),
                        ("sha256", "sha256"),
                    ):
                        if output_field in output and raw_field in raw and output[output_field] != raw[raw_field]:
                            errors.append(f"{output_path}.{output_field}: must match raw {raw_field}")
    scales = dataset.get("scales")
    if not isinstance(scales, dict):
        if "scales" in dataset:
            errors.append(f"{path}.scales: must be a mapping")
        return errors
    if not scales:
        errors.append(f"{path}.scales: must be a non-empty mapping")
    for required_scale in _SCALES:
        if required_scale not in scales:
            errors.append(f"{path}.scales: missing '{required_scale}'")
    for scale_id, raw_scale in scales.items():
        scale_path = f"{path}.scales.{scale_id}"
        if scale_id not in _SCALES:
            errors.append(f"{scale_path}: unknown scale '{scale_id}'")
        scale, scale_errors = _required(raw_scale, scale_path, ("artifacts",))
        errors += scale_errors
        if not isinstance(raw_scale, dict):
            continue
        errors += _unknown(scale, scale_path, frozenset({"artifacts"}))
        references = scale.get("artifacts")
        if "artifacts" in scale and (not isinstance(references, list) or not references):
            errors.append(f"{scale_path}.artifacts: must be a non-empty list")
        if not isinstance(references, list):
            continue
        seen_references: set[str] = set()
        scale_objects: set[str] = set()
        for index, artifact_id in enumerate(references):
            reference_path = f"{scale_path}.artifacts[{index}]"
            if not isinstance(artifact_id, str) or artifact_id not in artifacts:
                errors.append(f"{reference_path}: unknown artifact '{artifact_id}'")
            if isinstance(artifact_id, str) and artifact_id in seen_references:
                errors.append(f"{reference_path}: duplicate artifact '{artifact_id}'")
            elif isinstance(artifact_id, str):
                seen_references.add(artifact_id)
                referenced = artifacts.get(artifact_id)
                if isinstance(referenced, dict):
                    outputs = referenced.get("outputs")
                    if isinstance(outputs, list):
                        for output in outputs:
                            if not isinstance(output, dict):
                                continue
                            object_name = output.get("object_name")
                            if isinstance(object_name, str) and object_name in scale_objects:
                                errors.append(
                                    f"{reference_path}: landing object '{object_name}' conflicts "
                                    f"with another artifact selected by this scale"
                                )
                            elif isinstance(object_name, str):
                                scale_objects.add(object_name)
    return errors


def _validate_exact_mapping(value: object, path: str, expected: dict[str, object]) -> list[str]:
    mapping, errors = _required(value, path, tuple(expected))
    if not isinstance(value, dict):
        return errors
    errors += _unknown(mapping, path, frozenset(expected))
    for field, expected_value in expected.items():
        if field in mapping:
            errors += _exact(mapping[field], expected_value, f"{path}.{field}")
    return errors


def _validate_tpch_dataset(dataset: dict[str, object], path: str) -> list[str]:
    errors: list[str] = []
    schemas = dataset.get("schemas") if isinstance(dataset.get("schemas"), dict) else {}
    for table in _TABLES:
        if table not in schemas:
            errors.append(f"{path}.schemas: missing '{table}'")
    for schema_id in schemas:
        if schema_id not in _TABLES:
            errors.append(f"{path}.schemas.{schema_id}: not a TPC-H table schema")
    fetch, fetch_errors = _required(dataset.get("fetch"), f"{path}.fetch", ("kind",))
    errors += fetch_errors
    if isinstance(dataset.get("fetch"), dict):
        errors += _unknown(fetch, f"{path}.fetch", frozenset({"kind"}))
        if "kind" in fetch:
            errors += _exact(fetch["kind"], "tpch", f"{path}.fetch.kind")
    generator_fields = ("engine", "extension", "environment", "command", "export", "scales")
    generator, generator_errors = _required(dataset.get("generator"), f"{path}.generator", generator_fields)
    errors += generator_errors
    if not isinstance(dataset.get("generator"), dict):
        return errors
    errors += _unknown(generator, f"{path}.generator", frozenset(generator_fields))
    for section in ("engine", "extension", "environment", "command"):
        if section in generator:
            errors += _validate_exact_mapping(
                generator[section], f"{path}.generator.{section}", _TPCH_CONSTANTS[section]
            )
    extension = generator.get("extension")
    if isinstance(extension, dict) and "repository_url" in extension:
        errors += _https(
            extension["repository_url"],
            f"{path}.generator.extension.repository_url",
        )
    export_fields = ("format", "compression", "row_group_size", "order_by")
    export, export_errors = _required(generator.get("export"), f"{path}.generator.export", export_fields)
    if "export" in generator:
        errors += export_errors
    if isinstance(generator.get("export"), dict):
        errors += _unknown(export, f"{path}.generator.export", frozenset(export_fields))
        for field, expected in _TPCH_CONSTANTS["export"].items():
            if field in export:
                errors += _exact(export[field], expected, f"{path}.generator.export.{field}")
        order_by, order_errors = _required(export.get("order_by"), f"{path}.generator.export.order_by", _TABLES)
        if "order_by" in export:
            errors += order_errors
        if isinstance(export.get("order_by"), dict):
            errors += _unknown(order_by, f"{path}.generator.export.order_by", frozenset(_TABLES))
            for table, expected in _ORDER_BY.items():
                if table in order_by:
                    errors += _exact(order_by[table], expected, f"{path}.generator.export.order_by.{table}")
    scales, scale_errors = _required(generator.get("scales"), f"{path}.generator.scales", _SCALES)
    if "scales" in generator:
        errors += scale_errors
    if not isinstance(generator.get("scales"), dict):
        return errors
    errors += _unknown(scales, f"{path}.generator.scales", frozenset(_SCALES))
    scale_factors = {"tiny": 0.01, "small": 1, "medium": 10}
    scale_fields = ("scale_factor", "outputs")
    output_fields = ("table", "object_name", "size_bytes", "sha256", "schema")
    for scale_id, raw_scale in scales.items():
        scale_path = f"{path}.generator.scales.{scale_id}"
        scale, required_errors = _required(raw_scale, scale_path, scale_fields)
        errors += required_errors
        if not isinstance(raw_scale, dict):
            continue
        errors += _unknown(scale, scale_path, frozenset(scale_fields))
        if "scale_factor" in scale and scale_id in scale_factors:
            value = scale["scale_factor"]
            if isinstance(value, bool) or not isinstance(value, (int, float)) or value != scale_factors[scale_id]:
                errors.append(f"{scale_path}.scale_factor: must be {scale_factors[scale_id]!r}")
        outputs = scale.get("outputs")
        if "outputs" in scale and (not isinstance(outputs, list) or len(outputs) != 8):
            errors.append(f"{scale_path}.outputs: must contain exactly eight outputs")
        if not isinstance(outputs, list):
            continue
        seen_tables: set[str] = set()
        seen_schemas: set[str] = set()
        seen_objects: set[str] = set()
        for index, raw_output in enumerate(outputs):
            output_path = f"{scale_path}.outputs[{index}]"
            output, output_errors = _required(raw_output, output_path, output_fields)
            errors += output_errors
            if not isinstance(raw_output, dict):
                continue
            errors += _unknown(output, output_path, frozenset(output_fields))
            table = output.get("table")
            if "table" in output:
                if table not in _TABLES:
                    errors.append(f"{output_path}.table: unknown table '{table}'")
                elif index < len(_TABLES) and table != _TABLES[index]:
                    errors.append(f"{output_path}.table: must be '{_TABLES[index]}' in canonical order")
                if isinstance(table, str) and table in seen_tables:
                    errors.append(f"{output_path}.table: duplicate table '{table}'")
                elif isinstance(table, str):
                    seen_tables.add(table)
            object_name = output.get("object_name")
            if "object_name" in output:
                errors += validate_relative_path(object_name, f"{output_path}.object_name")
                if isinstance(table, str) and object_name != f"{table}.parquet":
                    errors.append(f"{output_path}.object_name: must be '{table}.parquet'")
                if isinstance(object_name, str) and object_name in seen_objects:
                    errors.append(f"{output_path}.object_name: duplicate object name '{object_name}'")
                elif isinstance(object_name, str):
                    seen_objects.add(object_name)
            if "size_bytes" in output:
                errors += validate_size(output["size_bytes"], f"{output_path}.size_bytes")
            if "sha256" in output:
                errors += validate_sha256(output["sha256"], f"{output_path}.sha256")
            schema_id = output.get("schema")
            if "schema" in output:
                if not isinstance(schema_id, str) or schema_id not in schemas:
                    errors.append(f"{output_path}.schema: unknown schema '{schema_id}'")
                if schema_id != table:
                    errors.append(f"{output_path}.schema: must match table '{table}'")
                if isinstance(schema_id, str) and schema_id in seen_schemas:
                    errors.append(f"{output_path}.schema: duplicate schema '{schema_id}'")
                elif isinstance(schema_id, str):
                    seen_schemas.add(schema_id)
    return errors


def validate_registry_v2(doc: object) -> list[str]:
    """Validate a registry-v2 object without performing I/O or raising on bad input."""
    root, errors = _required(doc, "registry", ("version", "lock", "datasets"))
    if not isinstance(doc, dict):
        return errors
    errors += _unknown(root, "registry", frozenset({"version", "lock", "datasets"}))
    if "version" in root and (type(root["version"]) is not int or root["version"] != 2):
        errors.append("registry.version: must be integer 2")
    lock_fields = (
        "algorithm",
        "source_drift",
        "object_drift",
        "schema_fingerprint",
        "update_policy",
    )
    lock, lock_errors = _required(root.get("lock"), "registry.lock", lock_fields)
    if "lock" in root:
        errors += lock_errors
    if isinstance(root.get("lock"), dict):
        errors += _unknown(lock, "registry.lock", frozenset(lock_fields))
        expected_lock = {
            "algorithm": "sha256",
            "source_drift": "fail",
            "object_drift": "fail",
            "schema_fingerprint": "sha256-canonical-json",
            "update_policy": "reviewed-lock-update",
        }
        for field, expected in expected_lock.items():
            if field in lock:
                errors += _exact(lock[field], expected, f"registry.lock.{field}")
    datasets = root.get("datasets")
    if not isinstance(datasets, dict):
        if "datasets" in root:
            errors.append("registry.datasets: must be a mapping")
        return errors
    if not datasets:
        errors.append("registry.datasets: must be a non-empty mapping")
    shared_fields = (
        "description",
        "format",
        "license",
        "landing_prefix",
        "fetch",
        "provenance",
        "schemas",
    )
    authoritative_urls: dict[tuple[str, str, int | None, str, str], str] = {}
    for dataset_id, raw_dataset in datasets.items():
        path = f"datasets.{dataset_id}"
        errors += _identifier(dataset_id, path)
        dataset, dataset_errors = _required(raw_dataset, path, shared_fields)
        errors += dataset_errors
        if not isinstance(raw_dataset, dict):
            continue
        fetch = dataset.get("fetch")
        declared_kind = fetch.get("kind") if isinstance(fetch, dict) else None
        if declared_kind == "http":
            kind = "http"
        elif declared_kind == "tpch" and "generator" in dataset:
            kind = "tpch"
        elif "artifacts" in dataset or "scales" in dataset:
            kind = "http"
        elif "generator" in dataset:
            kind = "tpch"
        else:
            kind = declared_kind
        required_fields = shared_fields
        if kind == "http":
            required_fields += ("artifacts", "scales")
            allowed = frozenset(required_fields)
        elif kind == "tpch":
            required_fields += ("generator",)
            allowed = frozenset(required_fields)
        else:
            allowed = frozenset(shared_fields + ("artifacts", "scales", "generator"))
        for field in required_fields[len(shared_fields) :]:
            if field not in dataset:
                errors.append(f"{path}: missing '{field}'")
        errors += _unknown(dataset, path, allowed)
        for field in ("description", "format", "license"):
            if field in dataset:
                errors += _nonempty_string(dataset[field], f"{path}.{field}")
        if "landing_prefix" in dataset:
            errors += validate_relative_path(dataset["landing_prefix"], f"{path}.landing_prefix")
        if "provenance" in dataset:
            errors += _validate_provenance(dataset["provenance"], f"{path}.provenance")
        if "schemas" in dataset:
            errors += _validate_schemas(dataset["schemas"], f"{path}.schemas")
        if kind == "http":
            errors += _validate_http_dataset(dataset, path)
            artifacts = dataset.get("artifacts")
            if isinstance(artifacts, dict):
                for artifact_id, artifact in artifacts.items():
                    if not isinstance(artifact, dict):
                        continue
                    url = artifact.get("url")
                    url_identity = _authoritative_url_identity(url)
                    if url_identity is None:
                        continue
                    url_path = f"{path}.artifacts.{artifact_id}.url"
                    first_path = authoritative_urls.get(url_identity)
                    if first_path is None:
                        authoritative_urls[url_identity] = url_path
                    else:
                        errors.append(f"{url_path}: duplicate authoritative URL first defined at {first_path}")
        elif kind == "tpch":
            errors += _validate_tpch_dataset(dataset, path)
        else:
            fetch_map, fetch_errors = _required(fetch, f"{path}.fetch", ("kind",))
            if "fetch" in dataset:
                errors += fetch_errors
            if isinstance(fetch, dict):
                errors += _unknown(fetch_map, f"{path}.fetch", frozenset({"kind", "unzip"}))
                if "kind" in fetch_map:
                    errors.append(f"{path}.fetch.kind: must be 'http' or 'tpch'")
    return errors


def validate_registry(doc: dict) -> list[str]:
    """Validate the production registry, which accepts only the lock-grade v2 contract."""
    if not isinstance(doc, dict) or doc.get("version") != 2:
        return ["registry: 'version' must be 2"]
    return validate_registry_v2(doc)
