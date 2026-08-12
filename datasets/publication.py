"""Canonical immutable publication models and verified dataset resolution."""

from __future__ import annotations

import ctypes
import hashlib
import json
import os
import re
import secrets
import stat
import sys
import tempfile
import threading
import uuid
from collections.abc import Mapping
from contextlib import contextmanager
from dataclasses import asdict, dataclass, replace
from datetime import UTC, datetime, timedelta
from enum import Enum
from pathlib import Path
from types import MappingProxyType
from typing import BinaryIO, Callable, Iterator, cast

from botocore.exceptions import BotoCoreError, ClientError

from datasets import acquisition
from datasets.locking import canonical_json, validate_relative_path
from datasets.registry import (
    Dataset,
    GeneratorOutput,
    LandingObject,
    Provenance,
    ScalePlan,
    SchemaContract,
    resolve_scale,
)
from datasets.s3 import (
    AmbiguousWrite,
    ConditionalConflict,
    Lease,
    acquire_lease,
    put_control_object,
    put_immutable_object,
    read_control_object,
    release_lease,
    renew_lease,
    stream_verify_object,
)
from datasets.schema_inspection import verify_physical_schema
from datasets.verification import (
    ExpectedObject,
    LockMismatch,
    VerificationContext,
    VerifiedFile,
    require_exact_names,
    verify_file,
)

_BUCKET = "landing"
_PUBLICATION_LAYOUT_VERSION = 1
_CONTROL_FORMAT_VERSION = 1
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_UUID4_HEX_RE = re.compile(r"^[0-9a-f]{12}4[0-9a-f]{3}[89ab][0-9a-f]{15}$")
_MAX_LEGACY_LIST_PAGES = 10_000
_MAX_LEGACY_LIST_KEYS = 100_000
_MAX_HISTORY_DEPTH = 1_000
_MAX_GENERATION_PREFIXES = 100_000
_DEFAULT_LOCK_POLICY: Mapping[str, object] = MappingProxyType(
    {
        "algorithm": "sha256",
        "object_drift": "fail",
        "schema_fingerprint": "sha256-canonical-json",
        "source_drift": "fail",
        "update_policy": "reviewed-lock-update",
    }
)


def _require_sha256(value: object, label: str) -> str:
    if not isinstance(value, str) or _SHA256_RE.fullmatch(value) is None:
        raise ValueError(f"{label} must be 64 lowercase hexadecimal characters")
    return value


def _require_uuid4_hex(value: object, label: str = "publication identifier") -> str:
    if not isinstance(value, str) or _UUID4_HEX_RE.fullmatch(value) is None:
        raise ValueError(f"{label} must be a uuid4-style 32-character lowercase hexadecimal value")
    return value


def _require_component(value: object, label: str) -> str:
    errors = validate_relative_path(value, label)
    if errors or not isinstance(value, str) or "/" in value:
        raise ValueError(f"{label} must be a safe single path component")
    return value


def _require_relative_key(value: object, label: str) -> str:
    errors = validate_relative_path(value, label)
    if errors or not isinstance(value, str):
        raise ValueError(f"{label} must be a safe relative POSIX path")
    return value


def _plain_mapping(value: Mapping[str, object]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, item in value.items():
        if isinstance(item, Mapping):
            result[str(key)] = _plain_mapping(cast(Mapping[str, object], item))
        elif isinstance(item, tuple):
            result[str(key)] = [_plain_value(part) for part in item]
        else:
            result[str(key)] = _plain_value(item)
    return result


def _plain_value(value: object) -> object:
    if isinstance(value, Mapping):
        return _plain_mapping(cast(Mapping[str, object], value))
    if isinstance(value, tuple):
        return [_plain_value(item) for item in value]
    return value


def _provenance_document(provenance: Provenance | None) -> dict[str, object] | None:
    return asdict(provenance) if provenance is not None else None


def _schema_document(contract: SchemaContract) -> dict[str, object]:
    return {
        "fields": [asdict(field) for field in contract.fields],
        "fingerprint": contract.fingerprint,
        "format": contract.format,
        "id": contract.id,
        "mode": contract.mode,
        "options": _plain_mapping(contract.options),
    }


def _output_document(output: LandingObject | GeneratorOutput, schema: SchemaContract) -> dict[str, object]:
    return {
        "object_name": output.object_name,
        "schema_fingerprint": schema.fingerprint,
        "schema_id": output.schema_id,
        "sha256": output.sha256,
        "size_bytes": output.size_bytes,
    }


def _selected_outputs(plan: ScalePlan) -> tuple[LandingObject | GeneratorOutput, ...]:
    if plan.generator_scale is not None:
        return tuple(plan.generator_scale.outputs)
    return tuple(output for artifact in plan.artifacts for output in artifact.outputs)


def selected_plan_document(plan: ScalePlan) -> dict[str, object]:
    """Return only the normalized selected scale contract used for correctness."""
    outputs = _selected_outputs(plan)
    source: dict[str, object]
    if plan.generator_scale is None:
        source = {
            "artifacts": [
                {
                    "id": artifact.id,
                    "provenance": _provenance_document(artifact.provenance or plan.dataset.provenance),
                    "raw": asdict(artifact.raw),
                    "outputs": [asdict(output) for output in artifact.outputs],
                    "stability": artifact.stability,
                    "url": artifact.url,
                    "version": asdict(artifact.version),
                }
                for artifact in plan.artifacts
            ],
            "kind": "http",
            "unzip": plan.dataset.unzip,
        }
    else:
        generator = plan.dataset.generator
        if generator is None:
            raise ValueError("generator scale requires a generator contract")
        source = {
            "compression": generator.compression,
            "engine": {
                "name": generator.engine_name,
                "version": generator.engine_version,
                "wheel_sha256": generator.engine_wheel_sha256,
            },
            "environment": asdict(generator.environment),
            "export_format": generator.export_format,
            "extension": {
                "name": generator.extension_name,
                "repository_url": generator.extension_repository_url,
                "sha256": generator.extension_sha256,
                "version_relation": generator.extension_version_relation,
            },
            "kind": "generator",
            "order_by": _plain_mapping(generator.order_by),
            "procedure": generator.procedure,
            "row_group_size": generator.row_group_size,
            "scale_factor": plan.generator_scale.scale_factor,
            "scale_parameter": generator.scale_parameter,
            "outputs": [asdict(output) for output in plan.generator_scale.outputs],
        }
    selected_schemas = {output.schema_id: plan.dataset.schemas[output.schema_id] for output in outputs}
    return {
        "dataset": plan.dataset.name,
        "dataset_contract": {
            "format": plan.dataset.format,
            "landing_prefix": plan.dataset.landing_prefix,
            "license": plan.dataset.license,
            "provenance": _provenance_document(plan.dataset.provenance),
        },
        "lock": _plain_mapping(_DEFAULT_LOCK_POLICY),
        "objects": [_output_document(output, plan.dataset.schemas[output.schema_id]) for output in outputs],
        "publication_layout_version": _PUBLICATION_LAYOUT_VERSION,
        "registry_schema_version": 2,
        "scale": plan.scale,
        "schemas": [_schema_document(selected_schemas[schema_id]) for schema_id in selected_schemas],
        "source": source,
    }


def plan_id(plan: ScalePlan) -> str:
    if not isinstance(plan, ScalePlan):
        raise TypeError("plan_id requires a ScalePlan")
    return hashlib.sha256(canonical_json(selected_plan_document(plan))).hexdigest()


def publication_prefix(plan: ScalePlan, publication_id: str) -> str:
    identifier = _require_uuid4_hex(publication_id)
    landing_prefix = _require_relative_key(plan.dataset.landing_prefix, "landing prefix")
    if landing_prefix.split("/", 1)[0] == "_data-eng-locks":
        raise ValueError("landing prefix must not enter the global control namespace")
    return f"{landing_prefix}/_generations/{plan_id(plan)}/{identifier}"


def active_pointer_key(dataset: str) -> str:
    return f"_data-eng-locks/current/{_require_component(dataset, 'dataset')}.json"


def immutable_manifest_key(dataset: str, digest: str) -> str:
    dataset_component = _require_component(dataset, "dataset")
    manifest_digest = _require_sha256(digest, "manifest digest")
    return f"_data-eng-locks/manifests/{dataset_component}/{manifest_digest}.json"


def _format_instant(value: datetime) -> str:
    if value.tzinfo is None or value.utcoffset() != timedelta(0):
        raise ValueError("publication timestamp must be timezone-aware UTC")
    if value.microsecond:
        raise ValueError("publication timestamp must use whole-second UTC")
    return value.astimezone(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")


def _parse_instant(value: object) -> datetime:
    if (
        not isinstance(value, str)
        or re.fullmatch(r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z", value) is None
    ):
        raise ValueError("publication timestamp must use canonical whole-second UTC")
    try:
        parsed = datetime.fromisoformat(value[:-1] + "+00:00")
    except ValueError as error:
        raise ValueError("publication timestamp is invalid") from error
    parsed = parsed.astimezone(UTC)
    if _format_instant(parsed) != value:
        raise ValueError("publication timestamp must use canonical whole-second UTC")
    return parsed


def _decode_canonical_mapping(body: bytes, label: str) -> dict[str, object]:
    try:
        document = json.loads(
            body,
            parse_constant=lambda value: (_ for _ in ()).throw(ValueError(value)),
        )
    except (RecursionError, UnicodeDecodeError, ValueError, json.JSONDecodeError) as error:
        raise ValueError(f"{label} must be valid canonical JSON") from error
    if not isinstance(document, dict):
        raise ValueError(f"{label} must be a JSON mapping")
    try:
        canonical = canonical_json(document)
    except RecursionError as error:
        raise ValueError(f"{label} must be encodable as canonical JSON") from error
    if canonical != body:
        raise ValueError(f"{label} must use exact canonical JSON")
    return cast(dict[str, object], document)


@dataclass(frozen=True)
class ManifestObject:
    object_name: str
    key: str
    size_bytes: int
    sha256: str
    schema_id: str
    schema_fingerprint: str

    def __post_init__(self) -> None:
        _require_relative_key(self.object_name, "object name")
        _require_relative_key(self.key, "physical key")
        if isinstance(self.size_bytes, bool) or not isinstance(self.size_bytes, int) or self.size_bytes <= 0:
            raise ValueError("object size must be a positive integer")
        _require_sha256(self.sha256, "object sha256")
        _require_component(self.schema_id, "schema identifier")
        _require_sha256(self.schema_fingerprint, "schema fingerprint")

    def to_document(self) -> dict[str, object]:
        return asdict(self)

    @classmethod
    def from_document(cls, document: object) -> ManifestObject:
        fields = {"object_name", "key", "size_bytes", "sha256", "schema_id", "schema_fingerprint"}
        if not isinstance(document, dict) or set(document) != fields:
            raise ValueError("manifest object fields are not exact")
        return cls(**document)


@dataclass(frozen=True)
class ImmutableManifest:
    format_version: int
    dataset: str
    scale: str
    raw_registry_sha256: str
    selected_plan_sha256: str
    plan_id: str
    publication_id: str
    physical_prefix: str
    objects: tuple[ManifestObject, ...]
    published_at: datetime
    previous_manifest_key: str | None
    previous_manifest_sha256: str | None

    def __post_init__(self) -> None:
        if type(self.format_version) is not int or self.format_version != _CONTROL_FORMAT_VERSION:
            raise ValueError("manifest format version is unsupported")
        _require_component(self.dataset, "dataset")
        _require_component(self.scale, "scale")
        _require_sha256(self.raw_registry_sha256, "raw registry sha256")
        _require_sha256(self.selected_plan_sha256, "selected plan sha256")
        _require_sha256(self.plan_id, "plan identifier")
        if self.selected_plan_sha256 != self.plan_id:
            raise ValueError("selected plan sha256 must equal plan identifier")
        _require_uuid4_hex(self.publication_id)
        _require_relative_key(self.physical_prefix, "physical prefix")
        if type(self.objects) is not tuple:
            raise ValueError("manifest objects must be an immutable tuple")
        if not self.objects:
            raise ValueError("manifest must contain at least one object")
        if not all(isinstance(item, ManifestObject) for item in self.objects):
            raise ValueError("manifest objects must contain only ManifestObject values")
        names = tuple(item.object_name for item in self.objects)
        if len(set(names)) != len(names):
            raise ValueError("manifest object names must be unique")
        _format_instant(self.published_at)
        if (self.previous_manifest_key is None) != (self.previous_manifest_sha256 is None):
            raise ValueError("previous manifest key and digest must be both present or both absent")
        if self.previous_manifest_sha256 is not None:
            expected_key = immutable_manifest_key(self.dataset, self.previous_manifest_sha256)
            if self.previous_manifest_key != expected_key:
                raise ValueError("previous manifest key does not match its digest")

    def to_document(self) -> dict[str, object]:
        return {
            "dataset": self.dataset,
            "format_version": self.format_version,
            "objects": [item.to_document() for item in self.objects],
            "physical_prefix": self.physical_prefix,
            "plan_id": self.plan_id,
            "previous_manifest_key": self.previous_manifest_key,
            "previous_manifest_sha256": self.previous_manifest_sha256,
            "publication_id": self.publication_id,
            "published_at": _format_instant(self.published_at),
            "raw_registry_sha256": self.raw_registry_sha256,
            "scale": self.scale,
            "selected_plan_sha256": self.selected_plan_sha256,
        }

    def to_bytes(self) -> bytes:
        return canonical_json(self.to_document())

    @classmethod
    def from_bytes(cls, body: bytes) -> ImmutableManifest:
        document = _decode_canonical_mapping(body, "immutable manifest")
        fields = {
            "dataset",
            "format_version",
            "objects",
            "physical_prefix",
            "plan_id",
            "previous_manifest_key",
            "previous_manifest_sha256",
            "publication_id",
            "published_at",
            "raw_registry_sha256",
            "scale",
            "selected_plan_sha256",
        }
        if set(document) != fields:
            raise ValueError("immutable manifest fields are not exact")
        objects = document["objects"]
        if not isinstance(objects, list):
            raise ValueError("immutable manifest objects must be a list")
        return cls(
            format_version=document["format_version"],
            dataset=document["dataset"],
            scale=document["scale"],
            raw_registry_sha256=document["raw_registry_sha256"],
            selected_plan_sha256=document["selected_plan_sha256"],
            plan_id=document["plan_id"],
            publication_id=document["publication_id"],
            physical_prefix=document["physical_prefix"],
            objects=tuple(ManifestObject.from_document(item) for item in objects),
            published_at=_parse_instant(document["published_at"]),
            previous_manifest_key=document["previous_manifest_key"],
            previous_manifest_sha256=document["previous_manifest_sha256"],
        )


def manifest_sha256(manifest: ImmutableManifest | bytes) -> str:
    body = manifest.to_bytes() if isinstance(manifest, ImmutableManifest) else manifest
    return hashlib.sha256(body).hexdigest()


@dataclass(frozen=True)
class ActivePointer:
    format_version: int
    dataset: str
    manifest_key: str
    manifest_sha256: str

    def __post_init__(self) -> None:
        if type(self.format_version) is not int or self.format_version != _CONTROL_FORMAT_VERSION:
            raise ValueError("pointer format version is unsupported")
        expected_key = immutable_manifest_key(self.dataset, self.manifest_sha256)
        if self.manifest_key != expected_key:
            raise ValueError("pointer manifest key does not match its digest")

    def to_document(self) -> dict[str, object]:
        return asdict(self)

    def to_bytes(self) -> bytes:
        return canonical_json(self.to_document())

    @classmethod
    def from_bytes(cls, body: bytes) -> ActivePointer:
        document = _decode_canonical_mapping(body, "active pointer")
        fields = {"format_version", "dataset", "manifest_key", "manifest_sha256"}
        if set(document) != fields:
            raise ValueError("active pointer fields are not exact")
        return cls(**document)


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


def _mismatch(
    plan: ScalePlan,
    field: str,
    expected: object,
    actual: object,
    *,
    stage: str = "manifest",
) -> LockMismatch:
    return LockMismatch(
        VerificationContext(plan.dataset.name, plan.scale, stage),
        field,
        expected,
        actual,
    )


def _validate_manifest_for_plan(manifest: ImmutableManifest, plan: ScalePlan) -> None:
    expected_plan_id = plan_id(plan)
    if manifest.dataset != plan.dataset.name:
        raise _mismatch(plan, "dataset", plan.dataset.name, manifest.dataset)
    if manifest.scale != plan.scale:
        raise _mismatch(plan, "scale", plan.scale, manifest.scale)
    if manifest.plan_id != expected_plan_id:
        raise _mismatch(plan, "plan_id", expected_plan_id, manifest.plan_id)
    expected_prefix = publication_prefix(plan, manifest.publication_id)
    if manifest.physical_prefix != expected_prefix:
        raise _mismatch(plan, "physical_prefix", expected_prefix, manifest.physical_prefix)
    outputs = _selected_outputs(plan)
    if tuple(item.object_name for item in manifest.objects) != tuple(item.object_name for item in outputs):
        raise _mismatch(
            plan,
            "object_names",
            tuple(item.object_name for item in outputs),
            tuple(item.object_name for item in manifest.objects),
        )
    for item, output in zip(manifest.objects, outputs, strict=True):
        schema = plan.dataset.schemas[output.schema_id]
        expected = ManifestObject(
            object_name=output.object_name,
            key=f"{expected_prefix}/{output.object_name}",
            size_bytes=output.size_bytes,
            sha256=output.sha256,
            schema_id=output.schema_id,
            schema_fingerprint=schema.fingerprint,
        )
        if item != expected:
            raise _mismatch(plan, f"object:{output.object_name}", expected, item)


class _CapturingBody:
    def __init__(self, stream: BinaryIO, destination: BinaryIO) -> None:
        self._stream = stream
        self._destination = destination
        self._closed = False

    def read(self, size: int = -1) -> bytes:
        try:
            chunk = self._stream.read(size)
        except AmbiguousWrite:
            raise
        except Exception as error:
            raise AmbiguousWrite("S3 object body read failed during schema capture") from error
        if not isinstance(chunk, bytes):
            raise AmbiguousWrite("S3 object body returned non-bytes during schema capture")
        try:
            written = self._destination.write(chunk)
        except Exception as error:
            raise AmbiguousWrite("local schema capture write failed") from error
        if isinstance(written, bool) or not isinstance(written, int) or written != len(chunk):
            raise AmbiguousWrite("local schema capture write was incomplete")
        return chunk

    def close(self) -> None:
        if not self._closed:
            self._closed = True
            self._stream.close()


class _CapturingClient:
    def __init__(self, client, destination: BinaryIO) -> None:
        self._client = client
        self._destination = destination

    def get_object(self, **request):
        response = self._client.get_object(**request)
        if not isinstance(response, Mapping) or "Body" not in response:
            return response
        body = response["Body"]
        read = getattr(body, "read", None)
        close = getattr(body, "close", None)
        if not callable(read) or not callable(close):
            if callable(close):
                try:
                    close()
                except Exception as error:
                    raise AmbiguousWrite("malformed S3 object body could not be closed") from error
            raise AmbiguousWrite("S3 object body lacks readable and closable binary capabilities")
        captured = dict(response)
        captured["Body"] = _CapturingBody(cast(BinaryIO, body), self._destination)
        return captured


def _verify_resolved_object(client, plan: ScalePlan, item: ManifestObject, bucket: str) -> ResolvedObject:
    context = VerificationContext(
        plan.dataset.name,
        plan.scale,
        "resolve",
        object_name=item.object_name,
    )
    expected = ExpectedObject(item.object_name, item.size_bytes, item.sha256, item.schema_id)
    try:
        with tempfile.NamedTemporaryFile(
            prefix="dataset-resolution-",
            suffix=Path(item.object_name).suffix,
        ) as temporary:
            stream_verify_object(
                _CapturingClient(client, temporary),
                bucket,
                item.key,
                expected,
                context,
            )
            temporary.flush()
            verify_physical_schema(Path(temporary.name), plan.dataset.schemas[item.schema_id], context)
    except OSError as error:
        raise AmbiguousWrite("local schema capture I/O failed") from error
    return ResolvedObject(
        object_name=item.object_name,
        uri=f"s3://{bucket}/{item.key}",
        size_bytes=item.size_bytes,
        sha256=item.sha256,
        schema_id=item.schema_id,
    )


def _read_manifest(client, plan: ScalePlan, digest: str, bucket: str) -> ImmutableManifest:
    key = immutable_manifest_key(plan.dataset.name, digest)
    snapshot = read_control_object(client, bucket, key)
    try:
        if manifest_sha256(snapshot.body) != digest:
            raise ValueError("immutable manifest digest does not match its content-addressed key")
        return ImmutableManifest.from_bytes(snapshot.body)
    except (TypeError, ValueError) as error:
        raise _mismatch(
            plan,
            "manifest",
            "canonical manifest matching its content-addressed key",
            type(error).__name__,
            stage="manifest",
        ) from error


def _resolve_manifest(
    client,
    plan: ScalePlan,
    digest: str,
    *,
    bucket: str,
) -> ResolvedDataset:
    resolved, _manifest = _resolve_manifest_with_document(client, plan, digest, bucket=bucket)
    return resolved


def _resolve_manifest_with_document(
    client,
    plan: ScalePlan,
    digest: str,
    *,
    bucket: str,
) -> tuple[ResolvedDataset, ImmutableManifest]:
    manifest = _read_manifest(client, plan, digest, bucket)
    _validate_manifest_for_plan(manifest, plan)
    objects = tuple(_verify_resolved_object(client, plan, item, bucket) for item in manifest.objects)
    return (
        ResolvedDataset(
            dataset=plan.dataset.name,
            scale=plan.scale,
            plan_id=manifest.plan_id,
            manifest_sha256=digest,
            publication_id=manifest.publication_id,
            objects=objects,
        ),
        manifest,
    )


def resolve_active_dataset(
    client,
    registry: Mapping[str, Dataset],
    dataset_id: str,
    expected_scale: str,
    *,
    bucket: str = _BUCKET,
) -> ResolvedDataset:
    dataset = registry[dataset_id]
    plan = resolve_scale(dataset, expected_scale)
    pointer_snapshot = read_control_object(client, bucket, active_pointer_key(dataset_id))
    try:
        pointer = ActivePointer.from_bytes(pointer_snapshot.body)
    except (TypeError, ValueError) as error:
        raise _mismatch(
            plan,
            "pointer",
            "canonical active pointer",
            type(error).__name__,
            stage="pointer",
        ) from error
    if pointer.dataset != dataset_id:
        raise _mismatch(plan, "dataset", dataset_id, pointer.dataset, stage="pointer")
    return _resolve_manifest(client, plan, pointer.manifest_sha256, bucket=bucket)


def rollback_manifest(
    client,
    registry: Mapping[str, Dataset],
    dataset_id: str,
    expected_scale: str,
    target_manifest_sha256: str,
    *,
    bucket: str = _BUCKET,
) -> ResolvedDataset:
    """Verify historical immutable state, then repoint with the observed pointer ETag."""
    digest = _require_sha256(target_manifest_sha256, "rollback manifest digest")
    plan = resolve_scale(registry[dataset_id], expected_scale)
    return _rollback_transaction(client, plan, digest, bucket=bucket).resolved


@dataclass(frozen=True)
class _RollbackOutcome:
    resolved: ResolvedDataset
    manifest: ImmutableManifest
    previous_manifest: ImmutableManifest
    pointer_state: _PointerState
    reconciled: bool


def _rollback_transaction(
    client,
    plan: ScalePlan,
    digest: str,
    *,
    bucket: str,
    dry_run: bool = False,
) -> _RollbackOutcome:
    current = _read_pointer_state(client, plan, bucket)
    if current.corruption is not None or current.pointer is None:
        _verify_pointer_state(client, plan, current, bucket)
        raise AssertionError("pointer verification unexpectedly returned without a pointer")
    previous_manifest = _read_historical_manifest(
        client,
        plan.dataset.name,
        current.pointer.manifest_sha256,
        bucket,
    )
    try:
        _validate_historical_manifest(previous_manifest, plan)
    except (TypeError, ValueError) as error:
        raise _mismatch(
            plan,
            "pointer",
            "canonical self-consistent current manifest",
            type(error).__name__,
            stage="pointer",
        ) from error
    resolved, manifest = _resolve_manifest_with_document(client, plan, digest, bucket=bucket)
    if dry_run:
        return _RollbackOutcome(resolved, manifest, previous_manifest, current, False)
    pointer = ActivePointer(
        format_version=_CONTROL_FORMAT_VERSION,
        dataset=plan.dataset.name,
        manifest_key=immutable_manifest_key(plan.dataset.name, digest),
        manifest_sha256=digest,
    )
    reconciled = _put_pointer_exact(
        client,
        bucket,
        active_pointer_key(plan.dataset.name),
        pointer.to_bytes(),
        current,
    )
    return _RollbackOutcome(resolved, manifest, previous_manifest, current, reconciled)


class PublishMode(Enum):
    """The four intentional publication actions exposed by the CLI."""

    DEFAULT = "default"
    VERIFY_ONLY = "verify-only"
    REFRESH = "refresh"
    ROLLBACK = "rollback"


@dataclass(frozen=True)
class PublishResult:
    dataset: str
    scale: str
    status: str
    manifest_sha256: str | None
    publication_id: str | None
    object_count: int
    cleanup_warning: str | None = None
    previous_manifest_key: str | None = None
    previous_manifest_sha256: str | None = None
    publication_prefix: str | None = None
    pointer_action: str | None = None
    pointer_precondition: str | None = None
    attempted_object_keys: tuple[str, ...] = ()
    proven_orphan_keys: tuple[str, ...] = ()
    possible_object_keys: tuple[str, ...] = ()
    manifest_key: str | None = None
    manifest_outcome: str = "not-attempted"
    pointer_outcome: str = "not-attempted"
    retained_manifest_keys: tuple[str, ...] = ()
    unreferenced_manifest_keys: tuple[str, ...] = ()
    ambiguous_manifest_keys: tuple[str, ...] = ()
    candidate_generation_prefixes: tuple[str, ...] = ()
    inventory_state: str = "not-requested"

    def to_document(self) -> dict[str, object]:
        return {
            "cleanup_warning": self.cleanup_warning,
            "dataset": self.dataset,
            "manifest_sha256": self.manifest_sha256,
            "object_count": self.object_count,
            "attempted_object_keys": list(self.attempted_object_keys),
            "ambiguous_manifest_keys": list(self.ambiguous_manifest_keys),
            "candidate_generation_prefixes": list(self.candidate_generation_prefixes),
            "inventory_state": self.inventory_state,
            "manifest_key": self.manifest_key,
            "manifest_outcome": self.manifest_outcome,
            "possible_object_keys": list(self.possible_object_keys),
            "pointer_action": self.pointer_action,
            "pointer_precondition": self.pointer_precondition,
            "pointer_outcome": self.pointer_outcome,
            "previous_manifest_key": self.previous_manifest_key,
            "previous_manifest_sha256": self.previous_manifest_sha256,
            "publication_id": self.publication_id,
            "publication_prefix": self.publication_prefix,
            "proven_orphan_keys": list(self.proven_orphan_keys),
            "retained_manifest_keys": list(self.retained_manifest_keys),
            "scale": self.scale,
            "status": self.status,
            "unreferenced_manifest_keys": list(self.unreferenced_manifest_keys),
        }


class PublicationFailure(RuntimeError):
    """A transaction failure with conservative mutation-attempt diagnostics."""

    def __init__(self, result: PublishResult, cause: BaseException) -> None:
        self.result = result
        super().__init__(f"dataset publication failed after immutable staging: {type(cause).__name__}")
        self.__cause__ = cause


Fetcher = Callable[[ScalePlan, Path], tuple[VerifiedFile, ...]]


@dataclass
class _MutationInventory:
    attempted_object_keys: list[str]
    proven_object_keys: list[str]
    possible_object_keys: list[str]
    manifest_key: str | None = None
    manifest_digest: str | None = None
    manifest_outcome: str = "not-attempted"
    pointer_outcome: str = "not-attempted"


@dataclass(frozen=True)
class _PointerState:
    snapshot: object | None
    pointer: ActivePointer | None
    corruption: BaseException | None = None


class _WriteTrackingClient:
    """Expose when the S3 adapter had to reconcile an uncertain write response."""

    def __init__(self, client) -> None:
        self._client = client
        self.write_failed = False

    def __getattr__(self, name: str):
        return getattr(self._client, name)

    def put_object(self, **request):
        try:
            return self._client.put_object(**request)
        except (BotoCoreError, ClientError):
            self.write_failed = True
            raise


class _LeaseKeepalive:
    """Renew synchronously; no background thread is ever capable of S3 mutation."""

    def __init__(self, client, lease: Lease) -> None:
        self._client = client
        self._lease = lease
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._error: BaseException | None = None
        self._started = False
        self._stopped = False

    def start(self) -> None:
        self._started = True

    def _renew_once(self) -> bool:
        with self._lock:
            if self._stop.is_set() or self._error is not None:
                return False
            try:
                self._lease = renew_lease(self._client, self._lease)
            except BaseException as error:
                self._error = error
                return False
            return True

    def run_while_renewing(self, operation: Callable[[], object]) -> object:
        """Run bounded acquisition work off-thread while this thread owns renewal."""
        completed = threading.Event()
        outcome: list[object] = []
        errors: list[BaseException] = []

        def invoke() -> None:
            try:
                outcome.append(operation())
            except BaseException as error:
                errors.append(error)
            finally:
                completed.set()

        worker = threading.Thread(target=invoke, name=f"dataset-source-{self._lease.dataset}", daemon=False)
        worker.start()
        while True:
            with self._lock:
                lease = self._lease
            remaining = max((lease.expires_at - datetime.now(UTC)).total_seconds(), 0.0)
            interval = max(0.05, min(5.0, remaining / 3.0))
            if completed.wait(interval):
                break
            if not self._renew_once():
                completed.wait()
                break
        worker.join()
        if errors:
            raise errors[0]
        with self._lock:
            if self._error is not None:
                raise ConditionalConflict("dataset lease renewal was lost") from self._error
        return outcome[0]

    def checkpoint(self) -> Lease:
        with self._lock:
            error = self._error
            if error is not None:
                raise ConditionalConflict("dataset lease renewal was lost") from error
            _assert_lease_current(self._client, self._lease)
            return self._lease

    def stop(self) -> Lease:
        if self._stopped:
            with self._lock:
                return self._lease
        self._stop.set()
        self._stopped = True
        with self._lock:
            return self._lease

    def stop_and_checkpoint(self) -> Lease:
        self.stop()
        with self._lock:
            if self._error is not None:
                raise ConditionalConflict("dataset lease renewal was lost") from self._error
            _assert_lease_current(self._client, self._lease)
            return self._lease


def _is_missing(error: ClientError) -> bool:
    details = error.response.get("Error", {})
    code = details.get("Code") if isinstance(details, Mapping) else None
    metadata = error.response.get("ResponseMetadata", {})
    status = metadata.get("HTTPStatusCode") if isinstance(metadata, Mapping) else None
    return code in {"404", "NoSuchKey", "NotFound"} or status == 404


def _read_pointer_state(client, plan: ScalePlan, bucket: str) -> _PointerState:
    try:
        snapshot = read_control_object(client, bucket, active_pointer_key(plan.dataset.name))
    except (ClientError, AmbiguousWrite) as error:
        cause = error.__cause__ if isinstance(error, AmbiguousWrite) else error
        if isinstance(cause, ClientError) and _is_missing(cause):
            return _PointerState(None, None)
        raise
    try:
        pointer = ActivePointer.from_bytes(snapshot.body)
        if pointer.dataset != plan.dataset.name:
            raise ValueError("active pointer dataset does not match the selected dataset")
    except (TypeError, ValueError) as error:
        return _PointerState(snapshot, None, error)
    return _PointerState(snapshot, pointer)


def _verify_pointer_state(client, plan: ScalePlan, state: _PointerState, bucket: str) -> ResolvedDataset:
    resolved, _manifest = _verify_pointer_state_with_document(client, plan, state, bucket)
    return resolved


def _verify_pointer_state_with_document(
    client,
    plan: ScalePlan,
    state: _PointerState,
    bucket: str,
) -> tuple[ResolvedDataset, ImmutableManifest]:
    if state.corruption is not None:
        raise _mismatch(
            plan,
            "pointer",
            "canonical active pointer",
            type(state.corruption).__name__,
            stage="pointer",
        ) from state.corruption
    if state.pointer is None:
        raise _mismatch(plan, "pointer", "existing active pointer", "missing", stage="pointer")
    return _resolve_manifest_with_document(client, plan, state.pointer.manifest_sha256, bucket=bucket)


def _result_from_resolved(resolved: ResolvedDataset, status: str) -> PublishResult:
    return PublishResult(
        dataset=resolved.dataset,
        scale=resolved.scale,
        status=status,
        manifest_sha256=resolved.manifest_sha256,
        publication_id=resolved.publication_id,
        object_count=len(resolved.objects),
    )


@dataclass(frozen=True)
class _HistoricalManifest:
    key: str
    digest: str
    manifest: ImmutableManifest


@dataclass
class _InventoryBudget:
    requests: int = 0
    prefixes: int = 0
    keys: int = 0

    def request(self) -> None:
        self.requests += 1
        if self.requests > _MAX_LEGACY_LIST_PAGES:
            raise AmbiguousWrite("publication inventory exceeds the global request budget")


def _manifest_history(
    client,
    plan: ScalePlan,
    current: ImmutableManifest,
    *,
    bucket: str,
    budget: _InventoryBudget | None = None,
) -> tuple[_HistoricalManifest, ...]:
    history: list[_HistoricalManifest] = []
    seen: set[str] = set()
    key = current.previous_manifest_key
    digest = current.previous_manifest_sha256
    for _depth in range(_MAX_HISTORY_DEPTH):
        if key is None and digest is None:
            return tuple(history)
        if key is None or digest is None or key != immutable_manifest_key(plan.dataset.name, digest):
            raise _mismatch(plan, "history", "coupled manifest key/digest", "invalid", stage="history")
        if digest in seen:
            raise _mismatch(plan, "history", "acyclic manifest chain", digest, stage="history")
        seen.add(digest)
        try:
            if budget is not None:
                budget.request()
            manifest = _read_historical_manifest(client, plan.dataset.name, digest, bucket)
            _validate_historical_manifest(manifest, plan)
        except (TypeError, ValueError) as error:
            raise _mismatch(
                plan,
                "history",
                "canonical self-consistent historical manifest",
                type(error).__name__,
                stage="history",
            ) from error
        history.append(_HistoricalManifest(key, digest, manifest))
        key = manifest.previous_manifest_key
        digest = manifest.previous_manifest_sha256
    raise _mismatch(plan, "history", f"at most {_MAX_HISTORY_DEPTH} manifests", "exceeded", stage="history")


def _read_historical_manifest(client, dataset: str, digest: str, bucket: str) -> ImmutableManifest:
    snapshot = read_control_object(client, bucket, immutable_manifest_key(dataset, digest))
    if manifest_sha256(snapshot.body) != digest:
        raise ValueError("historical manifest digest does not match its key")
    return ImmutableManifest.from_bytes(snapshot.body)


def _validate_historical_manifest(manifest: ImmutableManifest, plan: ScalePlan) -> None:
    if manifest.dataset != plan.dataset.name:
        raise ValueError("historical manifest dataset does not match its key")
    expected_physical_prefix = (
        f"{plan.dataset.landing_prefix}/_generations/{manifest.selected_plan_sha256}/{manifest.publication_id}"
    )
    if manifest.physical_prefix != expected_physical_prefix:
        raise ValueError("historical manifest physical prefix is not self-consistent")
    expected_prefix = f"{manifest.physical_prefix}/"
    for item in manifest.objects:
        if not item.key.startswith(expected_prefix) or item.key != f"{manifest.physical_prefix}/{item.object_name}":
            raise ValueError("historical manifest object key is not self-consistent")


def _result_with_verified_history(
    client,
    plan: ScalePlan,
    state: _PointerState,
    resolved: ResolvedDataset,
    status: str,
    *,
    bucket: str,
    current: ImmutableManifest | None = None,
) -> PublishResult:
    if current is None:
        current = _read_manifest(client, plan, resolved.manifest_sha256, bucket)
    return _attach_verified_history(
        client,
        plan,
        current,
        _result_from_resolved(resolved, status),
        bucket=bucket,
    )


def _attach_verified_history(
    client,
    plan: ScalePlan,
    current: ImmutableManifest,
    result: PublishResult,
    *,
    bucket: str,
    additional_reachable: tuple[tuple[str, ImmutableManifest], ...] = (),
) -> PublishResult:
    budget = _InventoryBudget()
    reachable_nodes = list(_manifest_history(client, plan, current, bucket=bucket, budget=budget))
    for key, manifest in additional_reachable:
        digest = key.removeprefix(f"_data-eng-locks/manifests/{plan.dataset.name}/").removesuffix(".json")
        reachable_nodes.append(_HistoricalManifest(key, digest, manifest))
        reachable_nodes.extend(_manifest_history(client, plan, manifest, bucket=bucket, budget=budget))
    reachable_prefixes = {current.physical_prefix, *(item.manifest.physical_prefix for item in reachable_nodes)}
    if result.manifest_sha256 is None:
        raise ValueError("history inventory requires a current manifest digest")
    current_key = immutable_manifest_key(plan.dataset.name, result.manifest_sha256)
    all_manifests = _bounded_object_keys(
        client,
        bucket,
        f"_data-eng-locks/manifests/{plan.dataset.name}/",
        budget=budget,
    )
    referenced = {
        current_key,
        *(item.key for item in reachable_nodes),
    }
    unreferenced: list[str] = []
    ambiguous: list[str] = []
    for key in all_manifests:
        if key in referenced:
            continue
        digest = key.removeprefix(f"_data-eng-locks/manifests/{plan.dataset.name}/").removesuffix(".json")
        try:
            _require_sha256(digest, "inventory manifest digest")
            budget.request()
            candidate = _read_historical_manifest(client, plan.dataset.name, digest, bucket)
            _validate_historical_manifest(candidate, plan)
        except (AmbiguousWrite, TypeError, ValueError):
            ambiguous.append(key)
        else:
            unreferenced.append(key)
            reachable_prefixes.add(candidate.physical_prefix)
    inactive: tuple[str, ...] = ()
    if not ambiguous:
        inactive = _inactive_generation_prefixes(
            client,
            plan,
            reachable_prefixes,
            bucket=bucket,
            budget=budget,
        )
    return replace(
        result,
        retained_manifest_keys=tuple(
            key
            for key in dict.fromkeys((*result.retained_manifest_keys, *(item.key for item in reachable_nodes)))
            if key != current_key
        ),
        previous_manifest_key=result.previous_manifest_key or current.previous_manifest_key,
        previous_manifest_sha256=result.previous_manifest_sha256 or current.previous_manifest_sha256,
        candidate_generation_prefixes=inactive,
        unreferenced_manifest_keys=tuple(unreferenced),
        ambiguous_manifest_keys=tuple(ambiguous),
        inventory_state="complete",
    )


def _inactive_generation_prefixes(
    client,
    plan: ScalePlan,
    reachable_prefixes: set[str],
    *,
    bucket: str,
    budget: _InventoryBudget,
) -> tuple[str, ...]:
    base = f"{plan.dataset.landing_prefix}/_generations/"
    plan_prefixes = _bounded_common_prefixes(client, bucket, base, budget=budget)
    publication_prefixes: list[str] = []
    for selected_plan_prefix in plan_prefixes:
        publication_prefixes.extend(_bounded_common_prefixes(client, bucket, selected_plan_prefix, budget=budget))
        if len(publication_prefixes) > _MAX_GENERATION_PREFIXES:
            raise AmbiguousWrite("generation inventory exceeds the bounded prefix count")
    return tuple(
        sorted(
            prefix.rstrip("/") for prefix in set(publication_prefixes) if prefix.rstrip("/") not in reachable_prefixes
        )
    )


def _bounded_common_prefixes(
    client,
    bucket: str,
    base: str,
    *,
    budget: _InventoryBudget,
) -> tuple[str, ...]:
    token: str | None = None
    seen_tokens: set[str] = set()
    prefixes: list[str] = []
    for _page in range(_MAX_LEGACY_LIST_PAGES):
        budget.request()
        request: dict[str, object] = {"Bucket": bucket, "Prefix": base, "Delimiter": "/"}
        if token is not None:
            request["ContinuationToken"] = token
        response = client.list_objects_v2(**request)
        if not isinstance(response, Mapping):
            raise AmbiguousWrite("generation inventory returned a malformed response")
        contents = response.get("Contents", ())
        common = response.get("CommonPrefixes", ())
        if not isinstance(contents, (list, tuple)) or not isinstance(common, (list, tuple)):
            raise AmbiguousWrite("generation inventory returned malformed collections")
        for entry in common:
            if not isinstance(entry, Mapping) or not isinstance(entry.get("Prefix"), str):
                raise AmbiguousWrite("generation inventory returned a malformed prefix")
            prefix = cast(str, entry["Prefix"])
            if not prefix.startswith(base):
                raise AmbiguousWrite("generation inventory returned an out-of-scope prefix")
            suffix = prefix.removeprefix(base)
            if not suffix.endswith("/") or not suffix[:-1] or "/" in suffix[:-1]:
                raise AmbiguousWrite("generation inventory returned a non-direct prefix")
            if prefix not in prefixes:
                prefixes.append(prefix)
                budget.prefixes += 1
            if budget.prefixes > _MAX_GENERATION_PREFIXES:
                raise AmbiguousWrite("generation inventory exceeds the bounded prefix count")
        truncated = response.get("IsTruncated", False)
        if not isinstance(truncated, bool):
            raise AmbiguousWrite("generation inventory truncation flag is invalid")
        if not truncated:
            break
        next_token = response.get("NextContinuationToken")
        if not isinstance(next_token, str) or not next_token or next_token in seen_tokens:
            raise AmbiguousWrite("generation inventory pagination token is invalid")
        seen_tokens.add(next_token)
        token = next_token
    else:
        raise AmbiguousWrite("generation inventory exceeds the bounded page count")
    return tuple(prefixes)


def _bounded_object_keys(
    client,
    bucket: str,
    base: str,
    *,
    budget: _InventoryBudget,
) -> tuple[str, ...]:
    token: str | None = None
    seen_tokens: set[str] = set()
    keys: list[str] = []
    for _page in range(_MAX_LEGACY_LIST_PAGES):
        budget.request()
        request: dict[str, object] = {"Bucket": bucket, "Prefix": base}
        if token is not None:
            request["ContinuationToken"] = token
        response = client.list_objects_v2(**request)
        if not isinstance(response, Mapping):
            raise AmbiguousWrite("manifest inventory returned a malformed response")
        contents = response.get("Contents", ())
        if not isinstance(contents, (list, tuple)):
            raise AmbiguousWrite("manifest inventory returned malformed contents")
        for entry in contents:
            if not isinstance(entry, Mapping) or not isinstance(entry.get("Key"), str):
                raise AmbiguousWrite("manifest inventory returned a malformed key")
            key = cast(str, entry["Key"])
            if not key.startswith(base):
                raise AmbiguousWrite("manifest inventory returned an out-of-scope key")
            keys.append(key)
            budget.keys += 1
            if budget.keys > _MAX_LEGACY_LIST_KEYS:
                raise AmbiguousWrite("manifest inventory exceeds the bounded key count")
        truncated = response.get("IsTruncated", False)
        if not isinstance(truncated, bool):
            raise AmbiguousWrite("manifest inventory truncation flag is invalid")
        if not truncated:
            break
        next_token = response.get("NextContinuationToken")
        if not isinstance(next_token, str) or not next_token or next_token in seen_tokens:
            raise AmbiguousWrite("manifest inventory pagination token is invalid")
        seen_tokens.add(next_token)
        token = next_token
    else:
        raise AmbiguousWrite("manifest inventory exceeds the bounded page count")
    if len(set(keys)) != len(keys):
        raise AmbiguousWrite("manifest inventory returned duplicate keys")
    return tuple(sorted(keys))


def _expected_output_files(plan: ScalePlan) -> tuple[ExpectedObject, ...]:
    return tuple(
        ExpectedObject(item.object_name, item.size_bytes, item.sha256, item.schema_id)
        for item in _selected_outputs(plan)
    )


def _verify_candidate_files(plan: ScalePlan, files: tuple[VerifiedFile, ...]) -> tuple[VerifiedFile, ...]:
    expected = _expected_output_files(plan)
    require_exact_names(
        tuple(item.object_name for item in expected),
        tuple(item.expected.object_name for item in files),
        VerificationContext(plan.dataset.name, plan.scale, "candidate set"),
    )
    if len(files) != len(expected):
        raise _mismatch(plan, "object_count", len(expected), len(files), stage="candidate set")
    verified: list[VerifiedFile] = []
    for supplied, locked in zip(files, expected, strict=True):
        if supplied.expected != locked:
            raise _mismatch(plan, f"object:{locked.object_name}", locked, supplied.expected, stage="candidate")
        context = VerificationContext(
            plan.dataset.name,
            plan.scale,
            "candidate",
            object_name=locked.object_name,
        )
        checked = verify_file(supplied.path, locked, context)
        verify_physical_schema(checked.path, plan.dataset.schemas[locked.schema_id], context)
        verified.append(checked)
    return tuple(verified)


@contextmanager
def _owned_staging(
    parent: Path | None = None,
    *,
    cleanup_notes: list[str] | None = None,
) -> Iterator[Path]:
    owns_parent = parent is None
    if parent is None:
        platform_parent = Path(tempfile.gettempdir())
        status = platform_parent.lstat()
        if not stat.S_ISDIR(status.st_mode) or stat.S_ISLNK(status.st_mode):
            raise ValueError("platform temporary root must be a directory")
        private_parent = Path(tempfile.mkdtemp(prefix="data-eng-lab-publication-", dir=platform_parent))
        private_parent.chmod(0o700)
    else:
        private_parent = Path(parent)
        acquisition._require_trusted_parent(private_parent)
    open_flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_CLOEXEC", 0)
    nofollow = getattr(os, "O_NOFOLLOW", 0)
    parent_fd = os.open(private_parent, open_flags | nofollow)
    root: Path | None = None
    root_fd: int | None = None
    try:
        root = Path(tempfile.mkdtemp(prefix="candidate-", dir=private_parent))
        root_fd = os.open(root.name, open_flags | nofollow, dir_fd=parent_fd)
        status = os.fstat(root_fd)
        identity = (status.st_dev, status.st_ino)
    except BaseException:
        if root is not None:
            try:
                os.rmdir(root.name, dir_fd=parent_fd)
            except OSError:
                pass
        os.close(parent_fd)
        if owns_parent:
            try:
                private_parent.rmdir()
            except OSError:
                pass
        raise
    primary: BaseException | None = None
    try:
        assert root is not None
        yield root
    except BaseException as error:
        primary = error
        raise
    finally:
        cleanup_error: BaseException | None = None
        try:
            assert root_fd is not None and root is not None
            _remove_owned_directory_contents(root_fd)
            _remove_owned_root_atomically(private_parent, parent_fd, root.name, identity)
            if cleanup_notes is not None:
                cleanup_notes.append("owned staging residue retained after descriptor-safe cleanup")
        except BaseException as error:
            cleanup_error = error
        finally:
            if root_fd is not None:
                os.close(root_fd)
            os.close(parent_fd)
        if cleanup_error is not None:
            if primary is None:
                raise cleanup_error
            primary.add_note(f"owned publication staging cleanup failed: {type(cleanup_error).__name__}")
        if owns_parent and primary is not None:
            primary.add_note("private publication staging parent retained for descriptor-safe cleanup")


def _remove_owned_directory_contents(directory_fd: int) -> None:
    """Delete entries only through a retained descriptor for the owned directory."""
    for name in os.listdir(directory_fd):
        status = os.stat(name, dir_fd=directory_fd, follow_symlinks=False)
        if stat.S_ISDIR(status.st_mode):
            child_fd = os.open(
                name,
                os.O_RDONLY
                | getattr(os, "O_DIRECTORY", 0)
                | getattr(os, "O_CLOEXEC", 0)
                | getattr(os, "O_NOFOLLOW", 0),
                dir_fd=directory_fd,
            )
            try:
                opened = os.fstat(child_fd)
                if (opened.st_dev, opened.st_ino) != (status.st_dev, status.st_ino):
                    raise ValueError("publication staging child identity changed")
                _remove_owned_directory_contents(child_fd)
            finally:
                os.close(child_fd)
            os.rmdir(name, dir_fd=directory_fd)
        else:
            os.unlink(name, dir_fd=directory_fd)


def _exchange_directory_names(parent_fd: int, left: str, right: str) -> None:
    """Atomically exchange two sibling names on Linux or Darwin."""
    libc = ctypes.CDLL(None, use_errno=True)
    if sys.platform == "linux":
        exchange = libc.renameat2
    elif sys.platform == "darwin":
        exchange = libc.renameatx_np
    else:
        raise OSError("atomic staging directory exchange is unsupported on this platform")
    exchange.argtypes = [ctypes.c_int, ctypes.c_char_p, ctypes.c_int, ctypes.c_char_p, ctypes.c_uint]
    exchange.restype = ctypes.c_int
    if exchange(parent_fd, os.fsencode(left), parent_fd, os.fsencode(right), 2) != 0:
        error_number = ctypes.get_errno()
        raise OSError(error_number, os.strerror(error_number))


def _remove_owned_root_atomically(
    parent: Path,
    parent_fd: int,
    root_name: str,
    root_identity: tuple[int, int],
) -> None:
    """Quarantine the owned root without path-unlinking either exchanged name."""
    placeholder = Path(tempfile.mkdtemp(prefix="cleanup-", dir=parent))
    placeholder_status = os.stat(placeholder.name, dir_fd=parent_fd, follow_symlinks=False)
    placeholder_identity = (placeholder_status.st_dev, placeholder_status.st_ino)
    try:
        _exchange_directory_names(parent_fd, root_name, placeholder.name)
        displaced = os.stat(placeholder.name, dir_fd=parent_fd, follow_symlinks=False)
        displaced_identity = (displaced.st_dev, displaced.st_ino)
        if displaced_identity != root_identity:
            _exchange_directory_names(parent_fd, root_name, placeholder.name)
            raise ValueError("publication staging identity changed; foreign replacement preserved")
        replacement = os.stat(root_name, dir_fd=parent_fd, follow_symlinks=False)
        if (replacement.st_dev, replacement.st_ino) != placeholder_identity:
            raise ValueError("publication staging cleanup placeholder identity changed")
    except FileNotFoundError as error:
        raise ValueError("publication staging identity changed; owned directory was displaced") from error


def _list_legacy_keys(client, plan: ScalePlan, bucket: str) -> tuple[str, ...]:
    prefix = f"{plan.dataset.landing_prefix}/"
    token: str | None = None
    seen_tokens: set[str] = set()
    keys: list[str] = []
    common_prefix_count = 0
    for _page in range(_MAX_LEGACY_LIST_PAGES):
        request: dict[str, object] = {"Bucket": bucket, "Prefix": prefix, "Delimiter": "/"}
        if token is not None:
            request["ContinuationToken"] = token
        response = client.list_objects_v2(**request)
        if not isinstance(response, Mapping):
            raise AmbiguousWrite("legacy object listing returned a malformed response")
        contents = response.get("Contents", ())
        if not isinstance(contents, (list, tuple)):
            raise AmbiguousWrite("legacy object listing returned malformed contents")
        common_prefixes = response.get("CommonPrefixes", ())
        if not isinstance(common_prefixes, (list, tuple)):
            raise AmbiguousWrite("legacy object listing returned malformed common prefixes")
        for entry in common_prefixes:
            if not isinstance(entry, Mapping) or not isinstance(entry.get("Prefix"), str):
                raise AmbiguousWrite("legacy object listing returned a malformed common prefix")
            common_prefix_count += 1
            if common_prefix_count > _MAX_LEGACY_LIST_KEYS:
                raise AmbiguousWrite("legacy object listing exceeds the bounded common prefix count")
        for entry in contents:
            if not isinstance(entry, Mapping) or not isinstance(entry.get("Key"), str):
                raise AmbiguousWrite("legacy object listing returned a malformed key")
            key = cast(str, entry["Key"])
            if not key.startswith(prefix):
                continue
            suffix = key[len(prefix) :]
            if suffix and "/" not in suffix:
                keys.append(key)
                if len(keys) > _MAX_LEGACY_LIST_KEYS:
                    raise AmbiguousWrite("legacy object listing exceeds the bounded key count")
        truncated = response.get("IsTruncated", False)
        if not isinstance(truncated, bool):
            raise AmbiguousWrite("legacy object listing truncation flag is invalid")
        if not truncated:
            break
        next_token = response.get("NextContinuationToken")
        if not isinstance(next_token, str) or not next_token or next_token in seen_tokens:
            raise AmbiguousWrite("legacy object listing pagination token is invalid")
        seen_tokens.add(next_token)
        token = next_token
    else:
        raise AmbiguousWrite("legacy object listing exceeds the bounded page count")
    if len(set(keys)) != len(keys):
        raise AmbiguousWrite("legacy object listing returned duplicate keys")
    return tuple(keys)


def _legacy_candidates(
    client,
    plan: ScalePlan,
    destination: Path,
    bucket: str,
) -> tuple[VerifiedFile, ...] | None:
    keys = _list_legacy_keys(client, plan, bucket)
    if not keys:
        return None
    expected = _expected_output_files(plan)
    expected_names = tuple(item.object_name for item in expected)
    actual_names = tuple(key.rsplit("/", 1)[-1] for key in keys)
    if set(actual_names) != set(expected_names) or len(actual_names) != len(expected_names):
        raise _mismatch(plan, "legacy_object_names", expected_names, actual_names, stage="legacy")
    key_by_name = dict(zip(actual_names, keys, strict=True))
    results: list[VerifiedFile] = []
    for item in expected:
        path = destination / item.object_name
        path.parent.mkdir(parents=True, exist_ok=True)
        context = VerificationContext(
            plan.dataset.name,
            plan.scale,
            "legacy",
            object_name=item.object_name,
        )
        with path.open("xb") as destination_stream:
            stream_verify_object(
                _CapturingClient(client, destination_stream),
                bucket,
                key_by_name[item.object_name],
                item,
                context,
            )
        verify_physical_schema(path, plan.dataset.schemas[item.schema_id], context)
        results.append(VerifiedFile(path.resolve(strict=True), item))
    return tuple(results)


def _assert_lease_current(client, lease: Lease) -> None:
    try:
        snapshot = read_control_object(client, lease.bucket, lease.key)
    except AmbiguousWrite as error:
        cause = error.__cause__
        if isinstance(cause, ClientError) and _is_missing(cause):
            raise ConditionalConflict("dataset lease has been lost") from error
        raise ConditionalConflict("dataset lease state is ambiguous") from error
    intended = canonical_json(
        {
            "created_at": lease.created_at.astimezone(UTC).isoformat(timespec="seconds").replace("+00:00", "Z"),
            "dataset": lease.dataset,
            "expires_at": lease.expires_at.astimezone(UTC).isoformat(timespec="seconds").replace("+00:00", "Z"),
            "owner_nonce": lease.owner_nonce,
            "publication_id": lease.publication_id,
            "state": "active",
        }
    )
    if snapshot.etag != lease.etag or snapshot.body != intended or snapshot.server_date >= lease.expires_at:
        raise ConditionalConflict("dataset lease has been lost")


def _object_metadata(plan: ScalePlan, publication_id: str, item: ExpectedObject) -> dict[str, str]:
    return {
        "object-name": item.object_name,
        "plan-id": plan_id(plan),
        "publication-id": publication_id,
        "sha256": item.sha256,
        "size-bytes": str(item.size_bytes),
    }


def _verify_exact_immutable(
    client,
    plan: ScalePlan,
    bucket: str,
    key: str,
    item: ExpectedObject,
    metadata: Mapping[str, str],
) -> None:
    context = VerificationContext(plan.dataset.name, plan.scale, "candidate remote", object_name=item.object_name)
    try:
        snapshot = stream_verify_object(client, bucket, key, item, context)
    except LockMismatch as error:
        raise ConditionalConflict("immutable object contains competing bytes") from error
    if dict(snapshot.metadata) != dict(metadata):
        raise ConditionalConflict("immutable object contains competing metadata")


def _put_manifest_exact(client, bucket: str, key: str, body: bytes) -> bool:
    tracking_client = _WriteTrackingClient(client)
    try:
        put_control_object(tracking_client, bucket, key, body, if_none_match=True)
        return tracking_client.write_failed
    except AmbiguousWrite:
        snapshot = read_control_object(client, bucket, key)
        if snapshot.body != body:
            raise ConditionalConflict("immutable manifest contains competing bytes") from None
        return True


def _put_pointer_exact(
    client,
    bucket: str,
    key: str,
    body: bytes,
    state: _PointerState,
) -> bool:
    snapshot = state.snapshot
    tracking_client = _WriteTrackingClient(client)
    try:
        if snapshot is None:
            put_control_object(tracking_client, bucket, key, body, if_none_match=True)
        else:
            put_control_object(tracking_client, bucket, key, body, if_match=snapshot.etag)
        return tracking_client.write_failed
    except AmbiguousWrite:
        observed = read_control_object(client, bucket, key)
        if observed.body != body:
            raise ConditionalConflict("active pointer contains a competing value") from None
        return True


def _build_manifest(
    plan: ScalePlan,
    publication_id: str,
    files: tuple[VerifiedFile, ...],
    active: _PointerState,
    raw_registry_sha256: str,
) -> ImmutableManifest:
    prefix = publication_prefix(plan, publication_id)
    previous_key = active.pointer.manifest_key if active.pointer is not None else None
    previous_digest = active.pointer.manifest_sha256 if active.pointer is not None else None
    return ImmutableManifest(
        format_version=_CONTROL_FORMAT_VERSION,
        dataset=plan.dataset.name,
        scale=plan.scale,
        raw_registry_sha256=_require_sha256(raw_registry_sha256, "raw registry sha256"),
        selected_plan_sha256=plan_id(plan),
        plan_id=plan_id(plan),
        publication_id=publication_id,
        physical_prefix=prefix,
        objects=tuple(
            ManifestObject(
                object_name=item.expected.object_name,
                key=f"{prefix}/{item.expected.object_name}",
                size_bytes=item.expected.size_bytes,
                sha256=item.expected.sha256,
                schema_id=item.expected.schema_id,
                schema_fingerprint=plan.dataset.schemas[item.expected.schema_id].fingerprint,
            )
            for item in files
        ),
        published_at=datetime.now(UTC).replace(microsecond=0),
        previous_manifest_key=previous_key,
        previous_manifest_sha256=previous_digest,
    )


def _publish_candidate(
    plan: ScalePlan,
    client,
    fetcher: Fetcher,
    active: _PointerState,
    *,
    bucket: str,
    raw_registry_sha256: str,
    allow_legacy: bool,
) -> PublishResult:
    publication_id = uuid.uuid4().hex
    owner_nonce = secrets.token_hex(16)
    lease = acquire_lease(client, plan.dataset.name, publication_id, owner_nonce, bucket=bucket)
    keepalive = _LeaseKeepalive(client, lease)
    try:
        keepalive.start()
    except BaseException as start_error:
        try:
            release_lease(client, lease)
        except BaseException as release_error:
            start_error.add_note(f"dataset lease release failed after keepalive startup: {release_error}")
        raise
    completed = False
    primary: BaseException | None = None
    result: PublishResult | None = None
    try:
        active = _read_pointer_state(client, plan, bucket)
        if active.corruption is not None and allow_legacy:
            _verify_pointer_state(client, plan, active, bucket)
        if active.pointer is not None and allow_legacy:
            resolved, manifest = _verify_pointer_state_with_document(client, plan, active, bucket)
            result = replace(
                _result_from_resolved(resolved, "verified-existing"),
                previous_manifest_key=manifest.previous_manifest_key,
                previous_manifest_sha256=manifest.previous_manifest_sha256,
            )
            completed = True
        else:
            result = _stage_and_commit_candidate(
                plan,
                client,
                fetcher,
                active,
                keepalive,
                publication_id,
                bucket=bucket,
                raw_registry_sha256=raw_registry_sha256,
                allow_legacy=allow_legacy,
            )
            completed = True
    except BaseException as error:
        primary = error
        raise
    finally:
        latest_lease: Lease | None = None
        stop_error: BaseException | None = None
        try:
            latest_lease = keepalive.stop()
        except BaseException as error:
            stop_error = error
            if primary is not None:
                primary.add_note(f"dataset keepalive stop failed: {error}")
        try:
            if latest_lease is not None:
                release_lease(client, latest_lease)
            elif stop_error is not None:
                release_lease(client, lease)
        except BaseException as cleanup_error:
            if primary is not None:
                primary.add_note(f"dataset lease release failed: {cleanup_error}")
            elif completed and result is not None:
                result = PublishResult(
                    **{
                        **asdict(result),
                        "cleanup_warning": f"dataset lease release failed: {type(cleanup_error).__name__}",
                    }
                )
            else:
                raise
        if primary is None and stop_error is not None:
            if completed and result is not None:
                result = replace(
                    result,
                    cleanup_warning=f"dataset keepalive stop/release outcome unknown: {type(stop_error).__name__}",
                )
            else:
                raise stop_error
    if result is None:
        raise RuntimeError("publication transaction ended without a result")
    return result


def _stage_and_commit_candidate(
    plan: ScalePlan,
    client,
    fetcher: Fetcher,
    active: _PointerState,
    keepalive: _LeaseKeepalive,
    publication_id: str,
    *,
    bucket: str,
    raw_registry_sha256: str,
    allow_legacy: bool,
) -> PublishResult:
    reconciled = False
    inventory = _MutationInventory([], [], [])
    prefix = publication_prefix(plan, publication_id)
    staging_cleanup_notes: list[str] = []
    try:
        with _owned_staging(cleanup_notes=staging_cleanup_notes) as root:

            def acquire_files() -> tuple[VerifiedFile, ...]:
                files = _legacy_candidates(client, plan, root, bucket) if allow_legacy else None
                return tuple(fetcher(plan, root)) if files is None else files

            files = cast(tuple[VerifiedFile, ...], keepalive.run_while_renewing(acquire_files))
            files = _verify_candidate_files(plan, files)
            staged: list[tuple[VerifiedFile, str, dict[str, str]]] = []
            for file in files:
                keepalive.checkpoint()
                key = f"{prefix}/{file.expected.object_name}"
                metadata = _object_metadata(plan, publication_id, file.expected)
                inventory.attempted_object_keys.append(key)
                tracking_client = _WriteTrackingClient(client)
                try:
                    put_immutable_object(
                        tracking_client,
                        bucket,
                        key,
                        file.path,
                        file.expected,
                        metadata,
                    )
                    inventory.proven_object_keys.append(key)
                    reconciled = tracking_client.write_failed or reconciled
                except AmbiguousWrite:
                    try:
                        _verify_exact_immutable(client, plan, bucket, key, file.expected, metadata)
                    except BaseException:
                        inventory.possible_object_keys.append(key)
                        raise
                    inventory.proven_object_keys.append(key)
                    reconciled = True
                except ConditionalConflict:
                    raise
                except BaseException:
                    inventory.possible_object_keys.append(key)
                    raise
                staged.append((file, key, metadata))
            for file, key, metadata in staged:
                keepalive.checkpoint()
                _verify_exact_immutable(client, plan, bucket, key, file.expected, metadata)
            keepalive.checkpoint()
            manifest = _build_manifest(plan, publication_id, files, active, raw_registry_sha256)
            body = manifest.to_bytes()
            digest = manifest_sha256(body)
            key = immutable_manifest_key(plan.dataset.name, digest)
            inventory.manifest_key = key
            inventory.manifest_digest = digest
            inventory.manifest_outcome = "attempted-ambiguous"
            try:
                reconciled = _put_manifest_exact(client, bucket, key, body) or reconciled
            except AmbiguousWrite:
                inventory.manifest_outcome = "possible"
                raise
            except ConditionalConflict:
                inventory.manifest_outcome = "conflict"
                raise
            except BaseException:
                inventory.manifest_outcome = "possible"
                raise
            inventory.manifest_outcome = "written-unreferenced"
            reread = _read_manifest(client, plan, digest, bucket)
            _validate_manifest_for_plan(reread, plan)
            keepalive.checkpoint()
        keepalive.stop_and_checkpoint()
        pointer = ActivePointer(
            format_version=_CONTROL_FORMAT_VERSION,
            dataset=plan.dataset.name,
            manifest_key=key,
            manifest_sha256=digest,
        )
        inventory.pointer_outcome = "attempted-ambiguous"
        try:
            pointer_reconciled = _put_pointer_exact(
                client,
                bucket,
                active_pointer_key(plan.dataset.name),
                pointer.to_bytes(),
                active,
            )
        except ConditionalConflict:
            inventory.pointer_outcome = "conflict"
            raise
        except BaseException:
            inventory.pointer_outcome = "ambiguous"
            raise
        inventory.pointer_outcome = "committed"
        reconciled = pointer_reconciled or reconciled
        committed = PublishResult(
            dataset=plan.dataset.name,
            scale=plan.scale,
            status="published-reconciled" if reconciled else "published",
            manifest_sha256=digest,
            publication_id=publication_id,
            object_count=len(files),
            previous_manifest_key=active.pointer.manifest_key if active.pointer is not None else None,
            previous_manifest_sha256=active.pointer.manifest_sha256 if active.pointer is not None else None,
            publication_prefix=publication_prefix(plan, publication_id),
            pointer_action="create" if active.snapshot is None else "replace",
            pointer_precondition=(
                "If-None-Match: *" if active.snapshot is None else f"If-Match: {active.snapshot.etag}"
            ),
            attempted_object_keys=tuple(inventory.attempted_object_keys),
            manifest_key=key,
            manifest_outcome="referenced",
            pointer_outcome="committed",
            retained_manifest_keys=(active.pointer.manifest_key,) if active.pointer is not None else (),
            cleanup_warning="; ".join(staging_cleanup_notes) or None,
        )
    except BaseException as error:
        pointer_ambiguous = inventory.pointer_outcome in {"ambiguous", "attempted-ambiguous"}
        pointer_conflict = inventory.pointer_outcome == "conflict"
        noncommit_proven = inventory.pointer_outcome in {"not-attempted"}
        if pointer_conflict:
            noncommit_proven = True
        manifest_outcome = inventory.manifest_outcome
        if pointer_ambiguous and manifest_outcome == "written-unreferenced":
            manifest_outcome = "reference-ambiguous"
        objects_are_manifest_referenced = inventory.manifest_outcome in {
            "written-unreferenced",
            "reference-ambiguous",
        }
        failure = PublishResult(
            dataset=plan.dataset.name,
            scale=plan.scale,
            status=(
                "pointer-outcome-ambiguous"
                if pointer_ambiguous
                else "concurrent-publisher"
                if pointer_conflict
                else "failed-candidate"
            ),
            manifest_sha256=inventory.manifest_digest,
            publication_id=publication_id,
            object_count=len(inventory.attempted_object_keys),
            publication_prefix=prefix,
            pointer_action=(
                "outcome-ambiguous" if pointer_ambiguous else "conflict" if pointer_conflict else "not-committed"
            ),
            pointer_precondition=(
                "If-None-Match: *" if active.snapshot is None else f"If-Match: {active.snapshot.etag}"
            ),
            attempted_object_keys=tuple(inventory.attempted_object_keys),
            proven_orphan_keys=(
                tuple(inventory.proven_object_keys) if noncommit_proven and not objects_are_manifest_referenced else ()
            ),
            possible_object_keys=tuple(inventory.possible_object_keys),
            manifest_key=inventory.manifest_key,
            manifest_outcome=manifest_outcome,
            pointer_outcome="ambiguous" if pointer_ambiguous else inventory.pointer_outcome,
        )
        diagnostic = canonical_json(failure.to_document()).decode("utf-8")
        if not isinstance(error, Exception):
            error.add_note(f"dataset publication diagnostic: {diagnostic}")
            raise
        if not isinstance(error, PublicationFailure) and inventory.attempted_object_keys:
            raise PublicationFailure(failure, error) from error
        raise
    else:
        try:
            return _attach_verified_history(client, plan, manifest, committed, bucket=bucket)
        except Exception as error:
            warning = f"post-commit inventory unavailable: {type(error).__name__}"
            return replace(
                committed,
                cleanup_warning=(f"{committed.cleanup_warning}; {warning}" if committed.cleanup_warning else warning),
                inventory_state="unavailable-warning",
            )
        except BaseException as error:
            diagnostic = canonical_json(committed.to_document()).decode("utf-8")
            error.add_note(f"dataset publication committed before inventory interruption: {diagnostic}")
            raise


def publish_dataset(
    plan: ScalePlan,
    *,
    mode: PublishMode,
    client,
    fetcher: Fetcher,
    rollback_sha256: str | None = None,
    dry_run: bool = False,
    bucket: str = _BUCKET,
    raw_registry_sha256: str | None = None,
) -> PublishResult:
    """Verify, publish, refresh, or roll back one dataset atomically."""
    if not isinstance(plan, ScalePlan):
        raise TypeError("publish_dataset requires a ScalePlan")
    if not isinstance(mode, PublishMode):
        raise TypeError("mode must be a PublishMode")
    if mode is PublishMode.ROLLBACK:
        digest = _require_sha256(rollback_sha256, "rollback manifest digest")
    elif rollback_sha256 is not None:
        raise ValueError("rollback digest is only valid in rollback mode")
    else:
        digest = None
    active = _read_pointer_state(client, plan, bucket)
    if dry_run:
        if mode is PublishMode.VERIFY_ONLY:
            raise ValueError("dry-run and verify-only are redundant")
        if mode is PublishMode.ROLLBACK:
            rollback = _rollback_transaction(
                client,
                plan,
                cast(str, digest),
                bucket=bucket,
                dry_run=True,
            )
            planned = replace(
                _result_from_resolved(rollback.resolved, "dry-run-rollback"),
                pointer_action="replace",
                pointer_outcome="would-replace",
                pointer_precondition=f"If-Match: {rollback.pointer_state.snapshot.etag}",
                previous_manifest_key=rollback.pointer_state.pointer.manifest_key,
                previous_manifest_sha256=rollback.pointer_state.pointer.manifest_sha256,
                retained_manifest_keys=(rollback.pointer_state.pointer.manifest_key,),
                manifest_key=immutable_manifest_key(plan.dataset.name, cast(str, digest)),
                manifest_outcome="would-retain-existing",
                publication_prefix=rollback.manifest.physical_prefix,
            )
            return _attach_verified_history(
                client,
                plan,
                rollback.manifest,
                planned,
                bucket=bucket,
                additional_reachable=((rollback.pointer_state.pointer.manifest_key, rollback.previous_manifest),),
            )
        if mode is PublishMode.REFRESH:
            current_digest = active.pointer.manifest_sha256 if active.pointer is not None else None
            intended_publication = uuid.uuid4().hex
            planned = PublishResult(
                plan.dataset.name,
                plan.scale,
                "dry-run-refresh",
                current_digest,
                intended_publication,
                len(_selected_outputs(plan)),
                previous_manifest_key=active.pointer.manifest_key if active.pointer is not None else None,
                previous_manifest_sha256=current_digest,
                publication_prefix=publication_prefix(plan, intended_publication),
                pointer_action="create" if active.snapshot is None else "replace",
                pointer_precondition=(
                    "If-None-Match: *" if active.snapshot is None else f"If-Match: {active.snapshot.etag}"
                ),
            )
            if active.pointer is not None and active.corruption is None:
                _resolved, current = _verify_pointer_state_with_document(client, plan, active, bucket)
                return _attach_verified_history(client, plan, current, planned, bucket=bucket)
            if active.corruption is not None:
                return replace(
                    planned,
                    inventory_state="unavailable-warning",
                    cleanup_warning="inventory unavailable because the active pointer is corrupt",
                )
            return planned
        if active.pointer is not None or active.corruption is not None:
            resolved, current = _verify_pointer_state_with_document(client, plan, active, bucket)
            return _result_with_verified_history(
                client,
                plan,
                active,
                resolved,
                "dry-run-noop",
                bucket=bucket,
                current=current,
            )
        with tempfile.TemporaryDirectory(prefix="dataset-publication-dry-run-") as temporary:
            legacy = _legacy_candidates(client, plan, Path(temporary), bucket)
        status = "dry-run-legacy-migration" if legacy is not None else "dry-run-initial"
        intended_publication = uuid.uuid4().hex
        return PublishResult(
            plan.dataset.name,
            plan.scale,
            status,
            None,
            intended_publication,
            len(_selected_outputs(plan)),
            publication_prefix=publication_prefix(plan, intended_publication),
            pointer_action="create",
            pointer_precondition="If-None-Match: *",
        )
    if mode is PublishMode.ROLLBACK:
        rollback = _rollback_transaction(
            client,
            plan,
            cast(str, digest),
            bucket=bucket,
        )
        committed = replace(
            _result_from_resolved(
                rollback.resolved,
                "rolled-back-reconciled" if rollback.reconciled else "rolled-back",
            ),
            previous_manifest_key=rollback.pointer_state.pointer.manifest_key,
            previous_manifest_sha256=rollback.pointer_state.pointer.manifest_sha256,
            retained_manifest_keys=(rollback.pointer_state.pointer.manifest_key,),
            pointer_action="replace",
            pointer_outcome="reconciled" if rollback.reconciled else "committed",
            pointer_precondition=f"If-Match: {rollback.pointer_state.snapshot.etag}",
            manifest_key=immutable_manifest_key(plan.dataset.name, cast(str, digest)),
            manifest_outcome="existing-retained",
            publication_prefix=rollback.manifest.physical_prefix,
        )
        try:
            return _attach_verified_history(
                client,
                plan,
                rollback.manifest,
                committed,
                bucket=bucket,
                additional_reachable=((rollback.pointer_state.pointer.manifest_key, rollback.previous_manifest),),
            )
        except Exception as error:
            return replace(
                committed,
                cleanup_warning=f"post-commit inventory unavailable: {type(error).__name__}",
                inventory_state="unavailable-warning",
            )
        except BaseException as error:
            diagnostic = canonical_json(committed.to_document()).decode("utf-8")
            error.add_note(f"dataset rollback committed before inventory interruption: {diagnostic}")
            raise
    if active.pointer is not None or active.corruption is not None:
        if mode is not PublishMode.REFRESH:
            resolved, manifest = _verify_pointer_state_with_document(client, plan, active, bucket)
            return replace(
                _result_from_resolved(resolved, "verified-existing"),
                previous_manifest_key=manifest.previous_manifest_key,
                previous_manifest_sha256=manifest.previous_manifest_sha256,
            )
    elif mode is PublishMode.VERIFY_ONLY:
        _verify_pointer_state(client, plan, active, bucket)
    audit_digest = _require_sha256(raw_registry_sha256, "raw registry sha256")
    return _publish_candidate(
        plan,
        client,
        fetcher,
        active,
        bucket=bucket,
        raw_registry_sha256=audit_digest,
        allow_legacy=mode is PublishMode.DEFAULT,
    )
