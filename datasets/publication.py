"""Canonical immutable publication models and verified dataset resolution."""

from __future__ import annotations

import hashlib
import json
import re
import tempfile
from collections.abc import Mapping
from dataclasses import asdict, dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import MappingProxyType
from typing import BinaryIO, cast

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
from datasets.s3 import AmbiguousWrite, put_control_object, read_control_object, stream_verify_object
from datasets.schema_inspection import verify_physical_schema
from datasets.verification import ExpectedObject, LockMismatch, VerificationContext

_BUCKET = "landing"
_PUBLICATION_LAYOUT_VERSION = 1
_CONTROL_FORMAT_VERSION = 1
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_UUID4_HEX_RE = re.compile(r"^[0-9a-f]{12}4[0-9a-f]{3}[89ab][0-9a-f]{15}$")
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
    except (UnicodeDecodeError, ValueError, json.JSONDecodeError) as error:
        raise ValueError(f"{label} must be valid canonical JSON") from error
    if not isinstance(document, dict):
        raise ValueError(f"{label} must be a JSON mapping")
    if canonical_json(document) != body:
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
    manifest = _read_manifest(client, plan, digest, bucket)
    _validate_manifest_for_plan(manifest, plan)
    objects = tuple(_verify_resolved_object(client, plan, item, bucket) for item in manifest.objects)
    return ResolvedDataset(
        dataset=plan.dataset.name,
        scale=plan.scale,
        plan_id=manifest.plan_id,
        manifest_sha256=digest,
        publication_id=manifest.publication_id,
        objects=objects,
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
    current = read_control_object(client, bucket, active_pointer_key(dataset_id))
    resolved = _resolve_manifest(client, plan, digest, bucket=bucket)
    pointer = ActivePointer(
        format_version=_CONTROL_FORMAT_VERSION,
        dataset=dataset_id,
        manifest_key=immutable_manifest_key(dataset_id, digest),
        manifest_sha256=digest,
    )
    put_control_object(
        client,
        bucket,
        active_pointer_key(dataset_id),
        pointer.to_bytes(),
        if_match=current.etag,
    )
    return resolved
