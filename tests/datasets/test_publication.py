from __future__ import annotations

import hashlib
import io
import json
from dataclasses import replace
from datetime import UTC, datetime
from email.utils import format_datetime
from pathlib import Path
from types import MappingProxyType

import pytest

from datasets.locking import canonical_json, schema_fingerprint
from datasets.publication import (
    ActivePointer,
    ImmutableManifest,
    ManifestObject,
    active_pointer_key,
    immutable_manifest_key,
    manifest_sha256,
    plan_id,
    publication_prefix,
    resolve_active_dataset,
    rollback_manifest,
    selected_plan_document,
)
from datasets.registry import (
    Dataset,
    HttpArtifact,
    LandingObject,
    Provenance,
    RawArtifact,
    SchemaContract,
    SourceVersion,
    resolve_scale,
)
from datasets.s3 import ConditionalConflict
from datasets.verification import LockMismatch

PUBLICATION_ID = "123e4567e89b42d3a456426614174000"
PREVIOUS_PUBLICATION_ID = "123e4567e89b42d3a456426614174001"
RAW_REGISTRY_SHA256 = "a" * 64


def _sha(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _schema() -> SchemaContract:
    raw = {
        "format": "text",
        "mode": "exact",
        "fields": [],
        "options": {"encoding": "utf-8"},
    }
    return SchemaContract(
        id="readme",
        format="text",
        mode="exact",
        fields=(),
        options=MappingProxyType({"encoding": "utf-8"}),
        fingerprint=schema_fingerprint(raw),
    )


def _dataset(*, description: str = "Selected dataset", payload: bytes = b"hello\n") -> Dataset:
    provenance = Provenance(
        publisher="Example",
        homepage="https://example.test",
        license_name="Example License",
        license_url="https://example.test/license",
        attribution="Example",
        source_stability="immutable",
        update_policy="reviewed-lock-update",
    )
    output = LandingObject(
        object_name="readme.txt",
        size_bytes=len(payload),
        sha256=_sha(payload),
        schema_id="readme",
        member_path=None,
        raw_identity=True,
    )
    artifact = HttpArtifact(
        id="release",
        url="https://example.test/readme.txt",
        version=SourceVersion("revision", "v1"),
        stability="immutable",
        evidence=MappingProxyType({"observed_at": "volatile"}),
        raw=RawArtifact("readme.txt", len(payload), _sha(payload)),
        outputs=(output,),
        provenance=None,
    )
    return Dataset(
        name="sample",
        description=description,
        format="text",
        license="Example License",
        landing_prefix="sample",
        kind="http",
        unzip=False,
        scales=MappingProxyType({"small": ("release",), "medium": ("release",)}),
        provenance=provenance,
        schemas=MappingProxyType({"readme": _schema()}),
        artifacts=MappingProxyType({"release": artifact}),
    )


def _manifest(plan, *, publication_id: str = PUBLICATION_ID) -> ImmutableManifest:
    selected_id = plan_id(plan)
    prefix = publication_prefix(plan, publication_id)
    output = plan.artifacts[0].outputs[0]
    return ImmutableManifest(
        format_version=1,
        dataset=plan.dataset.name,
        scale=plan.scale,
        raw_registry_sha256=RAW_REGISTRY_SHA256,
        selected_plan_sha256=selected_id,
        plan_id=selected_id,
        publication_id=publication_id,
        physical_prefix=prefix,
        objects=(
            ManifestObject(
                object_name=output.object_name,
                key=f"{prefix}/{output.object_name}",
                size_bytes=output.size_bytes,
                sha256=output.sha256,
                schema_id=output.schema_id,
                schema_fingerprint=plan.dataset.schemas[output.schema_id].fingerprint,
            ),
        ),
        published_at=datetime(2026, 8, 12, 12, tzinfo=UTC),
        previous_manifest_key=None,
        previous_manifest_sha256=None,
    )


class Body(io.BytesIO):
    pass


class FakeS3:
    def __init__(self) -> None:
        self.objects: dict[str, tuple[bytes, str, dict[str, str]]] = {}
        self.puts: list[dict[str, object]] = []
        self.gets: list[str] = []
        self.conflict = False

    def seed(self, key: str, body: bytes, *, etag: str, metadata: dict[str, str] | None = None) -> None:
        self.objects[key] = (body, etag, metadata or {})

    def get_object(self, *, Bucket: str, Key: str):
        del Bucket
        self.gets.append(Key)
        if Key not in self.objects:
            from botocore.exceptions import ClientError

            raise ClientError(
                {
                    "Error": {"Code": "NoSuchKey"},
                    "ResponseMetadata": {
                        "HTTPStatusCode": 404,
                        "HTTPHeaders": {"date": format_datetime(datetime.now(UTC), usegmt=True)},
                    },
                },
                "GetObject",
            )
        body, etag, metadata = self.objects[Key]
        return {
            "Body": Body(body),
            "ETag": etag,
            "Metadata": metadata,
            "ResponseMetadata": {"HTTPHeaders": {"date": format_datetime(datetime.now(UTC), usegmt=True)}},
        }

    def put_object(self, **request):
        self.puts.append(request)
        key = str(request["Key"])
        body = request["Body"]
        assert isinstance(body, bytes)
        if self.conflict:
            from botocore.exceptions import ClientError

            raise ClientError(
                {
                    "Error": {"Code": "PreconditionFailed"},
                    "ResponseMetadata": {"HTTPStatusCode": 412},
                },
                "PutObject",
            )
        current = self.objects.get(key)
        if request.get("IfMatch") is not None and (current is None or current[1] != request["IfMatch"]):
            from botocore.exceptions import ClientError

            raise ClientError(
                {
                    "Error": {"Code": "PreconditionFailed"},
                    "ResponseMetadata": {"HTTPStatusCode": 412},
                },
                "PutObject",
            )
        self.objects[key] = (body, '"new-pointer"', {})
        return {
            "ETag": '"new-pointer"',
            "ResponseMetadata": {"HTTPHeaders": {"date": format_datetime(datetime.now(UTC), usegmt=True)}},
        }


def _published_store(plan, manifest: ImmutableManifest | None = None) -> tuple[FakeS3, ImmutableManifest]:
    manifest = manifest or _manifest(plan)
    manifest_body = manifest.to_bytes()
    digest = manifest_sha256(manifest)
    pointer = ActivePointer(
        format_version=1,
        dataset=plan.dataset.name,
        manifest_key=immutable_manifest_key(plan.dataset.name, digest),
        manifest_sha256=digest,
    )
    store = FakeS3()
    store.seed(active_pointer_key(plan.dataset.name), pointer.to_bytes(), etag='"pointer"')
    store.seed(pointer.manifest_key, manifest_body, etag='"manifest"')
    store.seed(manifest.objects[0].key, b"hello\n", etag='"object"')
    return store, manifest


def test_selected_plan_is_exact_canonical_json_and_full_sha256() -> None:
    plan = resolve_scale(_dataset(), "small")
    document = selected_plan_document(plan)
    encoded = canonical_json(document)

    assert encoded == canonical_json(json.loads(encoded))
    assert not encoded.endswith(b"\n")
    assert len(plan_id(plan)) == 64
    assert plan_id(plan) == _sha(encoded)


def test_unrelated_or_volatile_registry_edit_does_not_change_selected_plan_id() -> None:
    plan = resolve_scale(_dataset(), "small")
    artifact = plan.dataset.artifacts["release"]
    changed_artifact = replace(artifact, evidence=MappingProxyType({"observed_at": "later"}))
    changed_dataset = replace(
        plan.dataset,
        description="unrelated prose edit",
        artifacts=MappingProxyType({"release": changed_artifact}),
    )

    assert plan_id(resolve_scale(changed_dataset, "small")) == plan_id(plan)


def test_selected_contract_edit_changes_plan_id() -> None:
    plan = resolve_scale(_dataset(), "small")
    changed = resolve_scale(_dataset(payload=b"different\n"), "small")
    assert plan_id(changed) != plan_id(plan)


def test_selected_source_output_mapping_changes_plan_id() -> None:
    plan = resolve_scale(_dataset(), "small")
    artifact = plan.dataset.artifacts["release"]
    changed_output = replace(artifact.outputs[0], member_path="archive/readme.txt")
    changed_artifact = replace(artifact, outputs=(changed_output,))
    changed_dataset = replace(
        plan.dataset,
        artifacts=MappingProxyType({"release": changed_artifact}),
    )

    assert plan_id(resolve_scale(changed_dataset, "small")) != plan_id(plan)


@pytest.mark.parametrize(
    "publication_id",
    ["", "a" * 31, "A" * 32, "0" * 32, "123e4567e89b12d3a456426614174000"],
)
def test_publication_prefix_rejects_non_uuid4_identifiers(publication_id: str) -> None:
    with pytest.raises(ValueError, match="publication"):
        publication_prefix(resolve_scale(_dataset(), "small"), publication_id)


def test_publication_prefix_uses_safe_deterministic_generation_key() -> None:
    plan = resolve_scale(_dataset(), "small")
    assert publication_prefix(plan, PUBLICATION_ID) == (f"sample/_generations/{plan_id(plan)}/{PUBLICATION_ID}")
    with pytest.raises(ValueError, match="landing prefix"):
        publication_prefix(replace(plan, dataset=replace(plan.dataset, landing_prefix="../bad")), PUBLICATION_ID)


def test_manifest_and_pointer_use_exact_canonical_minimal_models() -> None:
    plan = resolve_scale(_dataset(), "small")
    manifest = _manifest(plan)
    digest = manifest_sha256(manifest)
    assert len(digest) == 64
    assert immutable_manifest_key("sample", digest) == f"_data-eng-locks/manifests/sample/{digest}.json"
    assert ImmutableManifest.from_bytes(manifest.to_bytes()) == manifest

    pointer = ActivePointer(1, "sample", immutable_manifest_key("sample", digest), digest)
    assert set(json.loads(pointer.to_bytes())) == {
        "format_version",
        "dataset",
        "manifest_key",
        "manifest_sha256",
    }
    assert ActivePointer.from_bytes(pointer.to_bytes()) == pointer


@pytest.mark.parametrize("model", ["manifest", "pointer"])
def test_control_models_reject_noncanonical_or_unknown_fields(model: str) -> None:
    plan = resolve_scale(_dataset(), "small")
    value = (
        _manifest(plan)
        if model == "manifest"
        else ActivePointer(
            1,
            "sample",
            "_data-eng-locks/manifests/sample/" + "b" * 64 + ".json",
            "b" * 64,
        )
    )
    parser = type(value).from_bytes
    document = json.loads(value.to_bytes())
    document["unknown"] = True
    with pytest.raises(ValueError, match="fields"):
        parser(canonical_json(document))
    with pytest.raises(ValueError, match="canonical"):
        parser(json.dumps(json.loads(value.to_bytes()), indent=2).encode())


@pytest.mark.parametrize("model", ["manifest", "pointer"])
def test_control_models_reject_boolean_format_versions(model: str) -> None:
    plan = resolve_scale(_dataset(), "small")
    value = (
        _manifest(plan)
        if model == "manifest"
        else ActivePointer(
            1,
            "sample",
            "_data-eng-locks/manifests/sample/" + "b" * 64 + ".json",
            "b" * 64,
        )
    )
    document = json.loads(value.to_bytes())
    document["format_version"] = True
    with pytest.raises(ValueError, match="format version"):
        type(value).from_bytes(canonical_json(document))


def test_raw_registry_hash_is_audit_only() -> None:
    plan = resolve_scale(_dataset(), "small")
    before = _manifest(plan)
    after = replace(before, raw_registry_sha256="b" * 64)
    assert before.plan_id == after.plan_id == plan_id(plan)
    assert manifest_sha256(before) != manifest_sha256(after)


def test_resolver_verifies_complete_remote_generation_in_registry_order(monkeypatch) -> None:
    plan = resolve_scale(_dataset(), "small")
    store, manifest = _published_store(plan)
    inspected: list[Path] = []

    def inspect(path, contract, context):
        assert path.read_bytes() == b"hello\n"
        assert contract.id == "readme"
        assert context.object_name == "readme.txt"
        inspected.append(path)

    monkeypatch.setattr("datasets.publication.verify_physical_schema", inspect)
    result = resolve_active_dataset(store, {"sample": plan.dataset}, "sample", "small")

    assert result.dataset == "sample"
    assert result.scale == "small"
    assert result.plan_id == manifest.plan_id
    assert result.objects[0].object_name == "readme.txt"
    assert result.objects[0].uri == f"s3://landing/{manifest.objects[0].key}"
    assert len(inspected) == 1
    assert store.gets == [
        active_pointer_key("sample"),
        immutable_manifest_key("sample", manifest_sha256(manifest)),
        manifest.objects[0].key,
    ]


def test_resolver_rejects_wrong_expected_scale_before_object_result() -> None:
    small = resolve_scale(_dataset(), "small")
    store, _ = _published_store(small)
    with pytest.raises(LockMismatch, match="scale"):
        resolve_active_dataset(store, {"sample": small.dataset}, "sample", "medium")


@pytest.mark.parametrize("corruption", ["pointer", "manifest", "missing-object"])
def test_resolver_fails_closed_for_corrupt_or_incomplete_remote_state(monkeypatch, corruption: str) -> None:
    plan = resolve_scale(_dataset(), "small")
    store, manifest = _published_store(plan)
    if corruption == "pointer":
        key = active_pointer_key("sample")
        store.objects[key] = (b'{"dataset":"sample"}', '"pointer"', {})
    elif corruption == "manifest":
        key = immutable_manifest_key("sample", manifest_sha256(manifest))
        store.objects[key] = (manifest.to_bytes() + b"\n", '"manifest"', {})
    else:
        del store.objects[manifest.objects[0].key]
    monkeypatch.setattr("datasets.publication.verify_physical_schema", lambda *args: None)
    with pytest.raises((LockMismatch, ValueError, RuntimeError)):
        resolve_active_dataset(store, {"sample": plan.dataset}, "sample", "small")


def test_rollback_accepts_unrelated_registry_edit_and_cas_repoints(monkeypatch) -> None:
    old_plan = resolve_scale(_dataset(), "small")
    old_manifest = _manifest(old_plan, publication_id=PREVIOUS_PUBLICATION_ID)
    current_manifest = replace(
        _manifest(old_plan),
        previous_manifest_key=immutable_manifest_key("sample", manifest_sha256(old_manifest)),
        previous_manifest_sha256=manifest_sha256(old_manifest),
    )
    store, _ = _published_store(old_plan, current_manifest)
    store.seed(
        immutable_manifest_key("sample", manifest_sha256(old_manifest)),
        old_manifest.to_bytes(),
        etag='"old-manifest"',
    )
    store.seed(old_manifest.objects[0].key, b"hello\n", etag='"old-object"')
    unrelated_edit = replace(old_plan.dataset, description="new prose")
    monkeypatch.setattr("datasets.publication.verify_physical_schema", lambda *args: None)

    resolved = rollback_manifest(
        store,
        {"sample": unrelated_edit},
        "sample",
        "small",
        manifest_sha256(old_manifest),
    )

    assert resolved.publication_id == PREVIOUS_PUBLICATION_ID
    assert store.puts[-1]["IfMatch"] == '"pointer"'


def test_rollback_rejects_scale_or_selected_plan_change_before_pointer_write(monkeypatch) -> None:
    plan = resolve_scale(_dataset(), "small")
    store, manifest = _published_store(plan)
    monkeypatch.setattr("datasets.publication.verify_physical_schema", lambda *args: None)
    with pytest.raises(LockMismatch):
        rollback_manifest(store, {"sample": plan.dataset}, "sample", "medium", manifest_sha256(manifest))
    assert store.puts == []

    changed = _dataset(payload=b"different\n")
    with pytest.raises(LockMismatch, match="plan_id"):
        rollback_manifest(store, {"sample": changed}, "sample", "small", manifest_sha256(manifest))
    assert store.puts == []


def test_rollback_surfaces_concurrent_pointer_conflict(monkeypatch) -> None:
    plan = resolve_scale(_dataset(), "small")
    historical = _manifest(plan, publication_id=PREVIOUS_PUBLICATION_ID)
    store, _ = _published_store(plan)
    store.seed(
        immutable_manifest_key("sample", manifest_sha256(historical)),
        historical.to_bytes(),
        etag='"historical"',
    )
    store.seed(historical.objects[0].key, b"hello\n", etag='"historical-object"')
    store.conflict = True
    monkeypatch.setattr("datasets.publication.verify_physical_schema", lambda *args: None)
    with pytest.raises(ConditionalConflict):
        rollback_manifest(
            store,
            {"sample": plan.dataset},
            "sample",
            "small",
            manifest_sha256(historical),
        )
