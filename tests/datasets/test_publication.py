from __future__ import annotations

import hashlib
import inspect
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
    PublishMode,
    active_pointer_key,
    immutable_manifest_key,
    manifest_sha256,
    plan_id,
    publication_prefix,
    publish_dataset,
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
from datasets.s3 import AmbiguousWrite, ConditionalConflict, Lease
from datasets.verification import ExpectedObject, LockMismatch, VerifiedFile

PUBLICATION_ID = "123e4567e89b42d3a456426614174000"
PREVIOUS_PUBLICATION_ID = "123e4567e89b42d3a456426614174001"
RAW_REGISTRY_SHA256 = "a" * 64


def test_publish_mode_exposes_the_four_transaction_actions() -> None:
    assert tuple(mode.value for mode in PublishMode) == (
        "default",
        "verify-only",
        "refresh",
        "rollback",
    )


def test_publish_dataset_requires_a_verified_existing_pointer_without_mutation() -> None:
    plan = resolve_scale(_dataset(), "small")
    store, manifest = _published_store(plan)

    result = publish_dataset(plan, mode=PublishMode.DEFAULT, client=store, fetcher=lambda *_: ())

    assert result.status == "verified-existing"
    assert result.manifest_sha256 == manifest_sha256(manifest)
    assert store.puts == []


def _publication_lease(store: FakeS3, plan, publication_id: str, owner_nonce: str) -> Lease:
    now = datetime.now(UTC).replace(microsecond=0)
    key = f"_data-eng-locks/leases/{plan.dataset.name}.json"
    lease = Lease(
        dataset=plan.dataset.name,
        publication_id=publication_id,
        owner_nonce=owner_nonce,
        state="active",
        created_at=now,
        expires_at=now.replace(year=now.year + 1),
        etag='"lease"',
        bucket="landing",
        key=key,
    )
    store.seed(
        key,
        canonical_json(
            {
                "created_at": lease.created_at.isoformat(timespec="seconds").replace("+00:00", "Z"),
                "dataset": lease.dataset,
                "expires_at": lease.expires_at.isoformat(timespec="seconds").replace("+00:00", "Z"),
                "owner_nonce": lease.owner_nonce,
                "publication_id": lease.publication_id,
                "state": "active",
            }
        ),
        etag=lease.etag,
    )
    return lease


def test_first_publication_uploads_generation_manifest_then_pointer(tmp_path, monkeypatch) -> None:
    plan = resolve_scale(_dataset(), "small")
    store = FakeS3()
    payload = b"hello\n"
    source = tmp_path / "readme.txt"
    source.write_bytes(payload)
    expected = ExpectedObject("readme.txt", len(payload), _sha(payload), "readme")
    release_calls: list[Lease] = []

    def acquire(client, dataset, publication_id, owner_nonce, *, bucket):
        del client, dataset, bucket
        return _publication_lease(store, plan, publication_id, owner_nonce)

    monkeypatch.setattr("datasets.publication.acquire_lease", acquire)
    monkeypatch.setattr("datasets.publication.release_lease", lambda client, lease: release_calls.append(lease))
    monkeypatch.setattr("datasets.publication.verify_physical_schema", lambda *args: None)

    result = publish_dataset(
        plan,
        mode=PublishMode.DEFAULT,
        client=store,
        fetcher=lambda *_: (VerifiedFile(source, expected),),
        raw_registry_sha256=RAW_REGISTRY_SHA256,
    )

    assert result.status == "published"
    mutation_keys = [str(request["Key"]) for request in store.puts]
    assert "/_generations/" in mutation_keys[0]
    assert mutation_keys[-2].startswith("_data-eng-locks/manifests/sample/")
    assert mutation_keys[-1] == active_pointer_key("sample")
    assert store.puts[-1]["IfNoneMatch"] == "*"
    assert release_calls


def test_refresh_of_corrupt_pointer_preserves_observed_etag(tmp_path, monkeypatch) -> None:
    plan = resolve_scale(_dataset(), "small")
    store = FakeS3()
    store.seed(active_pointer_key("sample"), b'{"corrupt":true}', etag='"corrupt-etag"')
    source = tmp_path / "readme.txt"
    source.write_bytes(b"hello\n")
    expected = ExpectedObject("readme.txt", 6, _sha(b"hello\n"), "readme")

    def acquire(client, dataset, publication_id, owner_nonce, *, bucket):
        del client, dataset, bucket
        return _publication_lease(store, plan, publication_id, owner_nonce)

    monkeypatch.setattr("datasets.publication.acquire_lease", acquire)
    monkeypatch.setattr("datasets.publication.release_lease", lambda *_: None)
    monkeypatch.setattr("datasets.publication.verify_physical_schema", lambda *args: None)

    result = publish_dataset(
        plan,
        mode=PublishMode.REFRESH,
        client=store,
        fetcher=lambda *_: (VerifiedFile(source, expected),),
        raw_registry_sha256=RAW_REGISTRY_SHA256,
    )

    assert result.status == "published"
    assert store.puts[-1]["Key"] == active_pointer_key("sample")
    assert store.puts[-1]["IfMatch"] == '"corrupt-etag"'


def test_dry_run_missing_pointer_performs_no_lease_source_or_write(monkeypatch) -> None:
    plan = resolve_scale(_dataset(), "small")
    store = FakeS3()
    monkeypatch.setattr("datasets.publication.acquire_lease", lambda *args, **kwargs: pytest.fail("lease"))

    result = publish_dataset(
        plan,
        mode=PublishMode.DEFAULT,
        client=store,
        fetcher=lambda *_: pytest.fail("source"),
        dry_run=True,
    )

    assert result.status == "dry-run-initial"
    assert store.puts == []


def test_exact_legacy_set_migrates_without_source_request(tmp_path, monkeypatch) -> None:
    plan = resolve_scale(_dataset(), "small")
    store = FakeS3()
    store.seed("sample/readme.txt", b"hello\n", etag='"legacy"')

    def acquire(client, dataset, publication_id, owner_nonce, *, bucket):
        del client, dataset, bucket
        return _publication_lease(store, plan, publication_id, owner_nonce)

    monkeypatch.setattr("datasets.publication.acquire_lease", acquire)
    monkeypatch.setattr("datasets.publication.release_lease", lambda *_: None)
    monkeypatch.setattr("datasets.publication.verify_physical_schema", lambda *args: None)

    result = publish_dataset(
        plan,
        mode=PublishMode.DEFAULT,
        client=store,
        fetcher=lambda *_: pytest.fail("exact legacy migration must not fetch upstream"),
        raw_registry_sha256=RAW_REGISTRY_SHA256,
    )

    assert result.status == "published"
    assert store.objects["sample/readme.txt"][0] == b"hello\n"
    assert store.puts[-1]["Key"] == active_pointer_key("sample")


def test_lost_pointer_response_reconciles_exact_self_commit(tmp_path, monkeypatch) -> None:
    plan = resolve_scale(_dataset(), "small")
    store = FakeS3()
    source = tmp_path / "readme.txt"
    source.write_bytes(b"hello\n")
    expected = ExpectedObject("readme.txt", 6, _sha(b"hello\n"), "readme")

    def acquire(client, dataset, publication_id, owner_nonce, *, bucket):
        del client, dataset, bucket
        return _publication_lease(store, plan, publication_id, owner_nonce)

    from datasets import publication as publication_module

    original_put = publication_module.put_control_object

    def lost_response(client, bucket, key, body, **conditions):
        snapshot = original_put(client, bucket, key, body, **conditions)
        if key == active_pointer_key("sample"):
            raise AmbiguousWrite("response lost after pointer commit")
        return snapshot

    monkeypatch.setattr("datasets.publication.acquire_lease", acquire)
    monkeypatch.setattr("datasets.publication.release_lease", lambda *_: None)
    monkeypatch.setattr("datasets.publication.put_control_object", lost_response)
    monkeypatch.setattr("datasets.publication.verify_physical_schema", lambda *args: None)

    result = publish_dataset(
        plan,
        mode=PublishMode.DEFAULT,
        client=store,
        fetcher=lambda *_: (VerifiedFile(source, expected),),
        raw_registry_sha256=RAW_REGISTRY_SHA256,
    )

    assert result.status == "published-reconciled"
    pointer = ActivePointer.from_bytes(store.objects[active_pointer_key("sample")][0])
    assert pointer.manifest_sha256 == result.manifest_sha256


def test_lost_generation_object_response_is_reported_as_reconciled(tmp_path, monkeypatch) -> None:
    plan = resolve_scale(_dataset(), "small")
    store = FakeS3()
    store.lose_response_suffix = "/readme.txt"
    source = tmp_path / "readme.txt"
    source.write_bytes(b"hello\n")
    expected = ExpectedObject("readme.txt", 6, _sha(b"hello\n"), "readme")

    def acquire(client, dataset, publication_id, owner_nonce, *, bucket):
        del client, dataset, bucket
        return _publication_lease(store, plan, publication_id, owner_nonce)

    monkeypatch.setattr("datasets.publication.acquire_lease", acquire)
    monkeypatch.setattr("datasets.publication.release_lease", lambda *_: None)
    monkeypatch.setattr("datasets.publication.verify_physical_schema", lambda *args: None)

    result = publish_dataset(
        plan,
        mode=PublishMode.DEFAULT,
        client=store,
        fetcher=lambda *_: (VerifiedFile(source, expected),),
        raw_registry_sha256=RAW_REGISTRY_SHA256,
    )

    assert result.status == "published-reconciled"


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
    outputs = tuple(output for artifact in plan.artifacts for output in artifact.outputs)
    return ImmutableManifest(
        format_version=1,
        dataset=plan.dataset.name,
        scale=plan.scale,
        raw_registry_sha256=RAW_REGISTRY_SHA256,
        selected_plan_sha256=selected_id,
        plan_id=selected_id,
        publication_id=publication_id,
        physical_prefix=prefix,
        objects=tuple(
            ManifestObject(
                object_name=output.object_name,
                key=f"{prefix}/{output.object_name}",
                size_bytes=output.size_bytes,
                sha256=output.sha256,
                schema_id=output.schema_id,
                schema_fingerprint=plan.dataset.schemas[output.schema_id].fingerprint,
            )
            for output in outputs
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
        self.body_overrides: dict[str, object] = {}
        self.lose_response_suffix: str | None = None

    def list_objects_v2(self, *, Bucket: str, Prefix: str, ContinuationToken: str | None = None):
        del Bucket, ContinuationToken
        return {
            "Contents": [{"Key": key} for key in sorted(self.objects) if key.startswith(Prefix)],
            "IsTruncated": False,
        }

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
            "Body": self.body_overrides.get(Key, Body(body)),
            "ETag": etag,
            "Metadata": metadata,
            "ResponseMetadata": {"HTTPHeaders": {"date": format_datetime(datetime.now(UTC), usegmt=True)}},
        }

    def put_object(self, **request):
        self.puts.append(request)
        key = str(request["Key"])
        body = request["Body"]
        if hasattr(body, "read"):
            body = body.read()
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
        if request.get("IfNoneMatch") == "*" and current is not None:
            from botocore.exceptions import ClientError

            raise ClientError(
                {
                    "Error": {"Code": "PreconditionFailed"},
                    "ResponseMetadata": {"HTTPStatusCode": 412},
                },
                "PutObject",
            )
        metadata = dict(request.get("Metadata", {}))
        etag = f'"new-{len(self.puts)}"'
        self.objects[key] = (body, etag, metadata)
        if self.lose_response_suffix is not None and key.endswith(self.lose_response_suffix):
            from botocore.exceptions import ClientError

            self.lose_response_suffix = None
            raise ClientError(
                {
                    "Error": {"Code": "InternalError"},
                    "ResponseMetadata": {"HTTPStatusCode": 500},
                },
                "PutObject",
            )
        return {
            "ETag": etag,
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
    payloads = {"readme.txt": b"hello\n", "alpha.txt": b"alpha\n", "beta.txt": b"beta\n"}
    for item in manifest.objects:
        store.seed(item.key, payloads[item.object_name], etag=f'"{item.object_name}"')
    return store, manifest


def _multi_dataset() -> Dataset:
    dataset = _dataset()
    artifacts: dict[str, HttpArtifact] = {}
    for index, (name, payload) in enumerate((("alpha.txt", b"alpha\n"), ("beta.txt", b"beta\n"))):
        output = LandingObject(name, len(payload), _sha(payload), "readme", None, True)
        artifact_id = f"release-{index}"
        artifacts[artifact_id] = replace(
            dataset.artifacts["release"],
            id=artifact_id,
            url=f"https://example.test/{name}",
            raw=RawArtifact(name, len(payload), _sha(payload)),
            outputs=(output,),
        )
    return replace(
        dataset,
        artifacts=MappingProxyType(artifacts),
        scales=MappingProxyType({"small": tuple(artifacts), "medium": tuple(artifacts)}),
    )


def test_selected_plan_is_exact_canonical_json_and_full_sha256() -> None:
    plan = resolve_scale(_dataset(), "small")
    document = selected_plan_document(plan)
    encoded = canonical_json(document)

    assert encoded == canonical_json(json.loads(encoded))
    assert not encoded.endswith(b"\n")
    assert len(plan_id(plan)) == 64
    assert plan_id(plan) == _sha(encoded)


def test_selected_plan_uses_one_fixed_global_lock_policy_api() -> None:
    plan = resolve_scale(_dataset(), "small")
    assert "lock_policy" not in inspect.signature(selected_plan_document).parameters
    assert selected_plan_document(plan)["lock"] == {
        "algorithm": "sha256",
        "object_drift": "fail",
        "schema_fingerprint": "sha256-canonical-json",
        "source_drift": "fail",
        "update_policy": "reviewed-lock-update",
    }
    with pytest.raises(TypeError, match="ScalePlan"):
        plan_id({"lock": {"alternate": True}})


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


@pytest.mark.parametrize("landing_prefix", ["_data-eng-locks", "_data-eng-locks/nested"])
def test_publication_prefix_rejects_global_control_namespace(landing_prefix: str) -> None:
    plan = resolve_scale(_dataset(), "small")
    with pytest.raises(ValueError, match="control namespace"):
        publication_prefix(
            replace(plan, dataset=replace(plan.dataset, landing_prefix=landing_prefix)),
            PUBLICATION_ID,
        )


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


def test_manifest_timestamp_round_trips_and_rejects_noncanonical_instants() -> None:
    plan = resolve_scale(_dataset(), "small")
    manifest = _manifest(plan)
    assert ImmutableManifest.from_bytes(manifest.to_bytes()) == manifest
    with pytest.raises(ValueError, match="whole-second"):
        replace(manifest, published_at=datetime(2026, 8, 12, 12, 0, 0, 1, tzinfo=UTC))

    for spelling in (
        "2026-08-12 12:00:00Z",
        "2026-08-12T12:00:00+00:00",
        "2026-08-12T12:00:00.000000Z",
    ):
        document = json.loads(manifest.to_bytes())
        document["published_at"] = spelling
        with pytest.raises(ValueError, match="canonical"):
            ImmutableManifest.from_bytes(canonical_json(document))


def test_manifest_constructor_requires_deeply_immutable_typed_objects() -> None:
    manifest = _manifest(resolve_scale(_dataset(), "small"))
    with pytest.raises(ValueError, match="tuple"):
        replace(manifest, objects=list(manifest.objects))
    with pytest.raises(ValueError, match="ManifestObject"):
        replace(manifest, objects=({"object_name": "mutable"},))


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


@pytest.mark.parametrize(
    ("corruption", "expected_stage", "expected_field"),
    [("pointer", "pointer", "pointer"), ("manifest", "manifest", "manifest")],
)
def test_resolver_contextualizes_corrupt_control_state(
    monkeypatch, corruption: str, expected_stage: str, expected_field: str
) -> None:
    plan = resolve_scale(_dataset(), "small")
    store, manifest = _published_store(plan)
    if corruption == "pointer":
        key = active_pointer_key("sample")
        store.objects[key] = (b'{"dataset":"sample"}', '"pointer"', {})
    elif corruption == "manifest":
        key = immutable_manifest_key("sample", manifest_sha256(manifest))
        store.objects[key] = (manifest.to_bytes() + b"\n", '"manifest"', {})
    monkeypatch.setattr("datasets.publication.verify_physical_schema", lambda *args: None)
    with pytest.raises(LockMismatch) as caught:
        resolve_active_dataset(store, {"sample": plan.dataset}, "sample", "small")
    assert caught.value.context.dataset == "sample"
    assert caught.value.context.scale == "small"
    assert caught.value.context.stage == expected_stage
    assert caught.value.field == expected_field


@pytest.mark.parametrize("missing", ["pointer", "manifest", "object"])
def test_resolver_reports_missing_remote_state_as_typed_storage_failure(monkeypatch, missing: str) -> None:
    plan = resolve_scale(_dataset(), "small")
    store, manifest = _published_store(plan)
    keys = {
        "pointer": active_pointer_key("sample"),
        "manifest": immutable_manifest_key("sample", manifest_sha256(manifest)),
        "object": manifest.objects[0].key,
    }
    del store.objects[keys[missing]]
    monkeypatch.setattr("datasets.publication.verify_physical_schema", lambda *args: None)
    with pytest.raises(AmbiguousWrite):
        resolve_active_dataset(store, {"sample": plan.dataset}, "sample", "small")


def test_manifest_digest_corruption_has_manifest_context(monkeypatch) -> None:
    plan = resolve_scale(_dataset(), "small")
    store, manifest = _published_store(plan)
    key = immutable_manifest_key("sample", manifest_sha256(manifest))
    changed = replace(manifest, raw_registry_sha256="b" * 64).to_bytes()
    store.objects[key] = (changed, '"manifest"', {})
    monkeypatch.setattr("datasets.publication.verify_physical_schema", lambda *args: None)
    with pytest.raises(LockMismatch) as caught:
        resolve_active_dataset(store, {"sample": plan.dataset}, "sample", "small")
    assert caught.value.context.stage == "manifest"
    assert caught.value.field == "manifest"


def _nested_control_body(depth: int = 1500) -> bytes:
    return (b'{"nested":' * depth) + b"null" + (b"}" * depth)


@pytest.mark.parametrize("control", ["pointer", "manifest"])
def test_deeply_nested_control_decode_is_contextual_corruption(monkeypatch, control: str) -> None:
    plan = resolve_scale(_dataset(), "small")
    store, manifest = _published_store(plan)
    key = (
        active_pointer_key("sample")
        if control == "pointer"
        else immutable_manifest_key("sample", manifest_sha256(manifest))
    )
    body = _nested_control_body()
    assert len(body) < 1 << 20
    if control == "manifest":
        digest = _sha(body)
        pointer = ActivePointer(1, "sample", immutable_manifest_key("sample", digest), digest)
        store.seed(active_pointer_key("sample"), pointer.to_bytes(), etag='"deep-manifest-pointer"')
        key = pointer.manifest_key
    store.objects[key] = (body, f'"deep-{control}"', {})
    monkeypatch.setattr("datasets.publication.verify_physical_schema", lambda *args: None)

    with pytest.raises(LockMismatch) as caught:
        resolve_active_dataset(store, {"sample": plan.dataset}, "sample", "small")
    assert caught.value.context.dataset == "sample"
    assert caught.value.context.scale == "small"
    assert caught.value.context.stage == control
    assert caught.value.field == control


@pytest.mark.parametrize("control", ["pointer", "manifest"])
def test_deeply_nested_control_reencode_is_contextual_corruption(monkeypatch, control: str) -> None:
    plan = resolve_scale(_dataset(), "small")
    store, manifest = _published_store(plan)
    key = (
        active_pointer_key("sample")
        if control == "pointer"
        else immutable_manifest_key("sample", manifest_sha256(manifest))
    )
    document: dict[str, object] = {"leaf": None}
    for _ in range(1500):
        document = {"nested": document}
    original_loads = json.loads

    def loads(value, *args, **kwargs):
        if value == b"{}":
            return document
        return original_loads(value, *args, **kwargs)

    monkeypatch.setattr("datasets.publication.json.loads", loads)
    if control == "manifest":
        digest = _sha(b"{}")
        pointer = ActivePointer(1, "sample", immutable_manifest_key("sample", digest), digest)
        store.seed(active_pointer_key("sample"), pointer.to_bytes(), etag='"reencode-manifest-pointer"')
        key = pointer.manifest_key
    store.objects[key] = (b"{}", f'"reencode-{control}"', {})
    monkeypatch.setattr("datasets.publication.verify_physical_schema", lambda *args: None)

    with pytest.raises(LockMismatch) as caught:
        resolve_active_dataset(store, {"sample": plan.dataset}, "sample", "small")
    assert caught.value.context.stage == control
    assert caught.value.field == control


def test_multi_object_resolution_preserves_registry_order_and_rejects_reversal(monkeypatch) -> None:
    plan = resolve_scale(_multi_dataset(), "small")
    store, manifest = _published_store(plan)
    monkeypatch.setattr("datasets.publication.verify_physical_schema", lambda *args: None)
    resolved = resolve_active_dataset(store, {"sample": plan.dataset}, "sample", "small")
    assert tuple(item.object_name for item in resolved.objects) == ("alpha.txt", "beta.txt")

    reversed_manifest = replace(manifest, objects=tuple(reversed(manifest.objects)))
    reversed_digest = manifest_sha256(reversed_manifest)
    pointer = ActivePointer(1, "sample", immutable_manifest_key("sample", reversed_digest), reversed_digest)
    store.seed(active_pointer_key("sample"), pointer.to_bytes(), etag='"reversed-pointer"')
    store.seed(pointer.manifest_key, reversed_manifest.to_bytes(), etag='"reversed-manifest"')
    with pytest.raises(LockMismatch) as caught:
        resolve_active_dataset(store, {"sample": plan.dataset}, "sample", "small")
    assert caught.value.field == "object_names"


class TrackingMalformedBody:
    def __init__(self) -> None:
        self.close_calls = 0

    def close(self) -> None:
        self.close_calls += 1


class NonBinaryBody(TrackingMalformedBody):
    def read(self, _size: int = -1) -> str:
        return "not bytes"


def test_malformed_remote_body_is_typed_and_closed_exactly_once(monkeypatch) -> None:
    plan = resolve_scale(_dataset(), "small")
    store, manifest = _published_store(plan)
    body = TrackingMalformedBody()
    store.body_overrides[manifest.objects[0].key] = body
    monkeypatch.setattr("datasets.publication.verify_physical_schema", lambda *args: None)
    with pytest.raises(AmbiguousWrite, match="body"):
        resolve_active_dataset(store, {"sample": plan.dataset}, "sample", "small")
    assert body.close_calls == 1


def test_nonbinary_remote_body_is_typed_and_closed_exactly_once(monkeypatch) -> None:
    plan = resolve_scale(_dataset(), "small")
    store, manifest = _published_store(plan)
    body = NonBinaryBody()
    store.body_overrides[manifest.objects[0].key] = body
    monkeypatch.setattr("datasets.publication.verify_physical_schema", lambda *args: None)
    with pytest.raises(AmbiguousWrite, match="non-bytes"):
        resolve_active_dataset(store, {"sample": plan.dataset}, "sample", "small")
    assert body.close_calls == 1


class FailingCapture:
    name = "/owned/failing-capture"

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def write(self, _value) -> int:
        raise OSError("disk full")

    def flush(self) -> None:
        pass


def test_capture_io_failure_is_typed_closes_body_and_returns_no_partial(monkeypatch) -> None:
    plan = resolve_scale(_dataset(), "small")
    store, manifest = _published_store(plan)
    body = Body(b"hello\n")
    close_calls = 0
    original_close = body.close

    def close_once() -> None:
        nonlocal close_calls
        close_calls += 1
        original_close()

    body.close = close_once  # type: ignore[method-assign]
    store.body_overrides[manifest.objects[0].key] = body
    monkeypatch.setattr("datasets.publication.tempfile.NamedTemporaryFile", lambda **_kwargs: FailingCapture())
    with pytest.raises(AmbiguousWrite, match="capture"):
        resolve_active_dataset(store, {"sample": plan.dataset}, "sample", "small")
    assert close_calls == 1


def test_capture_allocation_failure_is_typed_and_reads_no_object(monkeypatch) -> None:
    plan = resolve_scale(_dataset(), "small")
    store, manifest = _published_store(plan)

    def fail_allocation(**_kwargs):
        raise OSError("no local storage")

    monkeypatch.setattr("datasets.publication.tempfile.NamedTemporaryFile", fail_allocation)
    with pytest.raises(AmbiguousWrite, match="capture"):
        resolve_active_dataset(store, {"sample": plan.dataset}, "sample", "small")
    assert manifest.objects[0].key not in store.gets


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


def test_rollback_to_current_selected_plan_after_active_scale_changed(monkeypatch) -> None:
    dataset = _multi_dataset()
    small_plan = resolve_scale(dataset, "small")
    target = _manifest(small_plan, publication_id=PREVIOUS_PUBLICATION_ID)
    medium_plan = resolve_scale(dataset, "medium")
    active = _manifest(medium_plan)
    store, _ = _published_store(medium_plan, active)
    store.seed(
        immutable_manifest_key("sample", manifest_sha256(target)),
        target.to_bytes(),
        etag='"target"',
    )
    for item, payload in zip(target.objects, (b"alpha\n", b"beta\n"), strict=True):
        store.seed(item.key, payload, etag=f'"{item.object_name}"')
    monkeypatch.setattr("datasets.publication.verify_physical_schema", lambda *args: None)

    result = rollback_manifest(
        store,
        {"sample": dataset},
        "sample",
        "small",
        manifest_sha256(target),
    )
    assert result.scale == "small"
    assert tuple(item.object_name for item in result.objects) == ("alpha.txt", "beta.txt")
    assert store.puts[-1]["IfMatch"] == '"pointer"'


def test_rollback_uses_etag_from_corrupt_current_pointer(monkeypatch) -> None:
    plan = resolve_scale(_dataset(), "small")
    target = _manifest(plan, publication_id=PREVIOUS_PUBLICATION_ID)
    store, _ = _published_store(plan)
    store.seed(active_pointer_key("sample"), b'{"corrupt":true}', etag='"corrupt-etag"')
    store.seed(
        immutable_manifest_key("sample", manifest_sha256(target)),
        target.to_bytes(),
        etag='"target"',
    )
    store.seed(target.objects[0].key, b"hello\n", etag='"target-object"')
    monkeypatch.setattr("datasets.publication.verify_physical_schema", lambda *args: None)

    rollback_manifest(
        store,
        {"sample": plan.dataset},
        "sample",
        "small",
        manifest_sha256(target),
    )
    assert store.puts[-1]["IfMatch"] == '"corrupt-etag"'


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
    plan = resolve_scale(_multi_dataset(), "small")
    historical = _manifest(plan, publication_id=PREVIOUS_PUBLICATION_ID)
    store, _ = _published_store(plan)
    store.seed(
        immutable_manifest_key("sample", manifest_sha256(historical)),
        historical.to_bytes(),
        etag='"historical"',
    )
    for item, payload in zip(historical.objects, (b"alpha\n", b"beta\n"), strict=True):
        store.seed(item.key, payload, etag=f'"{item.object_name}"')
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
