from __future__ import annotations

import hashlib
import inspect
import io
import json
import os
import threading
import time
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from email.utils import format_datetime
from pathlib import Path
from types import MappingProxyType

import pytest

from datasets.locking import canonical_json, schema_fingerprint
from datasets.publication import (
    ActivePointer,
    ImmutableManifest,
    ManifestObject,
    PublicationFailure,
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
    assert len(store.gets) == len(manifest.objects) + 2


def test_default_exact_reuse_does_not_list_inventory(monkeypatch) -> None:
    plan = resolve_scale(_dataset(), "small")
    store, _manifest_value = _published_store(plan)
    monkeypatch.setattr(store, "list_objects_v2", lambda **request: pytest.fail(f"inventory: {request}"))
    monkeypatch.setattr("datasets.publication.verify_physical_schema", lambda *args: None)

    result = publish_dataset(plan, mode=PublishMode.DEFAULT, client=store, fetcher=lambda *_: ())

    assert result.status == "verified-existing"
    assert result.inventory_state == "not-requested"


def _publication_lease(store: FakeS3, plan, publication_id: str, owner_nonce: str) -> Lease:
    now = datetime.now(UTC).replace(microsecond=0)
    key = f"_data-eng-locks/leases/{plan.dataset.name}.json"
    lease = Lease(
        dataset=plan.dataset.name,
        publication_id=publication_id,
        owner_nonce=owner_nonce,
        state="active",
        created_at=now,
        expires_at=now + timedelta(seconds=60),
        etag='"lease"',
        bucket="landing",
        key=key,
    )
    _seed_lease(store, lease)
    return lease


def _seed_lease(store: FakeS3, lease: Lease) -> None:
    store.seed(
        lease.key,
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


def test_mutating_publication_requires_explicit_registry_digest_before_lease(monkeypatch) -> None:
    plan = resolve_scale(_dataset(), "small")
    store = FakeS3()
    monkeypatch.setattr("datasets.publication.acquire_lease", lambda *args, **kwargs: pytest.fail("lease"))

    with pytest.raises(ValueError, match="raw registry sha256"):
        publish_dataset(
            plan,
            mode=PublishMode.DEFAULT,
            client=store,
            fetcher=lambda *_: pytest.fail("source"),
        )

    assert store.puts == []


def test_default_fails_if_pointer_becomes_corrupt_during_lease_acquisition(monkeypatch) -> None:
    plan = resolve_scale(_dataset(), "small")
    store = FakeS3()

    def acquire(client, dataset, publication_id, owner_nonce, *, bucket):
        del client, dataset, bucket
        lease = _publication_lease(store, plan, publication_id, owner_nonce)
        store.seed(active_pointer_key("sample"), b'{"corrupt":true}', etag='"corrupt-race"')
        return lease

    monkeypatch.setattr("datasets.publication.acquire_lease", acquire)
    monkeypatch.setattr("datasets.publication.release_lease", lambda *_: None)

    with pytest.raises(LockMismatch, match="pointer"):
        publish_dataset(
            plan,
            mode=PublishMode.DEFAULT,
            client=store,
            fetcher=lambda *_: pytest.fail("source"),
            raw_registry_sha256=RAW_REGISTRY_SHA256,
        )

    assert not any("/_generations/" in str(request["Key"]) for request in store.puts)


def test_long_fetch_renews_lease_and_releases_latest_version(tmp_path, monkeypatch) -> None:
    plan = resolve_scale(_dataset(), "small")
    store = FakeS3()
    source = tmp_path / "readme.txt"
    source.write_bytes(b"hello\n")
    expected = ExpectedObject("readme.txt", 6, _sha(b"hello\n"), "readme")
    renewed = threading.Event()
    released: list[Lease] = []

    def acquire(client, dataset, publication_id, owner_nonce, *, bucket):
        del client, dataset, bucket
        lease = _publication_lease(store, plan, publication_id, owner_nonce)
        short = replace(lease, expires_at=lease.created_at + timedelta(seconds=1))
        _seed_lease(store, short)
        return short

    def renew(client, lease):
        del client
        created = datetime.now(UTC).replace(microsecond=0)
        updated = replace(
            lease,
            created_at=created,
            expires_at=created + timedelta(seconds=60),
            etag='"lease-renewed"',
        )
        _seed_lease(store, updated)
        renewed.set()
        return updated

    def fetch(*_args):
        assert renewed.wait(2), "blocking acquisition was not protected by lease renewal"
        return (VerifiedFile(source, expected),)

    monkeypatch.setattr("datasets.publication.acquire_lease", acquire)
    monkeypatch.setattr("datasets.publication.renew_lease", renew)
    monkeypatch.setattr("datasets.publication.release_lease", lambda client, lease: released.append(lease))
    monkeypatch.setattr("datasets.publication.verify_physical_schema", lambda *args: None)

    publish_dataset(
        plan,
        mode=PublishMode.DEFAULT,
        client=store,
        fetcher=fetch,
        raw_registry_sha256=RAW_REGISTRY_SHA256,
    )

    assert released[-1].etag == '"lease-renewed"'


def test_lease_renewal_failure_aborts_before_pointer_mutation(tmp_path, monkeypatch) -> None:
    plan = resolve_scale(_dataset(), "small")
    store = FakeS3()
    source = tmp_path / "readme.txt"
    source.write_bytes(b"hello\n")
    expected = ExpectedObject("readme.txt", 6, _sha(b"hello\n"), "readme")
    renewal_attempted = threading.Event()

    def acquire(client, dataset, publication_id, owner_nonce, *, bucket):
        del client, dataset, bucket
        lease = _publication_lease(store, plan, publication_id, owner_nonce)
        short = replace(lease, expires_at=lease.created_at + timedelta(seconds=1))
        _seed_lease(store, short)
        return short

    def renew(*_args):
        renewal_attempted.set()
        raise ConditionalConflict("successor owns lease")

    def fetch(*_args):
        assert renewal_attempted.wait(2)
        return (VerifiedFile(source, expected),)

    monkeypatch.setattr("datasets.publication.acquire_lease", acquire)
    monkeypatch.setattr("datasets.publication.renew_lease", renew)
    monkeypatch.setattr("datasets.publication.release_lease", lambda *_: None)
    monkeypatch.setattr("datasets.publication.verify_physical_schema", lambda *args: None)

    with pytest.raises(ConditionalConflict, match="renewal"):
        publish_dataset(
            plan,
            mode=PublishMode.DEFAULT,
            client=store,
            fetcher=fetch,
            raw_registry_sha256=RAW_REGISTRY_SHA256,
        )

    assert active_pointer_key("sample") not in store.objects


def test_keepalive_checkpoint_waits_for_renewed_capability(monkeypatch) -> None:
    plan = resolve_scale(_dataset(), "small")
    store = FakeS3()
    lease = _publication_lease(store, plan, PUBLICATION_ID, "a" * 32)
    renewal_entered = threading.Event()
    allow_renewal = threading.Event()
    checked: list[str] = []

    def renew(client, predecessor):
        del client
        renewal_entered.set()
        assert allow_renewal.wait(2)
        successor = replace(predecessor, etag='"successor"')
        _seed_lease(store, successor)
        return successor

    monkeypatch.setattr("datasets.publication.renew_lease", renew)
    monkeypatch.setattr(
        "datasets.publication._assert_lease_current",
        lambda client, current: checked.append(current.etag),
    )
    from datasets import publication as publication_module

    keepalive = publication_module._LeaseKeepalive(store, lease)
    renew_thread = threading.Thread(target=keepalive._renew_once)
    renew_thread.start()
    assert renewal_entered.wait(1)
    checkpoint_thread = threading.Thread(target=keepalive.checkpoint)
    checkpoint_thread.start()
    time.sleep(0.05)
    assert checked == []
    allow_renewal.set()
    renew_thread.join(1)
    checkpoint_thread.join(1)
    assert checked == ['"successor"']


def test_keepalive_start_has_no_background_s3_mutator() -> None:
    plan = resolve_scale(_dataset(), "small")
    store = FakeS3()
    lease = _publication_lease(store, plan, PUBLICATION_ID, "a" * 32)
    from datasets import publication as publication_module

    keepalive = publication_module._LeaseKeepalive(store, lease)
    keepalive.start()

    assert not any(thread.name == f"dataset-lease-{lease.dataset}" for thread in threading.enumerate())
    assert keepalive.stop() == lease


def test_keepalive_start_failure_releases_acquired_lease(monkeypatch) -> None:
    plan = resolve_scale(_dataset(), "small")
    store = FakeS3()
    acquired: list[Lease] = []
    released: list[Lease] = []

    def acquire(client, dataset, publication_id, owner_nonce, *, bucket):
        del client, dataset, bucket
        lease = _publication_lease(store, plan, publication_id, owner_nonce)
        acquired.append(lease)
        return lease

    monkeypatch.setattr("datasets.publication.acquire_lease", acquire)
    monkeypatch.setattr("datasets.publication.release_lease", lambda client, lease: released.append(lease))
    monkeypatch.setattr(
        "datasets.publication._LeaseKeepalive.start",
        lambda self: (_ for _ in ()).throw(RuntimeError("thread start failed")),
    )

    with pytest.raises(RuntimeError, match="thread start"):
        publish_dataset(
            plan,
            mode=PublishMode.DEFAULT,
            client=store,
            fetcher=lambda *_: pytest.fail("source"),
            raw_registry_sha256=RAW_REGISTRY_SHA256,
        )

    assert released == acquired


@pytest.mark.parametrize("interrupt", [KeyboardInterrupt(), SystemExit(7)])
def test_control_flow_interrupt_at_manifest_boundary_is_preserved(tmp_path, monkeypatch, interrupt) -> None:
    plan = resolve_scale(_dataset(), "small")
    store = FakeS3()
    source = tmp_path / "readme.txt"
    source.write_bytes(b"hello\n")
    expected = ExpectedObject("readme.txt", 6, _sha(b"hello\n"), "readme")

    def acquire(client, dataset, publication_id, owner_nonce, *, bucket):
        del client, dataset, bucket
        return _publication_lease(store, plan, publication_id, owner_nonce)

    monkeypatch.setattr("datasets.publication.acquire_lease", acquire)
    monkeypatch.setattr("datasets.publication.release_lease", lambda *_: None)
    monkeypatch.setattr("datasets.publication.verify_physical_schema", lambda *args: None)
    monkeypatch.setattr(
        "datasets.publication._put_manifest_exact",
        lambda *args, **kwargs: (_ for _ in ()).throw(interrupt),
    )

    with pytest.raises(type(interrupt)) as caught:
        publish_dataset(
            plan,
            mode=PublishMode.DEFAULT,
            client=store,
            fetcher=lambda *_: (VerifiedFile(source, expected),),
            raw_registry_sha256=RAW_REGISTRY_SHA256,
        )

    assert caught.value is interrupt
    assert any("publication" in note for note in getattr(interrupt, "__notes__", ()))


def test_verified_existing_race_keeps_result_when_release_fails(monkeypatch) -> None:
    plan = resolve_scale(_dataset(), "small")
    store = FakeS3()
    published, manifest = _published_store(plan)

    def acquire(client, dataset, publication_id, owner_nonce, *, bucket):
        del client, dataset, bucket
        lease = _publication_lease(store, plan, publication_id, owner_nonce)
        store.objects.update(published.objects)
        return lease

    monkeypatch.setattr("datasets.publication.acquire_lease", acquire)
    monkeypatch.setattr(
        "datasets.publication.release_lease",
        lambda *_: (_ for _ in ()).throw(RuntimeError("release")),
    )

    result = publish_dataset(
        plan,
        mode=PublishMode.DEFAULT,
        client=store,
        fetcher=lambda *_: pytest.fail("source"),
        raw_registry_sha256=RAW_REGISTRY_SHA256,
    )

    assert result.manifest_sha256 == manifest_sha256(manifest)
    assert result.cleanup_warning is not None


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
    assert result.publication_id is not None
    assert result.publication_prefix == publication_prefix(plan, result.publication_id)
    assert result.pointer_action == "create"
    assert result.pointer_precondition == "If-None-Match: *"
    assert store.puts == []


def test_dry_run_refresh_runs_complete_inventory_without_mutation(monkeypatch) -> None:
    plan = resolve_scale(_dataset(), "small")
    store, _manifest_value = _published_store(plan)
    monkeypatch.setattr("datasets.publication.verify_physical_schema", lambda *args: None)

    result = publish_dataset(
        plan,
        mode=PublishMode.REFRESH,
        client=store,
        fetcher=lambda *_: pytest.fail("source"),
        dry_run=True,
    )

    assert result.status == "dry-run-refresh"
    assert result.inventory_state == "complete"
    assert store.puts == []


def test_failure_after_immutable_upload_reports_orphan_candidate(tmp_path, monkeypatch) -> None:
    plan = resolve_scale(_dataset(), "small")
    store = FakeS3()
    source = tmp_path / "readme.txt"
    source.write_bytes(b"hello\n")
    expected = ExpectedObject("readme.txt", 6, _sha(b"hello\n"), "readme")

    def acquire(client, dataset, publication_id, owner_nonce, *, bucket):
        del client, dataset, bucket
        return _publication_lease(store, plan, publication_id, owner_nonce)

    monkeypatch.setattr("datasets.publication.acquire_lease", acquire)
    monkeypatch.setattr("datasets.publication.release_lease", lambda *_: None)
    monkeypatch.setattr("datasets.publication.verify_physical_schema", lambda *args: None)
    monkeypatch.setattr(
        "datasets.publication._put_manifest_exact",
        lambda *args, **kwargs: (_ for _ in ()).throw(AmbiguousWrite("manifest lost")),
    )

    with pytest.raises(PublicationFailure) as caught:
        publish_dataset(
            plan,
            mode=PublishMode.DEFAULT,
            client=store,
            fetcher=lambda *_: (VerifiedFile(source, expected),),
            raw_registry_sha256=RAW_REGISTRY_SHA256,
        )

    result = caught.value.result
    assert result.status == "failed-candidate"
    assert result.publication_prefix is not None
    assert result.proven_orphan_keys == (f"{result.publication_prefix}/readme.txt",)


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


def test_legacy_listing_paginates_direct_keys_with_delimiter() -> None:
    plan = resolve_scale(_dataset(), "small")
    calls: list[dict[str, object]] = []

    class Paged:
        def list_objects_v2(self, **request):
            calls.append(request)
            token = request.get("ContinuationToken")
            if token is None:
                return {
                    "Contents": [{"Key": "sample/readme.txt"}],
                    "CommonPrefixes": [{"Prefix": "sample/_generations/"}],
                    "IsTruncated": True,
                    "NextContinuationToken": "page-2",
                }
            return {"Contents": [], "CommonPrefixes": [], "IsTruncated": False}

    from datasets import publication as publication_module

    assert publication_module._list_legacy_keys(Paged(), plan, "landing") == ("sample/readme.txt",)
    assert all(call["Delimiter"] == "/" for call in calls)


def test_legacy_listing_rejects_continuation_token_cycle() -> None:
    plan = resolve_scale(_dataset(), "small")

    class Cyclic:
        def list_objects_v2(self, **request):
            token = request.get("ContinuationToken")
            following = "A" if token in {None, "B"} else "B"
            return {
                "Contents": [],
                "CommonPrefixes": [],
                "IsTruncated": True,
                "NextContinuationToken": following,
            }

    from datasets import publication as publication_module

    with pytest.raises(AmbiguousWrite, match="pagination"):
        publication_module._list_legacy_keys(Cyclic(), plan, "landing")


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

    def list_objects_v2(
        self,
        *,
        Bucket: str,
        Prefix: str,
        Delimiter: str | None = None,
        ContinuationToken: str | None = None,
    ):
        del Bucket, ContinuationToken
        assert Delimiter in {None, "/"}
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


class InventoryS3(FakeS3):
    def list_objects_v2(
        self,
        *,
        Bucket,
        Prefix,
        Delimiter=None,
        ContinuationToken=None,
    ):
        del Bucket, ContinuationToken
        matches = sorted(key for key in self.objects if key.startswith(Prefix))
        if Delimiter is None:
            return {"Contents": [{"Key": key} for key in matches], "IsTruncated": False}
        direct = []
        common = set()
        for key in matches:
            remainder = key.removeprefix(Prefix)
            if "/" in remainder:
                common.add(f"{Prefix}{remainder.split('/', 1)[0]}/")
            else:
                direct.append({"Key": key})
        return {
            "Contents": direct,
            "CommonPrefixes": [{"Prefix": value} for value in sorted(common)],
            "IsTruncated": False,
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


def test_rollback_rejects_corrupt_current_pointer_without_mutation(monkeypatch) -> None:
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

    with pytest.raises(LockMismatch, match="pointer"):
        rollback_manifest(
            store,
            {"sample": plan.dataset},
            "sample",
            "small",
            manifest_sha256(target),
        )
    assert store.puts == []


def test_rollback_lost_pointer_response_reports_reconciled_status(monkeypatch) -> None:
    plan = resolve_scale(_dataset(), "small")
    historical = _manifest(plan, publication_id=PREVIOUS_PUBLICATION_ID)
    store, _ = _published_store(plan)
    store.seed(
        immutable_manifest_key("sample", manifest_sha256(historical)),
        historical.to_bytes(),
        etag='"historical"',
    )
    store.seed(historical.objects[0].key, b"hello\n", etag='"historical-object"')
    store.lose_response_suffix = active_pointer_key("sample")
    monkeypatch.setattr("datasets.publication.verify_physical_schema", lambda *args: None)

    result = publish_dataset(
        plan,
        mode=PublishMode.ROLLBACK,
        client=store,
        fetcher=lambda *_: pytest.fail("source"),
        rollback_sha256=manifest_sha256(historical),
    )

    assert result.status == "rolled-back-reconciled"


def test_rollback_dry_run_requires_an_existing_valid_current_pointer(monkeypatch) -> None:
    plan = resolve_scale(_dataset(), "small")
    historical = _manifest(plan, publication_id=PREVIOUS_PUBLICATION_ID)
    store = FakeS3()
    store.seed(
        immutable_manifest_key("sample", manifest_sha256(historical)),
        historical.to_bytes(),
        etag='"historical"',
    )
    store.seed(historical.objects[0].key, b"hello\n", etag='"historical-object"')
    monkeypatch.setattr("datasets.publication.verify_physical_schema", lambda *args: None)

    with pytest.raises(LockMismatch, match="pointer"):
        publish_dataset(
            plan,
            mode=PublishMode.ROLLBACK,
            client=store,
            fetcher=lambda *_: pytest.fail("source"),
            rollback_sha256=manifest_sha256(historical),
            dry_run=True,
        )

    assert store.puts == []


def test_rollback_result_uses_pointer_state_observed_by_transaction(monkeypatch) -> None:
    plan = resolve_scale(_dataset(), "small")
    target = _manifest(plan, publication_id=PREVIOUS_PUBLICATION_ID)
    first, current_manifest = _published_store(plan)
    store = FakeS3()
    store.objects.update(first.objects)
    store.seed(
        immutable_manifest_key("sample", manifest_sha256(target)),
        target.to_bytes(),
        etag='"target"',
    )
    store.seed(target.objects[0].key, b"hello\n", etag='"target-object"')
    pointer_b = ActivePointer(
        1,
        "sample",
        immutable_manifest_key("sample", manifest_sha256(current_manifest)),
        manifest_sha256(current_manifest),
    )
    store.seed(active_pointer_key("sample"), pointer_b.to_bytes(), etag='"pointer-B"')
    monkeypatch.setattr("datasets.publication.verify_physical_schema", lambda *args: None)

    result = publish_dataset(
        plan,
        mode=PublishMode.ROLLBACK,
        client=store,
        fetcher=lambda *_: pytest.fail("source"),
        rollback_sha256=manifest_sha256(target),
    )

    assert store.puts[-1]["IfMatch"] == '"pointer-B"'
    assert result.pointer_precondition == 'If-Match: "pointer-B"'
    assert result.previous_manifest_sha256 == manifest_sha256(current_manifest)
    assert result.pointer_outcome == "committed"
    assert result.manifest_outcome == "existing-retained"
    assert result.manifest_key == immutable_manifest_key("sample", manifest_sha256(target))
    assert result.publication_prefix == target.physical_prefix
    assert result.inventory_state == "complete"


def test_rollback_inventory_closes_target_and_previous_active_history_transitively(monkeypatch) -> None:
    plan = resolve_scale(_dataset(), "small")
    manifest_a = _manifest(plan, publication_id=PREVIOUS_PUBLICATION_ID)
    manifest_b = replace(
        _manifest(plan, publication_id="123e4567e89b42d3a456426614174002"),
        previous_manifest_key=immutable_manifest_key("sample", manifest_sha256(manifest_a)),
        previous_manifest_sha256=manifest_sha256(manifest_a),
    )
    manifest_c = replace(
        _manifest(plan),
        previous_manifest_key=immutable_manifest_key("sample", manifest_sha256(manifest_b)),
        previous_manifest_sha256=manifest_sha256(manifest_b),
    )
    seeded, _ = _published_store(plan, manifest_c)
    store = InventoryS3()
    store.objects.update(seeded.objects)
    for manifest in (manifest_a, manifest_b):
        store.seed(
            immutable_manifest_key("sample", manifest_sha256(manifest)),
            manifest.to_bytes(),
            etag='"history"',
        )
        store.seed(manifest.objects[0].key, b"hello\n", etag='"history-object"')
    monkeypatch.setattr("datasets.publication.verify_physical_schema", lambda *args: None)

    result = publish_dataset(
        plan,
        mode=PublishMode.ROLLBACK,
        client=store,
        fetcher=lambda *_: pytest.fail("source"),
        rollback_sha256=manifest_sha256(manifest_a),
    )

    assert set(result.retained_manifest_keys) == {
        immutable_manifest_key("sample", manifest_sha256(manifest_b)),
        immutable_manifest_key("sample", manifest_sha256(manifest_c)),
    }
    assert result.unreferenced_manifest_keys == ()
    assert result.candidate_generation_prefixes == ()


def test_verified_result_walks_predecessor_history_in_order(monkeypatch) -> None:
    plan = resolve_scale(_dataset(), "small")
    oldest = _manifest(plan, publication_id=PREVIOUS_PUBLICATION_ID)
    middle = replace(
        _manifest(plan, publication_id="123e4567e89b42d3a456426614174002"),
        previous_manifest_key=immutable_manifest_key("sample", manifest_sha256(oldest)),
        previous_manifest_sha256=manifest_sha256(oldest),
    )
    current = replace(
        _manifest(plan),
        previous_manifest_key=immutable_manifest_key("sample", manifest_sha256(middle)),
        previous_manifest_sha256=manifest_sha256(middle),
    )
    store, _ = _published_store(plan, current)
    for manifest in (middle, oldest):
        store.seed(
            immutable_manifest_key("sample", manifest_sha256(manifest)),
            manifest.to_bytes(),
            etag='"history"',
        )
        store.seed(manifest.objects[0].key, b"hello\n", etag='"history-object"')
    monkeypatch.setattr("datasets.publication.verify_physical_schema", lambda *args: None)

    result = publish_dataset(
        plan,
        mode=PublishMode.DEFAULT,
        client=store,
        fetcher=lambda *_: pytest.fail("source"),
        dry_run=True,
    )

    assert result.retained_manifest_keys == (
        immutable_manifest_key("sample", manifest_sha256(middle)),
        immutable_manifest_key("sample", manifest_sha256(oldest)),
    )


def test_verified_result_fails_closed_on_history_cycle(monkeypatch) -> None:
    plan = resolve_scale(_dataset(), "small")
    cyclic = _manifest(plan, publication_id=PREVIOUS_PUBLICATION_ID)
    current = replace(
        _manifest(plan),
        previous_manifest_key=immutable_manifest_key("sample", manifest_sha256(cyclic)),
        previous_manifest_sha256=manifest_sha256(cyclic),
    )
    store, _ = _published_store(plan, current)
    monkeypatch.setattr("datasets.publication.verify_physical_schema", lambda *args: None)
    monkeypatch.setattr(
        "datasets.publication._read_historical_manifest",
        lambda client, selected, digest, bucket: replace(
            cyclic,
            previous_manifest_key=immutable_manifest_key("sample", digest),
            previous_manifest_sha256=digest,
        ),
    )

    with pytest.raises(LockMismatch, match="history"):
        publish_dataset(
            plan,
            mode=PublishMode.DEFAULT,
            client=store,
            fetcher=lambda *_: pytest.fail("source"),
            dry_run=True,
        )


def test_history_accepts_prior_scale_contract_and_excludes_all_reachable_generations(monkeypatch) -> None:
    current_plan = resolve_scale(_dataset(), "small")
    prior_plan = resolve_scale(_dataset(), "medium")
    prior = _manifest(prior_plan, publication_id=PREVIOUS_PUBLICATION_ID)
    current = replace(
        _manifest(current_plan),
        previous_manifest_key=immutable_manifest_key("sample", manifest_sha256(prior)),
        previous_manifest_sha256=manifest_sha256(prior),
    )

    seeded, _ = _published_store(current_plan, current)
    store = InventoryS3()
    store.objects.update(seeded.objects)
    store.seed(immutable_manifest_key("sample", manifest_sha256(prior)), prior.to_bytes(), etag='"prior"')
    store.seed(prior.objects[0].key, b"hello\n", etag='"prior-object"')
    orphan_prefix = publication_prefix(current_plan, "123e4567e89b42d3a456426614174003")
    store.seed(f"{orphan_prefix}/readme.txt", b"hello\n", etag='"orphan"')
    monkeypatch.setattr("datasets.publication.verify_physical_schema", lambda *args: None)

    result = publish_dataset(
        current_plan,
        mode=PublishMode.DEFAULT,
        client=store,
        fetcher=lambda *_: pytest.fail("source"),
        dry_run=True,
    )

    assert result.retained_manifest_keys == (immutable_manifest_key("sample", manifest_sha256(prior)),)
    assert result.candidate_generation_prefixes == (orphan_prefix,)


def test_valid_unreferenced_manifest_keeps_its_generation_out_of_orphan_candidates(monkeypatch) -> None:
    plan = resolve_scale(_dataset(), "small")
    seeded, _current = _published_store(plan)
    store = InventoryS3()
    store.objects.update(seeded.objects)
    unreferenced = _manifest(plan, publication_id="123e4567e89b42d3a456426614174003")
    unreferenced_key = immutable_manifest_key("sample", manifest_sha256(unreferenced))
    store.seed(unreferenced_key, unreferenced.to_bytes(), etag='"unreferenced"')
    store.seed(unreferenced.objects[0].key, b"hello\n", etag='"unreferenced-object"')
    monkeypatch.setattr("datasets.publication.verify_physical_schema", lambda *args: None)

    result = publish_dataset(
        plan,
        mode=PublishMode.DEFAULT,
        client=store,
        fetcher=lambda *_: pytest.fail("source"),
        dry_run=True,
    )

    assert result.unreferenced_manifest_keys == (unreferenced_key,)
    assert unreferenced.physical_prefix not in result.candidate_generation_prefixes
    assert result.inventory_state == "complete"


def test_generation_inventory_uses_one_global_request_budget(monkeypatch) -> None:
    from datasets import publication as publication_module

    plan = resolve_scale(_dataset(), "small")
    calls = 0

    class Listing:
        def list_objects_v2(self, **request):
            nonlocal calls
            calls += 1
            return {
                "Contents": [],
                "CommonPrefixes": [{"Prefix": f"{request['Prefix']}child/"}],
                "IsTruncated": False,
            }

    monkeypatch.setattr(publication_module, "_MAX_LEGACY_LIST_PAGES", 1)
    with pytest.raises(AmbiguousWrite, match="global request budget"):
        publication_module._inactive_generation_prefixes(
            Listing(),
            plan,
            set(),
            bucket="landing",
            budget=publication_module._InventoryBudget(),
        )
    assert calls == 1


def test_common_prefix_listing_is_bounded_and_validated(monkeypatch) -> None:
    plan = resolve_scale(_dataset(), "small")
    from datasets import publication as publication_module

    monkeypatch.setattr(publication_module, "_MAX_LEGACY_LIST_KEYS", 1)

    class Excessive:
        def list_objects_v2(self, **_request):
            return {
                "Contents": [],
                "CommonPrefixes": [{"Prefix": "sample/a/"}, {"Prefix": "sample/b/"}],
                "IsTruncated": False,
            }

    with pytest.raises(AmbiguousWrite, match="common prefix"):
        publication_module._list_legacy_keys(Excessive(), plan, "landing")


def test_owned_staging_foreign_replacement_is_not_deleted(tmp_path) -> None:
    from datasets import publication as publication_module

    parent = tmp_path / "trusted"
    parent.mkdir(mode=0o700)
    with pytest.raises(ValueError, match="identity"):
        with publication_module._owned_staging(parent) as staging:
            original = staging.with_name(staging.name + "-original")
            staging.rename(original)
            staging.mkdir()
            (staging / "foreign.txt").write_text("foreign")
    survivors = list(parent.rglob("foreign.txt"))
    assert len(survivors) == 1
    assert survivors[0].parent == staging
    assert survivors[0].read_text() == "foreign"


def test_owned_staging_supports_sticky_world_writable_platform_temp(tmp_path, monkeypatch) -> None:
    from datasets import publication as publication_module

    platform_temp = tmp_path / "platform-temp"
    platform_temp.mkdir(mode=0o1777)
    platform_temp.chmod(0o1777)
    monkeypatch.setattr(publication_module.tempfile, "gettempdir", lambda: str(platform_temp))

    with publication_module._owned_staging() as staging:
        assert staging.parent.parent == platform_temp
        assert staging.parent.stat().st_mode & 0o777 == 0o700
        (staging / "owned.txt").write_text("owned")

    assert list(platform_temp.rglob("owned.txt")) == []


def test_owned_staging_cleans_owned_inode_after_path_is_moved(tmp_path) -> None:
    from datasets import publication as publication_module

    parent = tmp_path / "trusted"
    parent.mkdir(mode=0o700)
    moved = parent / "moved-owned"
    with pytest.raises(ValueError, match="displaced"):
        with publication_module._owned_staging(parent) as staging:
            (staging / "owned.txt").write_text("owned")
            staging.rename(moved)

    assert moved.is_dir()
    assert list(moved.iterdir()) == []


def test_owned_staging_atomic_cleanup_restores_last_moment_foreign_swap(tmp_path, monkeypatch) -> None:
    from datasets import publication as publication_module

    parent = tmp_path / "trusted"
    parent.mkdir(mode=0o700)
    moved = parent / "moved-owned"
    original_exchange = publication_module._exchange_directory_names
    swapped = False

    def swap_before_exchange(parent_fd, left, right):
        nonlocal swapped
        if not swapped:
            swapped = True
            os.rename(left, moved.name, src_dir_fd=parent_fd, dst_dir_fd=parent_fd)
            os.mkdir(left, mode=0o700, dir_fd=parent_fd)
            foreign_fd = os.open(
                f"{left}/foreign.txt",
                os.O_WRONLY | os.O_CREAT | os.O_EXCL,
                0o600,
                dir_fd=parent_fd,
            )
            os.write(foreign_fd, b"foreign")
            os.close(foreign_fd)
        original_exchange(parent_fd, left, right)

    monkeypatch.setattr(publication_module, "_exchange_directory_names", swap_before_exchange)
    with pytest.raises(ValueError, match="foreign replacement preserved"):
        with publication_module._owned_staging(parent) as staging:
            (staging / "owned.txt").write_text("owned")

    assert (staging / "foreign.txt").read_bytes() == b"foreign"
    assert list(moved.iterdir()) == []


def test_owned_staging_never_path_unlinks_after_atomic_identity_proof(tmp_path, monkeypatch) -> None:
    from datasets import publication as publication_module

    parent = tmp_path / "trusted"
    parent.mkdir(mode=0o700)
    exchanged = False
    original_exchange = publication_module._exchange_directory_names
    original_rmdir = publication_module.os.rmdir

    def exchange(*args):
        nonlocal exchanged
        original_exchange(*args)
        exchanged = True

    def reject_post_exchange_rmdir(*args, **kwargs):
        if exchanged:
            pytest.fail("path rmdir remained after atomic identity proof")
        return original_rmdir(*args, **kwargs)

    monkeypatch.setattr(publication_module, "_exchange_directory_names", exchange)
    monkeypatch.setattr(publication_module.os, "rmdir", reject_post_exchange_rmdir)

    cleanup_notes: list[str] = []
    with publication_module._owned_staging(parent, cleanup_notes=cleanup_notes) as staging:
        (staging / "owned.txt").write_text("owned")

    assert cleanup_notes == ["owned staging residue retained after descriptor-safe cleanup"]


def test_stop_failure_does_not_mask_primary_and_release_is_attempted(tmp_path, monkeypatch) -> None:
    plan = resolve_scale(_dataset(), "small")
    store = FakeS3()
    released: list[Lease] = []

    def acquire(client, dataset, publication_id, owner_nonce, *, bucket):
        del client, dataset, bucket
        return _publication_lease(store, plan, publication_id, owner_nonce)

    monkeypatch.setattr("datasets.publication.acquire_lease", acquire)
    monkeypatch.setattr("datasets.publication.release_lease", lambda client, lease: released.append(lease))
    monkeypatch.setattr(
        "datasets.publication._LeaseKeepalive.stop",
        lambda self: (_ for _ in ()).throw(AmbiguousWrite("blocked renew")),
    )

    primary = RuntimeError("source failed")
    with pytest.raises(RuntimeError) as caught:
        publish_dataset(
            plan,
            mode=PublishMode.DEFAULT,
            client=store,
            fetcher=lambda *_: (_ for _ in ()).throw(primary),
            raw_registry_sha256=RAW_REGISTRY_SHA256,
        )

    assert caught.value is primary
    assert "blocked renew" in " ".join(getattr(primary, "__notes__", ()))
    assert released


def test_pointer_commit_with_unavailable_reconciliation_is_explicitly_ambiguous(
    tmp_path,
    monkeypatch,
) -> None:
    plan = resolve_scale(_dataset(), "small")
    store = FakeS3()
    source = tmp_path / "readme.txt"
    source.write_bytes(b"hello\n")
    expected = ExpectedObject("readme.txt", 6, _sha(b"hello\n"), "readme")

    def acquire(client, dataset, publication_id, owner_nonce, *, bucket):
        del client, dataset, bucket
        return _publication_lease(store, plan, publication_id, owner_nonce)

    original_put = store.put_object

    def commit_then_hide(**request):
        response = original_put(**request)
        if request["Key"] == active_pointer_key("sample"):
            store.body_overrides[active_pointer_key("sample")] = NonBinaryBody()
            from botocore.exceptions import ClientError

            raise ClientError(
                {"Error": {"Code": "InternalError"}, "ResponseMetadata": {"HTTPStatusCode": 500}},
                "PutObject",
            )
        return response

    monkeypatch.setattr(store, "put_object", commit_then_hide)
    monkeypatch.setattr("datasets.publication.acquire_lease", acquire)
    monkeypatch.setattr("datasets.publication.release_lease", lambda *_: None)
    monkeypatch.setattr("datasets.publication.verify_physical_schema", lambda *args: None)

    with pytest.raises(PublicationFailure) as caught:
        publish_dataset(
            plan,
            mode=PublishMode.DEFAULT,
            client=store,
            fetcher=lambda *_: (VerifiedFile(source, expected),),
            raw_registry_sha256=RAW_REGISTRY_SHA256,
        )

    assert caught.value.result.status == "pointer-outcome-ambiguous"
    assert caught.value.result.pointer_outcome == "ambiguous"
    assert caught.value.result.proven_orphan_keys == ()
    assert caught.value.result.manifest_outcome == "reference-ambiguous"


def test_committed_publication_inventory_failure_is_warning_not_failure(tmp_path, monkeypatch) -> None:
    plan = resolve_scale(_dataset(), "small")
    store = FakeS3()
    source = tmp_path / "readme.txt"
    source.write_bytes(b"hello\n")
    expected = ExpectedObject("readme.txt", 6, _sha(b"hello\n"), "readme")

    def acquire(client, dataset, publication_id, owner_nonce, *, bucket):
        del client, dataset, bucket
        return _publication_lease(store, plan, publication_id, owner_nonce)

    monkeypatch.setattr("datasets.publication.acquire_lease", acquire)
    monkeypatch.setattr("datasets.publication.release_lease", lambda *_: None)
    monkeypatch.setattr("datasets.publication.verify_physical_schema", lambda *args: None)
    monkeypatch.setattr(
        "datasets.publication._attach_verified_history",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AmbiguousWrite("inventory unavailable")),
    )

    result = publish_dataset(
        plan,
        mode=PublishMode.DEFAULT,
        client=store,
        fetcher=lambda *_: (VerifiedFile(source, expected),),
        raw_registry_sha256=RAW_REGISTRY_SHA256,
    )

    assert result.pointer_outcome == "committed"
    assert result.inventory_state == "unavailable-warning"
    assert result.cleanup_warning is not None
    assert "inventory unavailable" in result.cleanup_warning


@pytest.mark.parametrize("interrupt", [KeyboardInterrupt(), SystemExit(9)])
def test_pointer_control_flow_after_possible_commit_keeps_category_and_ambiguity(
    tmp_path,
    monkeypatch,
    interrupt,
) -> None:
    plan = resolve_scale(_dataset(), "small")
    store = FakeS3()
    source = tmp_path / "readme.txt"
    source.write_bytes(b"hello\n")
    expected = ExpectedObject("readme.txt", 6, _sha(b"hello\n"), "readme")

    def acquire(client, dataset, publication_id, owner_nonce, *, bucket):
        del client, dataset, bucket
        return _publication_lease(store, plan, publication_id, owner_nonce)

    original = store.put_object

    def commit_then_interrupt(**request):
        response = original(**request)
        if request["Key"] == active_pointer_key("sample"):
            raise interrupt
        return response

    monkeypatch.setattr(store, "put_object", commit_then_interrupt)
    monkeypatch.setattr("datasets.publication.acquire_lease", acquire)
    monkeypatch.setattr("datasets.publication.release_lease", lambda *_: None)
    monkeypatch.setattr("datasets.publication.verify_physical_schema", lambda *args: None)

    with pytest.raises(type(interrupt)) as caught:
        publish_dataset(
            plan,
            mode=PublishMode.DEFAULT,
            client=store,
            fetcher=lambda *_: (VerifiedFile(source, expected),),
            raw_registry_sha256=RAW_REGISTRY_SHA256,
        )

    assert caught.value is interrupt
    notes = " ".join(getattr(interrupt, "__notes__", ()))
    assert '"status":"pointer-outcome-ambiguous"' in notes
    assert '"pointer_outcome":"ambiguous"' in notes
    assert '"manifest_outcome":"reference-ambiguous"' in notes


def test_pointer_conflict_reports_proven_unreferenced_candidate(tmp_path, monkeypatch) -> None:
    plan = resolve_scale(_dataset(), "small")
    store = FakeS3()
    store.conflict = True
    source = tmp_path / "readme.txt"
    source.write_bytes(b"hello\n")
    expected = ExpectedObject("readme.txt", 6, _sha(b"hello\n"), "readme")

    def acquire(client, dataset, publication_id, owner_nonce, *, bucket):
        del client, dataset, bucket
        store.conflict = False
        lease = _publication_lease(store, plan, publication_id, owner_nonce)
        return lease

    def conflict_pointer(client, bucket, key, body, state):
        del client, bucket, key, body, state
        raise ConditionalConflict("concurrent publisher owns pointer")

    monkeypatch.setattr("datasets.publication.acquire_lease", acquire)
    monkeypatch.setattr("datasets.publication.release_lease", lambda *_: None)
    monkeypatch.setattr("datasets.publication._put_pointer_exact", conflict_pointer)
    monkeypatch.setattr("datasets.publication.verify_physical_schema", lambda *args: None)

    with pytest.raises(PublicationFailure) as caught:
        publish_dataset(
            plan,
            mode=PublishMode.DEFAULT,
            client=store,
            fetcher=lambda *_: (VerifiedFile(source, expected),),
            raw_registry_sha256=RAW_REGISTRY_SHA256,
        )

    result = caught.value.result
    assert result.pointer_outcome == "conflict"
    assert result.status == "concurrent-publisher"
    assert result.manifest_outcome == "written-unreferenced"
    assert result.proven_orphan_keys == ()
    assert "concurrent publisher" in str(caught.value.__cause__)


def test_object_control_flow_attempt_is_reported_possible_without_wrapping(tmp_path, monkeypatch) -> None:
    plan = resolve_scale(_dataset(), "small")
    store = FakeS3()
    source = tmp_path / "readme.txt"
    source.write_bytes(b"hello\n")
    expected = ExpectedObject("readme.txt", 6, _sha(b"hello\n"), "readme")
    interrupt = KeyboardInterrupt()

    def acquire(client, dataset, publication_id, owner_nonce, *, bucket):
        del client, dataset, bucket
        return _publication_lease(store, plan, publication_id, owner_nonce)

    original = store.put_object

    def interrupt_object(**request):
        if "/_generations/" in str(request["Key"]):
            raise interrupt
        return original(**request)

    monkeypatch.setattr(store, "put_object", interrupt_object)
    monkeypatch.setattr("datasets.publication.acquire_lease", acquire)
    monkeypatch.setattr("datasets.publication.release_lease", lambda *_: None)
    monkeypatch.setattr("datasets.publication.verify_physical_schema", lambda *args: None)

    with pytest.raises(KeyboardInterrupt) as caught:
        publish_dataset(
            plan,
            mode=PublishMode.DEFAULT,
            client=store,
            fetcher=lambda *_: (VerifiedFile(source, expected),),
            raw_registry_sha256=RAW_REGISTRY_SHA256,
        )

    assert caught.value is interrupt
    notes = " ".join(getattr(interrupt, "__notes__", ()))
    assert '"possible_object_keys":["sample/_generations/' in notes


def test_manifest_conflict_reports_explicit_outcome_and_proven_object_orphans(tmp_path, monkeypatch) -> None:
    plan = resolve_scale(_dataset(), "small")
    store = FakeS3()
    source = tmp_path / "readme.txt"
    source.write_bytes(b"hello\n")
    expected = ExpectedObject("readme.txt", 6, _sha(b"hello\n"), "readme")

    def acquire(client, dataset, publication_id, owner_nonce, *, bucket):
        del client, dataset, bucket
        return _publication_lease(store, plan, publication_id, owner_nonce)

    monkeypatch.setattr("datasets.publication.acquire_lease", acquire)
    monkeypatch.setattr("datasets.publication.release_lease", lambda *_: None)
    monkeypatch.setattr("datasets.publication.verify_physical_schema", lambda *args: None)
    monkeypatch.setattr(
        "datasets.publication._put_manifest_exact",
        lambda *_: (_ for _ in ()).throw(ConditionalConflict("manifest conflict")),
    )

    with pytest.raises(PublicationFailure) as caught:
        publish_dataset(
            plan,
            mode=PublishMode.DEFAULT,
            client=store,
            fetcher=lambda *_: (VerifiedFile(source, expected),),
            raw_registry_sha256=RAW_REGISTRY_SHA256,
        )

    assert caught.value.result.manifest_outcome == "conflict"
    assert caught.value.result.proven_orphan_keys == caught.value.result.attempted_object_keys


def test_renewal_loss_after_last_work_checkpoint_prevents_pointer_cas(tmp_path, monkeypatch) -> None:
    plan = resolve_scale(_dataset(), "small")
    store = FakeS3()
    source = tmp_path / "readme.txt"
    source.write_bytes(b"hello\n")
    expected = ExpectedObject("readme.txt", 6, _sha(b"hello\n"), "readme")

    def acquire(client, dataset, publication_id, owner_nonce, *, bucket):
        del client, dataset, bucket
        return _publication_lease(store, plan, publication_id, owner_nonce)

    def stop_and_lose(self):
        self.stop()
        raise ConditionalConflict("successor acquired immediately before CAS")

    monkeypatch.setattr("datasets.publication.acquire_lease", acquire)
    monkeypatch.setattr("datasets.publication.release_lease", lambda *_: None)
    monkeypatch.setattr("datasets.publication._LeaseKeepalive.stop_and_checkpoint", stop_and_lose)
    monkeypatch.setattr("datasets.publication.verify_physical_schema", lambda *args: None)

    with pytest.raises(PublicationFailure) as caught:
        publish_dataset(
            plan,
            mode=PublishMode.DEFAULT,
            client=store,
            fetcher=lambda *_: (VerifiedFile(source, expected),),
            raw_registry_sha256=RAW_REGISTRY_SHA256,
        )

    assert active_pointer_key("sample") not in store.objects
    assert caught.value.result.pointer_outcome == "not-attempted"
    assert caught.value.result.manifest_outcome == "written-unreferenced"


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
