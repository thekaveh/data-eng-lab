import hashlib
import io
import json
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from email.utils import format_datetime
from pathlib import Path
from typing import Any

import boto3
import pytest
from botocore.exceptions import ClientError, ReadTimeoutError
from botocore.response import StreamingBody
from botocore.stub import ANY, Stubber
from moto import mock_aws

import datasets.s3 as s3mod
from datasets.verification import ExpectedObject, LockMismatch, VerificationContext

NOW = datetime.now(UTC).replace(microsecond=0)
CONTEXT = VerificationContext("nyc_taxi", "yellow", "remote", object_name="trips.parquet")
PUBLICATION_A = "publication-a"
PUBLICATION_B = "publication-b"
NONCE_A = "nonce-a"
NONCE_B = "nonce-b"
_DEFAULT_DATE = object()


def _response_metadata(when: datetime | None = NOW, status: int = 200) -> dict[str, object]:
    headers = {} if when is None else {"date": format_datetime(when, usegmt=True)}
    return {
        "HTTPStatusCode": status,
        "HTTPHeaders": headers,
        "RetryAttempts": 0,
        "RequestId": "request-id",
        "HostId": "host-id",
    }


def _client_error(code: str, status: int, when: datetime | None = NOW) -> ClientError:
    return ClientError(
        {
            "Error": {"Code": code, "Message": code},
            "ResponseMetadata": _response_metadata(when, status),
        },
        "S3Operation",
    )


def _streaming_response(
    body: bytes,
    *,
    etag: str = '"opaque/version:1"',
    metadata: dict[str, str] | None = None,
    when: datetime | None = NOW,
) -> dict[str, object]:
    return {
        "Body": StreamingBody(io.BytesIO(body), len(body)),
        "ContentLength": len(body),
        "ETag": etag,
        "Metadata": metadata or {},
        "ResponseMetadata": _response_metadata(when),
    }


@dataclass
class _StoredObject:
    body: bytes
    metadata: dict[str, str]
    etag: str


class FakeS3:
    """A small stateful S3 protocol fake, including conditional writes."""

    def __init__(self, now: datetime = NOW):
        self.now = now
        self.objects: dict[tuple[str, str], _StoredObject] = {}
        self.get_calls: list[dict[str, object]] = []
        self.put_calls: list[dict[str, object]] = []
        self.next_ambiguous: str | None = None
        self.next_put_date: datetime | None | object = _DEFAULT_DATE
        self.get_date: datetime | None = self.now
        self._version = 0

    @staticmethod
    def _bytes(body: Any) -> bytes:
        if isinstance(body, bytes):
            return body
        return body.read()

    def seed(
        self,
        bucket: str,
        key: str,
        body: bytes,
        *,
        metadata: dict[str, str] | None = None,
        etag: str = '"opaque/seed"',
    ) -> None:
        self.objects[(bucket, key)] = _StoredObject(body, metadata or {}, etag)

    def get_object(self, **request):
        self.get_calls.append(request)
        try:
            stored = self.objects[(request["Bucket"], request["Key"])]
        except KeyError:
            raise _client_error("NoSuchKey", 404, self.get_date) from None
        return _streaming_response(
            stored.body,
            etag=stored.etag,
            metadata=stored.metadata,
            when=self.get_date,
        )

    def put_object(self, **request):
        body = self._bytes(request["Body"])
        recorded = dict(request)
        recorded["Body"] = body
        self.put_calls.append(recorded)
        identity = (request["Bucket"], request["Key"])
        current = self.objects.get(identity)
        if request.get("IfNoneMatch") == "*" and current is not None:
            raise _client_error("PreconditionFailed", 412, self.now)
        if "IfMatch" in request and (current is None or current.etag != request["IfMatch"]):
            raise _client_error("ConditionalRequestConflict", 409, self.now)

        outcome = self.next_ambiguous
        self.next_ambiguous = None
        if outcome != "absent":
            self._version += 1
            committed_body = b"competing bytes" if outcome == "competing" else body
            self.objects[identity] = _StoredObject(
                committed_body,
                dict(request.get("Metadata", {})),
                f'"opaque/{self._version}:etag"',
            )
        if outcome is not None:
            raise ReadTimeoutError(endpoint_url="https://s3.invalid", error="lost response")

        response_date = self.now if self.next_put_date is _DEFAULT_DATE else self.next_put_date
        self.next_put_date = _DEFAULT_DATE
        response: dict[str, object] = {
            "ETag": self.objects[identity].etag,
            "ResponseMetadata": _response_metadata(response_date if isinstance(response_date, datetime) else None),
        }
        return response


def _client():
    return boto3.client(
        "s3",
        region_name="us-east-1",
        aws_access_key_id="test",
        aws_secret_access_key="test",
    )


def _expected(body: bytes) -> ExpectedObject:
    return ExpectedObject("trips.parquet", len(body), hashlib.sha256(body).hexdigest(), "schema-v1")


def _freeze_local_time(monkeypatch: pytest.MonkeyPatch, now: datetime = NOW) -> None:
    monkeypatch.setattr(s3mod, "_utc_now", lambda: now)


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
    (infra / ".env").write_text("MINIO_ROOT_USER=minioadmin\nMINIO_ROOT_PASSWORD=secret\nMINIO_PORT=64093\n")
    client = s3mod.s3_client_from_env(infra)
    assert client.meta.endpoint_url == "http://localhost:64093"


def test_client_prefers_exported_minio_endpoint(tmp_path):
    infra = tmp_path / "infra"
    infra.mkdir()
    (infra / ".env").write_text("MINIO_ROOT_USER=minioadmin\nMINIO_ROOT_PASSWORD=secret\nMINIO_PORT=64093\n")
    (tmp_path / "atlas-consumer.env").write_text("ATLAS_MINIO_HOST_ENDPOINT=http://localhost:65120\n")
    assert s3mod.s3_client_from_env(infra).meta.endpoint_url == "http://localhost:65120"


def test_client_from_env_missing_creds_raises(tmp_path: Path):
    infra = tmp_path / "infra"
    infra.mkdir()
    (infra / ".env").write_text("MINIO_PORT=64093\n")
    with pytest.raises(RuntimeError):
        s3mod.s3_client_from_env(infra)


@mock_aws
def test_head_metadata_never_substitutes_for_get_hash(monkeypatch: pytest.MonkeyPatch):
    client = boto3.client("s3", region_name="us-east-1")
    client.create_bucket(Bucket="landing")
    monkeypatch.setattr(s3mod, "_response_server_date", lambda _response: NOW)
    locked = b"locked bytes"
    metadata = {"sha256": hashlib.sha256(locked).hexdigest(), "schema": "schema-v1"}
    client.put_object(
        Bucket="landing",
        Key="nyc_taxi/trips.parquet",
        Body=b"evil content",
        Metadata=metadata,
    )

    with pytest.raises(LockMismatch, match="sha256"):
        s3mod.stream_verify_object(
            client,
            "landing",
            "nyc_taxi/trips.parquet",
            ExpectedObject("trips.parquet", len(b"evil content"), hashlib.sha256(locked).hexdigest(), "schema-v1"),
            CONTEXT,
        )


def test_stream_verify_preserves_opaque_quoted_etag():
    body = b"verified bytes"
    client = FakeS3()
    client.seed(
        "landing",
        "object",
        body,
        metadata={"lock": "exact"},
        etag='"multipart/opaque-7:part"',
    )

    snapshot = s3mod.stream_verify_object(client, "landing", "object", _expected(body), CONTEXT)

    assert snapshot.etag == '"multipart/opaque-7:part"'
    assert snapshot.metadata == {"lock": "exact"}
    assert snapshot.sha256 == hashlib.sha256(body).hexdigest()
    assert snapshot.size_bytes == len(body)


def test_put_immutable_uses_if_none_match_and_get_verifies_bytes(tmp_path: Path):
    body = b"immutable bytes"
    path = tmp_path / "object.bin"
    path.write_bytes(body)
    metadata = {"sha256": hashlib.sha256(body).hexdigest(), "schema": "schema-v1"}
    client = _client()

    with Stubber(client) as stubber:
        stubber.add_response(
            "put_object",
            {"ETag": '"write-response"', "ResponseMetadata": _response_metadata()},
            {
                "Bucket": "landing",
                "Key": "immutable/object",
                "Body": ANY,
                "Metadata": metadata,
                "IfNoneMatch": "*",
            },
        )
        stubber.add_response(
            "get_object",
            _streaming_response(body, etag='"opaque/get:etag"', metadata=metadata),
            {"Bucket": "landing", "Key": "immutable/object"},
        )

        snapshot = s3mod.put_immutable_object(client, "landing", "immutable/object", path, _expected(body), metadata)

    assert snapshot.etag == '"opaque/get:etag"'
    assert snapshot.sha256 == hashlib.sha256(body).hexdigest()


def test_put_immutable_rejects_post_upload_metadata_mismatch(tmp_path: Path):
    body = b"immutable bytes"
    path = tmp_path / "object.bin"
    path.write_bytes(body)
    client = FakeS3()
    original_put = client.put_object

    def corrupt_metadata(**request):
        response = original_put(**request)
        stored = client.objects[("landing", "immutable/object")]
        client.objects[("landing", "immutable/object")] = _StoredObject(stored.body, {"sha256": "wrong"}, stored.etag)
        return response

    client.put_object = corrupt_metadata

    with pytest.raises(LockMismatch) as caught:
        s3mod.put_immutable_object(
            client,
            "landing",
            "immutable/object",
            path,
            _expected(body),
            {"sha256": "expected"},
        )

    assert caught.value.field == "metadata"


@pytest.mark.parametrize("status", [409, 412])
def test_immutable_conditional_error_reconciles_exact_existing_bytes(tmp_path: Path, status: int):
    body = b"immutable bytes"
    path = tmp_path / "object.bin"
    path.write_bytes(body)
    metadata = {"lock": "exact"}
    client = FakeS3()
    client.seed("landing", "immutable/object", body, metadata=metadata)
    if status == 409:

        def conflict(**request):
            client.put_calls.append({**request, "Body": client._bytes(request["Body"])})
            raise _client_error("ConditionalRequestConflict", 409)

        client.put_object = conflict

    snapshot = s3mod.put_immutable_object(client, "landing", "immutable/object", path, _expected(body), metadata)

    assert snapshot.sha256 == hashlib.sha256(body).hexdigest()
    assert len(client.put_calls) == 1


def test_immutable_ambiguous_response_reconciles_exact_committed_bytes(tmp_path: Path):
    body = b"immutable bytes"
    path = tmp_path / "object.bin"
    path.write_bytes(body)
    client = FakeS3()
    client.next_ambiguous = "exact"

    snapshot = s3mod.put_immutable_object(
        client, "landing", "immutable/object", path, _expected(body), {"lock": "exact"}
    )

    assert snapshot.sha256 == hashlib.sha256(body).hexdigest()
    assert len(client.put_calls) == 1


def test_immutable_ambiguous_response_rejects_competing_bytes(tmp_path: Path):
    body = b"immutable bytes"
    path = tmp_path / "object.bin"
    path.write_bytes(body)
    client = FakeS3()
    client.next_ambiguous = "competing"

    with pytest.raises(s3mod.ConditionalConflict):
        s3mod.put_immutable_object(client, "landing", "immutable/object", path, _expected(body), {"lock": "exact"})

    assert len(client.put_calls) == 1


def test_immutable_ambiguous_response_maps_missing_get_to_ambiguous_write(tmp_path: Path):
    body = b"immutable bytes"
    path = tmp_path / "object.bin"
    path.write_bytes(body)
    client = FakeS3()
    client.next_ambiguous = "absent"

    with pytest.raises(s3mod.AmbiguousWrite):
        s3mod.put_immutable_object(client, "landing", "immutable/object", path, _expected(body), {"lock": "exact"})


def test_put_control_passes_opaque_if_match_and_rereads_exact_bytes():
    body = b'{"manifest":"sha256"}'
    client = _client()

    with Stubber(client) as stubber:
        stubber.add_response(
            "put_object",
            {"ETag": '"ignored"', "ResponseMetadata": _response_metadata()},
            {
                "Bucket": "landing",
                "Key": "active.json",
                "Body": body,
                "IfMatch": '"opaque/version:7"',
            },
        )
        stubber.add_response(
            "get_object",
            _streaming_response(body, etag='"opaque/version:8"'),
            {"Bucket": "landing", "Key": "active.json"},
        )

        snapshot = s3mod.put_control_object(
            client,
            "landing",
            "active.json",
            body,
            if_match='"opaque/version:7"',
        )

    assert snapshot.body == body
    assert snapshot.etag == '"opaque/version:8"'


def test_put_control_passes_if_none_match_star():
    body = b"first pointer"
    client = FakeS3()

    snapshot = s3mod.put_control_object(client, "landing", "active.json", body, if_none_match=True)

    assert client.put_calls[0]["IfNoneMatch"] == "*"
    assert "IfMatch" not in client.put_calls[0]
    assert snapshot.body == body


@pytest.mark.parametrize("status", [409, 412])
def test_control_conditional_errors_reconcile_exact_self_write(status: int):
    body = b"intended pointer"
    client = FakeS3()
    client.seed("landing", "active.json", body, etag='"successor"')

    def conflict(**request):
        client.put_calls.append({**request, "Body": client._bytes(request["Body"])})
        raise _client_error("ConditionalRequestConflict" if status == 409 else "PreconditionFailed", status)

    client.put_object = conflict

    snapshot = s3mod.put_control_object(client, "landing", "active.json", body, if_match='"stale"')

    assert snapshot.body == body
    assert len(client.put_calls) == 1


def test_control_conflict_never_retries_stale_cas():
    client = FakeS3()
    client.seed("landing", "active.json", b"competing pointer", etag='"successor"')

    with pytest.raises(s3mod.ConditionalConflict):
        s3mod.put_control_object(
            client,
            "landing",
            "active.json",
            b"intended pointer",
            if_match='"stale"',
        )

    assert len(client.put_calls) == 1


def test_control_lost_response_reconciles_only_exact_bytes():
    exact = FakeS3()
    exact.next_ambiguous = "exact"
    assert s3mod.put_control_object(exact, "landing", "active.json", b"intended").body == b"intended"

    competing = FakeS3()
    competing.next_ambiguous = "competing"
    with pytest.raises(s3mod.ConditionalConflict):
        s3mod.put_control_object(competing, "landing", "active.json", b"intended")

    absent = FakeS3()
    absent.next_ambiguous = "absent"
    with pytest.raises(s3mod.AmbiguousWrite):
        s3mod.put_control_object(absent, "landing", "active.json", b"intended")


def test_read_control_rejects_missing_or_implausibly_skewed_server_date(
    monkeypatch: pytest.MonkeyPatch,
):
    _freeze_local_time(monkeypatch)
    missing = FakeS3()
    missing.seed("landing", "active.json", b"pointer")
    missing.get_date = None
    with pytest.raises(s3mod.AmbiguousWrite, match="Date"):
        s3mod.read_control_object(missing, "landing", "active.json")

    skewed = FakeS3()
    skewed.seed("landing", "active.json", b"pointer")
    skewed.get_date = NOW + timedelta(seconds=301)
    with pytest.raises(s3mod.AmbiguousWrite, match="skew"):
        s3mod.read_control_object(skewed, "landing", "active.json")


def test_acquire_missing_lease_uses_if_none_match(monkeypatch: pytest.MonkeyPatch):
    client = FakeS3()
    _freeze_local_time(monkeypatch)

    lease = s3mod.acquire_lease(client, "nyc_taxi", PUBLICATION_A, NONCE_A)

    assert client.put_calls[-1]["IfNoneMatch"] == "*"
    assert "IfMatch" not in client.put_calls[-1]
    assert lease.dataset == "nyc_taxi"
    assert lease.publication_id == PUBLICATION_A
    assert lease.owner_nonce == NONCE_A
    assert lease.state == "active"
    assert lease.created_at == NOW
    assert lease.expires_at == NOW + timedelta(seconds=60)


def test_acquire_released_lease_uses_opaque_if_match(monkeypatch: pytest.MonkeyPatch):
    client = FakeS3()
    _freeze_local_time(monkeypatch)
    key = "_leases/nyc_taxi.json"
    released = {
        "created_at": (NOW - timedelta(hours=1)).isoformat().replace("+00:00", "Z"),
        "dataset": "nyc_taxi",
        "expires_at": (NOW - timedelta(minutes=59)).isoformat().replace("+00:00", "Z"),
        "owner_nonce": "old-nonce",
        "publication_id": "old-publication",
        "state": "released",
    }
    client.seed(
        "landing",
        key,
        json.dumps(released, sort_keys=True, separators=(",", ":")).encode(),
        etag='"opaque/released:7"',
    )

    lease = s3mod.acquire_lease(client, "nyc_taxi", PUBLICATION_A, NONCE_A)

    assert client.put_calls[-1]["IfMatch"] == '"opaque/released:7"'
    assert lease.owner_nonce == NONCE_A


def test_acquire_rejects_unexpired_active_lease(monkeypatch: pytest.MonkeyPatch):
    client = FakeS3()
    _freeze_local_time(monkeypatch)
    first = s3mod.acquire_lease(client, "nyc_taxi", PUBLICATION_A, NONCE_A)
    put_count = len(client.put_calls)

    with pytest.raises(s3mod.ConditionalConflict, match="active"):
        s3mod.acquire_lease(client, "nyc_taxi", PUBLICATION_B, NONCE_B)

    assert len(client.put_calls) == put_count
    assert s3mod.read_control_object(client, first.bucket, first.key).etag == first.etag


def test_acquire_expired_lease_uses_if_match(monkeypatch: pytest.MonkeyPatch):
    client = FakeS3()
    _freeze_local_time(monkeypatch)
    first = s3mod.acquire_lease(client, "nyc_taxi", PUBLICATION_A, NONCE_A)
    client.now = first.expires_at
    client.get_date = first.expires_at
    monkeypatch.setattr(s3mod, "_utc_now", lambda: first.expires_at)

    successor = s3mod.acquire_lease(client, "nyc_taxi", PUBLICATION_B, NONCE_B)

    assert client.put_calls[-1]["IfMatch"] == first.etag
    assert successor.owner_nonce == NONCE_B


def test_acquire_restarts_after_five_second_proposal_window(
    monkeypatch: pytest.MonkeyPatch,
):
    client = FakeS3()
    _freeze_local_time(monkeypatch)
    ticks = iter((0.0, 5.001, 10.0, 10.1))
    monkeypatch.setattr(s3mod, "_monotonic", lambda: next(ticks))

    s3mod.acquire_lease(client, "nyc_taxi", PUBLICATION_A, NONCE_A)

    assert len(client.get_calls) == 3
    assert len(client.put_calls) == 1


def test_acquire_reconciles_lost_response_for_exact_lease(
    monkeypatch: pytest.MonkeyPatch,
):
    client = FakeS3()
    client.next_ambiguous = "exact"
    _freeze_local_time(monkeypatch)

    lease = s3mod.acquire_lease(client, "nyc_taxi", PUBLICATION_A, NONCE_A)

    assert lease.owner_nonce == NONCE_A
    assert len(client.put_calls) == 1


def test_acquire_fails_closed_when_write_date_is_outside_proposal(
    monkeypatch: pytest.MonkeyPatch,
):
    client = FakeS3()
    client.next_put_date = NOW + timedelta(seconds=61)
    _freeze_local_time(monkeypatch)

    with pytest.raises(s3mod.AmbiguousWrite, match="proposal"):
        s3mod.acquire_lease(client, "nyc_taxi", PUBLICATION_A, NONCE_A)


def test_renew_uses_latest_opaque_etag_and_returns_successor(
    monkeypatch: pytest.MonkeyPatch,
):
    client = FakeS3()
    _freeze_local_time(monkeypatch)
    first = s3mod.acquire_lease(client, "nyc_taxi", PUBLICATION_A, NONCE_A)
    client.now = NOW + timedelta(seconds=20)
    client.get_date = client.now
    monkeypatch.setattr(s3mod, "_utc_now", lambda: client.now)

    renewed = s3mod.renew_lease(client, first)

    assert client.put_calls[-1]["IfMatch"] == first.etag
    assert renewed.etag != first.etag
    assert renewed.owner_nonce == first.owner_nonce
    assert renewed.expires_at == client.now + timedelta(seconds=60)


def test_lost_lease_cannot_be_renewed(monkeypatch: pytest.MonkeyPatch):
    client = FakeS3()
    _freeze_local_time(monkeypatch)
    first = s3mod.acquire_lease(client, "nyc_taxi", PUBLICATION_A, NONCE_A)
    client.now = first.expires_at
    client.get_date = first.expires_at
    monkeypatch.setattr(s3mod, "_utc_now", lambda: client.now)
    successor = s3mod.acquire_lease(client, "nyc_taxi", PUBLICATION_B, NONCE_B)

    with pytest.raises(s3mod.ConditionalConflict):
        s3mod.renew_lease(client, first)

    current = s3mod.read_control_object(client, successor.bucket, successor.key)
    assert current.etag == successor.etag


def test_release_is_conditional_put(monkeypatch: pytest.MonkeyPatch):
    client = FakeS3()
    _freeze_local_time(monkeypatch)
    lease = s3mod.acquire_lease(client, "nyc_taxi", PUBLICATION_A, NONCE_A)

    released = s3mod.release_lease(client, lease)

    assert client.put_calls[-1]["IfMatch"] == lease.etag
    assert released.state == "released"
    assert released.expires_at == NOW


def test_stale_lease_owner_cannot_release_successor(monkeypatch: pytest.MonkeyPatch):
    client = FakeS3()
    _freeze_local_time(monkeypatch)
    first = s3mod.acquire_lease(client, "nyc_taxi", PUBLICATION_A, NONCE_A)
    client.now = first.expires_at
    client.get_date = first.expires_at
    monkeypatch.setattr(s3mod, "_utc_now", lambda: client.now)
    successor = s3mod.acquire_lease(client, "nyc_taxi", PUBLICATION_B, NONCE_B)

    with pytest.raises(s3mod.ConditionalConflict):
        s3mod.release_lease(client, first)

    current = s3mod.read_control_object(client, successor.bucket, successor.key)
    assert current.etag == successor.etag
