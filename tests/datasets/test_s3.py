import hashlib
import io
import json
from dataclasses import dataclass
from dataclasses import replace as dataclass_replace
from datetime import UTC, datetime, timedelta
from email.utils import format_datetime
from pathlib import Path
from typing import Any

import boto3
import pytest
from botocore.exceptions import ClientError, ConnectTimeoutError, ReadTimeoutError
from botocore.response import StreamingBody
from botocore.stub import ANY, Stubber
from moto import mock_aws

import datasets.s3 as s3mod
from datasets.locking import canonical_json
from datasets.verification import ExpectedObject, LockMismatch, VerificationContext

NOW = datetime.now(UTC).replace(microsecond=0)
CONTEXT = VerificationContext("nyc_taxi", "yellow", "remote", object_name="trips.parquet")
PUBLICATION_A = "123e4567e89b42d3a456426614174000"
PUBLICATION_B = "123e4567e89b42d3a456426614174001"
NONCE_A = "abcdefabcdefabcdefabcdefabcdefab"
NONCE_B = "abcdefabcdefabcdefabcdefabcdefac"
OLD_PUBLICATION = "550e8400e29b41d4a716446655440000"
OLD_NONCE = "fedcbafedcbafedcbafedcbafedcbafe"
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


class TrackingBody:
    def __init__(self, body: bytes, *, fail_read: bool = False, fail_close: bool = False):
        self._body = io.BytesIO(body)
        self.fail_read = fail_read
        self.fail_close = fail_close
        self.close_calls = 0

    def read(self, size: int = -1) -> bytes:
        if self.fail_read:
            raise ReadTimeoutError(endpoint_url="https://s3.invalid", error="read failed")
        return self._body.read(size)

    def close(self) -> None:
        self.close_calls += 1
        if self.fail_close:
            raise OSError("close failed")


class ResponseClient:
    def __init__(self, response: dict[str, object] | Exception):
        self.response = response
        self.calls = 0

    def get_object(self, **_request):
        self.calls += 1
        if isinstance(self.response, Exception):
            raise self.response
        return self.response


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


class ContentEtagS3(FakeS3):
    """S3 fake whose ETag is derived only from stored bytes."""

    def put_object(self, **request):
        response = super().put_object(**request)
        identity = (request["Bucket"], request["Key"])
        stored = self.objects[identity]
        etag = f'"{hashlib.md5(stored.body, usedforsecurity=False).hexdigest()}"'
        self.objects[identity] = _StoredObject(stored.body, stored.metadata, etag)
        response["ETag"] = etag
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


def _lease_document(
    *,
    state: str = "active",
    publication_id: object = PUBLICATION_A,
    owner_nonce: object = NONCE_A,
    created_at: object = _DEFAULT_DATE,
    expires_at: object = _DEFAULT_DATE,
    dataset: object = "nyc_taxi",
) -> dict[str, object]:
    created = NOW if created_at is _DEFAULT_DATE else created_at
    expires = NOW + timedelta(seconds=60) if expires_at is _DEFAULT_DATE else expires_at
    return {
        "created_at": created.isoformat().replace("+00:00", "Z") if isinstance(created, datetime) else created,
        "dataset": dataset,
        "expires_at": expires.isoformat().replace("+00:00", "Z") if isinstance(expires, datetime) else expires,
        "owner_nonce": owner_nonce,
        "publication_id": publication_id,
        "state": state,
    }


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
    assert client.meta.config.retries == {"total_max_attempts": 1, "mode": "legacy"}


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


@pytest.mark.parametrize("when", [None, NOW + timedelta(seconds=301)])
def test_stream_verify_closes_body_once_when_date_is_untrusted(monkeypatch: pytest.MonkeyPatch, when: datetime | None):
    _freeze_local_time(monkeypatch)
    body = TrackingBody(b"verified bytes")
    response = _streaming_response(b"", when=when)
    response["Body"] = body

    with pytest.raises(s3mod.AmbiguousWrite):
        s3mod.stream_verify_object(
            ResponseClient(response), "landing", "secret-object-key", _expected(b"verified bytes"), CONTEXT
        )

    assert body.close_calls == 1


def test_stream_verify_close_failure_preserves_primary_hash_mismatch():
    actual = b"wrong bytes"
    body = TrackingBody(actual, fail_close=True)
    response = _streaming_response(b"")
    response["Body"] = body

    with pytest.raises(LockMismatch, match="sha256") as caught:
        s3mod.stream_verify_object(
            ResponseClient(response),
            "landing",
            "object",
            ExpectedObject("trips.parquet", len(actual), hashlib.sha256(b"expected").hexdigest(), "schema-v1"),
            CONTEXT,
        )

    assert body.close_calls == 1
    assert any("close failed" in note for note in caught.value.__notes__)


def test_stream_verify_sole_close_failure_is_ambiguous():
    content = b"verified bytes"
    body = TrackingBody(content, fail_close=True)
    response = _streaming_response(b"")
    response["Body"] = body

    with pytest.raises(s3mod.AmbiguousWrite, match="close") as caught:
        s3mod.stream_verify_object(ResponseClient(response), "landing", "object", _expected(content), CONTEXT)

    assert body.close_calls == 1
    assert isinstance(caught.value.__cause__, OSError)


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

    with pytest.raises(s3mod.AmbiguousWrite) as caught:
        s3mod.put_immutable_object(
            client,
            "landing",
            "immutable/object",
            path,
            _expected(body),
            {"sha256": "expected"},
        )

    assert isinstance(caught.value.__cause__, LockMismatch)
    assert caught.value.__cause__.field == "metadata"


def test_put_immutable_maps_post_success_hash_mismatch_to_ambiguous(tmp_path: Path):
    intended = b"immutable bytes"
    path = tmp_path / "object.bin"
    path.write_bytes(intended)
    client = FakeS3()
    original_put = client.put_object

    def corrupt_bytes(**request):
        response = original_put(**request)
        stored = client.objects[("landing", "immutable/object")]
        client.objects[("landing", "immutable/object")] = _StoredObject(
            b"competing bytes", stored.metadata, stored.etag
        )
        return response

    client.put_object = corrupt_bytes

    with pytest.raises(s3mod.AmbiguousWrite) as caught:
        s3mod.put_immutable_object(client, "landing", "immutable/object", path, _expected(intended), {"lock": "exact"})

    assert isinstance(caught.value.__cause__, LockMismatch)


def test_immutable_5xx_write_reconciles_exact_value_without_retry(tmp_path: Path):
    body = b"immutable bytes"
    path = tmp_path / "object.bin"
    path.write_bytes(body)
    client = FakeS3()
    client.seed("landing", "immutable/object", body, metadata={"lock": "exact"})

    def server_error(**request):
        client.put_calls.append({**request, "Body": client._bytes(request["Body"])})
        raise _client_error("InternalError", 500)

    client.put_object = server_error

    snapshot = s3mod.put_immutable_object(
        client, "landing", "immutable/object", path, _expected(body), {"lock": "exact"}
    )

    assert snapshot.sha256 == hashlib.sha256(body).hexdigest()
    assert len(client.put_calls) == 1


def test_immutable_request_timeout_reconciles_exact_value(tmp_path: Path):
    body = b"immutable bytes"
    path = tmp_path / "object.bin"
    path.write_bytes(body)
    client = FakeS3()
    client.seed("landing", "immutable/object", body, metadata={"lock": "exact"})

    def request_timeout(**request):
        client.put_calls.append({**request, "Body": client._bytes(request["Body"])})
        raise _client_error("RequestTimeout", 400)

    client.put_object = request_timeout

    snapshot = s3mod.put_immutable_object(
        client, "landing", "immutable/object", path, _expected(body), {"lock": "exact"}
    )

    assert snapshot.sha256 == hashlib.sha256(body).hexdigest()
    assert len(client.put_calls) == 1
    assert len(client.get_calls) == 1


def test_immutable_post_success_transport_failure_is_ambiguous_and_redacted(tmp_path: Path):
    body = b"immutable bytes"
    path = tmp_path / "object.bin"
    path.write_bytes(body)
    client = FakeS3()

    def unavailable(**_request):
        raise ConnectTimeoutError(endpoint_url="https://s3.invalid", error="unavailable")

    client.get_object = unavailable

    with pytest.raises(s3mod.AmbiguousWrite) as caught:
        s3mod.put_immutable_object(client, "landing", "secret-object-key", path, _expected(body), {"lock": "exact"})

    assert isinstance(caught.value.__cause__, ConnectTimeoutError)
    assert "secret-object-key" not in str(caught.value)


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
    body = canonical_json({"pointer": "first"})
    client = FakeS3()

    snapshot = s3mod.put_control_object(client, "landing", "active.json", body, if_none_match=True)

    assert client.put_calls[0]["IfNoneMatch"] == "*"
    assert "IfMatch" not in client.put_calls[0]
    assert snapshot.body == body


def test_put_control_canonicalizes_unicode_json_with_shared_encoder():
    client = FakeS3()
    noncanonical = '{ "z": 1, "label": "café" }'.encode()
    expected = canonical_json({"z": 1, "label": "café"})

    snapshot = s3mod.put_control_object(client, "landing", "active.json", noncanonical)

    assert client.put_calls[0]["Body"] == expected
    assert snapshot.body == expected
    assert b"caf\xc3\xa9" in snapshot.body


def test_put_control_rejects_oversized_canonical_body_before_put():
    client = FakeS3()
    oversized = canonical_json({"value": "x" * (1 << 20)})

    with pytest.raises(ValueError, match="too large"):
        s3mod.put_control_object(client, "landing", "active.json", oversized)

    assert client.put_calls == []


@pytest.mark.parametrize("status", [409, 412])
def test_control_conditional_errors_reconcile_exact_self_write(status: int):
    body = canonical_json({"pointer": "intended"})
    client = FakeS3()
    client.seed("landing", "active.json", body, etag='"successor"')

    def conflict(**request):
        client.put_calls.append({**request, "Body": client._bytes(request["Body"])})
        raise _client_error("ConditionalRequestConflict" if status == 409 else "PreconditionFailed", status)

    client.put_object = conflict

    snapshot = s3mod.put_control_object(client, "landing", "active.json", body, if_match='"stale"')

    assert snapshot.body == body
    assert len(client.put_calls) == 1


def test_control_5xx_write_reconciles_exact_value_without_retry():
    body = canonical_json({"pointer": "intended"})
    client = FakeS3()
    client.seed("landing", "active.json", body, etag='"successor"')

    def server_error(**request):
        client.put_calls.append({**request, "Body": client._bytes(request["Body"])})
        raise _client_error("InternalError", 500)

    client.put_object = server_error

    snapshot = s3mod.put_control_object(client, "landing", "active.json", body, if_match='"stale"')

    assert snapshot.body == body
    assert len(client.put_calls) == 1


def test_control_request_timeout_reconciles_exact_value():
    body = canonical_json({"pointer": "intended"})
    client = FakeS3()
    client.seed("landing", "active.json", body, etag='"successor"')

    def request_timeout(**request):
        client.put_calls.append({**request, "Body": client._bytes(request["Body"])})
        raise _client_error("RequestTimeout", 400)

    client.put_object = request_timeout

    snapshot = s3mod.put_control_object(client, "landing", "active.json", body, if_match='"stale"')

    assert snapshot.body == body
    assert len(client.put_calls) == 1
    assert len(client.get_calls) == 1


def test_control_conflict_never_retries_stale_cas():
    client = FakeS3()
    client.seed("landing", "active.json", canonical_json({"pointer": "competing"}), etag='"successor"')

    with pytest.raises(s3mod.ConditionalConflict):
        s3mod.put_control_object(
            client,
            "landing",
            "active.json",
            canonical_json({"pointer": "intended"}),
            if_match='"stale"',
        )

    assert len(client.put_calls) == 1


def test_control_lost_response_reconciles_only_exact_bytes():
    intended = canonical_json({"pointer": "intended"})
    exact = FakeS3()
    exact.next_ambiguous = "exact"
    assert s3mod.put_control_object(exact, "landing", "active.json", intended).body == intended

    competing = FakeS3()
    competing.next_ambiguous = "competing"
    with pytest.raises(s3mod.ConditionalConflict):
        s3mod.put_control_object(competing, "landing", "active.json", intended)

    absent = FakeS3()
    absent.next_ambiguous = "absent"
    with pytest.raises(s3mod.AmbiguousWrite):
        s3mod.put_control_object(absent, "landing", "active.json", intended)


@pytest.mark.parametrize(
    "failure",
    [
        ConnectTimeoutError(endpoint_url="https://s3.invalid", error="unavailable"),
        _client_error("InternalError", 500),
    ],
)
def test_control_post_success_read_failure_is_ambiguous_and_redacted(failure: Exception):
    client = FakeS3()
    original_put = client.put_object

    def put_then_break_get(**request):
        response = original_put(**request)

        def unavailable(**_read_request):
            raise failure

        client.get_object = unavailable
        return response

    client.put_object = put_then_break_get

    with pytest.raises(s3mod.AmbiguousWrite) as caught:
        s3mod.put_control_object(client, "landing", "secret-pointer-key", canonical_json({"pointer": "intended"}))

    assert caught.value.__cause__ is failure
    assert "secret-pointer-key" not in str(caught.value)


def test_control_reconciliation_invalid_response_is_ambiguous_and_closes_body():
    client = FakeS3()
    body = TrackingBody(canonical_json({"pointer": "intended"}))

    def lost(**_request):
        raise ReadTimeoutError(endpoint_url="https://s3.invalid", error="lost response")

    client.put_object = lost
    client.get_object = lambda **_request: {
        "Body": body,
        "ResponseMetadata": _response_metadata(),
    }

    with pytest.raises(s3mod.AmbiguousWrite):
        s3mod.put_control_object(client, "landing", "active.json", canonical_json({"pointer": "intended"}))

    assert body.close_calls == 1


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


@pytest.mark.parametrize("when", [None, NOW + timedelta(seconds=301)])
def test_read_control_closes_body_once_when_date_is_untrusted(monkeypatch: pytest.MonkeyPatch, when: datetime | None):
    _freeze_local_time(monkeypatch)
    body = TrackingBody(b"pointer")
    response = _streaming_response(b"", when=when)
    response["Body"] = body

    with pytest.raises(s3mod.AmbiguousWrite):
        s3mod.read_control_object(ResponseClient(response), "landing", "active.json")

    assert body.close_calls == 1


def test_read_control_bounds_body_and_closes_once():
    body = TrackingBody(b"x" * ((1 << 20) + 1))
    response = _streaming_response(b"")
    response["Body"] = body

    with pytest.raises(s3mod.AmbiguousWrite, match="too large"):
        s3mod.read_control_object(ResponseClient(response), "landing", "active.json")

    assert body.close_calls == 1


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
    assert lease.key == "_data-eng-locks/leases/nyc_taxi.json"


def test_acquire_lease_uses_shared_canonical_unicode_json(monkeypatch: pytest.MonkeyPatch):
    client = FakeS3()
    _freeze_local_time(monkeypatch)

    lease = s3mod.acquire_lease(client, "café", PUBLICATION_A, NONCE_A)

    expected = canonical_json(_lease_document(dataset="café"))
    assert client.put_calls[-1]["Body"] == expected
    assert client.objects[(lease.bucket, lease.key)].body == expected
    assert b"caf\xc3\xa9" in expected


@pytest.mark.parametrize(
    ("publication_id", "owner_nonce"),
    [
        ("publication-a", NONCE_A),
        (PUBLICATION_A.upper(), NONCE_A),
        ("0" * 32, NONCE_A),
        (PUBLICATION_A, "nonce-a"),
        (PUBLICATION_A, "g" * 32),
    ],
)
def test_acquire_rejects_non_uuid4_publication_or_nonce(
    monkeypatch: pytest.MonkeyPatch, publication_id: str, owner_nonce: str
):
    client = FakeS3()
    _freeze_local_time(monkeypatch)

    with pytest.raises(ValueError, match="128-bit"):
        s3mod.acquire_lease(client, "nyc_taxi", publication_id, owner_nonce)

    assert client.get_calls == []
    assert client.put_calls == []


@pytest.mark.parametrize("lease_seconds", [0, -1, True, 1.5, 301, float("inf")])
def test_acquire_rejects_invalid_lease_duration(monkeypatch: pytest.MonkeyPatch, lease_seconds: object):
    client = FakeS3()
    _freeze_local_time(monkeypatch)

    with pytest.raises(ValueError, match="lease duration"):
        s3mod.acquire_lease(
            client,
            "nyc_taxi",
            PUBLICATION_A,
            NONCE_A,
            lease_seconds=lease_seconds,
        )

    assert client.get_calls == []
    assert client.put_calls == []


def test_acquire_released_lease_uses_opaque_if_match(monkeypatch: pytest.MonkeyPatch):
    client = FakeS3()
    _freeze_local_time(monkeypatch)
    key = "_data-eng-locks/leases/nyc_taxi.json"
    released = {
        "created_at": (NOW - timedelta(hours=1)).isoformat().replace("+00:00", "Z"),
        "dataset": "nyc_taxi",
        "expires_at": (NOW - timedelta(minutes=59)).isoformat().replace("+00:00", "Z"),
        "owner_nonce": OLD_NONCE,
        "publication_id": OLD_PUBLICATION,
        "state": "released",
    }
    client.seed(
        "landing",
        key,
        canonical_json(released),
        etag='"opaque/released:7"',
    )

    lease = s3mod.acquire_lease(client, "nyc_taxi", PUBLICATION_A, NONCE_A)

    assert client.put_calls[-1]["IfMatch"] == '"opaque/released:7"'
    assert lease.owner_nonce == NONCE_A


@pytest.mark.parametrize(
    "document",
    [
        _lease_document(publication_id="invalid"),
        _lease_document(owner_nonce="invalid"),
        _lease_document(created_at="2026-08-12T12:00:00"),
        _lease_document(created_at=NOW, expires_at=NOW),
        _lease_document(created_at=NOW + timedelta(seconds=1), expires_at=NOW),
        _lease_document(state="unknown"),
        _lease_document(state="released", expires_at=NOW + timedelta(seconds=60)),
    ],
)
def test_acquire_rejects_malformed_stored_lease(monkeypatch: pytest.MonkeyPatch, document: dict[str, object]):
    client = FakeS3()
    _freeze_local_time(monkeypatch)
    key = "_data-eng-locks/leases/nyc_taxi.json"
    client.seed("landing", key, canonical_json(document))

    with pytest.raises(s3mod.AmbiguousWrite, match="malformed"):
        s3mod.acquire_lease(client, "nyc_taxi", PUBLICATION_B, NONCE_B)

    assert client.put_calls == []


def test_acquire_rejects_noncanonical_stored_lease(monkeypatch: pytest.MonkeyPatch):
    client = FakeS3()
    _freeze_local_time(monkeypatch)
    key = "_data-eng-locks/leases/nyc_taxi.json"
    body = json.dumps(_lease_document(), sort_keys=False, indent=2).encode()
    assert body != canonical_json(_lease_document())
    client.seed("landing", key, body)

    with pytest.raises(s3mod.AmbiguousWrite, match="canonical"):
        s3mod.acquire_lease(client, "nyc_taxi", PUBLICATION_B, NONCE_B)

    assert client.put_calls == []


def test_acquire_closes_lease_body_once_on_decode_failure(monkeypatch: pytest.MonkeyPatch):
    _freeze_local_time(monkeypatch)
    body = TrackingBody(b"not-json")
    response = _streaming_response(b"")
    response["Body"] = body

    with pytest.raises(s3mod.AmbiguousWrite, match="malformed"):
        s3mod.acquire_lease(ResponseClient(response), "nyc_taxi", PUBLICATION_A, NONCE_A)

    assert body.close_calls == 1


def test_acquire_maps_observation_transport_failure_to_ambiguous(monkeypatch: pytest.MonkeyPatch):
    _freeze_local_time(monkeypatch)
    failure = ConnectTimeoutError(endpoint_url="https://s3.invalid", error="unavailable")

    with pytest.raises(s3mod.AmbiguousWrite) as caught:
        s3mod.acquire_lease(ResponseClient(failure), "nyc_taxi", PUBLICATION_A, NONCE_A)

    assert caught.value.__cause__ is failure


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
    ticks = iter((0.0, 5.001, 10.0, 10.1, 10.2))
    monkeypatch.setattr(s3mod, "_monotonic", lambda: next(ticks))

    s3mod.acquire_lease(client, "nyc_taxi", PUBLICATION_A, NONCE_A)

    assert len(client.get_calls) == 3
    assert len(client.put_calls) == 1


def test_acquire_rechecks_proposal_window_after_serialization(monkeypatch: pytest.MonkeyPatch):
    client = FakeS3()
    _freeze_local_time(monkeypatch)
    clock = [0.0]
    serialization_calls = 0
    original_lease_body = s3mod._lease_body

    def delayed_first_serialization(lease):
        nonlocal serialization_calls
        serialization_calls += 1
        body = original_lease_body(lease)
        if serialization_calls == 1:
            clock[0] = 5.1
        return body

    monkeypatch.setattr(s3mod, "_monotonic", lambda: clock[0])
    monkeypatch.setattr(s3mod, "_lease_body", delayed_first_serialization)

    s3mod.acquire_lease(client, "nyc_taxi", PUBLICATION_A, NONCE_A)

    assert serialization_calls == 2
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


@pytest.mark.parametrize("ambiguous", [True, False])
def test_acquire_never_returns_lease_when_reconciliation_get_is_expired(
    monkeypatch: pytest.MonkeyPatch, ambiguous: bool
):
    client = FakeS3()
    _freeze_local_time(monkeypatch)
    original_put = client.put_object
    if ambiguous:
        client.next_ambiguous = "exact"
    else:
        client.next_put_date = NOW + timedelta(seconds=60)

    def expire_before_reconciliation(**request):
        try:
            return original_put(**request)
        finally:
            client.get_date = NOW + timedelta(seconds=60)

    client.put_object = expire_before_reconciliation

    with pytest.raises(s3mod.AmbiguousWrite, match="expired"):
        s3mod.acquire_lease(client, "nyc_taxi", PUBLICATION_A, NONCE_A)

    assert len(client.put_calls) == 1


def test_acquire_fails_closed_when_write_date_is_outside_proposal(
    monkeypatch: pytest.MonkeyPatch,
):
    client = FakeS3()
    client.next_put_date = NOW + timedelta(seconds=61)
    _freeze_local_time(monkeypatch)

    with pytest.raises(s3mod.AmbiguousWrite, match="proposal"):
        s3mod.acquire_lease(client, "nyc_taxi", PUBLICATION_A, NONCE_A)


def test_acquire_request_timeout_reconciles_exact_lease(monkeypatch: pytest.MonkeyPatch):
    client = FakeS3()
    _freeze_local_time(monkeypatch)
    original_put = client.put_object

    def timeout_after_commit(**request):
        original_put(**request)
        raise _client_error("RequestTimeout", 400)

    client.put_object = timeout_after_commit

    lease = s3mod.acquire_lease(client, "nyc_taxi", PUBLICATION_A, NONCE_A)

    assert lease.owner_nonce == NONCE_A
    assert len(client.put_calls) == 1
    assert len(client.get_calls) == 2


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


def test_renew_waits_for_changed_canonical_version_and_rejects_stale_handle(
    monkeypatch: pytest.MonkeyPatch,
):
    client = ContentEtagS3()
    _freeze_local_time(monkeypatch)
    first = s3mod.acquire_lease(client, "nyc_taxi", PUBLICATION_A, NONCE_A)
    dates = iter(
        (
            NOW,
            NOW + timedelta(seconds=1),
            NOW + timedelta(seconds=1),
            NOW + timedelta(seconds=1),
        )
    )
    original_get = client.get_object

    def advance_date(**request):
        client.get_date = next(dates)
        client.now = client.get_date
        return original_get(**request)

    client.get_object = advance_date
    monkeypatch.setattr(s3mod, "_utc_now", lambda: client.get_date)

    renewed = s3mod.renew_lease(client, first)

    assert renewed.expires_at > first.expires_at
    assert renewed.etag != first.etag
    assert len(client.put_calls) == 2
    with pytest.raises(s3mod.ConditionalConflict):
        s3mod.release_lease(client, first)


def test_renew_fails_closed_after_bounded_unchanged_observations(monkeypatch: pytest.MonkeyPatch):
    client = ContentEtagS3()
    _freeze_local_time(monkeypatch)
    first = s3mod.acquire_lease(client, "nyc_taxi", PUBLICATION_A, NONCE_A)
    get_count = len(client.get_calls)
    put_count = len(client.put_calls)

    with pytest.raises(s3mod.AmbiguousWrite, match="changed canonical version"):
        s3mod.renew_lease(client, first)

    assert len(client.get_calls) - get_count == 5
    assert len(client.put_calls) == put_count


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
    client.now = NOW + timedelta(seconds=1)
    client.get_date = client.now
    monkeypatch.setattr(s3mod, "_utc_now", lambda: client.now)

    released = s3mod.release_lease(client, lease)

    assert client.put_calls[-1]["IfMatch"] == lease.etag
    assert released.state == "released"
    assert released.created_at == NOW
    assert released.expires_at == client.now


def test_release_succeeds_with_same_server_date(monkeypatch: pytest.MonkeyPatch):
    client = FakeS3()
    _freeze_local_time(monkeypatch)
    lease = s3mod.acquire_lease(client, "nyc_taxi", PUBLICATION_A, NONCE_A)

    released = s3mod.release_lease(client, lease)

    assert released.state == "released"
    assert released.created_at == NOW
    assert released.expires_at == NOW


def test_one_second_lease_release_succeeds_same_second(monkeypatch: pytest.MonkeyPatch):
    client = FakeS3()
    _freeze_local_time(monkeypatch)
    lease = s3mod.acquire_lease(client, "nyc_taxi", PUBLICATION_A, NONCE_A, lease_seconds=1)

    released = s3mod.release_lease(client, lease)

    assert released.created_at == released.expires_at == NOW


def test_release_rejects_expired_lease(monkeypatch: pytest.MonkeyPatch):
    client = FakeS3()
    _freeze_local_time(monkeypatch)
    lease = s3mod.acquire_lease(client, "nyc_taxi", PUBLICATION_A, NONCE_A)
    client.now = lease.expires_at
    client.get_date = lease.expires_at
    monkeypatch.setattr(s3mod, "_utc_now", lambda: lease.expires_at)
    put_count = len(client.put_calls)

    with pytest.raises(s3mod.ConditionalConflict, match="lost"):
        s3mod.release_lease(client, lease)

    assert len(client.put_calls) == put_count


def test_renew_and_release_reject_malformed_lease_capability(monkeypatch: pytest.MonkeyPatch):
    client = FakeS3()
    _freeze_local_time(monkeypatch)
    lease = s3mod.acquire_lease(client, "nyc_taxi", PUBLICATION_A, NONCE_A)
    malformed = dataclass_replace(lease, owner_nonce="invalid")
    get_count = len(client.get_calls)

    with pytest.raises(ValueError, match="128-bit"):
        s3mod.renew_lease(client, malformed)
    with pytest.raises(ValueError, match="128-bit"):
        s3mod.release_lease(client, malformed)

    assert len(client.get_calls) == get_count


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
