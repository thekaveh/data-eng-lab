"""Thin boto3 helper for landing objects into MinIO, configured from infra/.env."""

from __future__ import annotations

import json
import re
import time
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
from email.utils import parsedate_to_datetime
from pathlib import Path

import boto3
from botocore.client import Config
from botocore.exceptions import BotoCoreError, ClientError

from datasets.locking import canonical_json
from datasets.verification import ExpectedObject, LockMismatch, VerificationContext, verify_stream
from lakehouse.atlas_endpoints import resolve_http_endpoint

_MAX_CLOCK_SKEW = timedelta(seconds=300)
_PROPOSAL_WINDOW_SECONDS = 5.0
_LEASE_SECONDS = 60
_MAX_LEASE_SECONDS = 300
_MAX_RENEW_OBSERVATIONS = 5
_LEASE_BUCKET = "landing"
_LEASE_PREFIX = "_data-eng-locks/leases"
_MAX_CONTROL_BYTES = 1 << 20
_UUID4_HEX_RE = re.compile(r"^[0-9a-f]{12}4[0-9a-f]{3}[89ab][0-9a-f]{15}$")


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


@dataclass(frozen=True)
class Lease:
    dataset: str
    publication_id: str
    owner_nonce: str
    state: str
    created_at: datetime
    expires_at: datetime
    etag: str
    bucket: str
    key: str


class ConditionalConflict(RuntimeError):
    """A conditional write lost to a different object value."""


class AmbiguousWrite(RuntimeError):
    """A write cannot be proven successful from an exact subsequent GET."""


class _ProposalExpired(RuntimeError):
    """An observed server Date became too old before its conditional write."""


def _utc_now() -> datetime:
    return datetime.now(UTC)


def _monotonic() -> float:
    return time.monotonic()


def _response_server_date(response: object) -> datetime:
    if not isinstance(response, Mapping):
        raise AmbiguousWrite("S3 response is not a mapping")
    response_metadata = response.get("ResponseMetadata")
    headers = response_metadata.get("HTTPHeaders", {}) if isinstance(response_metadata, Mapping) else {}
    raw_date = headers.get("date") if isinstance(headers, Mapping) else None
    if not isinstance(raw_date, str):
        raise AmbiguousWrite("S3 response is missing its Date header")
    try:
        server_date = parsedate_to_datetime(raw_date)
    except (TypeError, ValueError) as error:
        raise AmbiguousWrite("S3 response has an invalid Date header") from error
    if server_date.tzinfo is None:
        server_date = server_date.replace(tzinfo=UTC)
    server_date = server_date.astimezone(UTC)
    if abs(server_date - _utc_now()) > _MAX_CLOCK_SKEW:
        raise AmbiguousWrite("S3 response Date has implausible clock skew")
    return server_date


def _response_etag(response: Mapping[str, object]) -> str:
    etag = response.get("ETag")
    if not isinstance(etag, str) or not etag:
        raise AmbiguousWrite("S3 response has an invalid ETag")
    return etag


def _error_status(error: ClientError) -> int | None:
    metadata = error.response.get("ResponseMetadata", {})
    status = metadata.get("HTTPStatusCode") if isinstance(metadata, Mapping) else None
    return status if isinstance(status, int) else None


def _error_code(error: ClientError) -> str:
    details = error.response.get("Error", {})
    code = details.get("Code") if isinstance(details, Mapping) else None
    return code if isinstance(code, str) else ""


def _is_not_found(error: ClientError) -> bool:
    return _error_status(error) == 404 or _error_code(error) in {
        "404",
        "NoSuchKey",
        "NotFound",
    }


def _is_conditional_error(error: ClientError) -> bool:
    return _error_status(error) in {409, 412} or _error_code(error) in {
        "409",
        "412",
        "ConditionalRequestConflict",
        "PreconditionFailed",
    }


def _is_server_error(error: ClientError) -> bool:
    status = _error_status(error)
    return status is not None and 500 <= status <= 599


def _is_timeout_error(error: ClientError) -> bool:
    return _error_status(error) == 408 or _error_code(error) in {
        "RequestExpired",
        "RequestTimeout",
        "RequestTimeoutException",
    }


def _close_body(body: object) -> None:
    close = getattr(body, "close", None)
    if callable(close):
        close()


@contextmanager
def _owned_body(body: object) -> Iterator[object]:
    try:
        yield body
    except BaseException as primary:
        try:
            _close_body(body)
        except BaseException as close_error:
            primary.add_note(f"S3 response body close failed: {type(close_error).__name__}: {close_error}")
        raise
    else:
        try:
            _close_body(body)
        except BaseException as close_error:
            raise AmbiguousWrite("S3 response body close failed") from close_error


def _ambiguous_read(error: BaseException) -> AmbiguousWrite:
    return AmbiguousWrite("S3 object read could not establish an exact value")


def _get_object(client, bucket: str, key: str) -> Mapping[str, object]:
    try:
        response = client.get_object(Bucket=bucket, Key=key)
    except (BotoCoreError, ClientError) as error:
        raise _ambiguous_read(error) from error
    if not isinstance(response, Mapping):
        error = TypeError("S3 GetObject response must be a mapping")
        raise _ambiguous_read(error) from error
    return response


def _read_bounded(stream: object, limit: int = _MAX_CONTROL_BYTES) -> bytes:
    read = getattr(stream, "read", None)
    if not callable(read):
        raise TypeError("S3 response body is not readable")
    chunks: list[bytes] = []
    size = 0
    while size <= limit:
        chunk = read(min(64 << 10, limit + 1 - size))
        if not isinstance(chunk, bytes):
            raise TypeError("S3 response body returned non-bytes")
        if not chunk:
            break
        chunks.append(chunk)
        size += len(chunk)
    if size > limit:
        raise AmbiguousWrite("S3 control object is too large")
    return b"".join(chunks)


def _canonical_control_body(body: bytes) -> bytes:
    try:
        document = json.loads(
            body,
            parse_constant=lambda value: (_ for _ in ()).throw(ValueError(f"non-finite JSON value {value}")),
        )
    except (UnicodeDecodeError, ValueError, json.JSONDecodeError) as error:
        raise ValueError("control object body must be a canonical JSON mapping") from error
    if not isinstance(document, Mapping):
        raise ValueError("control object body must be a canonical JSON mapping")
    canonical = canonical_json(document)
    if len(canonical) > _MAX_CONTROL_BYTES:
        raise ValueError("S3 control object is too large")
    return canonical


def _envval(key: str, env_file: Path) -> str:
    if not env_file.exists():
        return ""
    val = ""
    for line in env_file.read_text(encoding="utf-8").splitlines():
        if line.startswith(f"{key}="):
            val = line.split("=", 1)[1].strip()  # last wins
    return val


def s3_client_from_env(infra_dir: Path):
    env_file = Path(infra_dir) / ".env"
    export_file = Path(infra_dir).parent / "atlas-consumer.env"
    user = _envval("MINIO_ROOT_USER", env_file)
    password = _envval("MINIO_ROOT_PASSWORD", env_file)
    port = _envval("MINIO_PORT", env_file)
    if not (user and password and port):
        raise RuntimeError(
            f"MinIO creds/port missing in {env_file} — start the stack (make up) first so Atlas generates them."
        )
    minio_endpoint = resolve_http_endpoint(
        "MINIO_HOST_ENDPOINT",
        "MINIO_PORT",
        env_file=env_file,
        export_key="ATLAS_MINIO_HOST_ENDPOINT",
        export_file=export_file,
    )
    return boto3.client(
        "s3",
        endpoint_url=minio_endpoint,
        aws_access_key_id=user,
        aws_secret_access_key=password,
        region_name="us-east-1",
        config=Config(
            s3={"addressing_style": "path"},
            retries={"total_max_attempts": 1, "mode": "legacy"},
        ),
    )


def object_exists(client, bucket: str, key: str) -> bool:
    from botocore.exceptions import ClientError

    try:
        client.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError as exc:
        if exc.response["Error"]["Code"] in ("404", "NoSuchKey", "NotFound"):
            return False
        raise


def upload_file(client, path: Path, bucket: str, key: str) -> None:
    client.upload_file(str(path), bucket, key)


def stream_verify_object(
    client,
    bucket: str,
    key: str,
    expected: ExpectedObject,
    context: VerificationContext,
) -> ObjectSnapshot:
    """GET an object and verify its bytes; HEAD data is never trusted."""
    response = _get_object(client, bucket, key)
    try:
        body = response["Body"]
    except (KeyError, TypeError) as error:
        raise _ambiguous_read(error) from error
    with _owned_body(body):
        try:
            server_date = _response_server_date(response)
            etag = _response_etag(response)
            metadata = dict(response.get("Metadata", {}))
            size_bytes, sha256 = verify_stream(body, expected.size_bytes, expected.sha256, context)
        except LockMismatch:
            raise
        except AmbiguousWrite:
            raise
        except (BotoCoreError, ClientError, KeyError, TypeError, ValueError) as error:
            raise _ambiguous_read(error) from error
    return ObjectSnapshot(
        etag=etag,
        metadata=metadata,
        server_date=server_date,
        size_bytes=size_bytes,
        sha256=sha256,
    )


def _verified_immutable_snapshot(
    client,
    bucket: str,
    key: str,
    expected: ExpectedObject,
    metadata: Mapping[str, str],
) -> ObjectSnapshot:
    context = VerificationContext(
        dataset=bucket,
        scale="object-store",
        stage="remote object",
        object_name=expected.object_name,
    )
    snapshot = stream_verify_object(client, bucket, key, expected, context)
    expected_metadata = dict(metadata)
    if dict(snapshot.metadata) != expected_metadata:
        raise LockMismatch(context, "metadata", expected_metadata, dict(snapshot.metadata))
    return snapshot


def _reconcile_immutable_write(
    client,
    bucket: str,
    key: str,
    expected: ExpectedObject,
    metadata: Mapping[str, str],
) -> ObjectSnapshot:
    try:
        return _verified_immutable_snapshot(client, bucket, key, expected, metadata)
    except ClientError as error:
        if _is_not_found(error):
            raise AmbiguousWrite("immutable object write was not observed") from error
        raise
    except LockMismatch as error:
        raise ConditionalConflict("immutable object contains competing bytes or metadata") from error


def put_immutable_object(
    client,
    bucket: str,
    key: str,
    path: Path,
    expected: ExpectedObject,
    metadata: Mapping[str, str],
) -> ObjectSnapshot:
    """Create an immutable object conditionally, then verify a full remote GET."""
    with path.open("rb") as body:
        try:
            client.put_object(
                Bucket=bucket,
                Key=key,
                Body=body,
                Metadata=dict(metadata),
                IfNoneMatch="*",
            )
        except ClientError as error:
            if not (_is_conditional_error(error) or _is_server_error(error) or _is_timeout_error(error)):
                raise
            return _reconcile_immutable_write(client, bucket, key, expected, metadata)
        except BotoCoreError:
            return _reconcile_immutable_write(client, bucket, key, expected, metadata)
    try:
        return _verified_immutable_snapshot(client, bucket, key, expected, metadata)
    except LockMismatch as error:
        raise AmbiguousWrite("post-upload GET did not establish the immutable object") from error


def read_control_object(client, bucket: str, key: str) -> ControlSnapshot:
    response = _get_object(client, bucket, key)
    try:
        stream = response["Body"]
    except (KeyError, TypeError) as error:
        raise _ambiguous_read(error) from error
    with _owned_body(stream):
        try:
            server_date = _response_server_date(response)
            etag = _response_etag(response)
            body = _read_bounded(stream)
        except AmbiguousWrite:
            raise
        except (BotoCoreError, ClientError, KeyError, TypeError, ValueError) as error:
            raise _ambiguous_read(error) from error
    return ControlSnapshot(body=body, etag=etag, server_date=server_date)


def _reconcile_control_write(client, bucket: str, key: str, intended_body: bytes) -> ControlSnapshot:
    try:
        snapshot = read_control_object(client, bucket, key)
    except ClientError as error:
        if _is_not_found(error):
            raise AmbiguousWrite("control object write was not observed") from error
        raise
    if snapshot.body != intended_body:
        raise ConditionalConflict("control object contains competing bytes")
    return snapshot


def _put_control_request(
    client,
    bucket: str,
    key: str,
    body: bytes,
    *,
    if_match: str | None = None,
    if_none_match: bool = False,
) -> tuple[ControlSnapshot, Mapping[str, object] | None]:
    if if_match is not None and if_none_match:
        raise ValueError("IfMatch and IfNoneMatch are mutually exclusive")
    request: dict[str, object] = {"Bucket": bucket, "Key": key, "Body": body}
    if if_match is not None:
        request["IfMatch"] = if_match
    if if_none_match:
        request["IfNoneMatch"] = "*"
    try:
        response = client.put_object(**request)
    except ClientError as error:
        if not (_is_conditional_error(error) or _is_server_error(error) or _is_timeout_error(error)):
            raise
        return _reconcile_control_write(client, bucket, key, body), None
    except BotoCoreError:
        return _reconcile_control_write(client, bucket, key, body), None
    snapshot = _reconcile_control_write(client, bucket, key, body)
    if not isinstance(response, Mapping):
        raise AmbiguousWrite("S3 successful write response is not a mapping")
    return snapshot, response


def put_control_object(
    client,
    bucket: str,
    key: str,
    body: bytes,
    *,
    if_match: str | None = None,
    if_none_match: bool = False,
) -> ControlSnapshot:
    """Conditionally write and exactly re-read a small control object."""
    intended_body = _canonical_control_body(body)
    snapshot, _response = _put_control_request(
        client,
        bucket,
        key,
        intended_body,
        if_match=if_match,
        if_none_match=if_none_match,
    )
    return snapshot


def _lease_key(dataset: str) -> str:
    return f"{_LEASE_PREFIX}/{dataset}.json"


def _format_instant(value: datetime) -> str:
    return value.astimezone(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")


def _parse_instant(value: object) -> datetime:
    if not isinstance(value, str):
        raise ValueError("lease instant must be a string")
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None or parsed.utcoffset() != timedelta(0):
        raise ValueError("lease instant must use UTC")
    return parsed.astimezone(UTC)


def _validate_lease_identity(publication_id: object, owner_nonce: object) -> None:
    if not isinstance(publication_id, str) or _UUID4_HEX_RE.fullmatch(publication_id) is None:
        raise ValueError("publication identifier must be a uuid4-style 128-bit lowercase hexadecimal value")
    if not isinstance(owner_nonce, str) or re.fullmatch(r"[0-9a-f]{32}", owner_nonce) is None:
        raise ValueError("owner nonce must be a 128-bit lowercase hexadecimal value")


def _validate_lease_duration(lease_seconds: object) -> int:
    if (
        isinstance(lease_seconds, bool)
        or not isinstance(lease_seconds, int)
        or not 1 <= lease_seconds <= _MAX_LEASE_SECONDS
    ):
        raise ValueError(f"lease duration must be an integer from 1 to {_MAX_LEASE_SECONDS} seconds")
    return lease_seconds


def _validate_lease(lease: Lease, *, observed_at: datetime | None = None) -> None:
    _validate_lease_identity(lease.publication_id, lease.owner_nonce)
    if lease.state not in {"active", "released"}:
        raise ValueError("lease state is invalid")
    if (
        lease.created_at.tzinfo is None
        or lease.created_at.utcoffset() != timedelta(0)
        or lease.expires_at.tzinfo is None
        or lease.expires_at.utcoffset() != timedelta(0)
    ):
        raise ValueError("lease instants must be timezone-aware UTC")
    if lease.state == "active" and lease.created_at >= lease.expires_at:
        raise ValueError("active lease creation must precede expiry")
    if lease.state == "released" and lease.created_at > lease.expires_at:
        raise ValueError("released lease creation must not follow expiry")
    duration = lease.expires_at - lease.created_at
    if duration > timedelta(seconds=_MAX_LEASE_SECONDS):
        raise ValueError("lease duration exceeds the allowed maximum")
    if observed_at is not None:
        if lease.created_at > observed_at:
            raise ValueError("lease creation is after the server observation")
        if lease.state == "released" and lease.expires_at > observed_at:
            raise ValueError("released lease is not expired at the server observation")
    if lease.key != _lease_key(lease.dataset):
        raise ValueError("lease key does not match its dataset")


def _lease_body(lease: Lease) -> bytes:
    _validate_lease(lease)
    document = {
        "created_at": _format_instant(lease.created_at),
        "dataset": lease.dataset,
        "expires_at": _format_instant(lease.expires_at),
        "owner_nonce": lease.owner_nonce,
        "publication_id": lease.publication_id,
        "state": lease.state,
    }
    return canonical_json(document)


def _lease_from_snapshot(snapshot: ControlSnapshot, *, dataset: str, bucket: str, key: str) -> Lease:
    try:
        document = json.loads(snapshot.body)
        expected_fields = {
            "created_at",
            "dataset",
            "expires_at",
            "owner_nonce",
            "publication_id",
            "state",
        }
        if not isinstance(document, dict) or set(document) != expected_fields:
            raise ValueError("lease fields are not exact")
        if document["dataset"] != dataset:
            raise ValueError("lease dataset does not match its key")
        if document["state"] not in {"active", "released"}:
            raise ValueError("lease state is invalid")
        created_at = _parse_instant(document["created_at"])
        expires_at = _parse_instant(document["expires_at"])
        if canonical_json(document) != snapshot.body:
            raise AmbiguousWrite("stored lease is not canonical JSON")
        lease = Lease(
            dataset=dataset,
            publication_id=document["publication_id"],
            owner_nonce=document["owner_nonce"],
            state=document["state"],
            created_at=created_at,
            expires_at=expires_at,
            etag=snapshot.etag,
            bucket=bucket,
            key=key,
        )
        _validate_lease(lease, observed_at=snapshot.server_date)
        return lease
    except AmbiguousWrite:
        raise
    except (KeyError, TypeError, UnicodeDecodeError, ValueError, json.JSONDecodeError) as error:
        raise AmbiguousWrite("stored lease is malformed") from error


def _read_lease_observation(client, bucket: str, key: str) -> tuple[ControlSnapshot | None, datetime, float]:
    try:
        response = client.get_object(Bucket=bucket, Key=key)
    except ClientError as error:
        observed_at = _monotonic()
        if _is_not_found(error):
            try:
                return None, _response_server_date(error.response), observed_at
            except AmbiguousWrite:
                raise
            except (KeyError, TypeError, ValueError) as invalid:
                raise _ambiguous_read(invalid) from invalid
        raise _ambiguous_read(error) from error
    except BotoCoreError as error:
        raise _ambiguous_read(error) from error
    observed_at = _monotonic()
    if not isinstance(response, Mapping):
        error = TypeError("S3 GetObject response must be a mapping")
        raise _ambiguous_read(error) from error
    try:
        stream = response["Body"]
    except (KeyError, TypeError) as error:
        raise _ambiguous_read(error) from error
    with _owned_body(stream):
        try:
            server_date = _response_server_date(response)
            etag = _response_etag(response)
            body = _read_bounded(stream)
        except AmbiguousWrite:
            raise
        except (BotoCoreError, ClientError, KeyError, TypeError, ValueError) as error:
            raise _ambiguous_read(error) from error
    snapshot = ControlSnapshot(body=body, etag=etag, server_date=server_date)
    return snapshot, server_date, observed_at


def _write_lease(
    client,
    proposed: Lease,
    *,
    observed_at: float,
    if_match: str | None = None,
    if_none_match: bool = False,
) -> Lease:
    _validate_lease(proposed)
    body = _lease_body(proposed)
    if _monotonic() - observed_at > _PROPOSAL_WINDOW_SECONDS:
        raise _ProposalExpired
    snapshot, write_response = _put_control_request(
        client,
        proposed.bucket,
        proposed.key,
        body,
        if_match=if_match,
        if_none_match=if_none_match,
    )
    written = _lease_from_snapshot(
        snapshot,
        dataset=proposed.dataset,
        bucket=proposed.bucket,
        key=proposed.key,
    )
    if written.state == "active" and not (proposed.created_at <= snapshot.server_date < proposed.expires_at):
        raise AmbiguousWrite("reconciled lease is expired or outside its proposal")
    if written.state == "released" and snapshot.server_date < proposed.expires_at:
        raise AmbiguousWrite("reconciled release precedes its proposed expiry")
    if write_response is not None:
        try:
            write_date = _response_server_date(write_response)
        except AmbiguousWrite as error:
            raise AmbiguousWrite("lease write response Date cannot prove the proposal window") from error
        if written.state == "active" and not proposed.created_at <= write_date < proposed.expires_at:
            raise AmbiguousWrite("lease write Date falls outside its proposal")
        if written.state == "released" and write_date < proposed.expires_at:
            raise AmbiguousWrite("lease release Date precedes its expiry")
    return written


def _new_lease(
    dataset: str,
    publication_id: str,
    owner_nonce: str,
    server_date: datetime,
    *,
    bucket: str,
    key: str,
    lease_seconds: int,
) -> Lease:
    duration = _validate_lease_duration(lease_seconds)
    _validate_lease_identity(publication_id, owner_nonce)
    lease = Lease(
        dataset=dataset,
        publication_id=publication_id,
        owner_nonce=owner_nonce,
        state="active",
        created_at=server_date,
        expires_at=server_date + timedelta(seconds=duration),
        etag="",
        bucket=bucket,
        key=key,
    )
    _validate_lease(lease)
    return lease


def acquire_lease(
    client,
    dataset: str,
    publication_id: str,
    owner_nonce: str,
    *,
    bucket: str = _LEASE_BUCKET,
    lease_seconds: int = _LEASE_SECONDS,
) -> Lease:
    """Acquire a missing, released, or expired dataset lease with one CAS."""
    key = _lease_key(dataset)
    _validate_lease_identity(publication_id, owner_nonce)
    _validate_lease_duration(lease_seconds)
    while True:
        snapshot, server_date, observed_at = _read_lease_observation(client, bucket, key)
        if snapshot is None:
            if_match = None
            if_none_match = True
        else:
            current = _lease_from_snapshot(snapshot, dataset=dataset, bucket=bucket, key=key)
            if current.state == "active" and server_date < current.expires_at:
                raise ConditionalConflict("dataset lease is already active")
            if_match = snapshot.etag
            if_none_match = False

        proposed = _new_lease(
            dataset,
            publication_id,
            owner_nonce,
            server_date,
            bucket=bucket,
            key=key,
            lease_seconds=lease_seconds,
        )
        if _monotonic() - observed_at > _PROPOSAL_WINDOW_SECONDS:
            continue
        try:
            return _write_lease(
                client,
                proposed,
                observed_at=observed_at,
                if_match=if_match,
                if_none_match=if_none_match,
            )
        except _ProposalExpired:
            continue


def _same_lease_owner(left: Lease, right: Lease) -> bool:
    return (
        left.dataset == right.dataset
        and left.publication_id == right.publication_id
        and left.owner_nonce == right.owner_nonce
    )


def renew_lease(client, lease: Lease, *, lease_seconds: int = _LEASE_SECONDS) -> Lease:
    """Renew only the exact active lease version held by this owner."""
    _validate_lease(lease)
    _validate_lease_duration(lease_seconds)
    for _observation in range(_MAX_RENEW_OBSERVATIONS):
        snapshot, server_date, observed_at = _read_lease_observation(client, lease.bucket, lease.key)
        if snapshot is None:
            raise ConditionalConflict("dataset lease no longer exists")
        current = _lease_from_snapshot(
            snapshot,
            dataset=lease.dataset,
            bucket=lease.bucket,
            key=lease.key,
        )
        if (
            snapshot.etag != lease.etag
            or not _same_lease_owner(current, lease)
            or current.state != "active"
            or server_date >= current.expires_at
        ):
            raise ConditionalConflict("dataset lease has been lost")
        proposed = _new_lease(
            lease.dataset,
            lease.publication_id,
            lease.owner_nonce,
            server_date,
            bucket=lease.bucket,
            key=lease.key,
            lease_seconds=lease_seconds,
        )
        proposed_body = _lease_body(proposed)
        if proposed.expires_at <= current.expires_at or proposed_body == snapshot.body:
            continue
        if _monotonic() - observed_at > _PROPOSAL_WINDOW_SECONDS:
            continue
        try:
            written = _write_lease(
                client,
                proposed,
                observed_at=observed_at,
                if_match=lease.etag,
            )
        except _ProposalExpired:
            continue
        if written.expires_at <= current.expires_at or written.etag == lease.etag:
            raise AmbiguousWrite("lease renewal did not establish a changed canonical version")
        return written
    raise AmbiguousWrite("lease renewal could not establish a changed canonical version")


def release_lease(client, lease: Lease) -> Lease:
    """Release by conditional PUT so an old owner cannot delete a successor."""
    _validate_lease(lease)
    while True:
        snapshot, server_date, observed_at = _read_lease_observation(client, lease.bucket, lease.key)
        if snapshot is None:
            raise ConditionalConflict("dataset lease no longer exists")
        current = _lease_from_snapshot(
            snapshot,
            dataset=lease.dataset,
            bucket=lease.bucket,
            key=lease.key,
        )
        if (
            snapshot.etag != lease.etag
            or not _same_lease_owner(current, lease)
            or current.state != "active"
            or server_date >= current.expires_at
        ):
            raise ConditionalConflict("dataset lease has been lost")
        proposed = replace(
            current,
            state="released",
            expires_at=server_date,
            etag="",
        )
        if _monotonic() - observed_at > _PROPOSAL_WINDOW_SECONDS:
            continue
        try:
            return _write_lease(
                client,
                proposed,
                observed_at=observed_at,
                if_match=lease.etag,
            )
        except _ProposalExpired:
            continue
