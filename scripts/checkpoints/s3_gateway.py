"""Bounded S3 operations for exact checkpoint-retention prefixes."""

from __future__ import annotations

import os
import re
import threading
import time
import uuid
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from typing import Callable, Iterable, Mapping

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

from scripts.checkpoints.policy import CheckpointPolicy, PolicyError
from scripts.checkpoints.records import ObjectRecord, RecordFailure, canonical_records


class GatewayFailure(ValueError):
    """A closed, sanitized S3 gateway failure category."""

    def __init__(self, code: str, *, deleted_keys: tuple[str, ...] = ()) -> None:
        super().__init__(code)
        self.code = code
        self.deleted_keys = deleted_keys


_CONTROL_KEY = re.compile(
    r"_retention/(?:"
    r"(?:leases|terminals)/[a-z0-9][a-z0-9_-]{0,127}\.json|"
    r"tombstones/[0-9a-f-]{36}/(?:manifest/[0-9]+-[0-9a-f]{64}\.json|prepared\.json|"
    r"results/(?:attempts/[0-9]{6}-[0-9a-f]{64}\.json|shards/[0-9a-f]{64}\.json))|"
    r"audits/[0-9a-f-]{36}/[0-9a-f]{64}\.json|"
    r"capability/[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}\.json"
    r")"
)
_ETAG = re.compile(r"[0-9a-f]{32}(?:-[1-9][0-9]{0,9})?")


def _normalize_s3_timestamp(value: object) -> datetime:
    if not isinstance(value, datetime):
        raise RecordFailure("object_timestamp_invalid")
    try:
        offset = value.utcoffset()
    except BaseException:
        raise RecordFailure("object_timestamp_invalid") from None
    if offset != timedelta(0):
        raise RecordFailure("object_timestamp_invalid")
    return value.astimezone(timezone.utc).replace(microsecond=0)


def build_s3_client(access_key: str, secret_key: str):
    """Build the one fixed internal MinIO client without environment routing."""

    if not isinstance(access_key, str) or not access_key or not isinstance(secret_key, str) or not secret_key:
        raise GatewayFailure("credential_invalid")
    os.environ["AWS_EC2_METADATA_DISABLED"] = "true"
    config = Config(
        signature_version="s3v4",
        s3={"addressing_style": "path"},
        connect_timeout=5,
        read_timeout=10,
        retries={"max_attempts": 2, "mode": "standard"},
        proxies={},
    )
    try:
        return boto3.client(
            "s3",
            endpoint_url="http://minio:9000",
            region_name="us-east-1",
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            config=config,
        )
    except (KeyboardInterrupt, SystemExit):
        raise
    except BaseException:
        raise GatewayFailure("client_creation_failed") from None


class S3Gateway:
    """Expose only exact-prefix data and validated retention-control operations."""

    def __init__(
        self,
        client: object,
        policy: CheckpointPolicy,
        *,
        monotonic: Callable[[], float] = time.monotonic,
    ) -> None:
        self._client = client
        self._policy = policy
        self._monotonic = monotonic
        self._operation = threading.local()

    @contextmanager
    def operation_deadline(self, check: Callable[[], None]):
        prior = getattr(self._operation, "check", None)
        self._operation.check = check
        try:
            check()
            yield
            check()
        finally:
            self._operation.check = prior

    def _check_operation_deadline(self) -> None:
        check = getattr(self._operation, "check", None)
        if callable(check):
            check()

    def inventory(self, prefix: str) -> tuple[ObjectRecord, ...]:
        try:
            self._policy.match_prefix(prefix)
        except PolicyError:
            raise GatewayFailure("inventory_prefix_invalid") from None
        started = self._monotonic()
        records: list[ObjectRecord] = []
        seen_tokens: set[str] = set()
        token: str | None = None
        total_bytes = 0
        pages = 0
        while True:
            self._check_deadline(started)
            request: dict[str, object] = {
                "Bucket": self._policy.bucket,
                "Prefix": prefix,
                "MaxKeys": 1000,
            }
            if token is not None:
                request["ContinuationToken"] = token
            try:
                self._check_operation_deadline()
                page = self._client.list_objects_v2(**request)
                self._check_operation_deadline()
            except (KeyboardInterrupt, SystemExit):
                raise
            except BaseException:
                raise GatewayFailure("inventory_failed") from None
            self._check_deadline(started)
            pages += 1
            if pages > self._policy.bounds.max_pages or not isinstance(page, Mapping):
                code = "inventory_page_bound" if pages > self._policy.bounds.max_pages else "inventory_page_invalid"
                raise GatewayFailure(code)
            contents = page.get("Contents", [])
            if not isinstance(contents, list):
                raise GatewayFailure("inventory_page_invalid")
            for item in contents:
                if not isinstance(item, Mapping):
                    raise GatewayFailure("inventory_record_invalid")
                key = item.get("Key")
                if not isinstance(key, str) or not key.startswith(prefix) or key == prefix:
                    raise GatewayFailure("inventory_prefix_escape")
                try:
                    record = ObjectRecord(
                        key,
                        _parse_etag(item.get("ETag")),
                        item.get("Size"),
                        _normalize_s3_timestamp(item.get("LastModified")),
                    )
                except RecordFailure:
                    raise GatewayFailure("inventory_record_invalid") from None
                records.append(record)
                total_bytes += record.size_bytes
                if len(records) > self._policy.bounds.max_objects:
                    raise GatewayFailure("inventory_object_bound")
                if total_bytes > self._policy.bounds.max_bytes:
                    raise GatewayFailure("inventory_byte_bound")
            truncated = page.get("IsTruncated")
            if type(truncated) is not bool:
                raise GatewayFailure("inventory_page_invalid")
            if not truncated:
                break
            next_token = page.get("NextContinuationToken")
            if not isinstance(next_token, str) or not next_token:
                raise GatewayFailure("inventory_token_missing")
            if next_token == token or next_token in seen_tokens:
                raise GatewayFailure("inventory_token_nonprogress")
            seen_tokens.add(next_token)
            token = next_token
        try:
            return canonical_records(records)
        except RecordFailure as error:
            raise GatewayFailure(error.code) from None

    def read_control(self, key: str, *, max_bytes: int) -> tuple[bytes, str]:
        _validate_control_key(key)
        bound = (
            self._policy.bounds.max_manifest_shard_bytes
            if "/manifest/" in key or "/results/shards/" in key
            else self._policy.bounds.max_summary_bytes
        )
        if type(max_bytes) is not int or max_bytes < 1 or max_bytes > bound:
            raise GatewayFailure("control_bound_invalid")
        try:
            self._check_operation_deadline()
            response = self._client.get_object(Bucket=self._policy.bucket, Key=key)
            self._check_operation_deadline()
        except (KeyboardInterrupt, SystemExit):
            raise
        except ClientError as error:
            code = error.response.get("Error", {}).get("Code") if isinstance(error.response, Mapping) else None
            if code in {"NoSuchKey", "NoSuchObject", "404"}:
                raise GatewayFailure("control_missing") from None
            raise GatewayFailure("control_read_failed") from None
        except BaseException:
            raise GatewayFailure("control_read_failed") from None
        if not isinstance(response, Mapping):
            raise GatewayFailure("control_response_invalid")
        stream = response.get("Body")
        primary: BaseException | None = None
        try:
            if stream is None or not hasattr(stream, "read"):
                raise GatewayFailure("control_response_invalid")
            length = response.get("ContentLength")
            if type(length) is not int or length < 0 or length > max_bytes:
                raise GatewayFailure("control_body_bound")
            body = stream.read(max_bytes + 1)
            self._check_operation_deadline()
            if type(body) is not bytes or len(body) != length or len(body) > max_bytes:
                raise GatewayFailure("control_body_invalid")
            etag = _parse_etag(response.get("ETag"))
            return body, etag
        except (KeyboardInterrupt, SystemExit, GatewayFailure) as error:
            primary = error
            raise
        except BaseException:
            primary = GatewayFailure("control_read_failed")
            raise primary from None
        finally:
            try:
                if stream is not None and hasattr(stream, "close"):
                    stream.close()
            except (KeyboardInterrupt, SystemExit):
                if primary is None:
                    raise
            except BaseException:
                if primary is None:
                    raise GatewayFailure("control_close_failed") from None

    def create_control(self, key: str, body: bytes) -> str:
        return self._write_control(key, body, if_none_match=True, etag=None)

    def replace_lease(self, key: str, etag: str, body: bytes) -> str:
        if not isinstance(etag, str) or _ETAG.fullmatch(etag) is None:
            raise GatewayFailure("control_etag_invalid")
        return self._write_control(key, body, if_none_match=False, etag=etag)

    def list_controls(self, prefix: str, *, max_keys: int) -> tuple[str, ...]:
        if (
            not isinstance(prefix, str)
            or not prefix.isascii()
            or not prefix.startswith("_retention/")
            or type(max_keys) is not int
            or max_keys < 1
            or max_keys > 1_024
        ):
            raise GatewayFailure("control_prefix_invalid")
        try:
            self._check_operation_deadline()
            response = self._client.list_objects_v2(
                Bucket=self._policy.bucket,
                Prefix=prefix,
                MaxKeys=max_keys + 1,
            )
            self._check_operation_deadline()
        except (KeyboardInterrupt, SystemExit):
            raise
        except BaseException:
            raise GatewayFailure("control_list_failed") from None
        contents = response.get("Contents", []) if isinstance(response, Mapping) else None
        if not isinstance(contents, list) or response.get("IsTruncated") is not False or len(contents) > max_keys:
            raise GatewayFailure("control_list_bound")
        keys = []
        for item in contents:
            key = item.get("Key") if isinstance(item, Mapping) else None
            if not isinstance(key, str) or not key.startswith(prefix):
                raise GatewayFailure("control_list_invalid")
            _validate_control_key(key)
            keys.append(key)
        if len(keys) != len(set(keys)):
            raise GatewayFailure("control_list_invalid")
        return tuple(sorted(keys))

    def head_record(self, record: ObjectRecord) -> None:
        if not isinstance(record, ObjectRecord):
            raise GatewayFailure("object_record_invalid")
        self._validate_data_key(record.key)
        try:
            self._check_operation_deadline()
            response = self._client.head_object(Bucket=self._policy.bucket, Key=record.key)
            self._check_operation_deadline()
            actual_etag = _parse_etag(response.get("ETag"))
        except (KeyboardInterrupt, SystemExit):
            raise
        except GatewayFailure:
            raise
        except BaseException:
            raise GatewayFailure("head_failed") from None
        if (
            not isinstance(response, Mapping)
            or actual_etag != record.etag
            or response.get("ContentLength") != record.size_bytes
            or response.get("LastModified") != record.last_modified
        ):
            raise GatewayFailure("head_mismatch")

    def delete_records(self, records: Iterable[ObjectRecord]) -> tuple[str, ...]:
        try:
            ordered = canonical_records(records)
        except RecordFailure as error:
            raise GatewayFailure(error.code) from None
        if not ordered or len(ordered) > self._policy.bounds.max_delete_keys:
            raise GatewayFailure("delete_batch_bound")
        prefixes = {self._validate_data_key(record.key) for record in ordered}
        if len(prefixes) != 1:
            raise GatewayFailure("delete_prefix_mismatch")
        request = {
            "Bucket": self._policy.bucket,
            "Delete": {"Objects": [{"Key": record.key} for record in ordered], "Quiet": False},
        }
        try:
            self._check_operation_deadline()
            response = self._client.delete_objects(**request)
            self._check_operation_deadline()
        except (KeyboardInterrupt, SystemExit):
            raise
        except BaseException:
            raise GatewayFailure("delete_failed") from None
        if not isinstance(response, Mapping):
            raise GatewayFailure("delete_response_invalid")
        deleted = response.get("Deleted")
        errors = response.get("Errors", [])
        if not isinstance(deleted, list) or not isinstance(errors, list):
            raise GatewayFailure("delete_response_invalid")
        expected = tuple(record.key for record in ordered)
        deleted_keys = tuple(item.get("Key") for item in deleted if isinstance(item, Mapping))
        error_keys = tuple(item.get("Key") for item in errors if isinstance(item, Mapping))
        if (
            len(deleted_keys) != len(deleted)
            or len(error_keys) != len(errors)
            or len(deleted_keys) != len(set(deleted_keys))
            or len(error_keys) != len(set(error_keys))
            or set(deleted_keys) & set(error_keys)
            or any(not isinstance(key, str) for key in (*deleted_keys, *error_keys))
            or any(key not in expected for key in (*deleted_keys, *error_keys))
        ):
            raise GatewayFailure("delete_response_invalid")
        if errors or len(deleted_keys) != len(expected) or set(deleted_keys) != set(expected):
            raise GatewayFailure("delete_partial", deleted_keys=deleted_keys)
        return expected

    def probe_capabilities(self) -> Mapping[str, object]:
        probe_uuid = str(uuid.uuid4())
        missing_probe_uuid = str(uuid.uuid4())
        key = f"_retention/capability/{probe_uuid}.json"
        body = b'{"profile":"minio-2025-09-manual-verified-readback","schema_version":1}'
        data_prefix = f"streaming_test/{probe_uuid}/"
        data_keys = (f"{data_prefix}capability-a", f"{data_prefix}capability-b")
        try:
            try:
                etag = self.create_control(key, body)
            except GatewayFailure:
                existing, etag = self.read_control(key, max_bytes=len(body))
                if existing != body:
                    raise GatewayFailure("capability_failed")
            self._expect_client_error(
                lambda: self._client.put_object(
                    Bucket=self._policy.bucket,
                    Key=key,
                    Body=body,
                    IfNoneMatch="*",
                ),
                {"PreconditionFailed", "412"},
            )
            replaced = self.replace_lease(key, etag, body)
            readback, read_etag = self.read_control(key, max_bytes=len(body))
            if readback != body or read_etag != replaced:
                raise GatewayFailure("capability_failed")
            self._expect_client_error(
                lambda: self._client.put_object(
                    Bucket=self._policy.bucket,
                    Key=key,
                    Body=body,
                    IfMatch=f'"{"0" * 32}"',
                ),
                {"PreconditionFailed", "412"},
            )
            self._expect_client_error(
                lambda: self._client.put_object(
                    Bucket=self._policy.bucket,
                    Key=f"_retention/capability/{missing_probe_uuid}.json",
                    Body=body,
                    IfMatch=f'"{"0" * 32}"',
                ),
                {"NoSuchKey", "NoSuchObject", "PreconditionFailed", "404", "412"},
            )
            listed = self._client.list_objects_v2(
                Bucket=self._policy.bucket,
                Prefix=data_prefix,
                MaxKeys=1,
            )
            if not isinstance(listed, Mapping) or listed.get("IsTruncated") is not False:
                raise GatewayFailure("capability_failed")
            self._expect_client_error(
                lambda: self._client.get_object(Bucket=self._policy.bucket, Key=data_keys[0]),
                {"NoSuchKey", "NoSuchObject", "404"},
            )
            deleted = self._client.delete_objects(
                Bucket=self._policy.bucket,
                Delete={"Objects": [{"Key": data_keys[0]}], "Quiet": False},
            )
            multi_deleted = self._client.delete_objects(
                Bucket=self._policy.bucket,
                Delete={"Objects": [{"Key": value} for value in data_keys], "Quiet": False},
            )
            if not self._exact_deleted(deleted, data_keys[:1]) or not self._exact_deleted(multi_deleted, data_keys):
                raise GatewayFailure("capability_failed")
            self._expect_client_error(
                lambda: self._client.list_objects_v2(Bucket=self._policy.bucket, Prefix="", MaxKeys=1),
                {"AccessDenied", "403"},
            )
            self._expect_client_error(
                lambda: self._client.list_objects_v2(Bucket="checkpoint-retention-denied", Prefix="", MaxKeys=1),
                {"AccessDenied", "AllAccessDisabled", "403"},
            )
            self._expect_client_error(
                lambda: self._client.put_object(Bucket=self._policy.bucket, Key=data_keys[0], Body=b"denied"),
                {"AccessDenied", "403"},
            )
            self._expect_client_error(
                lambda: self._client.put_object(
                    Bucket=self._policy.bucket,
                    Key=f"unknown/{probe_uuid}.json",
                    Body=b"denied",
                ),
                {"AccessDenied", "403"},
            )
            try:
                self._client.delete_object(Bucket=self._policy.bucket, Key=key)
            except ClientError as error:
                code = error.response.get("Error", {}).get("Code") if isinstance(error.response, Mapping) else None
                if code not in {"AccessDenied", "MethodNotAllowed"}:
                    raise GatewayFailure("capability_failed") from None
            else:
                raise GatewayFailure("capability_failed")
        except (KeyboardInterrupt, SystemExit):
            raise
        except GatewayFailure:
            raise
        except BaseException:
            raise GatewayFailure("capability_failed") from None
        return {
            "profile": "minio-2025-09-manual-verified-readback",
            "conditional_create": True,
            "conditional_create_conflict": True,
            "conditional_replace_verified_readback": True,
            "stale_replace_denied": True,
            "conditional_delete": False,
            "exact_leaf_list": True,
            "exact_leaf_get": True,
            "exact_leaf_delete": True,
            "multi_delete": True,
            "root_list_denied": True,
            "other_bucket_denied": True,
            "data_put_denied": True,
            "unknown_control_denied": True,
            "automatic_apply": False,
            "observed": True,
        }

    @staticmethod
    def _expect_client_error(call: Callable[[], object], codes: set[str]) -> None:
        try:
            response = call()
        except ClientError as error:
            code = error.response.get("Error", {}).get("Code") if isinstance(error.response, Mapping) else None
            if code in codes:
                return
            raise GatewayFailure("capability_failed") from None
        except (KeyboardInterrupt, SystemExit):
            raise
        except BaseException:
            raise GatewayFailure("capability_failed") from None
        stream = response.get("Body") if isinstance(response, Mapping) else None
        if stream is not None and hasattr(stream, "close"):
            try:
                stream.close()
            except BaseException:
                pass
        raise GatewayFailure("capability_failed")

    @staticmethod
    def _exact_deleted(response: object, expected: tuple[str, ...]) -> bool:
        if not isinstance(response, Mapping) or response.get("Errors", []) != []:
            return False
        deleted = response.get("Deleted")
        return (
            isinstance(deleted, list)
            and tuple(value.get("Key") if isinstance(value, Mapping) else None for value in deleted) == expected
        )

    def _write_control(self, key: str, body: bytes, *, if_none_match: bool, etag: str | None) -> str:
        _validate_control_key(key)
        bound = (
            self._policy.bounds.max_manifest_shard_bytes
            if "/manifest/" in key or "/results/shards/" in key
            else self._policy.bounds.max_summary_bytes
        )
        if type(body) is not bytes or not body or len(body) > bound:
            raise GatewayFailure("control_body_bound")
        request: dict[str, object] = {"Bucket": self._policy.bucket, "Key": key, "Body": body}
        if if_none_match:
            request["IfNoneMatch"] = "*"
        else:
            request["IfMatch"] = f'"{etag}"'
        try:
            self._check_operation_deadline()
            response = self._client.put_object(**request)
            self._check_operation_deadline()
            written_etag = _parse_etag(response.get("ETag"))
        except (KeyboardInterrupt, SystemExit):
            raise
        except BaseException:
            raise GatewayFailure("control_write_failed") from None
        read_body, read_etag = self.read_control(key, max_bytes=len(body))
        if read_body != body or read_etag != written_etag:
            raise GatewayFailure("control_readback_mismatch")
        return written_etag

    def _validate_data_key(self, key: str) -> str:
        for entry in self._policy.entries.values():
            if "{run_uuid}" in entry.prefix:
                match = re.match(r"(streaming_test/[0-9a-f-]{36}/)", key)
                prefix = match.group(1) if match else ""
            elif "{scale}" in entry.prefix:
                match = re.match(r"(gh_events_file/(?:tiny|small|medium)/[0-9a-f]{32}/[0-9a-f]{64}/)", key)
                prefix = match.group(1) if match else ""
            else:
                prefix = entry.prefix if key.startswith(entry.prefix) else ""
            if prefix and key != prefix:
                try:
                    self._policy.match_prefix(prefix)
                except PolicyError:
                    break
                return prefix
        raise GatewayFailure("data_key_invalid")

    def _check_deadline(self, started: float) -> None:
        try:
            elapsed = self._monotonic() - started
        except (KeyboardInterrupt, SystemExit):
            raise
        except BaseException:
            raise GatewayFailure("clock_failed") from None
        if elapsed < 0 or elapsed > self._policy.bounds.max_active_seconds:
            raise GatewayFailure("gateway_deadline")


def _validate_control_key(key: object) -> None:
    if not isinstance(key, str) or not key.isascii() or _CONTROL_KEY.fullmatch(key) is None:
        raise GatewayFailure("control_key_invalid")


def _parse_etag(value: object) -> str:
    if not isinstance(value, str):
        raise RecordFailure("object_etag_invalid")
    normalized = value[1:-1] if len(value) >= 2 and value.startswith('"') and value.endswith('"') else value
    if _ETAG.fullmatch(normalized) is None:
        raise RecordFailure("object_etag_invalid")
    return normalized
