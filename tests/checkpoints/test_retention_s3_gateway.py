from __future__ import annotations

import io
import re
import traceback
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
from botocore.exceptions import ClientError

from scripts.checkpoints.policy import load_policy
from scripts.checkpoints.records import ObjectRecord
from scripts.checkpoints.s3_gateway import GatewayFailure, S3Gateway, build_s3_client

ROOT = Path(__file__).resolve().parents[2]
POLICY = load_policy(ROOT / "checkpoints" / "retention-policy.yaml")
NOW = datetime(2026, 8, 13, 12, 0, 0, tzinfo=timezone.utc)
RUN_UUID = "550e8400-e29b-41d4-a716-446655440000"
PREFIX = f"streaming_test/{RUN_UUID}/"
CONTROL = "_retention/leases/go-live-streaming-test-v1.json"
MANIFEST = f"_retention/tombstones/550e8400-e29b-41d4-a716-446655440000/manifest/0-{'a' * 64}.json"


class BoundedBody:
    def __init__(self, body: bytes, *, close_error: BaseException | None = None):
        self._stream = io.BytesIO(body)
        self.close_error = close_error
        self.read_sizes: list[int] = []
        self.close_count = 0

    def read(self, size=None):
        assert isinstance(size, int) and size > 0, "body reads must always be bounded"
        self.read_sizes.append(size)
        return self._stream.read(size)

    def close(self):
        self.close_count += 1
        if self.close_error is not None:
            raise self.close_error


class FakeS3:
    def __init__(self):
        self.pages: list[dict] = []
        self.objects: dict[str, tuple[bytes, str, datetime]] = {}
        self.calls: list[tuple[str, dict]] = []
        self.bodies: list[BoundedBody] = []
        self.delete_response: dict = {"Deleted": [], "Errors": []}

    def list_objects_v2(self, **request):
        self.calls.append(("list", request))
        if request.get("Bucket") != "checkpoints" or request.get("Prefix") == "":
            raise ClientError({"Error": {"Code": "AccessDenied"}}, "ListObjectsV2")
        if not self.pages:
            return {"IsTruncated": False, "Contents": []}
        return self.pages.pop(0)

    def get_object(self, **request):
        self.calls.append(("get", request))
        if request["Key"] not in self.objects:
            raise ClientError({"Error": {"Code": "NoSuchKey"}}, "GetObject")
        body, etag, _modified = self.objects[request["Key"]]
        stream = BoundedBody(body)
        self.bodies.append(stream)
        return {"Body": stream, "ETag": f'"{etag}"', "ContentLength": len(body)}

    def put_object(self, **request):
        self.calls.append(("put", request))
        if request["Key"].startswith("streaming_test/") or request["Key"].startswith("unknown/"):
            raise ClientError({"Error": {"Code": "AccessDenied"}}, "PutObject")
        current = self.objects.get(request["Key"])
        if request.get("IfNoneMatch") == "*" and current is not None:
            raise ClientError({"Error": {"Code": "PreconditionFailed"}}, "PutObject")
        if "IfMatch" in request and (current is None or request["IfMatch"].strip('"') != current[1]):
            raise ClientError({"Error": {"Code": "PreconditionFailed"}}, "PutObject")
        body = bytes(request["Body"])
        etag = __import__("hashlib").md5(body, usedforsecurity=False).hexdigest()
        self.objects[request["Key"]] = (body, etag, NOW)
        return {"ETag": f'"{etag}"'}

    def head_object(self, **request):
        self.calls.append(("head", request))
        body, etag, modified = self.objects[request["Key"]]
        return {"ETag": f'"{etag}"', "ContentLength": len(body), "LastModified": modified}

    def delete_objects(self, **request):
        self.calls.append(("delete", request))
        keys = tuple(value["Key"] for value in request["Delete"]["Objects"])
        if keys and all("capability-" in key for key in keys):
            return {"Deleted": [{"Key": key} for key in keys], "Errors": []}
        return self.delete_response

    def delete_object(self, **request):
        self.calls.append(("delete-control", request))
        raise ClientError({"Error": {"Code": "AccessDenied", "Message": "denied"}}, "DeleteObject")


def _page(*keys: str, truncated: bool = False, token: str | None = None) -> dict:
    page = {
        "IsTruncated": truncated,
        "Contents": [
            {"Key": key, "ETag": f'"{index + 1:032x}"', "Size": index + 1, "LastModified": NOW}
            for index, key in enumerate(keys)
        ],
    }
    if token is not None:
        page["NextContinuationToken"] = token
    return page


def test_inventory_uses_exact_prefix_and_canonical_order_across_pages():
    client = FakeS3()
    client.pages = [
        _page(f"{PREFIX}z", truncated=True, token="page-2"),
        _page(f"{PREFIX}a"),
    ]

    records = S3Gateway(client, POLICY, monotonic=lambda: 0.0).inventory(PREFIX)

    assert tuple(record.key for record in records) == (f"{PREFIX}a", f"{PREFIX}z")
    assert client.calls == [
        ("list", {"Bucket": "checkpoints", "Prefix": PREFIX, "MaxKeys": 1000}),
        (
            "list",
            {"Bucket": "checkpoints", "Prefix": PREFIX, "MaxKeys": 1000, "ContinuationToken": "page-2"},
        ),
    ]


def test_inventory_normalizes_real_s3_utc_timezone_and_fractional_seconds():
    client = FakeS3()
    s3_utc = timezone(timedelta(0), "tzlocal")
    client.pages = [
        {
            "IsTruncated": False,
            "Contents": [
                {
                    "Key": f"{PREFIX}state/offset",
                    "ETag": f'"{"a" * 32}"',
                    "Size": 7,
                    "LastModified": datetime(2026, 8, 13, 17, 13, 25, 698000, tzinfo=s3_utc),
                }
            ],
        }
    ]

    records = S3Gateway(client, POLICY, monotonic=lambda: 0.0).inventory(PREFIX)

    assert records[0].last_modified == datetime(2026, 8, 13, 17, 13, 25, tzinfo=timezone.utc)
    assert records[0].last_modified.tzinfo is timezone.utc


@pytest.mark.parametrize(
    "modified",
    [
        datetime(2026, 8, 13, 17, 13, 25),
        datetime(2026, 8, 13, 17, 13, 25, tzinfo=timezone(timedelta(hours=1))),
        "2026-08-13T17:13:25Z",
    ],
)
def test_inventory_rejects_untrusted_non_utc_or_untyped_s3_timestamps(modified):
    client = FakeS3()
    page = _page(f"{PREFIX}state/offset")
    page["Contents"][0]["LastModified"] = modified
    client.pages = [page]

    with pytest.raises(GatewayFailure, match="inventory_record_invalid"):
        S3Gateway(client, POLICY, monotonic=lambda: 0.0).inventory(PREFIX)


@pytest.mark.parametrize(
    ("pages", "code"),
    [
        ([_page(f"{PREFIX}a", truncated=True)], "inventory_token_missing"),
        (
            [
                _page(f"{PREFIX}a", truncated=True, token="same"),
                _page(f"{PREFIX}b", truncated=True, token="same"),
            ],
            "inventory_token_nonprogress",
        ),
        (
            [
                _page(f"{PREFIX}a", truncated=True, token="page-2"),
                _page(f"{PREFIX}a"),
            ],
            "duplicate_object",
        ),
        ([_page("streaming_test/foreign/object")], "inventory_prefix_escape"),
    ],
)
def test_inventory_fails_closed_on_pagination_duplicates_and_prefix_escape(pages, code):
    client = FakeS3()
    client.pages = pages

    with pytest.raises(GatewayFailure, match=code):
        S3Gateway(client, POLICY, monotonic=lambda: 0.0).inventory(PREFIX)


def test_inventory_enforces_deadline_object_byte_and_page_bounds():
    client = FakeS3()
    client.pages = [_page(f"{PREFIX}a")]
    clock = iter((0.0, 901.0))
    with pytest.raises(GatewayFailure, match="gateway_deadline"):
        S3Gateway(client, POLICY, monotonic=lambda: next(clock)).inventory(PREFIX)

    too_many = FakeS3()
    too_many.pages = [_page(f"{PREFIX}a", f"{PREFIX}b")]
    bounded_policy = replace(POLICY, bounds=replace(POLICY.bounds, max_objects=1))
    with pytest.raises(GatewayFailure, match="inventory_object_bound"):
        S3Gateway(too_many, bounded_policy, monotonic=lambda: 0.0).inventory(PREFIX)


def test_control_reads_are_bounded_closed_and_exact_key_only():
    client = FakeS3()
    client.objects[CONTROL] = (b'{"state":"active"}', "a" * 32, NOW)
    gateway = S3Gateway(client, POLICY, monotonic=lambda: 0.0)

    body, etag = gateway.read_control(CONTROL, max_bytes=64)

    assert body == b'{"state":"active"}'
    assert etag == "a" * 32
    assert client.bodies[0].read_sizes == [65]
    assert client.bodies[0].close_count == 1
    for key in ("_retention/", "_retention/../events/x", f"{PREFIX}data", "other/control.json"):
        with pytest.raises(GatewayFailure, match="control_key_invalid"):
            gateway.read_control(key, max_bytes=64)


def test_audit_control_key_requires_exact_operation_and_attempt_uuids():
    client = FakeS3()
    attempt_id = "11111111-1111-5111-8111-111111111111"
    key = f"_retention/audits/{RUN_UUID}/{attempt_id}.json"
    client.objects[key] = (b"{}", "a" * 32, NOW)
    gateway = S3Gateway(client, POLICY, monotonic=lambda: 0.0)

    assert gateway.read_control(key, max_bytes=64)[0] == b"{}"
    with pytest.raises(GatewayFailure, match="control_key_invalid"):
        gateway.read_control(f"_retention/audits/{RUN_UUID}/{'a' * 64}.json", max_bytes=64)


def test_manifest_read_uses_manifest_shard_bound_while_summary_controls_stay_small():
    client = FakeS3()
    client.objects[MANIFEST] = (b"[]", "a" * 32, NOW)
    client.objects[CONTROL] = (b"{}", "b" * 32, NOW)
    gateway = S3Gateway(client, POLICY, monotonic=lambda: 0.0)

    assert gateway.read_control(MANIFEST, max_bytes=POLICY.bounds.max_manifest_shard_bytes)[0] == b"[]"
    with pytest.raises(GatewayFailure, match="control_bound_invalid"):
        gateway.read_control(MANIFEST, max_bytes=POLICY.bounds.max_manifest_shard_bytes + 1)
    with pytest.raises(GatewayFailure, match="control_bound_invalid"):
        gateway.read_control(CONTROL, max_bytes=POLICY.bounds.max_summary_bytes + 1)


@pytest.mark.parametrize(
    "response",
    [
        {"Deleted": [{"Key": f"{PREFIX}a"}], "Errors": [{"Key": f"{PREFIX}a"}]},
        {"Deleted": [], "Errors": [{"Key": f"{PREFIX}a"}, {"Key": f"{PREFIX}a"}]},
    ],
)
def test_delete_rejects_overlapping_or_duplicate_response_classification(response):
    client = FakeS3()
    client.delete_response = response
    record = ObjectRecord(f"{PREFIX}a", "a" * 32, 1, NOW)

    with pytest.raises(GatewayFailure, match="delete_response_invalid"):
        S3Gateway(client, POLICY, monotonic=lambda: 0.0).delete_records((record,))


def test_result_classification_shard_write_uses_same_one_megabyte_bound_as_read():
    client = FakeS3()
    gateway = S3Gateway(client, POLICY, monotonic=lambda: 0.0)
    key = f"_retention/tombstones/550e8400-e29b-41d4-a716-446655440000/results/shards/{'b' * 64}.json"
    body = b"x" * 70_000

    gateway.create_control(key, body)

    assert client.objects[key][0] == body


@pytest.mark.parametrize("code", ["NoSuchKey", "NoSuchObject", "404"])
def test_missing_control_is_distinguished_from_ambiguous_transport_failure(code):
    class MissingS3(FakeS3):
        def get_object(self, **request):
            self.calls.append(("get", request))
            raise ClientError({"Error": {"Code": code, "Message": "secret must not escape"}}, "GetObject")

    with pytest.raises(GatewayFailure, match="control_missing") as failure:
        S3Gateway(MissingS3(), POLICY, monotonic=lambda: 0.0).read_control(CONTROL, max_bytes=64)

    assert failure.value.__cause__ is None


def test_create_and_replace_use_conditions_and_verify_exact_readback():
    client = FakeS3()
    gateway = S3Gateway(client, POLICY, monotonic=lambda: 0.0)

    first_etag = gateway.create_control(CONTROL, b'{"epoch":1}')
    second_etag = gateway.replace_lease(CONTROL, first_etag, b'{"epoch":2}')

    puts = [request for operation, request in client.calls if operation == "put"]
    assert puts[0]["IfNoneMatch"] == "*"
    assert "IfMatch" not in puts[0]
    assert puts[1]["IfMatch"] == f'"{first_etag}"'
    assert "IfNoneMatch" not in puts[1]
    assert second_etag != first_etag
    assert client.objects[CONTROL][0] == b'{"epoch":2}'


def test_capability_probe_performs_observed_create_replace_readback_and_denied_delete():
    client = FakeS3()
    gateway = S3Gateway(client, POLICY, monotonic=lambda: 0.0)

    result = gateway.probe_capabilities()

    assert result == {
        "automatic_apply": False,
        "conditional_create": True,
        "conditional_create_conflict": True,
        "conditional_delete": False,
        "conditional_replace_verified_readback": True,
        "data_put_denied": True,
        "exact_leaf_delete": True,
        "exact_leaf_get": True,
        "exact_leaf_list": True,
        "multi_delete": True,
        "observed": True,
        "other_bucket_denied": True,
        "profile": "minio-2025-09-manual-verified-readback",
        "root_list_denied": True,
        "stale_replace_denied": True,
        "unknown_control_denied": True,
    }
    assert client.calls[-1][0] == "delete-control"
    capability_keys = [request["Key"] for operation, request in client.calls if operation == "put"]
    assert capability_keys
    assert re.fullmatch(r"_retention/capability/[0-9a-f-]{36}\.json", capability_keys[0])
    assert "runtime-probe" not in capability_keys[0]
    stale_replace_keys = {
        request["Key"]
        for operation, request in client.calls
        if operation == "put" and request.get("IfMatch") == f'"{"0" * 32}"'
    }
    assert len(stale_replace_keys) == 2
    assert capability_keys[0] in stale_replace_keys


def test_capability_probe_observes_cas_failures_exact_leaf_access_and_scope_denials():
    class ProbeS3(FakeS3):
        def list_objects_v2(self, **request):
            self.calls.append(("list", request))
            if request["Bucket"] != "checkpoints" or request["Prefix"] == "":
                raise ClientError({"Error": {"Code": "AccessDenied"}}, "ListObjectsV2")
            return {"IsTruncated": False, "Contents": []}

        def get_object(self, **request):
            if request["Key"].startswith("streaming_test/"):
                self.calls.append(("get-data", request))
                raise ClientError({"Error": {"Code": "NoSuchKey"}}, "GetObject")
            return super().get_object(**request)

        def put_object(self, **request):
            if request["Key"].startswith("streaming_test/") or request["Key"].endswith("unknown.json"):
                self.calls.append(("put-denied", request))
                raise ClientError({"Error": {"Code": "AccessDenied"}}, "PutObject")
            current = self.objects.get(request["Key"])
            if request.get("IfNoneMatch") == "*" and current is not None:
                self.calls.append(("put-conflict", request))
                raise ClientError({"Error": {"Code": "PreconditionFailed"}}, "PutObject")
            if "IfMatch" in request and (current is None or request["IfMatch"].strip('"') != current[1]):
                self.calls.append(("put-stale", request))
                raise ClientError({"Error": {"Code": "PreconditionFailed"}}, "PutObject")
            return super().put_object(**request)

        def delete_objects(self, **request):
            self.calls.append(("delete", request))
            return {"Deleted": list(request["Delete"]["Objects"]), "Errors": []}

    result = S3Gateway(ProbeS3(), POLICY, monotonic=lambda: 0.0).probe_capabilities()

    assert result["conditional_create_conflict"] is True
    assert result["stale_replace_denied"] is True
    assert result["exact_leaf_list"] is True
    assert result["exact_leaf_get"] is True
    assert result["exact_leaf_delete"] is True
    assert result["multi_delete"] is True
    assert result["root_list_denied"] is True
    assert result["other_bucket_denied"] is True
    assert result["data_put_denied"] is True
    assert result["unknown_control_denied"] is True


def test_capability_probe_does_not_treat_missing_foreign_bucket_as_authorization_denial():
    failure = ClientError({"Error": {"Code": "NoSuchBucket"}}, "ListObjectsV2")
    with pytest.raises(GatewayFailure, match="capability_failed"):
        S3Gateway._expect_client_error(lambda: (_ for _ in ()).throw(failure), {"AccessDenied", "403"})


def test_head_and_delete_require_every_exact_original_record_result():
    client = FakeS3()
    record = ObjectRecord(f"{PREFIX}state", "a" * 32, 4, NOW)
    client.objects[record.key] = (b"data", record.etag, NOW)
    gateway = S3Gateway(client, POLICY, monotonic=lambda: 0.0)

    gateway.head_record(record)
    client.delete_response = {"Deleted": [{"Key": record.key}]}
    assert gateway.delete_records((record,)) == (record.key,)
    request = [value for operation, value in client.calls if operation == "delete"][0]
    assert request == {
        "Bucket": "checkpoints",
        "Delete": {"Objects": [{"Key": record.key}], "Quiet": False},
    }

    client.delete_response = {"Deleted": [], "Errors": [{"Key": record.key, "Code": "Denied"}]}
    with pytest.raises(GatewayFailure, match="delete_partial"):
        gateway.delete_records((record,))

    other = ObjectRecord(f"{PREFIX}other", "b" * 32, 5, NOW)
    client.delete_response = {
        "Deleted": [{"Key": record.key}],
        "Errors": [{"Key": other.key, "Code": "InternalError"}],
    }
    with pytest.raises(GatewayFailure, match="delete_partial") as partial:
        gateway.delete_records((record, other))
    assert partial.value.deleted_keys == (record.key,)

    client.delete_response = {"Deleted": [{"Key": f"{PREFIX}foreign"}], "Errors": []}
    with pytest.raises(GatewayFailure, match="delete_response_invalid"):
        gateway.delete_records((record,))


def test_operation_deadline_is_not_sanitized_as_head_failure():
    from scripts.checkpoints.operations import OperationFailure

    client = FakeS3()
    record = ObjectRecord(f"{PREFIX}state", "a" * 32, 4, NOW)
    client.objects[record.key] = (b"data", record.etag, NOW)
    gateway = S3Gateway(client, POLICY, monotonic=lambda: 0.0)
    checks = 0

    def expire_after_head():
        nonlocal checks
        checks += 1
        if checks == 2:
            raise OperationFailure("operation_deadline")

    with gateway.operation_deadline(expire_after_head):
        with pytest.raises(OperationFailure, match="operation_deadline"):
            gateway.head_record(record)


def test_successful_delete_response_is_not_erased_by_post_call_deadline():
    from scripts.checkpoints.operations import OperationFailure

    client = FakeS3()
    record = ObjectRecord(f"{PREFIX}state", "a" * 32, 4, NOW)
    client.delete_response = {"Deleted": [{"Key": record.key}], "Errors": []}
    gateway = S3Gateway(client, POLICY, monotonic=lambda: 0.0)
    checks = 0

    def expire_after_delete():
        nonlocal checks
        checks += 1
        if checks == 2:
            raise OperationFailure("operation_deadline")

    gateway._operation.check = expire_after_delete
    try:
        assert gateway.delete_records((record,)) == (record.key,)
    finally:
        del gateway._operation.check
    assert any(operation == "delete" for operation, _request in client.calls)

    client.delete_response = {"Deleted": [{"Key": record.key}, {"Key": record.key}], "Errors": []}
    with pytest.raises(GatewayFailure, match="delete_response_invalid"):
        gateway.delete_records((record,))

    client.delete_response = {"Errors": []}
    with pytest.raises(GatewayFailure, match="delete_response_invalid"):
        gateway.delete_records((record,))


def test_post_get_deadline_still_closes_response_body_exactly_once():
    from scripts.checkpoints.operations import OperationFailure

    client = FakeS3()
    client.objects[CONTROL] = (b"{}", "a" * 32, NOW)
    gateway = S3Gateway(client, POLICY, monotonic=lambda: 0.0)
    checks = 0

    def expire_after_get():
        nonlocal checks
        checks += 1
        if checks == 3:
            raise OperationFailure("operation_deadline")

    with pytest.raises(OperationFailure, match="operation_deadline"):
        with gateway.operation_deadline(expire_after_get):
            gateway.read_control(CONTROL, max_bytes=65_536)
    assert client.bodies[-1].close_count == 1


def test_dependency_and_cleanup_failures_are_bounded_and_never_chain_raw_details():
    class BrokenS3(FakeS3):
        def list_objects_v2(self, **_request):
            raise RuntimeError("credential=super-secret endpoint=http://private.invalid")

    with pytest.raises(GatewayFailure, match="inventory_failed") as failure:
        S3Gateway(BrokenS3(), POLICY, monotonic=lambda: 0.0).inventory(PREFIX)

    rendered = "".join(traceback.format_exception(failure.value))
    assert "super-secret" not in rendered
    assert "private.invalid" not in rendered
    assert failure.value.__cause__ is None


def test_client_factory_pins_origin_config_and_disables_environment_discovery(monkeypatch):
    captured = {}

    def fake_client(service, **kwargs):
        captured.update(service=service, **kwargs)
        return object()

    monkeypatch.setenv("HTTP_PROXY", "http://proxy.invalid")
    monkeypatch.setenv("AWS_EC2_METADATA_DISABLED", "false")
    monkeypatch.setattr("scripts.checkpoints.s3_gateway.boto3.client", fake_client)

    build_s3_client("retention-access", "retention-secret")

    assert captured["service"] == "s3"
    assert captured["endpoint_url"] == "http://minio:9000"
    assert captured["region_name"] == "us-east-1"
    assert captured["config"].signature_version == "s3v4"
    assert captured["config"].s3 == {"addressing_style": "path"}
    assert captured["config"].connect_timeout == 5
    assert captured["config"].read_timeout == 10
    assert captured["config"].proxies == {}
