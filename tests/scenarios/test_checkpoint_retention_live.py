from __future__ import annotations

import json
import os
import re
import subprocess
import tempfile
import time
import urllib.error
import urllib.request
import uuid
from contextlib import ExitStack, contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path

import boto3
import pytest
from botocore.config import Config
from botocore.exceptions import ClientError

from scripts.checkpoints.policy import _policy_sha256, load_policy

ROOT = Path(__file__).resolve().parents[2]
INFRA_ENV = ROOT / "infra/.env"
RUN_UUID = re.compile(r"[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}")
SHA256 = re.compile(r"[0-9a-f]{64}")
MAX_METRICS_BYTES = 65_536
MAX_RESPONSE_BYTES = 65_536
LIVE_RETENTION_ACCESS_KEY = "retention86live"
LIVE_LEASE_TOKEN = "issue86-live-lease-token"
LIVE_OPERATOR_TOKEN = "issue86-live-operator-token"
PRODUCTION_PREFIXES = ("events/", "event_windows/", "online_retail_cdc/", "gh_events_file/")
OWNED_FIXTURE_KEY = re.compile(
    rf"(?:streaming_test/{RUN_UUID.pattern}/(?:state/(?:offset|changed)|commits/0)"
    rf"|unknown-retention/{RUN_UUID.pattern}/sentinel)"
)
OWNED_CAPABILITY_KEY = re.compile(rf"_retention/capability/{RUN_UUID.pattern}\.json")
CAPABILITY_BODY = b'{"profile":"minio-2025-09-manual-verified-readback","schema_version":1}'
CHECKPOINT_IDS = {
    "streaming-events-v1",
    "streaming-event-windows-v1",
    "streaming-online-retail-cdc-v1",
    "streaming-gh-archive-file-v1",
    "go-live-streaming-test-v1",
}
DECISIONS = {"eligible", "refused", "not_ready", "partial", "completed"}
METRIC_CONTRACTS = {
    "checkpoint_retention_objects": ("checkpoint_id", CHECKPOINT_IDS),
    "checkpoint_retention_bytes": ("checkpoint_id", CHECKPOINT_IDS),
    "checkpoint_retention_eligible_bytes": ("checkpoint_id", CHECKPOINT_IDS),
    "checkpoint_retention_lease_heartbeat_age_seconds": ("checkpoint_id", CHECKPOINT_IDS),
    "checkpoint_retention_last_success_unixtime": ("checkpoint_id", CHECKPOINT_IDS),
    "checkpoint_retention_plans_total": ("decision", DECISIONS),
    "checkpoint_retention_refusals_total": (
        "refusal_code",
        {
            "clock_overflow",
            "exclusive_run_required",
            "future_clock",
            "invalid_fact_type",
            "invalid_lease_terminal_state",
            "invalid_terminal_state",
            "invalid_utc_timestamp",
            "inventory_invalid",
            "lease_active",
            "lease_clock_invalid",
            "lease_missing",
            "lease_conflicting",
            "lease_etag_invalid",
            "lease_identity_mismatch",
            "lease_malformed",
            "lease_state_invalid",
            "lease_terminal_clock_conflict",
            "object_after_terminal",
            "partial_retry_broadened",
            "recovery_not_approved",
            "registry_active_durable",
            "registry_retirement_review_invalid",
            "registry_retirement_review_missing",
            "retention_quarantine",
            "retirement_clock_missing",
            "retirement_review_mismatch",
            "retirement_review_missing",
            "sink_disposition_not_approved",
            "source_unavailable",
            "successful_run_required",
            "terminal_missing",
            "inventory_changed",
            "policy_drift",
            "revalidation_mismatch",
        },
    ),
    "checkpoint_retention_prepared_total": ("outcome", DECISIONS),
    "checkpoint_retention_deleted_objects_total": ("outcome", DECISIONS),
    "checkpoint_retention_deleted_bytes_total": ("outcome", DECISIONS),
    "checkpoint_retention_partial_total": ("outcome", DECISIONS),
    "checkpoint_retention_request_failures_total": (
        "outcome",
        {"backend_failure", "invalid_request", "unauthorized", "timeout", "capability_failed"},
    ),
}

pytestmark = pytest.mark.infra


def _env(key: str, default: str = "") -> str:
    if key in os.environ:
        return os.environ[key]
    if INFRA_ENV.exists():
        for line in INFRA_ENV.read_text(encoding="utf-8").splitlines():
            if line.startswith(f"{key}="):
                return line.split("=", 1)[1] or default
    return default


def _run(*command: str, timeout: int = 1800) -> subprocess.CompletedProcess[str]:
    return subprocess.run(command, cwd=ROOT, check=True, capture_output=True, text=True, timeout=timeout)


def _client(endpoint: str, access_key: str, secret_key: str):
    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        region_name="us-east-1",
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        config=Config(
            signature_version="s3v4",
            s3={"addressing_style": "path"},
            retries={"max_attempts": 1},
            proxies={},
        ),
    )


def _stack_containers(runner=None) -> tuple[str, ...]:
    execute = runner or _run
    result = execute(
        "docker",
        "ps",
        "--all",
        "--filter",
        f"label=com.docker.compose.project={_env('PROJECT_NAME', 'data-eng-lab')}",
        "--format",
        "{{.Names}}",
    )
    return tuple(line for line in result.stdout.splitlines() if line)


@contextmanager
def _owned_stack(runner=None, probe=None):
    execute = runner or _run
    inspect = probe or _stack_containers
    existing = tuple(inspect())
    if existing:
        raise RuntimeError("project stack already exists in some state; exclusive acceptance refused")
    primary = None
    try:
        execute("./scripts/start-all.sh")
        yield
    except BaseException as error:
        primary = error
        raise
    finally:
        try:
            execute("./scripts/stop-all.sh")
            if tuple(inspect()):
                raise RuntimeError("cleanup_failed")
        except BaseException:
            if primary is None:
                raise
            primary.add_note("cleanup_failed")


@contextmanager
def _owned_fixture_objects(client):
    keys: list[str] = []
    primary = None

    def put(key: str, body: bytes) -> None:
        if not isinstance(key, str) or OWNED_FIXTURE_KEY.fullmatch(key) is None:
            raise AssertionError("invalid owned fixture key")
        if not isinstance(body, bytes) or not body or len(body) > 64:
            raise AssertionError("invalid owned fixture body")
        if key in keys or len(keys) >= 16:
            raise AssertionError("invalid owned fixture key set")
        keys.append(key)
        client.put_object(Bucket="checkpoints", Key=key, Body=body)

    try:
        yield put
    except BaseException as error:
        primary = error
        raise
    finally:
        if keys:
            try:
                response = client.delete_objects(
                    Bucket="checkpoints",
                    Delete={"Objects": [{"Key": key} for key in keys], "Quiet": False},
                )
                if response.get("Errors"):
                    raise RuntimeError("owned_fixture_cleanup_failed")
                deleted = tuple(sorted(item.get("Key") for item in response.get("Deleted", [])))
                if deleted != tuple(sorted(keys)):
                    raise RuntimeError("owned_fixture_cleanup_failed")
                prefixes = tuple(sorted({key.split("/", 2)[0] + "/" + key.split("/", 2)[1] + "/" for key in keys}))
                for prefix in prefixes:
                    listed = client.list_objects_v2(Bucket="checkpoints", Prefix=prefix, MaxKeys=1000)
                    if listed.get("IsTruncated") is not False or listed.get("Contents"):
                        raise RuntimeError("owned_fixture_cleanup_failed")
            except BaseException:
                if primary is None:
                    raise RuntimeError("owned_fixture_cleanup_failed") from None
                primary.add_note("owned_fixture_cleanup_failed")


@contextmanager
def _owned_runtime_capability(client, started_at: datetime):
    if not isinstance(started_at, datetime) or started_at.tzinfo is None or started_at.utcoffset() != timedelta(0):
        raise AssertionError("invalid capability ownership clock")
    owned: list[str] = []
    primary = None
    try:
        response = client.list_objects_v2(Bucket="checkpoints", Prefix="_retention/capability/", MaxKeys=1000)
        if response.get("IsTruncated") is not False or not isinstance(response.get("Contents", []), list):
            raise AssertionError("capability inventory invalid")
        for item in response.get("Contents", []):
            if not isinstance(item, dict):
                raise AssertionError("capability inventory invalid")
            key = item.get("Key")
            modified = item.get("LastModified")
            if (
                not isinstance(key, str)
                or not isinstance(modified, datetime)
                or modified.tzinfo is None
                or modified.utcoffset() != timedelta(0)
            ):
                raise AssertionError("capability inventory invalid")
            if modified >= started_at:
                if OWNED_CAPABILITY_KEY.fullmatch(key) is None or len(owned) >= 2:
                    raise AssertionError("capability ownership invalid")
                owned.append(key)
        if len(owned) != 1:
            raise AssertionError("capability ownership invalid")
        result = client.get_object(Bucket="checkpoints", Key=owned[0])
        body = result.get("Body") if isinstance(result, dict) else None
        if body is None or not hasattr(body, "read") or not hasattr(body, "close"):
            raise AssertionError("capability evidence invalid")
        try:
            raw = body.read(129)
        finally:
            body.close()
        if raw != CAPABILITY_BODY:
            raise AssertionError("capability evidence invalid")
        yield owned[0]
    except BaseException as error:
        primary = error
        raise
    finally:
        if owned:
            try:
                result = client.delete_objects(
                    Bucket="checkpoints",
                    Delete={"Objects": [{"Key": key} for key in owned], "Quiet": False},
                )
                deleted = sorted(item.get("Key") for item in result.get("Deleted", []))
                if result.get("Errors") or deleted != sorted(owned):
                    raise RuntimeError("capability_cleanup_failed")
                remaining = client.list_objects_v2(
                    Bucket="checkpoints",
                    Prefix="_retention/capability/",
                    MaxKeys=1000,
                )
                present = {item.get("Key") for item in remaining.get("Contents", [])}
                if remaining.get("IsTruncated") is not False or any(key in present for key in owned):
                    raise RuntimeError("capability_cleanup_failed")
            except BaseException:
                if primary is None:
                    raise RuntimeError("capability_cleanup_failed") from None
                primary.add_note("capability_cleanup_failed")


def _fixture_identity(run_uuid: str) -> dict[str, object]:
    if not isinstance(run_uuid, str) or RUN_UUID.fullmatch(run_uuid) is None:
        raise AssertionError("fixture UUID must be canonical")
    return {
        "checkpoint_id": "go-live-streaming-test-v1",
        "generation": {"run_uuid": run_uuid},
        "prefix": f"streaming_test/{run_uuid}/",
        "workload": "go-live-streaming-test",
    }


def _review_facts() -> dict[str, str]:
    return {"actor": "issue86-live-acceptance"}


def _assert_stable_snapshot(before: dict[str, object], after: dict[str, object]) -> None:
    if before != after:
        raise AssertionError("production snapshot changed")


def _assert_operation_evidence(evidence: dict[str, object], operation_id: str, plan_sha256: str, prefix: str) -> None:
    if set(evidence) != {"audit", "plan", "prepared", "result"}:
        raise AssertionError("operation evidence mismatch")
    plan = evidence["plan"]
    prepared = evidence["prepared"]
    result = evidence["result"]
    audit = evidence["audit"]
    if (
        not isinstance(plan, dict)
        or not isinstance(plan.get("summary"), dict)
        or plan["summary"].get("decision") != "eligible"
        or plan["summary"].get("prefix") != prefix
        or plan.get("plan_sha256") != plan_sha256
        or not isinstance(prepared, dict)
        or prepared.get("operation_id") != operation_id
        or prepared.get("plan_sha256") != plan_sha256
        or prepared.get("prefix") != prefix
        or not isinstance(result, dict)
        or result.get("operation_id") != operation_id
        or result.get("plan_sha256") != plan_sha256
        or result.get("state") != "completed"
        or not isinstance(audit, dict)
        or audit.get("operation_id") != operation_id
        or audit.get("plan_sha256") != plan_sha256
        or audit.get("decision") != "completed"
    ):
        raise AssertionError("operation evidence mismatch")


def _parse_metrics(body: bytes) -> dict[str, int]:
    if type(body) is not bytes or len(body) > MAX_METRICS_BYTES:
        raise AssertionError("metrics body invalid")
    result: dict[str, int] = {}
    for raw_line in body.splitlines():
        try:
            line = raw_line.decode("ascii")
            name, raw_value = line.rsplit(" ", 1)
            value = int(raw_value)
        except (UnicodeError, ValueError):
            raise AssertionError("metrics body invalid") from None
        match = re.fullmatch(r'([a-z_]+)(?:\{([a-z_]+)="([a-z0-9_-]+)"\})?', name)
        if match is None or match.group(1) not in METRIC_CONTRACTS:
            raise AssertionError("metrics name invalid")
        label_name, allowed_values = METRIC_CONTRACTS[match.group(1)]
        if match.group(2) is not None and (match.group(2) != label_name or match.group(3) not in allowed_values):
            raise AssertionError("metrics name invalid")
        if value < 0 or name in result:
            raise AssertionError("metrics value invalid")
        result[name] = value
    return result


def _service(method: str, path: str, payload: dict[str, object] | None = None) -> dict[str, object]:
    body = None if payload is None else json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("ascii")
    token_name = (
        "CHECKPOINT_RETENTION_LEASE_TOKEN" if path.startswith("/v1/leases/") else "CHECKPOINT_RETENTION_OPERATOR_TOKEN"
    )
    script = (
        "import json,os,urllib.request;"
        f"body={body!r};"
        "request=urllib.request.Request("
        f"'http://127.0.0.1:8080{path}',data=body,method='{method}',"
        f"headers={{'Authorization':'Bearer '+os.environ['{token_name}'],"
        "'Content-Type':'application/json',**({'Content-Length':str(len(body))} if body is not None else {})});"
        "response=urllib.request.urlopen(request,timeout=30);"
        f"raw=response.read({MAX_RESPONSE_BYTES + 1});response.close();"
        "print(raw.decode('ascii'))"
    )
    result = _run(
        "docker",
        "exec",
        f"{_env('PROJECT_NAME', 'data-eng-lab')}-checkpoint-retention-1",
        "/opt/venv/bin/python",
        "-c",
        script,
        timeout=60,
    )
    if len(result.stdout.encode("utf-8")) > MAX_RESPONSE_BYTES + 1:
        raise AssertionError("retention response exceeded bound")
    value = json.loads(result.stdout)
    if not isinstance(value, dict):
        raise AssertionError("retention response must be an object")
    return value


def _service_status(
    method: str,
    path: str,
    token_name: str,
    payload: dict[str, object] | None = None,
) -> tuple[int, dict[str, object]]:
    body = b"{}" if payload is None else json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("ascii")
    script = (
        "import json,os,urllib.error,urllib.request;"
        f"body={body!r};"
        "request=urllib.request.Request("
        f"'http://127.0.0.1:8080{path}',data=body,method='{method}',"
        f"headers={{'Authorization':'Bearer '+os.environ['{token_name}'],"
        "'Content-Type':'application/json','Content-Length':str(len(body))});"
        "\ntry:\n response=urllib.request.urlopen(request,timeout=30);status=response.status"
        "\nexcept urllib.error.HTTPError as error:\n response=error;status=error.code"
        f"\nraw=response.read({MAX_RESPONSE_BYTES + 1});response.close();"
        "print(json.dumps({'status':status,'body':json.loads(raw)}))"
    )
    result = _run(
        "docker",
        "exec",
        f"{_env('PROJECT_NAME', 'data-eng-lab')}-checkpoint-retention-1",
        "/opt/venv/bin/python",
        "-c",
        script,
        timeout=60,
    )
    value = json.loads(result.stdout)
    return value["status"], value["body"]


def _wait_service(timeout: int = 300) -> dict[str, object]:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            health = _service("GET", "/healthz")
            if health.get("ready") is True:
                return health
        except (AssertionError, OSError, subprocess.SubprocessError, ValueError):
            time.sleep(2)
    raise TimeoutError("checkpoint retention service did not become ready")


def _inventory(client, prefix: str) -> tuple[tuple[str, str, int], ...]:
    response = client.list_objects_v2(Bucket="checkpoints", Prefix=prefix, MaxKeys=1000)
    if response.get("IsTruncated") is not False:
        raise AssertionError("live inventory exceeded one bounded page")
    return tuple(sorted((item["Key"], item["ETag"].strip('"'), item["Size"]) for item in response.get("Contents", [])))


def _production_snapshot(client) -> dict[str, object]:
    return {
        "policy_sha256": _policy_sha256(load_policy(ROOT / "checkpoints/retention-policy.yaml")),
        "production": {prefix: _inventory(client, prefix) for prefix in PRODUCTION_PREFIXES},
        "controls": {
            prefix: _inventory(client, prefix)
            for prefix in (
                "_retention/leases/streaming-events",
                "_retention/leases/streaming-event-windows",
                "_retention/leases/streaming-online-retail",
                "_retention/leases/streaming-gh-archive",
            )
        },
    }


def _prove_maintenance_iam(root, maintenance, fixture_prefix: str, denied_key: str) -> dict[str, object]:
    capability_key = f"_retention/capability/{uuid.uuid4()}/proof.json"
    primary = None

    def denied(call) -> None:
        try:
            call()
        except ClientError as error:
            code = error.response.get("Error", {}).get("Code")
            if code not in {"AccessDenied", "AllAccessDisabled"}:
                raise AssertionError("unexpected IAM refusal") from None
        else:
            raise AssertionError("forbidden IAM call succeeded")

    try:
        listed = maintenance.list_objects_v2(Bucket="checkpoints", Prefix=fixture_prefix, MaxKeys=1000)
        if listed.get("IsTruncated") is not False or len(listed.get("Contents", [])) != 2:
            raise AssertionError("exact-prefix IAM list failed")
        key = f"{fixture_prefix}state/offset"
        response = maintenance.get_object(Bucket="checkpoints", Key=key)
        body = response["Body"]
        try:
            if body.read(65) == b"":
                raise AssertionError("exact-prefix IAM get failed")
        finally:
            body.close()
        maintenance.put_object(Bucket="checkpoints", Key=capability_key, Body=b'{"schema_version":1}')
        denied(lambda: maintenance.list_objects_v2(Bucket="checkpoints", Prefix="streaming_test/", MaxKeys=1))
        denied(lambda: maintenance.get_object(Bucket="checkpoints", Key=denied_key))
        denied(lambda: maintenance.put_object(Bucket="checkpoints", Key=f"{fixture_prefix}forbidden", Body=b"x"))
        denied(lambda: maintenance.delete_object(Bucket="checkpoints", Key=capability_key))
        return {"allowed": 3, "denied": 4, "control_key": capability_key}
    except BaseException as error:
        primary = error
        raise
    finally:
        try:
            root.delete_object(Bucket="checkpoints", Key=capability_key)
            if _inventory(root, capability_key):
                raise RuntimeError("iam_probe_cleanup_failed")
        except BaseException:
            if primary is None:
                raise RuntimeError("iam_probe_cleanup_failed") from None
            primary.add_note("iam_probe_cleanup_failed")


def _lease_payload(identity: dict[str, object], session_id: str) -> dict[str, object]:
    return {
        "checkpoint_id": identity["checkpoint_id"],
        "owner_id": "issue86-live-acceptance",
        "prefix": identity["prefix"],
        "session_id": session_id,
        "workload": identity["workload"],
    }


def _terminal_payload(identity: dict[str, object], epoch: str) -> dict[str, object]:
    return {
        "checkpoint_id": identity["checkpoint_id"],
        "epoch": epoch,
        "evidence": {
            "exclusive_run": True,
            "generation": identity["generation"],
            "recovery_approved": True,
            "sink_disposition_approved": True,
            "source_available": True,
            "successful": True,
        },
        "prefix": identity["prefix"],
        "state": "stopped",
    }


def _plan(identity: dict[str, object]) -> tuple[dict[str, object], str]:
    artifact = _service(
        "POST",
        "/v1/plans",
        {
            "actor": "issue86-live-acceptance",
            "checkpoint_id": identity["checkpoint_id"],
            "prefix": identity["prefix"],
        },
    )
    body = json.dumps(artifact, sort_keys=True, separators=(",", ":")).encode("ascii")
    return artifact, __import__("hashlib").sha256(body).hexdigest()


def _prepare(artifact: dict[str, object], digest: str) -> dict[str, object]:
    return _service(
        "POST",
        "/v1/operations/prepare",
        {
            "actor": "issue86-live-acceptance",
            "plan": artifact,
            "plan_sha256": digest,
            "review": "issue86-live-reviewed",
        },
    )


def _wait_apply(operation_id: str, prefix: str, plan_sha256: str) -> dict[str, object]:
    deadline = time.monotonic() + 930
    while True:
        result = _service(
            "POST",
            f"/v1/operations/{operation_id}/apply",
            {"confirm_prefix": prefix, "plan_sha256": plan_sha256},
        )
        if result.get("state") != "not_ready":
            return result
        if time.monotonic() >= deadline:
            raise AssertionError("operation quiescence deadline exceeded")
        time.sleep(5)


def _read_json_object(client, key: str, *, max_bytes: int = 65_536) -> dict[str, object]:
    response = client.get_object(Bucket="checkpoints", Key=key)
    body = response.get("Body") if isinstance(response, dict) else None
    if body is None or not hasattr(body, "read") or not hasattr(body, "close"):
        raise AssertionError("operation evidence invalid")
    try:
        raw = body.read(max_bytes + 1)
    finally:
        body.close()
    if type(raw) is not bytes or len(raw) > max_bytes:
        raise AssertionError("operation evidence invalid")
    try:
        value = json.loads(raw)
    except (UnicodeError, json.JSONDecodeError):
        raise AssertionError("operation evidence invalid") from None
    if not isinstance(value, dict):
        raise AssertionError("operation evidence invalid")
    return value


def _operation_evidence(
    client,
    artifact: dict[str, object],
    operation_id: str,
    plan_sha256: str,
) -> dict[str, object]:
    base = f"_retention/tombstones/{operation_id}"
    prepared = _read_json_object(client, f"{base}/prepared.json")
    attempts = client.list_objects_v2(Bucket="checkpoints", Prefix=f"{base}/results/attempts/", MaxKeys=1000)
    audits = client.list_objects_v2(Bucket="checkpoints", Prefix=f"_retention/audits/{operation_id}/", MaxKeys=1000)
    if attempts.get("IsTruncated") is not False or audits.get("IsTruncated") is not False:
        raise AssertionError("operation evidence invalid")
    attempt_keys = sorted(item.get("Key") for item in attempts.get("Contents", []))
    audit_keys = sorted(item.get("Key") for item in audits.get("Contents", []))
    results = [_read_json_object(client, key) for key in attempt_keys]
    audit_values = [_read_json_object(client, key) for key in audit_keys]
    completed = [value for value in results if value.get("state") == "completed"]
    completed_audits = [value for value in audit_values if value.get("decision") == "completed"]
    if len(completed) != 1 or len(completed_audits) != 1:
        raise AssertionError("operation evidence invalid")
    plan = dict(artifact)
    plan["plan_sha256"] = plan_sha256
    return {"audit": completed_audits[0], "plan": plan, "prepared": prepared, "result": completed[0]}


def _validate_accelerated_result(raw: bytes) -> dict[str, object]:
    if type(raw) is not bytes or not raw or len(raw) > MAX_RESPONSE_BYTES:
        raise AssertionError("accelerated evidence invalid")
    try:
        value = json.loads(raw)
    except (UnicodeError, json.JSONDecodeError):
        raise AssertionError("accelerated evidence invalid") from None
    if not isinstance(value, dict) or set(value) != {
        "artifact",
        "completed",
        "image_id",
        "metrics",
        "not_ready",
        "operation_id",
        "plan_sha256",
    }:
        raise AssertionError("accelerated evidence invalid")
    artifact = value["artifact"]
    completed = value["completed"]
    not_ready = value["not_ready"]
    if (
        not isinstance(artifact, dict)
        or not isinstance(artifact.get("summary"), dict)
        or artifact["summary"].get("decision") != "eligible"
        or not isinstance(completed, dict)
        or completed.get("state") != "completed"
        or not isinstance(not_ready, dict)
        or not_ready.get("state") != "not_ready"
        or not isinstance(value["image_id"], str)
        or re.fullmatch(r"sha256:[0-9a-f]{64}", value["image_id"]) is None
        or not isinstance(value["operation_id"], str)
        or RUN_UUID.fullmatch(value["operation_id"]) is None
        or not isinstance(value["plan_sha256"], str)
        or SHA256.fullmatch(value["plan_sha256"]) is None
    ):
        raise AssertionError("accelerated evidence invalid")
    if not isinstance(value["metrics"], str):
        raise AssertionError("accelerated evidence invalid")
    _parse_metrics(value["metrics"].encode("ascii"))
    return value


def _run_accelerated_exact_image(identity: dict[str, object]) -> tuple[dict[str, object], datetime]:
    project = _env("PROJECT_NAME", "data-eng-lab")
    container = f"{project}-checkpoint-retention-1"
    image_id = _run("docker", "inspect", container, "--format", "{{.Image}}").stdout.strip()
    network = _run(
        "docker",
        "inspect",
        container,
        "--format",
        "{{range $name, $_ := .NetworkSettings.Networks}}{{$name}}{{end}}",
    ).stdout.strip()
    if (
        re.fullmatch(r"sha256:[0-9a-f]{64}", image_id) is None
        or re.fullmatch(r"[A-Za-z0-9_.-]{1,128}", network) is None
    ):
        raise AssertionError("accelerated runtime identity invalid")
    script = f"""\
import hashlib
import json
from datetime import datetime, timedelta, timezone

import scripts.checkpoints.service as service

identity = {json.dumps(identity, sort_keys=True)!r}
identity = json.loads(identity)
clock = [datetime.now(timezone.utc).replace(microsecond=0)]
service._now = lambda: clock[0]
runtime = service.build_runtime()
try:
    lease = runtime.invoke("lease_acquire", {{
        "checkpoint_id": identity["checkpoint_id"],
        "owner_id": "issue86-accelerated-exact-image",
        "prefix": identity["prefix"],
        "session_id": "issue86-accelerated-exact-image",
        "workload": identity["workload"],
    }}, None)
    runtime.invoke("lease_terminal", {{
        "checkpoint_id": identity["checkpoint_id"],
        "epoch": lease["epoch"],
        "evidence": {{
            "exclusive_run": True,
            "generation": identity["generation"],
            "recovery_approved": True,
            "sink_disposition_approved": True,
            "source_available": True,
            "successful": True,
        }},
        "prefix": identity["prefix"],
        "state": "stopped",
    }}, None)
    clock[0] += timedelta(seconds=86401)
    artifact = runtime.invoke("plan", {{
        "actor": "issue86-accelerated-exact-image",
        "checkpoint_id": identity["checkpoint_id"],
        "prefix": identity["prefix"],
    }}, None)
    plan_body = json.dumps(artifact, sort_keys=True, separators=(",", ":")).encode("ascii")
    plan_sha256 = hashlib.sha256(plan_body).hexdigest()
    prepared = runtime.invoke("prepare", {{
        "actor": "issue86-accelerated-exact-image",
        "plan": artifact,
        "plan_sha256": plan_sha256,
        "review": "issue86-live-reviewed",
    }}, None)
    operation_id = prepared["operation_id"]
    request = {{"confirm_prefix": identity["prefix"], "plan_sha256": plan_sha256}}
    not_ready = runtime.invoke("apply", request, operation_id)
    clock[0] += timedelta(seconds=901)
    completed = runtime.invoke("apply", request, operation_id)
    print(json.dumps({{
        "artifact": artifact,
        "completed": completed,
        "image_id": {image_id!r},
        "metrics": runtime.metrics().decode("ascii"),
        "not_ready": not_ready,
        "operation_id": operation_id,
        "plan_sha256": plan_sha256,
    }}, sort_keys=True, separators=(",", ":")))
finally:
    runtime.close()
"""
    started_at = datetime.now(timezone.utc) - timedelta(seconds=1)
    environment = dict(os.environ)
    environment.update(
        {
            "MINIO_RETENTION_ACCESS_KEY": LIVE_RETENTION_ACCESS_KEY,
            "MINIO_RETENTION_SECRET_KEY": _env("MINIO_RETENTION_SECRET_KEY"),
        }
    )
    with tempfile.TemporaryDirectory(prefix="issue86-accelerated-") as directory:
        script_path = Path(directory) / "proof.py"
        script_path.write_text(script, encoding="utf-8")
        result = subprocess.run(
            [
                "docker",
                "run",
                "--rm",
                "--network",
                network,
                "--env",
                "MINIO_RETENTION_ACCESS_KEY",
                "--env",
                "MINIO_RETENTION_SECRET_KEY",
                "--env",
                "DESTRUCTIVE_ENABLED=true",
                "--env",
                "PYTHONPATH=/workspace",
                "--mount",
                f"type=bind,source={script_path},target=/proof/proof.py,readonly",
                "--entrypoint",
                "/opt/venv/bin/python",
                image_id,
                "/proof/proof.py",
            ],
            cwd=ROOT,
            env=environment,
            capture_output=True,
            timeout=180,
        )
    if result.returncode != 0 or len(result.stdout) > MAX_RESPONSE_BYTES or len(result.stderr) > MAX_RESPONSE_BYTES:
        raise AssertionError("accelerated exact-image proof failed")
    for secret in (_env("MINIO_RETENTION_SECRET_KEY"),):
        if secret and secret.encode() in result.stdout + result.stderr:
            raise AssertionError("accelerated exact-image proof leaked a credential")
    return _validate_accelerated_result(result.stdout), started_at


def _run_layered_partial_retry() -> None:
    environment = dict(os.environ)
    environment["RUN_MINIO_INTEGRATION"] = "1"
    result = subprocess.run(
        [
            "uv",
            "run",
            "pytest",
            "-q",
            "tests/checkpoints/test_retention_s3_minio.py::test_real_gateway_partial_retry_is_original_set_confined_and_idempotent",
        ],
        cwd=ROOT,
        env=environment,
        check=True,
        capture_output=True,
        text=True,
        timeout=180,
    )
    if "1 passed" not in result.stdout:
        raise AssertionError("partial retry evidence missing")


def _volume_inventory() -> tuple[str, ...]:
    project = _env("PROJECT_NAME", "data-eng-lab")
    result = _run("docker", "volume", "ls", "--format", "{{.Name}}")
    return tuple(sorted(line for line in result.stdout.splitlines() if line.startswith(f"{project}_")))


def _airflow_token() -> str:
    request = urllib.request.Request(
        f"http://127.0.0.1:{_env('AIRFLOW_PORT', '20070')}/auth/token",
        data=json.dumps(
            {"username": _env("AIRFLOW_ADMIN_USER", "admin"), "password": _env("AIRFLOW_ADMIN_PASSWORD")}
        ).encode(),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=30) as response:
        return json.load(response)["access_token"]


def _airflow_dag() -> dict[str, object]:
    token = _airflow_token()
    request = urllib.request.Request(
        f"http://127.0.0.1:{_env('AIRFLOW_PORT', '20070')}/api/v2/dags/checkpoint_retention",
        headers={"Authorization": f"Bearer {token}"},
    )
    with urllib.request.urlopen(request, timeout=30) as response:
        value = json.load(response)
    if not isinstance(value, dict):
        raise AssertionError("Airflow DAG response invalid")
    return value


@pytest.mark.skipif(os.environ.get("RUN_INFRA") != "1", reason="needs the canonical exclusive Atlas stack")
def test_checkpoint_retention_live_acceptance():
    previous_destructive = os.environ.get("CHECKPOINT_RETENTION_DESTRUCTIVE_ENABLED")
    previous_access_key = os.environ.get("MINIO_RETENTION_ACCESS_KEY")
    previous_lease_token = os.environ.get("CHECKPOINT_RETENTION_LEASE_TOKEN")
    previous_operator_token = os.environ.get("CHECKPOINT_RETENTION_OPERATOR_TOKEN")
    os.environ["CHECKPOINT_RETENTION_DESTRUCTIVE_ENABLED"] = "true"
    os.environ["MINIO_RETENTION_ACCESS_KEY"] = LIVE_RETENTION_ACCESS_KEY
    os.environ["CHECKPOINT_RETENTION_LEASE_TOKEN"] = LIVE_LEASE_TOKEN
    os.environ["CHECKPOINT_RETENTION_OPERATOR_TOKEN"] = LIVE_OPERATOR_TOKEN
    stack_started_at = datetime.now(timezone.utc) - timedelta(seconds=1)
    volumes_before = _volume_inventory()
    try:
        with _owned_stack(), ExitStack() as owned_resources:
            health = _wait_service()
            assert health == {
                "capability_profile": "minio-2025-09-manual-verified-readback",
                "destructive_enabled": True,
                "ready": True,
            }
            assert _service_status("POST", "/v1/plans", "CHECKPOINT_RETENTION_LEASE_TOKEN") == (
                401,
                {"code": "unauthorized"},
            )
            assert _service_status("POST", "/v1/leases/acquire", "CHECKPOINT_RETENTION_OPERATOR_TOKEN") == (
                401,
                {"code": "unauthorized"},
            )
            endpoint = _env("ATLAS_MINIO_HOST_ENDPOINT")
            if not endpoint:
                endpoint_file = ROOT / "atlas-consumer.env"
                values = dict(
                    line.split("=", 1) for line in endpoint_file.read_text(encoding="utf-8").splitlines() if "=" in line
                )
                endpoint = values["ATLAS_MINIO_HOST_ENDPOINT"]
            root = _client(endpoint, _env("MINIO_ROOT_USER"), _env("MINIO_ROOT_PASSWORD"))
            owned_resources.enter_context(_owned_runtime_capability(root, stack_started_at))
            put_owned = owned_resources.enter_context(_owned_fixture_objects(root))
            before = _production_snapshot(root)
            denied_key = f"unknown-retention/{uuid.uuid4()}/sentinel"
            put_owned(denied_key, b"preserve")

            identities = [_fixture_identity(str(uuid.uuid4())) for _ in range(2)]
            for index, identity in enumerate(identities):
                prefix = identity["prefix"]
                put_owned(f"{prefix}state/offset", f"state-{index}".encode())
                put_owned(f"{prefix}commits/0", f"commit-{index}".encode())

            maintenance = _client(
                endpoint,
                LIVE_RETENTION_ACCESS_KEY,
                _env("MINIO_RETENTION_SECRET_KEY"),
            )
            assert _prove_maintenance_iam(root, maintenance, identities[0]["prefix"], denied_key)["denied"] == 4

            active = _service("POST", "/v1/leases/acquire", _lease_payload(identities[0], "issue86-live-active"))
            active_plan, _active_sha = _plan(identities[0])
            assert active_plan["summary"]["decision"] == "refused"
            assert "lease_active" in active_plan["summary"]["refusal_codes"]
            _service("POST", "/v1/leases/terminal", _terminal_payload(identities[0], active["epoch"]))

            retained_plan, _retained_sha = _plan(identities[0])
            assert retained_plan["summary"]["decision"] == "refused"
            assert retained_plan["summary"]["refusal_codes"] == ["future_clock", "retention_quarantine"]
            assert len(_inventory(root, identities[0]["prefix"])) == 2
            injected_status, injected_body = _service_status(
                "POST",
                "/v1/plans",
                "CHECKPOINT_RETENTION_OPERATOR_TOKEN",
                {
                    "actor": "issue86-live-acceptance",
                    "checkpoint_id": identities[0]["checkpoint_id"],
                    "evaluated_at": "2099-01-01T00:00:00Z",
                    "prefix": identities[0]["prefix"],
                },
            )
            assert (injected_status, injected_body) == (400, {"code": "request_invalid"})

            accelerated, accelerated_started_at = _run_accelerated_exact_image(identities[1])
            owned_resources.enter_context(_owned_runtime_capability(root, accelerated_started_at))
            assert accelerated["not_ready"]["state"] == "not_ready"
            assert accelerated["completed"]["state"] == "completed"
            accelerated_metrics = _parse_metrics(accelerated["metrics"].encode("ascii"))
            assert accelerated_metrics['checkpoint_retention_plans_total{decision="eligible"}'] == 1
            assert accelerated_metrics['checkpoint_retention_prepared_total{outcome="completed"}'] == 1
            assert accelerated_metrics['checkpoint_retention_deleted_objects_total{outcome="completed"}'] == 2
            evidence = _operation_evidence(
                root,
                accelerated["artifact"],
                accelerated["operation_id"],
                accelerated["plan_sha256"],
            )
            _assert_operation_evidence(
                evidence,
                accelerated["operation_id"],
                accelerated["plan_sha256"],
                identities[1]["prefix"],
            )
            assert len(_inventory(root, identities[0]["prefix"])) == 2
            assert _inventory(root, identities[1]["prefix"]) == ()
            assert root.head_object(Bucket="checkpoints", Key=denied_key)["ContentLength"] == len(b"preserve")

            dag = _airflow_dag()
            assert dag["is_paused"] is True
            metrics_result = _run(
                "docker",
                "exec",
                f"{_env('PROJECT_NAME', 'data-eng-lab')}-checkpoint-retention-1",
                "/opt/venv/bin/python",
                "-c",
                "import sys,urllib.request;"
                "response=urllib.request.urlopen('http://127.0.0.1:8080/metrics',timeout=30);"
                f"body=response.read({MAX_METRICS_BYTES + 1});response.close();sys.stdout.buffer.write(body)",
            )
            metrics = _parse_metrics(metrics_result.stdout.encode("ascii"))
            assert metrics.get('checkpoint_retention_plans_total{decision="eligible"}', 0) == 0
            assert metrics['checkpoint_retention_plans_total{decision="refused"}'] >= 2
            assert metrics.get('checkpoint_retention_prepared_total{outcome="completed"}', 0) == 0
            assert (
                metrics['checkpoint_retention_lease_heartbeat_age_seconds{checkpoint_id="go-live-streaming-test-v1"}']
                >= 0
            )
            assert metrics['checkpoint_retention_last_success_unixtime{checkpoint_id="go-live-streaming-test-v1"}'] > 0
            _assert_stable_snapshot(before, _production_snapshot(root))
        _run_layered_partial_retry()
        assert _volume_inventory() == volumes_before
    finally:
        if previous_destructive is None:
            os.environ.pop("CHECKPOINT_RETENTION_DESTRUCTIVE_ENABLED", None)
        else:
            os.environ["CHECKPOINT_RETENTION_DESTRUCTIVE_ENABLED"] = previous_destructive
        if previous_access_key is None:
            os.environ.pop("MINIO_RETENTION_ACCESS_KEY", None)
        else:
            os.environ["MINIO_RETENTION_ACCESS_KEY"] = previous_access_key
        if previous_lease_token is None:
            os.environ.pop("CHECKPOINT_RETENTION_LEASE_TOKEN", None)
        else:
            os.environ["CHECKPOINT_RETENTION_LEASE_TOKEN"] = previous_lease_token
        if previous_operator_token is None:
            os.environ.pop("CHECKPOINT_RETENTION_OPERATOR_TOKEN", None)
        else:
            os.environ["CHECKPOINT_RETENTION_OPERATOR_TOKEN"] = previous_operator_token
