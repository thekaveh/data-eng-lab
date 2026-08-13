from __future__ import annotations

import json
import os
import re
import subprocess
import time
import urllib.error
import urllib.request
import uuid
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path

import boto3
import pytest
from botocore.config import Config

from scripts.checkpoints.policy import _policy_sha256, load_policy

ROOT = Path(__file__).resolve().parents[2]
INFRA_ENV = ROOT / "infra/.env"
RUN_UUID = re.compile(r"[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}")
SHA256 = re.compile(r"[0-9a-f]{64}")
MAX_METRICS_BYTES = 65_536
MAX_RESPONSE_BYTES = 65_536
PRODUCTION_PREFIXES = ("events/", "event_windows/", "online_retail_cdc/", "gh_events_file/")

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


def _fixture_identity(run_uuid: str) -> dict[str, object]:
    if not isinstance(run_uuid, str) or RUN_UUID.fullmatch(run_uuid) is None:
        raise AssertionError("fixture UUID must be canonical")
    return {
        "checkpoint_id": "go-live-streaming-test-v1",
        "generation": {"run_uuid": run_uuid},
        "prefix": f"streaming_test/{run_uuid}/",
        "workload": "go-live-streaming-test",
    }


def _review_facts(evaluated_at: str) -> dict[str, str]:
    if not isinstance(evaluated_at, str) or re.fullmatch(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z", evaluated_at) is None:
        raise AssertionError("review clock must be canonical UTC")
    return {"actor": "issue86-live-acceptance", "evaluated_at": evaluated_at}


def _assert_stable_snapshot(before: dict[str, object], after: dict[str, object]) -> None:
    if before != after:
        raise AssertionError("production snapshot changed")


def _assert_operation_evidence(
    evidence: dict[str, object], operation_id: str, plan_sha256: str, prefix: str
) -> None:
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
        pattern = r'checkpoint_retention_[a-z_]+(?:\{decision="(?:eligible|refused|completed|partial)"\})?'
        if not re.fullmatch(pattern, name):
            raise AssertionError("metrics name invalid")
        if value < 0 or name in result:
            raise AssertionError("metrics value invalid")
        result[name] = value
    return result


def _service(method: str, path: str, payload: dict[str, object] | None = None) -> dict[str, object]:
    body = None if payload is None else json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("ascii")
    script = (
        "import json,os,urllib.request;"
        f"body={body!r};"
        "request=urllib.request.Request("
        f"'http://127.0.0.1:8080{path}',data=body,method='{method}',"
        "headers={'Authorization':'Bearer '+os.environ['CHECKPOINT_RETENTION_API_TOKEN'],"
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
    return tuple(
        sorted(
            (item["Key"], item["ETag"].strip('"'), item["Size"])
            for item in response.get("Contents", [])
        )
    )


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


def _plan(identity: dict[str, object], evaluated_at: str) -> tuple[dict[str, object], str]:
    artifact = _service(
        "POST",
        "/v1/plans",
        {
            "actor": "issue86-live-acceptance",
            "checkpoint_id": identity["checkpoint_id"],
            "evaluated_at": evaluated_at,
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
    os.environ["CHECKPOINT_RETENTION_DESTRUCTIVE_ENABLED"] = "true"
    os.environ["MINIO_RETENTION_ACCESS_KEY"] = "retention86live"
    try:
        with _owned_stack():
            health = _wait_service()
            assert health == {
                "capability_profile": "minio-2025-09-manual-verified-readback",
                "destructive_enabled": True,
                "ready": True,
            }
            endpoint = _env("ATLAS_MINIO_HOST_ENDPOINT")
            if not endpoint:
                endpoint_file = ROOT / "atlas-consumer.env"
                values = dict(
                    line.split("=", 1)
                    for line in endpoint_file.read_text(encoding="utf-8").splitlines()
                    if "=" in line
                )
                endpoint = values["ATLAS_MINIO_HOST_ENDPOINT"]
            root = _client(endpoint, _env("MINIO_ROOT_USER"), _env("MINIO_ROOT_PASSWORD"))
            before = _production_snapshot(root)
            denied_key = f"unknown-retention/{uuid.uuid4()}/sentinel"
            root.put_object(Bucket="checkpoints", Key=denied_key, Body=b"preserve")

            identities = [_fixture_identity(str(uuid.uuid4())) for _ in range(2)]
            for index, identity in enumerate(identities):
                prefix = identity["prefix"]
                root.put_object(Bucket="checkpoints", Key=f"{prefix}state/offset", Body=f"state-{index}".encode())
                root.put_object(Bucket="checkpoints", Key=f"{prefix}commits/0", Body=f"commit-{index}".encode())

            active = _service("POST", "/v1/leases/acquire", _lease_payload(identities[0], "issue86-live-active"))
            active_plan, _active_sha = _plan(identities[0], datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"))
            assert active_plan["summary"]["decision"] == "refused"
            assert "lease_active" in active_plan["summary"]["refusal_codes"]
            _service("POST", "/v1/leases/terminal", _terminal_payload(identities[0], active["epoch"]))

            evaluated_at = (datetime.now(timezone.utc) + timedelta(days=2)).replace(microsecond=0).strftime(
                "%Y-%m-%dT%H:%M:%SZ"
            )
            first_plan, first_sha = _plan(identities[0], evaluated_at)
            repeated_plan, repeated_sha = _plan(identities[0], evaluated_at)
            assert first_plan == repeated_plan and first_sha == repeated_sha
            assert first_plan["summary"]["decision"] == "eligible"
            first_prepared = _prepare(first_plan, first_sha)

            second_lease = _service(
                "POST", "/v1/leases/acquire", _lease_payload(identities[1], "issue86-live-success")
            )
            _service("POST", "/v1/leases/terminal", _terminal_payload(identities[1], second_lease["epoch"]))
            second_plan, second_sha = _plan(identities[1], evaluated_at)
            assert second_plan["summary"]["decision"] == "eligible"
            second_prepared = _prepare(second_plan, second_sha)

            not_ready = _service(
                "POST",
                f"/v1/operations/{second_prepared['operation_id']}/apply",
                {"confirm_prefix": identities[1]["prefix"], "plan_sha256": second_sha},
            )
            assert not_ready["state"] == "not_ready"
            root.put_object(Bucket="checkpoints", Key=f"{identities[0]['prefix']}state/changed", Body=b"changed")

            deadline = time.monotonic() + 930
            while time.monotonic() < deadline:
                status = _service(
                    "POST",
                    f"/v1/operations/{second_prepared['operation_id']}/apply",
                    {"confirm_prefix": identities[1]["prefix"], "plan_sha256": second_sha},
                )
                if status.get("state") != "not_ready":
                    break
                time.sleep(10)
            assert status["state"] == "completed"
            assert _inventory(root, identities[1]["prefix"]) == ()

            with pytest.raises(subprocess.CalledProcessError):
                _service(
                    "POST",
                    f"/v1/operations/{first_prepared['operation_id']}/apply",
                    {"confirm_prefix": identities[0]["prefix"], "plan_sha256": first_sha},
                )
            assert len(_inventory(root, identities[0]["prefix"])) == 3
            assert root.head_object(Bucket="checkpoints", Key=denied_key)["ContentLength"] == len(b"preserve")

            dag = _airflow_dag()
            assert dag["is_paused"] is True
            metrics_result = _run(
                "docker",
                "exec",
                f"{_env('PROJECT_NAME', 'data-eng-lab')}-checkpoint-retention-1",
                "/opt/venv/bin/python",
                "-c",
                "import urllib.request;print(urllib.request.urlopen('http://127.0.0.1:8080/metrics').read().decode())",
            )
            metrics = _parse_metrics(metrics_result.stdout.encode("ascii"))
            assert metrics['checkpoint_retention_plans_total{decision="eligible"}'] >= 3
            assert metrics['checkpoint_retention_plans_total{decision="refused"}'] >= 1
            assert metrics['checkpoint_retention_deleted_objects_total{outcome="completed"}'] == 2
            _assert_stable_snapshot(before, _production_snapshot(root))
            root.delete_object(Bucket="checkpoints", Key=denied_key)
    finally:
        if previous_destructive is None:
            os.environ.pop("CHECKPOINT_RETENTION_DESTRUCTIVE_ENABLED", None)
        else:
            os.environ["CHECKPOINT_RETENTION_DESTRUCTIVE_ENABLED"] = previous_destructive
        if previous_access_key is None:
            os.environ.pop("MINIO_RETENTION_ACCESS_KEY", None)
        else:
            os.environ["MINIO_RETENTION_ACCESS_KEY"] = previous_access_key
