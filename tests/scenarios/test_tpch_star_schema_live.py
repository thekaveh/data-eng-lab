from __future__ import annotations

import hashlib
import importlib.util
import json
import os
import subprocess
import time
import urllib.error
import urllib.request
import uuid
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
APP = ROOT / "spark-apps/tpch-star-schema"
INFRA_ENV = ROOT / "infra/.env"
DAG_ID = "tpch_star_schema"
PROVENANCE_KEYS = (
    "data_eng_lab.dataset",
    "data_eng_lab.dataset.scale",
    "data_eng_lab.dataset.plan_id",
    "data_eng_lab.dataset.publication_id",
    "data_eng_lab.dataset.manifest_sha256",
)
_AIRFLOW_TOKEN = ""

pytestmark = pytest.mark.infra


def _env(key: str, default: str = "") -> str:
    if key in os.environ:
        return os.environ[key]
    value = ""
    if INFRA_ENV.exists():
        for line in INFRA_ENV.read_text(encoding="utf-8").splitlines():
            if line.startswith(f"{key}="):
                value = line.split("=", 1)[1]
    return value or default


def _run(*command: str, timeout: int = 1800) -> subprocess.CompletedProcess[str]:
    return subprocess.run(command, cwd=ROOT, check=True, capture_output=True, text=True, timeout=timeout)


def _stack_containers(runner=None) -> tuple[str, ...]:
    execute = runner or _run
    project = _env("PROJECT_NAME", "data-eng-lab")
    result = execute(
        "docker",
        "ps",
        "--filter",
        f"label=com.docker.compose.project={project}",
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
        raise RuntimeError(f"project stack is already running; exclusive acceptance refused: {existing}")
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
        except BaseException as cleanup_error:
            if primary is None:
                raise
            primary.add_note(f"volume-preserving stack cleanup also failed: {cleanup_error}")


def _airflow(method: str, path: str, body: dict | None = None) -> dict:
    global _AIRFLOW_TOKEN
    base = f"http://127.0.0.1:{_env('AIRFLOW_PORT', '20070')}"
    if not _AIRFLOW_TOKEN:
        token_request = urllib.request.Request(
            base + "/auth/token",
            data=json.dumps(
                {"username": _env("AIRFLOW_ADMIN_USER", "admin"), "password": _env("AIRFLOW_ADMIN_PASSWORD")}
            ).encode(),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(token_request, timeout=30) as response:
            _AIRFLOW_TOKEN = json.load(response)["access_token"]
    headers = {
        "Authorization": "Bearer " + _AIRFLOW_TOKEN,
        "Content-Type": "application/json",
    }
    request = urllib.request.Request(
        base + "/api/v2" + path,
        data=None if body is None else json.dumps(body).encode(),
        headers=headers,
        method=method,
    )
    with urllib.request.urlopen(request, timeout=30) as response:
        return json.load(response)


def _wait_for_dag(timeout: int = 300) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            _airflow("GET", f"/dags/{DAG_ID}")
            return
        except (OSError, urllib.error.HTTPError):
            time.sleep(5)
    raise TimeoutError(f"Airflow did not load {DAG_ID}")


def _wait_for_run(run_id: str, timeout: int = 900) -> dict:
    deadline = time.time() + timeout
    while time.time() < deadline:
        run = _airflow("GET", f"/dags/{DAG_ID}/dagRuns/{run_id}")
        state = str(run.get("state", "")).lower()
        if state == "success":
            return run
        if state in {"failed", "upstream_failed"}:
            raise AssertionError(f"Airflow run {run_id} ended in {state}")
        time.sleep(5)
    raise TimeoutError(f"Airflow run {run_id} did not finish")


@contextmanager
def _paused_dag(api=None):
    request = api or _airflow
    initial = bool(request("GET", f"/dags/{DAG_ID}")["is_paused"])
    request("PATCH", f"/dags/{DAG_ID}", {"is_paused": True})
    primary = None
    try:
        yield initial
    except BaseException as error:
        primary = error
        raise
    finally:
        try:
            request("PATCH", f"/dags/{DAG_ID}", {"is_paused": initial})
        except BaseException as cleanup_error:
            if primary is None:
                raise
            primary.add_note(f"DAG pause-state restoration also failed: {cleanup_error}")


def _run_timestamp(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def _assert_owned_runs(api, window_start: str, expected: set[str], *, require_terminal: bool = False) -> dict:
    document = api("GET", f"/dags/{DAG_ID}/dagRuns?limit=100&order_by=start_date")
    runs = document.get("dag_runs", [])
    start = _run_timestamp(window_start)
    in_window = {run["dag_run_id"]: run for run in runs if _run_timestamp(run["start_date"]) >= start}
    unexpected = set(in_window) - expected
    missing = expected - set(in_window)
    if unexpected or missing:
        raise AssertionError(
            f"acceptance-window run mismatch: unexpected={sorted(unexpected)}, missing={sorted(missing)}"
        )
    active_unexpected = {
        run["dag_run_id"]
        for run in runs
        if str(run.get("state", "")).lower() in {"queued", "running"} and run["dag_run_id"] not in expected
    }
    if active_unexpected:
        raise AssertionError(f"unexpected active DAG runs before teardown: {sorted(active_unexpected)}")
    if require_terminal:
        non_success = {
            run_id: in_window[run_id].get("state")
            for run_id in expected
            if in_window[run_id].get("state") != "success"
        }
        if non_success:
            raise AssertionError(f"owned DAG runs are not terminal successes: {non_success}")
    return in_window


def _resolve_or_publish_tiny(runner=None) -> dict:
    execute = runner or _run
    resolve = ("uv", "run", "python", "scripts/resolve_dataset.py", "tpch", "--scale", "tiny")
    try:
        execute(*resolve)
    except subprocess.CalledProcessError:
        execute(
            "uv", "run", "python", "scripts/download_datasets.py",
            "--scale", "tiny", "--only", "tpch", "--refresh",
        )
    execute(
        "uv", "run", "python", "scripts/download_datasets.py",
        "--scale", "tiny", "--only", "tpch", "--verify-only",
    )
    return json.loads(execute(*resolve).stdout)


def _driver_ids() -> set[str]:
    master_ui = f"http://127.0.0.1:{_env('SPARK_MASTER_UI_PORT', '20027')}"
    with urllib.request.urlopen(f"{master_ui}/json", timeout=30) as response:
        document = json.load(response)
    found: set[str] = set()

    def collect(value):
        if isinstance(value, dict):
            for item in value.values():
                collect(item)
        elif isinstance(value, list):
            for item in value:
                collect(item)
        elif isinstance(value, str) and value.startswith("driver-"):
            found.add(value)

    collect(document)
    return found


def _spark_terminal(driver_id: str) -> dict:
    project = _env("PROJECT_NAME", "data-eng-lab")
    result = _run(
        "docker",
        "exec",
        f"{project}-airflow-scheduler",
        "curl",
        "-fsS",
        f"http://spark-master:6066/v1/submissions/status/{driver_id}",
    )
    status = json.loads(result.stdout)
    assert status["driverState"] == "FINISHED"
    assert status["success"] is True
    return status


def _snapshot_table(table: str) -> dict:
    spec = importlib.util.spec_from_file_location("tpch_live_exec", ROOT / "tests/scenarios/live_exec.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module.snapshot_table(table)


def _trino(sql: str) -> list[list[str]]:
    project = _env("PROJECT_NAME", "data-eng-lab")
    result = _run(
        "docker",
        "exec",
        f"{project}-trino",
        "trino",
        "--output-format",
        "JSON",
        "--execute",
        sql,
    )
    return [list(row.values()) for row in (json.loads(line) for line in result.stdout.splitlines() if line.strip())]


def _properties(table: str) -> dict[str, str]:
    keys = ",".join(f"'{key}'" for key in PROVENANCE_KEYS)
    rows = _trino(
        f'SELECT key, value FROM lakehouse.gold."{table}$properties" '
        f"WHERE key IN ({keys}) ORDER BY key"
    )
    return {str(key): str(value) for key, value in rows}


def _trigger_and_verify(run_id: str) -> tuple[dict, str]:
    before = _driver_ids()
    _airflow(
        "POST",
        f"/dags/{DAG_ID}/dagRuns",
        {"dag_run_id": run_id, "logical_date": None, "conf": {"dataset_scale": "tiny"}},
    )
    run = _wait_for_run(run_id)
    new_drivers = _driver_ids() - before
    assert len(new_drivers) == 1, f"expected one new Spark driver, got {sorted(new_drivers)}"
    driver_id = new_drivers.pop()
    _spark_terminal(driver_id)
    return run, driver_id


@pytest.mark.skipif(os.environ.get("RUN_INFRA") != "1", reason="needs the canonical live Atlas stack")
def test_tpch_star_schema_live_acceptance():
    """Exercise build/publication, resolver, serialized Airflow/Spark, Iceberg/Trino, and safe cleanup."""
    with _owned_stack():
        _wait_for_dag()
        with _paused_dag():
            window_start = datetime.now(timezone.utc).isoformat()
            first_run_id = f"issue107_first_{uuid.uuid4().hex}"
            second_run_id = f"issue107_rerun_{uuid.uuid4().hex}"
            _assert_owned_runs(_airflow, window_start, set())

            _run("mvn", "-q", "-B", "-f", str(APP / "pom.xml"), "package")
            jar = APP / "target/tpch-star-schema-0.1.0.jar"
            jar_sha256 = hashlib.sha256(jar.read_bytes()).hexdigest()
            import boto3  # noqa: PLC0415

            minio = boto3.client(
                "s3",
                endpoint_url=f"http://127.0.0.1:{_env('MINIO_PORT', '20020')}",
                aws_access_key_id=_env("MINIO_ICEBERG_ACCESS_KEY"),
                aws_secret_access_key=_env("MINIO_ICEBERG_SECRET_KEY"),
                region_name=_env("MINIO_REGION", "us-east-1"),
            )
            bucket = _env("MINIO_BUCKET_ICEBERG_JARS", "jars")
            key = "tpch-star-schema/0.1.0/app.jar"
            minio.upload_file(str(jar), bucket, key)
            published = minio.get_object(Bucket=bucket, Key=key)["Body"].read()
            assert hashlib.sha256(published).hexdigest() == jar_sha256

            resolved = _resolve_or_publish_tiny()
            assert resolved["dataset"] == "tpch" and resolved["scale"] == "tiny"
            assert len(resolved["objects"]) == 8 and all(item["size_bytes"] > 0 for item in resolved["objects"])

            _assert_owned_runs(_airflow, window_start, set())
            first_run, first_driver = _trigger_and_verify(first_run_id)
            _assert_owned_runs(_airflow, window_start, {first_run_id})
            first = {
                "dim": _snapshot_table("lakehouse.gold.dim_customer"),
                "fact": _snapshot_table("lakehouse.gold.fct_orders"),
                "dim_properties": _properties("dim_customer"),
                "fact_properties": _properties("fct_orders"),
            }
            _assert_owned_runs(_airflow, window_start, {first_run_id})
            second_run, second_driver = _trigger_and_verify(second_run_id)
            _assert_owned_runs(_airflow, window_start, {first_run_id, second_run_id})
            second = {
                "dim": _snapshot_table("lakehouse.gold.dim_customer"),
                "fact": _snapshot_table("lakehouse.gold.fct_orders"),
                "dim_properties": _properties("dim_customer"),
                "fact_properties": _properties("fct_orders"),
            }

            assert first == second
            assert first["dim"]["row_count"] > 0 and first["fact"]["row_count"] > 0
            assert first["dim"]["schema"] == sorted(
                ["c_custkey:long", "c_name:string", "c_nationkey:int", "c_mktsegment:string"]
            )
            assert first["fact"]["schema"] == sorted(
                ["o_orderkey:long", "o_custkey:long", "o_orderdate:date", "revenue:decimal(25, 2)", "line_count:long"]
            )
            expected_properties = {
                "data_eng_lab.dataset": "tpch",
                "data_eng_lab.dataset.scale": "tiny",
                "data_eng_lab.dataset.plan_id": resolved["plan_id"],
                "data_eng_lab.dataset.publication_id": resolved["publication_id"],
                "data_eng_lab.dataset.manifest_sha256": resolved["manifest_sha256"],
            }
            assert first["dim_properties"] == first["fact_properties"] == expected_properties
            measures = _trino(
                "SELECT count(*), CAST(sum(revenue) AS varchar), sum(line_count) "
                "FROM lakehouse.gold.fct_orders"
            )[0]
            assert int(measures[0]) > 0 and float(measures[1]) > 0 and int(measures[2]) > 0
            joined = _trino(
                "SELECT count(*) FROM lakehouse.gold.fct_orders f "
                "JOIN lakehouse.gold.dim_customer d ON f.o_custkey = d.c_custkey"
            )[0]
            assert int(joined[0]) == first["fact"]["row_count"]
            assert first_driver != second_driver
            assert datetime.fromisoformat(second_run["start_date"]) >= datetime.fromisoformat(first_run["end_date"])
            _assert_owned_runs(
                _airflow, window_start, {first_run_id, second_run_id}, require_terminal=True,
            )
