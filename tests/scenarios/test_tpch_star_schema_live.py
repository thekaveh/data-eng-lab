from __future__ import annotations

import hashlib
import importlib.util
import json
import os
import subprocess
import time
import urllib.error
import urllib.request
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
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
_RUN_PAGE_LIMIT = 100
_MAX_RUN_INVENTORY = 1000
_MAX_RUN_REQUESTS = 10

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
        "--all",
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
        raise RuntimeError(
            f"project stack already exists (running or stopped); exclusive acceptance refused: {existing}"
        )
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


def _list_runs(api) -> list[dict]:
    found: list[dict] = []
    run_ids: set[str] = set()
    expected_total = None
    offset = 0
    for _request_number in range(_MAX_RUN_REQUESTS):
        document = api(
            "GET",
            f"/dags/{DAG_ID}/dagRuns?limit={_RUN_PAGE_LIMIT}&offset={offset}&order_by=start_date",
        )
        if not isinstance(document, dict):
            raise AssertionError("Airflow DagRun inventory response must be an object")
        total = document.get("total_entries")
        if isinstance(total, bool) or not isinstance(total, int) or total < 0:
            raise AssertionError("Airflow DagRun inventory total_entries must be a nonnegative integer")
        if total > _MAX_RUN_INVENTORY:
            raise AssertionError(
                f"Airflow DagRun inventory exceeds safe maximum {_MAX_RUN_INVENTORY}"
            )
        if expected_total is None:
            expected_total = total
        elif total != expected_total:
            raise AssertionError("Airflow DagRun inventory total_entries changed during pagination")

        page = document.get("dag_runs")
        if not isinstance(page, list):
            raise AssertionError("Airflow DagRun inventory dag_runs must be a list")
        if len(page) > _RUN_PAGE_LIMIT:
            raise AssertionError("Airflow DagRun inventory page exceeds requested limit")
        if not page and len(found) < expected_total:
            raise AssertionError("Airflow DagRun inventory pagination made no progress")
        for run in page:
            if not isinstance(run, dict):
                raise AssertionError("Airflow DagRun inventory entries must be objects")
            run_id = run.get("dag_run_id")
            if not isinstance(run_id, str) or not run_id:
                raise AssertionError("Airflow DagRun inventory entry has invalid dag_run_id")
            if run_id in run_ids:
                raise AssertionError(f"Airflow DagRun inventory contains duplicate run ID {run_id}")
            run_ids.add(run_id)
            found.append(run)
        if len(found) > expected_total:
            raise AssertionError("Airflow DagRun inventory contains more entries than total_entries")
        if len(found) == expected_total:
            return found
        offset += len(page)
    raise AssertionError(
        f"Airflow DagRun inventory exceeded safe request count {_MAX_RUN_REQUESTS}"
    )


def _acceptance_timestamp(run: dict) -> datetime | None:
    for key in ("start_date", "logical_date", "queued_at"):
        value = run.get(key)
        if isinstance(value, str) and value:
            return _run_timestamp(value)
    return None


def _validate_owned_runs(
    runs: list[dict],
    window_start: str,
    expected: set[str],
    *,
    require_terminal: bool = False,
    baseline: set[str] | None = None,
) -> dict:
    if baseline is None:
        start = _run_timestamp(window_start)
        in_window = {
            run["dag_run_id"]: run
            for run in runs
            if (timestamp := _acceptance_timestamp(run)) is not None and timestamp >= start
        }
    else:
        in_window = {run["dag_run_id"]: run for run in runs if run["dag_run_id"] not in baseline}
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


def _assert_owned_runs(
    api,
    window_start: str,
    expected: set[str],
    *,
    require_terminal: bool = False,
    baseline: set[str] | None = None,
) -> dict:
    return _validate_owned_runs(
        _list_runs(api),
        window_start,
        expected,
        require_terminal=require_terminal,
        baseline=baseline,
    )


def _resolve_or_publish_tiny(runner=None) -> dict:
    execute = runner or _run
    resolve = ("uv", "run", "python", "scripts/resolve_dataset.py", "tpch", "--scale", "tiny")
    execute(*resolve)
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


def _execute_dag_test(
    logical_date: datetime, *, runner=None, secrets: tuple[str, ...] = (),
) -> subprocess.CompletedProcess[str]:
    execute = runner or _run
    project = _env("PROJECT_NAME", "data-eng-lab")
    command = (
        "docker",
        "exec",
        f"{project}-airflow-scheduler",
        "bash",
        "-o",
        "pipefail",
        "-c",
        'airflow dags test "$@" 2>&1 | tail -n 200',
        "airflow-dags-test",
        DAG_ID,
        logical_date.replace(microsecond=0).isoformat(),
        "--use-executor",
        "--conf",
        '{"dataset_scale":"tiny"}',
    )
    try:
        return execute(*command, timeout=900)
    except (subprocess.CalledProcessError, subprocess.TimeoutExpired) as error:
        output = "\n".join(
            str(value) for value in (
                getattr(error, "stdout", None),
                getattr(error, "output", None),
                getattr(error, "stderr", None),
            ) if value
        )
        for secret in secrets:
            if secret:
                output = output.replace(secret, "<redacted>")
        if len(output) > 4400:
            output = output[:500] + "\n...<truncated>...\n" + output[-3800:]
        raise AssertionError(f"paused Airflow test execution failed:\n{output}") from error


def _execute_paused_test_run(
    *,
    api,
    runner,
    drivers,
    terminal,
    window_start: str,
    owned: set[str],
    logical_date: datetime,
    baseline: set[str] | None = None,
) -> tuple[dict, str]:
    before_runs = _list_runs(api)
    _validate_owned_runs(
        before_runs, window_start, owned, require_terminal=True, baseline=baseline,
    )
    before_run_ids = {run["dag_run_id"] for run in before_runs}
    before_drivers = drivers()

    _execute_dag_test(
        logical_date,
        runner=runner,
        secrets=(
            _env("AIRFLOW_ADMIN_PASSWORD"),
            _env("MINIO_ROOT_PASSWORD"),
            _env("MINIO_ICEBERG_SECRET_KEY"),
        ),
    )

    after_runs = _list_runs(api)
    new_run_ids = {run["dag_run_id"] for run in after_runs} - before_run_ids
    if len(new_run_ids) != 1:
        raise AssertionError(f"expected exactly one new API-visible run, got {sorted(new_run_ids)}")
    run_id = new_run_ids.pop()
    expected = owned | {run_id}
    in_window = _validate_owned_runs(
        after_runs, window_start, expected, require_terminal=True, baseline=baseline,
    )

    new_drivers = drivers() - before_drivers
    assert len(new_drivers) == 1, f"expected exactly one new Spark driver, got {sorted(new_drivers)}"
    driver_id = new_drivers.pop()
    status = terminal(driver_id)
    assert status["driverState"] == "FINISHED" and status["success"] is True
    return in_window[run_id], driver_id


@pytest.mark.skipif(os.environ.get("RUN_INFRA") != "1", reason="needs the canonical live Atlas stack")
def test_tpch_star_schema_live_acceptance():
    """Exercise build/publication, resolver, serialized Airflow/Spark, Iceberg/Trino, and safe cleanup."""
    with _owned_stack():
        _wait_for_dag()
        with _paused_dag():
            window_start = datetime.now(timezone.utc).isoformat()
            first_logical_date = datetime.now(timezone.utc).replace(microsecond=0)
            baseline = {run["dag_run_id"] for run in _list_runs(_airflow)}
            _assert_owned_runs(_airflow, window_start, set(), baseline=baseline)

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

            _assert_owned_runs(_airflow, window_start, set(), baseline=baseline)
            first_run, first_driver = _execute_paused_test_run(
                api=_airflow,
                runner=_run,
                drivers=_driver_ids,
                terminal=_spark_terminal,
                window_start=window_start,
                owned=set(),
                logical_date=first_logical_date,
                baseline=baseline,
            )
            first_run_id = first_run["dag_run_id"]
            _assert_owned_runs(_airflow, window_start, {first_run_id}, baseline=baseline)
            first = {
                "dim": _snapshot_table("lakehouse.gold.dim_customer"),
                "fact": _snapshot_table("lakehouse.gold.fct_orders"),
                "dim_properties": _properties("dim_customer"),
                "fact_properties": _properties("fct_orders"),
            }
            _assert_owned_runs(_airflow, window_start, {first_run_id}, baseline=baseline)
            second_logical_date = max(
                datetime.now(timezone.utc).replace(microsecond=0),
                first_logical_date + timedelta(seconds=1),
            )
            second_run, second_driver = _execute_paused_test_run(
                api=_airflow,
                runner=_run,
                drivers=_driver_ids,
                terminal=_spark_terminal,
                window_start=window_start,
                owned={first_run_id},
                logical_date=second_logical_date,
                baseline=baseline,
            )
            second_run_id = second_run["dag_run_id"]
            _assert_owned_runs(
                _airflow, window_start, {first_run_id, second_run_id}, baseline=baseline,
            )
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
                _airflow,
                window_start,
                {first_run_id, second_run_id},
                require_terminal=True,
                baseline=baseline,
            )
