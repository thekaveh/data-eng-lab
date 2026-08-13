from __future__ import annotations

import hashlib
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
INFRA_ENV = ROOT / "infra/.env"
DAG_IDS = ("tpch_bi_query", "nyc_taxi_trino_daily")
TASK_ID = "run_bounded_bi_query"
PROVENANCE_KEYS = (
    "data_eng_lab.dataset",
    "data_eng_lab.dataset.scale",
    "data_eng_lab.dataset.plan_id",
    "data_eng_lab.dataset.publication_id",
    "data_eng_lab.dataset.manifest_sha256",
)
_RUN_PAGE_LIMIT = 100
_MAX_RUN_INVENTORY = 1000
_MAX_RUN_REQUESTS = 10
_MAX_XCOM_BYTES = 256 * 1024
_MAX_POINTER_BYTES = 1024 * 1024
_AIRFLOW_TOKEN = ""

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
        raise RuntimeError(f"project stack already exists in some state; exclusive acceptance refused: {existing}")
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
            remaining = tuple(inspect())
            if remaining:
                raise RuntimeError(f"volume-preserving cleanup left project containers: {remaining}")
        except BaseException as cleanup_error:
            if primary is None:
                raise
            primary.add_note(f"volume-preserving cleanup also failed: {cleanup_error}")


def _airflow(method: str, path: str, body: dict | None = None) -> dict:
    global _AIRFLOW_TOKEN
    base = f"http://127.0.0.1:{_env('AIRFLOW_PORT', '20070')}"
    if not _AIRFLOW_TOKEN:
        request = urllib.request.Request(
            base + "/auth/token",
            data=json.dumps(
                {
                    "username": _env("AIRFLOW_ADMIN_USER", "admin"),
                    "password": _env("AIRFLOW_ADMIN_PASSWORD"),
                }
            ).encode(),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(request, timeout=30) as response:
            _AIRFLOW_TOKEN = json.load(response)["access_token"]
    request = urllib.request.Request(
        base + "/api/v2" + path,
        data=None if body is None else json.dumps(body).encode(),
        headers={"Authorization": "Bearer " + _AIRFLOW_TOKEN, "Content-Type": "application/json"},
        method=method,
    )
    with urllib.request.urlopen(request, timeout=30) as response:
        return json.load(response)


def _wait_for_dags(timeout: int = 300) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            if all(_airflow("GET", f"/dags/{dag_id}") for dag_id in DAG_IDS):
                return
        except (OSError, urllib.error.HTTPError):
            time.sleep(5)
    raise TimeoutError("Airflow did not load both Trino BI DAGs")


@contextmanager
def _paused_dags(api=None):
    request = api or _airflow
    initial = {dag_id: bool(request("GET", f"/dags/{dag_id}")["is_paused"]) for dag_id in DAG_IDS}
    primary = None
    try:
        for dag_id in DAG_IDS:
            request("PATCH", f"/dags/{dag_id}", {"is_paused": True})
        yield initial
    except BaseException as error:
        primary = error
        raise
    finally:
        try:
            for dag_id in reversed(DAG_IDS):
                request("PATCH", f"/dags/{dag_id}", {"is_paused": initial[dag_id]})
        except BaseException as cleanup_error:
            if primary is None:
                raise
            primary.add_note(f"DAG pause-state restoration also failed: {cleanup_error}")


def _list_runs(api, dag_id: str) -> list[dict]:
    found: list[dict] = []
    ids: set[str] = set()
    expected_total = None
    offset = 0
    for _ in range(_MAX_RUN_REQUESTS):
        document = api(
            "GET",
            f"/dags/{dag_id}/dagRuns?limit={_RUN_PAGE_LIMIT}&offset={offset}&order_by=start_date",
        )
        if not isinstance(document, dict):
            raise AssertionError("Airflow DagRun inventory must be an object")
        total = document.get("total_entries")
        if isinstance(total, bool) or not isinstance(total, int) or total < 0:
            raise AssertionError("Airflow DagRun inventory total_entries must be a nonnegative integer")
        if total > _MAX_RUN_INVENTORY:
            raise AssertionError(f"Airflow DagRun inventory exceeds safe maximum {_MAX_RUN_INVENTORY}")
        if expected_total is None:
            expected_total = total
        elif total != expected_total:
            raise AssertionError("Airflow DagRun inventory total changed during pagination")
        page = document.get("dag_runs")
        if not isinstance(page, list):
            raise AssertionError("Airflow DagRun inventory dag_runs must be a list")
        if len(page) > _RUN_PAGE_LIMIT:
            raise AssertionError("Airflow DagRun page exceeds requested bound")
        if not page and len(found) < expected_total:
            raise AssertionError("Airflow DagRun pagination made no progress")
        for item in page:
            run_id = item.get("dag_run_id") if isinstance(item, dict) else None
            if not isinstance(run_id, str) or not run_id:
                raise AssertionError("Airflow DagRun inventory has invalid run ID")
            if run_id in ids:
                raise AssertionError(f"Airflow DagRun inventory has duplicate run ID {run_id}")
            ids.add(run_id)
            found.append(item)
        if len(found) > expected_total:
            raise AssertionError("Airflow DagRun inventory exceeds total_entries")
        if len(found) == expected_total:
            return found
        offset += len(page)
    raise AssertionError(f"Airflow DagRun pagination exceeded safe request count {_MAX_RUN_REQUESTS}")


def _assert_owned_runs(
    api,
    dag_id: str,
    baseline: set[str],
    expected: set[str],
    *,
    require_terminal: bool = False,
) -> dict[str, dict]:
    runs = _list_runs(api, dag_id)
    current = {run["dag_run_id"]: run for run in runs if run["dag_run_id"] not in baseline}
    if set(current) != expected:
        raise AssertionError(
            f"{dag_id} acceptance run mismatch: unexpected={sorted(set(current) - expected)}, "
            f"missing={sorted(expected - set(current))}"
        )
    active_foreign = {
        run["dag_run_id"]
        for run in runs
        if str(run.get("state", "")).lower() in {"queued", "running"}
        and run["dag_run_id"] not in expected
    }
    if active_foreign:
        raise AssertionError(f"{dag_id} has unexpected active runs: {sorted(active_foreign)}")
    if require_terminal:
        bad = {run_id: current[run_id].get("state") for run_id in expected if current[run_id].get("state") != "success"}
        if bad:
            raise AssertionError(f"{dag_id} owned runs are not terminal successes: {bad}")
    return current


def _verify_existing_tiny(dataset: str, runner=None) -> dict:
    execute = runner or _run
    resolve = ("uv", "run", "python", "scripts/resolve_dataset.py", dataset, "--scale", "tiny")
    before = json.loads(execute(*resolve).stdout)
    execute(
        "uv",
        "run",
        "python",
        "scripts/download_datasets.py",
        "--scale",
        "tiny",
        "--only",
        dataset,
        "--verify-only",
    )
    after = json.loads(execute(*resolve).stdout)
    if before != after:
        raise AssertionError(f"{dataset} resolver identity changed during verify-only")
    return after


def _pointer_snapshot(client, dataset: str) -> tuple[bytes, str]:
    response = client.get_object(
        Bucket=_env("MINIO_BUCKET_LANDING", "landing"),
        Key=f"_data-eng-locks/current/{dataset}.json",
    )
    body = response["Body"].read()
    etag = response.get("ETag")
    if not isinstance(body, bytes) or not body or not isinstance(etag, str) or not etag:
        raise AssertionError(f"{dataset} pointer body/ETag must be nonempty")
    return body, etag


def _optional_pointer_snapshot(client, dataset: str) -> tuple:
    """Capture exact pointer state; only an explicit NoSuchKey means absent."""
    try:
        response = client.get_object(
            Bucket=_env("MINIO_BUCKET_LANDING", "landing"),
            Key=f"_data-eng-locks/current/{dataset}.json",
        )
    except Exception as error:
        error_document = getattr(error, "response", None)
        code = (
            error_document.get("Error", {}).get("Code")
            if isinstance(error_document, dict)
            else None
        )
        if code == "NoSuchKey":
            return ("absent",)
        raise AssertionError(f"{dataset} pointer read failed closed") from error

    body_stream = response.get("Body") if isinstance(response, dict) else None
    etag = response.get("ETag") if isinstance(response, dict) else None
    if body_stream is None or not callable(getattr(body_stream, "read", None)):
        raise AssertionError(f"{dataset} pointer response is malformed")
    try:
        body = body_stream.read(_MAX_POINTER_BYTES + 1)
    except Exception as error:
        raise AssertionError(f"{dataset} pointer response is malformed") from error
    finally:
        close = getattr(body_stream, "close", None)
        if callable(close):
            close()
    if (
        not isinstance(body, bytes)
        or not body
        or len(body) > _MAX_POINTER_BYTES
        or not isinstance(etag, str)
        or not etag
    ):
        raise AssertionError(f"{dataset} pointer response is malformed")
    try:
        document = json.loads(body)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise AssertionError(f"{dataset} pointer response is malformed") from error
    if not isinstance(document, dict):
        raise AssertionError(f"{dataset} pointer response is malformed")
    return ("present", body, etag)


def _driver_ids() -> set[str]:
    with urllib.request.urlopen(
        f"http://127.0.0.1:{_env('SPARK_MASTER_UI_PORT', '20027')}/json", timeout=30
    ) as response:
        document = json.load(response)
    found: set[str] = set()

    def collect(value):
        if isinstance(value, dict):
            for child in value.values():
                collect(child)
        elif isinstance(value, list):
            for child in value:
                collect(child)
        elif isinstance(value, str) and value.startswith("driver-"):
            found.add(value)

    collect(document)
    return found


def _assert_no_spark_driver_delta(before: set[str], after: set[str]) -> None:
    if after != before:
        raise AssertionError(f"read-only Trino acceptance produced Spark driver delta: {sorted(after - before)}")


def _trino(sql: str) -> list[list]:
    result = _run(
        "docker",
        "exec",
        f"{_env('PROJECT_NAME', 'data-eng-lab')}-trino",
        "trino",
        "--output-format",
        "JSON",
        "--execute",
        sql,
    )
    return [list(row.values()) for row in (json.loads(line) for line in result.stdout.splitlines() if line.strip())]


def _table_state() -> dict:
    properties = _trino(
        "SELECT source_table, key, value FROM ("
        "SELECT 'dim_customer' source_table,key,value FROM lakehouse.gold.\"dim_customer$properties\" "
        f"WHERE key IN ({','.join(repr(key) for key in PROVENANCE_KEYS)}) UNION ALL "
        "SELECT 'fct_orders' source_table,key,value FROM lakehouse.gold.\"fct_orders$properties\" "
        f"WHERE key IN ({','.join(repr(key) for key in PROVENANCE_KEYS)})) ORDER BY 1,2"
    )
    snapshots = _trino(
        "SELECT source_table,snapshot_id FROM ("
        "SELECT 'dim_customer' source_table,snapshot_id FROM lakehouse.gold.\"dim_customer$refs\" WHERE name='main' "
        "UNION ALL SELECT 'fct_orders',snapshot_id FROM lakehouse.gold.\"fct_orders$refs\" WHERE name='main' "
        "UNION ALL SELECT 'nyc_taxi_trips',snapshot_id FROM lakehouse.bronze.\"nyc_taxi_trips$refs\" "
        "WHERE name='main') ORDER BY 1"
    )
    return {"properties": properties, "snapshots": snapshots}


def _execute_dag_test(dag_id: str, logical_date: datetime, runner=None):
    execute = runner or _run
    return execute(
        "docker",
        "exec",
        f"{_env('PROJECT_NAME', 'data-eng-lab')}-airflow-scheduler",
        "bash",
        "-o",
        "pipefail",
        "-c",
        'airflow dags test "$@" 2>&1 | tail -n 200',
        "airflow-dags-test",
        dag_id,
        logical_date.replace(microsecond=0).isoformat(),
        "--use-executor",
        timeout=900,
    )


def _decode_xcom(document: dict) -> dict:
    value = document.get("value") if isinstance(document, dict) else None
    if isinstance(value, str):
        raw = value.encode("utf-8")
        if len(raw) > _MAX_XCOM_BYTES:
            raise AssertionError("XCom exceeds byte bound")
        try:
            artifact = json.loads(value)
        except json.JSONDecodeError as error:
            raise AssertionError("XCom value is not canonical JSON") from error
        if json.dumps(artifact, sort_keys=True, separators=(",", ":")) != value:
            raise AssertionError("XCom value is not canonical JSON")
    elif isinstance(value, dict):
        artifact = value
        if len(json.dumps(value, sort_keys=True, separators=(",", ":")).encode()) > _MAX_XCOM_BYTES:
            raise AssertionError("XCom exceeds byte bound")
    else:
        raise AssertionError("XCom value must be a typed object or canonical JSON string")
    return artifact


def _xcom(dag_id: str, run_id: str) -> dict:
    return _decode_xcom(
        _airflow(
            "GET",
            f"/dags/{dag_id}/dagRuns/{run_id}/taskInstances/{TASK_ID}/xcomEntries/return_value",
        )
    )


def _artifact_result_bytes(artifact: dict) -> bytes:
    return json.dumps(
        {"columns": artifact["columns"], "rows": artifact["rows"]},
        sort_keys=True,
        separators=(",", ":"),
    ).encode()


def _assert_artifact(artifact: dict, dag_id: str) -> None:
    assert artifact["artifact_version"] == 1 and artifact["pipeline"] == dag_id
    assert artifact["row_count"] == len(artifact["rows"]) > 0
    assert hashlib.sha256(_artifact_result_bytes(artifact)).hexdigest() == artifact["result_sha256"]
    expected_queries = 7 if dag_id == "tpch_bi_query" else 5
    assert len(artifact["query_ids"]) == len(set(artifact["query_ids"])) == expected_queries
    assert all(isinstance(query_id, str) and query_id for query_id in artifact["query_ids"])


def _execute_owned_run(dag_id: str, logical_date: datetime, baseline: set[str], owned: set[str]):
    before = _list_runs(_airflow, dag_id)
    _assert_owned_runs(_airflow, dag_id, baseline, owned, require_terminal=True)
    before_ids = {run["dag_run_id"] for run in before}
    _execute_dag_test(dag_id, logical_date)
    after = _list_runs(_airflow, dag_id)
    delta = {run["dag_run_id"] for run in after} - before_ids
    if len(delta) != 1:
        raise AssertionError(f"{dag_id} expected exactly one new API-visible run, got {sorted(delta)}")
    run_id = delta.pop()
    current = _assert_owned_runs(
        _airflow, dag_id, baseline, owned | {run_id}, require_terminal=True
    )
    return current[run_id], _xcom(dag_id, run_id)


@pytest.mark.skipif(os.environ.get("RUN_INFRA") != "1", reason="needs the canonical live Atlas stack")
def test_trino_bi_pipelines_live_acceptance():
    """Prove four paused Trino-only DAG runs, durable artifacts, and zero source mutation."""
    with _owned_stack():
        _wait_for_dags()
        with _paused_dags():
            baselines = {dag_id: {run["dag_run_id"] for run in _list_runs(_airflow, dag_id)} for dag_id in DAG_IDS}
            for dag_id in DAG_IDS:
                _assert_owned_runs(_airflow, dag_id, baselines[dag_id], set())

            tpch = _verify_existing_tiny("tpch")
            import boto3  # noqa: PLC0415

            minio = boto3.client(
                "s3",
                endpoint_url=f"http://127.0.0.1:{_env('MINIO_PORT', '20020')}",
                aws_access_key_id=_env("MINIO_ICEBERG_ACCESS_KEY"),
                aws_secret_access_key=_env("MINIO_ICEBERG_SECRET_KEY"),
                region_name=_env("MINIO_REGION", "us-east-1"),
            )
            pointers_before = {
                "tpch": _pointer_snapshot(minio, "tpch"),
                "nyc_taxi": _optional_pointer_snapshot(minio, "nyc_taxi"),
            }
            tables_before = _table_state()
            drivers_before = _driver_ids()

            owned = {dag_id: set() for dag_id in DAG_IDS}
            artifacts: dict[str, list[dict]] = {dag_id: [] for dag_id in DAG_IDS}
            runs = {}
            logical = datetime.now(timezone.utc).replace(microsecond=0)
            for iteration in range(2):
                for dag_index, dag_id in enumerate(DAG_IDS):
                    run, artifact = _execute_owned_run(
                        dag_id,
                        logical + timedelta(seconds=iteration * len(DAG_IDS) + dag_index),
                        baselines[dag_id],
                        owned[dag_id],
                    )
                    owned[dag_id].add(run["dag_run_id"])
                    artifacts[dag_id].append(artifact)
                    runs[(dag_id, iteration)] = run
                    _assert_artifact(artifact, dag_id)

            for dag_id in DAG_IDS:
                assert len(owned[dag_id]) == 2
                _assert_owned_runs(_airflow, dag_id, baselines[dag_id], owned[dag_id], require_terminal=True)
                assert _artifact_result_bytes(artifacts[dag_id][0]) == _artifact_result_bytes(artifacts[dag_id][1])
                assert artifacts[dag_id][0]["result_sha256"] == artifacts[dag_id][1]["result_sha256"]
            assert artifacts["tpch_bi_query"][0]["source"]["provenance"] == {
                "data_eng_lab.dataset": "tpch",
                "data_eng_lab.dataset.scale": "tiny",
                "data_eng_lab.dataset.plan_id": tpch["plan_id"],
                "data_eng_lab.dataset.publication_id": tpch["publication_id"],
                "data_eng_lab.dataset.manifest_sha256": tpch["manifest_sha256"],
            }
            assert artifacts["nyc_taxi_trino_daily"][0]["source"]["binding"] == "iceberg_snapshot"
            assert sum(row[1] for row in artifacts["nyc_taxi_trino_daily"][0]["rows"]) == (
                artifacts["nyc_taxi_trino_daily"][0]["source"]["row_count"]
            )
            assert _table_state() == tables_before
            assert _pointer_snapshot(minio, "tpch") == pointers_before["tpch"]
            assert _optional_pointer_snapshot(minio, "nyc_taxi") == pointers_before["nyc_taxi"]
            _assert_no_spark_driver_delta(drivers_before, _driver_ids())
            print(
                json.dumps(
                    {
                        "dag_runs": {dag_id: sorted(owned[dag_id]) for dag_id in DAG_IDS},
                        "query_ids": {
                            dag_id: [artifact["query_ids"] for artifact in artifacts[dag_id]]
                            for dag_id in DAG_IDS
                        },
                        "result_sha256": {
                            dag_id: artifacts[dag_id][0]["result_sha256"] for dag_id in DAG_IDS
                        },
                        "source": {"tpch": tpch, "nyc_pointer_state": pointers_before["nyc_taxi"][0]},
                        "snapshots": tables_before["snapshots"],
                        "spark_driver_delta": [],
                    },
                    sort_keys=True,
                )
            )
