from __future__ import annotations

import ast
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
APP = ROOT / "spark-apps/nyc-taxi-data-quality"
ETL_APP = ROOT / "spark-apps/nyc-taxi-etl"
INFRA_ENV = ROOT / "infra/.env"
DAG_IDS = ("nyc_taxi_etl", "nyc_taxi_data_quality")
QUALITY_PROPERTIES = (
    "data_eng_lab.quality.binding",
    "data_eng_lab.quality.source_table",
    "data_eng_lab.quality.source_snapshot_id",
    "data_eng_lab.quality.rule_version",
    "data_eng_lab.quality.run_id",
)
_AIRFLOW_TOKEN = ""
_PAGE_LIMIT = 100
_MAX_RUNS = 1000
_MAX_REQUESTS = 10
_MAX_POINTER_BYTES = 1 << 20
_MAX_TASK_LOG_BYTES = 1 << 20
_MAX_TASK_LOG_ATTEMPTS = 4
BRONZE_SCHEMA = sorted(
    (
        "VendorID:long",
        "tpep_pickup_datetime:timestamp",
        "tpep_dropoff_datetime:timestamp",
        "passenger_count:double",
        "trip_distance:double",
        "RatecodeID:double",
        "store_and_fwd_flag:string",
        "PULocationID:long",
        "DOLocationID:long",
        "payment_type:long",
        "fare_amount:double",
        "extra:double",
        "mta_tax:double",
        "tip_amount:double",
        "tolls_amount:double",
        "improvement_surcharge:double",
        "total_amount:double",
        "congestion_surcharge:double",
        "airport_fee:double",
        "trip_date:date",
    )
)

pytestmark = pytest.mark.infra


def _env(key: str, default: str = "") -> str:
    if key in os.environ:
        return os.environ[key]
    found = ""
    if INFRA_ENV.exists():
        for line in INFRA_ENV.read_text(encoding="utf-8").splitlines():
            if line.startswith(f"{key}="):
                found = line.split("=", 1)[1]
    return found or default


def _run(*command: str, timeout: int = 1800) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        command,
        cwd=ROOT,
        check=True,
        capture_output=True,
        text=True,
        timeout=timeout,
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
        raise RuntimeError(f"exclusive acceptance refused; project containers exist: {existing}")
    primary = None
    started = False
    try:
        execute("./scripts/start-all.sh")
        started = True
        yield
    except BaseException as error:
        primary = error
        raise
    finally:
        try:
            if started:
                execute("./scripts/stop-all.sh")
            remaining = tuple(inspect())
            if remaining:
                raise RuntimeError(f"volume-preserving cleanup left project containers: {remaining}")
        except BaseException as cleanup:
            if primary is None:
                raise
            primary.add_note(f"volume-preserving stack cleanup also failed: {cleanup}")


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
        headers={
            "Authorization": "Bearer " + _AIRFLOW_TOKEN,
            "Content-Type": "application/json",
        },
        method=method,
    )
    with urllib.request.urlopen(request, timeout=30) as response:
        return json.load(response)


def _wait_for_dags(timeout: int = 300) -> None:
    deadline = time.time() + timeout
    pending = set(DAG_IDS)
    while pending and time.time() < deadline:
        for dag_id in tuple(pending):
            try:
                _airflow("GET", f"/dags/{dag_id}")
                pending.remove(dag_id)
            except (OSError, urllib.error.HTTPError):
                pass
        if pending:
            time.sleep(5)
    if pending:
        raise TimeoutError(f"Airflow did not load reviewed DAGs: {sorted(pending)}")


@contextmanager
def _paused_dags(api=None):
    request = api or _airflow
    initial = {dag_id: bool(request("GET", f"/dags/{dag_id}")["is_paused"]) for dag_id in DAG_IDS}
    mutated = []
    primary = None
    try:
        for dag_id in DAG_IDS:
            request("PATCH", f"/dags/{dag_id}", {"is_paused": True})
            mutated.append(dag_id)
        yield initial
    except BaseException as error:
        primary = error
        raise
    finally:
        for dag_id in reversed(mutated):
            try:
                request("PATCH", f"/dags/{dag_id}", {"is_paused": initial[dag_id]})
            except BaseException as cleanup:
                if primary is None:
                    raise
                primary.add_note(f"DAG pause restoration failed for {dag_id}: {cleanup}")


def _list_runs(api, dag_id: str) -> list[dict]:
    found: list[dict] = []
    ids: set[str] = set()
    expected_total = None
    offset = 0
    for _ in range(_MAX_REQUESTS):
        document = api(
            "GET",
            f"/dags/{dag_id}/dagRuns?limit={_PAGE_LIMIT}&offset={offset}&order_by=start_date",
        )
        if not isinstance(document, dict):
            raise AssertionError("DagRun inventory must be an object")
        total = document.get("total_entries")
        if isinstance(total, bool) or not isinstance(total, int) or not 0 <= total <= _MAX_RUNS:
            raise AssertionError("DagRun inventory total is invalid or over bound")
        if expected_total is None:
            expected_total = total
        elif total != expected_total:
            raise AssertionError("DagRun inventory changed during pagination")
        page = document.get("dag_runs")
        if not isinstance(page, list) or len(page) > _PAGE_LIMIT:
            raise AssertionError("DagRun inventory page is invalid")
        if not page and len(found) < total:
            raise AssertionError("DagRun pagination made no progress")
        for run in page:
            run_id = run.get("dag_run_id") if isinstance(run, dict) else None
            if not isinstance(run_id, str) or not run_id or run_id in ids:
                raise AssertionError("DagRun inventory contains invalid or duplicate run ID")
            ids.add(run_id)
            found.append(run)
        if len(found) > total:
            raise AssertionError("DagRun inventory exceeds total_entries")
        if len(found) == total:
            return found
        offset += len(page)
    raise AssertionError("DagRun inventory exceeded request bound")


def _validate_runs(runs: list[dict], baseline: set[str], expected: set[str]) -> dict[str, dict]:
    created = {run["dag_run_id"]: run for run in runs if run["dag_run_id"] not in baseline}
    if set(created) != expected:
        raise AssertionError(
            f"owned run mismatch: unexpected={sorted(set(created) - expected)}, "
            f"missing={sorted(expected - set(created))}"
        )
    active = {
        run["dag_run_id"]
        for run in runs
        if str(run.get("state", "")).lower() in {"queued", "running"}
        and run["dag_run_id"] not in expected
    }
    if active:
        raise AssertionError(f"unexpected active runs: {sorted(active)}")
    failures = {run_id: run.get("state") for run_id, run in created.items() if run.get("state") != "success"}
    if failures:
        raise AssertionError(f"owned runs did not succeed: {failures}")
    return created


def _validate_tiny_run_conf(run: dict) -> None:
    if run.get("conf") != {"dataset_scale": "tiny"}:
        raise AssertionError("owned DagRun did not preserve the exact tiny dataset scale")


def _validate_same_date_replacement(
    before: list[dict],
    after: list[dict],
    baseline: set[str],
    owned: set[str],
    replaced_run_id: str,
) -> dict:
    before_ids = {run["dag_run_id"] for run in before}
    after_by_id = {run["dag_run_id"]: run for run in after}
    after_ids = set(after_by_id)
    if replaced_run_id not in owned or replaced_run_id not in before_ids:
        raise AssertionError("same-date retry does not bind the first owned run")
    if replaced_run_id in after_ids:
        raise AssertionError("same-date retry did not remove the first owned run")
    unrelated = before_ids - {replaced_run_id}
    if after_ids & unrelated != unrelated:
        raise AssertionError("same-date retry changed unrelated run inventory")
    additions = after_ids - unrelated
    if len(additions) != 1:
        raise AssertionError("same-date retry did not create exactly one replacement")
    replacement = after_by_id[additions.pop()]
    expected_after = baseline | (owned - {replaced_run_id}) | {replacement["dag_run_id"]}
    if after_ids != expected_after:
        raise AssertionError("same-date retry changed foreign or unrelated run inventory")
    if replacement.get("state") != "success":
        raise AssertionError("same-date replacement did not succeed")
    _validate_tiny_run_conf(replacement)
    active = {
        run["dag_run_id"]
        for run in after
        if str(run.get("state", "")).lower() in {"queued", "running"}
        and run["dag_run_id"] != replacement["dag_run_id"]
    }
    if active:
        raise AssertionError(f"same-date retry left unexpected active runs: {sorted(active)}")
    return replacement


def _validate_retry_evidence(first: dict, second: dict, logical_date: str) -> tuple[str, str]:
    drivers = (first.get("driver_id"), second.get("driver_id"))
    if not all(isinstance(value, str) and value.startswith("driver-") for value in drivers):
        raise AssertionError("same-date retry driver identity is invalid")
    if len(set(drivers)) != 2:
        raise AssertionError("same-date retry did not create two distinct drivers")
    sensor_probe = (
        "Poking for tasks ['submit_nyc_taxi_etl'] in dag nyc_taxi_etl "
        f"on {logical_date}"
    )
    for evidence, driver_id in zip((first, second), drivers, strict=True):
        run = evidence.get("run")
        if (
            not isinstance(run, dict)
            or run.get("dag_run_id") != evidence.get("run_id")
            or run.get("state") != "success"
        ):
            raise AssertionError("same-date retry DagRun is not a terminal success")
        _validate_tiny_run_conf(run)
        observed_logical = str(run.get("logical_date", "")).replace("Z", "+00:00")
        if observed_logical != logical_date:
            raise AssertionError("same-date retry DagRun logical date is invalid")
        terminal = evidence.get("driver")
        if (
            not isinstance(terminal, dict)
            or terminal.get("driverState") != "FINISHED"
            or terminal.get("success") is not True
        ):
            raise AssertionError("same-date retry driver is not a terminal success")
        sensor_log = evidence.get("sensor_log")
        if (
            not isinstance(sensor_log, str)
            or sensor_probe not in sensor_log
            or "Success criteria met. Exiting." not in sensor_log
        ):
            raise AssertionError("same-date retry did not prove the exact successful ETL sensor dependency")
        spark_log = evidence.get("spark_log")
        if (
            not isinstance(spark_log, str)
            or f"submitted as {driver_id}" not in spark_log
            or f"driver {driver_id} is FINISHED" not in spark_log
        ):
            raise AssertionError("same-date retry Spark task log does not bind its terminal driver")
    return drivers


def _task_log(run_id: str, task_id: str, runner=None) -> str:
    execute = runner or _run
    script = f"""
import pathlib, sys
root = pathlib.Path('/opt/airflow/logs') / ('dag_id=nyc_taxi_data_quality')
root = root / ('run_id=' + sys.argv[1]) / ('task_id=' + sys.argv[2])
files = sorted(root.glob('attempt=*.log'))
if not files:
    raise SystemExit('owned task log is missing')
if len(files) > {_MAX_TASK_LOG_ATTEMPTS}:
    raise SystemExit('owned task log exceeds attempt bound')
remaining = {_MAX_TASK_LOG_BYTES}
for path in files:
    with path.open('rb') as stream:
        while True:
            chunk = stream.read(min(16384, remaining + 1))
            if not chunk:
                break
            if len(chunk) > remaining:
                raise SystemExit('owned task log exceeds byte bound')
            sys.stdout.buffer.write(chunk)
            remaining -= len(chunk)
""".strip()
    result = execute(
        "docker",
        "exec",
        f"{_env('PROJECT_NAME', 'data-eng-lab')}-airflow-scheduler",
        "python",
        "-c",
        script,
        run_id,
        task_id,
        timeout=30,
    )
    if not isinstance(result.stdout, str) or len(result.stdout.encode("utf-8")) > _MAX_TASK_LOG_BYTES:
        raise AssertionError("owned task log exceeds byte bound")
    return result.stdout


def _quality_run_evidence(run_id: str, driver_id: str, logical_date: datetime) -> dict:
    matches = [
        run
        for run in _list_runs(_airflow, "nyc_taxi_data_quality")
        if run.get("dag_run_id") == run_id
    ]
    if len(matches) != 1:
        raise AssertionError("owned quality DagRun is missing before evidence capture")
    return {
        "run_id": run_id,
        "run": matches[0],
        "driver_id": driver_id,
        "driver": _spark_terminal(driver_id),
        "sensor_log": _task_log(run_id, "wait_for_nyc_taxi_etl"),
        "spark_log": _task_log(run_id, "submit_nyc_taxi_data_quality"),
        "logical_date": logical_date.isoformat(),
    }


def _terminalize_failed_dag_test(api, dag_id: str, run: dict) -> None:
    run_id = str(run.get("dag_run_id", ""))
    if not (
        run_id.startswith("manual__")
        and run.get("triggered_by") == "test"
        and run.get("triggering_user_name") == "dag_test"
        and run.get("conf") == {"dataset_scale": "tiny"}
    ):
        raise AssertionError("failed DagRun is not test-owned")
    path = f"/dags/{dag_id}/dagRuns/{run_id}"
    tasks = api("GET", path + "/taskInstances").get("task_instances")
    if not isinstance(tasks, list) or not tasks:
        raise AssertionError("failed test-owned DagRun has no bounded task inventory")
    stopped_states = {
        "success",
        "failed",
        "upstream_failed",
        "skipped",
        "removed",
        "up_for_retry",
        "up_for_reschedule",
    }
    states = {str(task.get("state", "")).lower() for task in tasks}
    if not states or not states.issubset(stopped_states):
        raise AssertionError(f"failed test-owned DagRun still active: {sorted(states)}")
    updated = api("PATCH", path, {"state": "failed"})
    confirmed = api("GET", path)
    for document in (updated, confirmed):
        if (
            document.get("dag_run_id") != run_id
            or document.get("state") != "failed"
            or not document.get("end_date")
        ):
            raise AssertionError("Airflow did not terminalize the exact failed test-owned DagRun")


def _resolve_existing_tiny(runner=None) -> dict:
    execute = runner or _run
    resolve = ("uv", "run", "python", "scripts/resolve_dataset.py", "nyc_taxi", "--scale", "tiny")
    before = json.loads(execute(*resolve).stdout)
    execute(
        "uv",
        "run",
        "python",
        "scripts/download_datasets.py",
        "--scale",
        "tiny",
        "--only",
        "nyc_taxi",
        "--verify-only",
    )
    after = json.loads(execute(*resolve).stdout)
    if before != after:
        raise AssertionError("NYC Taxi resolution changed during verify-only")
    return after


def _pointer_snapshot(client) -> tuple[bytes, str]:
    response = client.get_object(
        Bucket=_env("MINIO_BUCKET_LANDING", "landing"),
        Key="_data-eng-locks/current/nyc_taxi.json",
    )
    body = response.get("Body")
    try:
        payload = body.read(_MAX_POINTER_BYTES + 1)
    finally:
        body.close()
    etag = response.get("ETag")
    if not isinstance(payload, bytes) or not payload or len(payload) > _MAX_POINTER_BYTES:
        raise AssertionError("NYC Taxi pointer body is invalid or over bound")
    if not isinstance(etag, str) or not etag:
        raise AssertionError("NYC Taxi pointer ETag is invalid")
    return payload, etag


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


def _spark_terminal(driver_id: str) -> dict:
    result = _run(
        "docker",
        "exec",
        f"{_env('PROJECT_NAME', 'data-eng-lab')}-airflow-scheduler",
        "curl",
        "-fsS",
        f"http://spark-master:6066/v1/submissions/status/{driver_id}",
    )
    document = json.loads(result.stdout)
    if document.get("driverState") != "FINISHED" or document.get("success") is not True:
        raise AssertionError(f"Spark driver {driver_id} is not a terminal success")
    return document


def _execute_dag_test(dag_id: str, logical_date: datetime, runner=None) -> None:
    execute = runner or _run
    command = (
        "docker",
        "exec",
        f"{_env('PROJECT_NAME', 'data-eng-lab')}-airflow-scheduler",
        "bash",
        "-o",
        "pipefail",
        "-c",
        'airflow dags test "$@" 2>&1 | tail -n 240',
        "airflow-dags-test",
        dag_id,
        logical_date.replace(microsecond=0).isoformat(),
        "--use-executor",
        "--conf",
        '{"dataset_scale":"tiny"}',
    )
    try:
        execute(*command, timeout=1200)
    except (subprocess.CalledProcessError, subprocess.TimeoutExpired) as error:
        output = "\n".join(
            str(value)
            for value in (getattr(error, "stdout", None), getattr(error, "stderr", None))
            if value
        )
        for secret in (
            _env("AIRFLOW_ADMIN_PASSWORD"),
            _env("MINIO_ROOT_PASSWORD"),
            _env("MINIO_ICEBERG_SECRET_KEY"),
        ):
            if secret:
                output = output.replace(secret, "<redacted>")
        output = output[-8000:]
        raise AssertionError(f"paused {dag_id} execution failed:\n{output}") from None


def _execute_owned_run(
    dag_id: str,
    logical_date: datetime,
    *,
    baseline: set[str],
    owned: set[str],
) -> tuple[str, str]:
    before_runs = _list_runs(_airflow, dag_id)
    _validate_runs(before_runs, baseline, owned)
    before_ids = {run["dag_run_id"] for run in before_runs}
    before_drivers = _driver_ids()
    try:
        _execute_dag_test(dag_id, logical_date)
    except BaseException as primary:
        try:
            after_failure = _list_runs(_airflow, dag_id)
            created = [run for run in after_failure if run["dag_run_id"] not in before_ids]
            if len(created) != 1:
                raise AssertionError(
                    f"failed {dag_id} test created {len(created)} runs; exact recovery is unsafe"
                )
            _terminalize_failed_dag_test(_airflow, dag_id, created[0])
        except BaseException as cleanup:
            primary.add_note(f"failed DagRun terminalization also failed: {cleanup}")
        raise
    after_runs = _list_runs(_airflow, dag_id)
    new_runs = {run["dag_run_id"] for run in after_runs} - before_ids
    if len(new_runs) != 1:
        raise AssertionError(f"{dag_id} created {len(new_runs)} runs instead of one")
    run_id = new_runs.pop()
    _validate_runs(after_runs, baseline, owned | {run_id})
    _validate_tiny_run_conf(next(run for run in after_runs if run["dag_run_id"] == run_id))
    new_drivers = _driver_ids() - before_drivers
    if len(new_drivers) != 1:
        raise AssertionError(f"{dag_id} created {len(new_drivers)} drivers instead of one")
    driver_id = new_drivers.pop()
    _spark_terminal(driver_id)
    return run_id, driver_id


def _execute_same_date_replacement(
    logical_date: datetime,
    *,
    baseline: set[str],
    owned: set[str],
    replaced_run_id: str,
    first_evidence: dict,
) -> tuple[str, str, dict]:
    dag_id = "nyc_taxi_data_quality"
    before_runs = _list_runs(_airflow, dag_id)
    _validate_runs(before_runs, baseline, owned)
    before_drivers = _driver_ids()
    try:
        _execute_dag_test(dag_id, logical_date)
    except BaseException as primary:
        try:
            after_failure = _list_runs(_airflow, dag_id)
            created = [
                run for run in after_failure
                if run["dag_run_id"] not in {item["dag_run_id"] for item in before_runs}
            ]
            if len(created) == 1:
                _terminalize_failed_dag_test(_airflow, dag_id, created[0])
        except BaseException as cleanup:
            primary.add_note(f"failed replacement DagRun terminalization also failed: {cleanup}")
        raise
    after_runs = _list_runs(_airflow, dag_id)
    replacement = _validate_same_date_replacement(
        before_runs,
        after_runs,
        baseline,
        owned,
        replaced_run_id,
    )
    new_drivers = _driver_ids() - before_drivers
    if len(new_drivers) != 1:
        raise AssertionError(f"same-date retry created {len(new_drivers)} drivers instead of one")
    driver_id = new_drivers.pop()
    evidence = _quality_run_evidence(replacement["dag_run_id"], driver_id, logical_date)
    _validate_retry_evidence(first_evidence, evidence, logical_date.isoformat())
    return replacement["dag_run_id"], driver_id, evidence


def _trino(sql: str) -> list[dict]:
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
    return [json.loads(line) for line in result.stdout.splitlines() if line.strip()]


def _snapshot_table(table: str) -> dict:
    spec = importlib.util.spec_from_file_location("quality_live_exec", ROOT / "tests/scenarios/live_exec.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module.snapshot_table(table)


def _assert_bronze_schema(snapshot: dict) -> None:
    if snapshot.get("schema") != BRONZE_SCHEMA:
        raise AssertionError("Bronze catalog schema does not match the exact producer/quality contract")


def _snapshot_id(namespace: str, table: str) -> str:
    rows = _trino(
        f'SELECT snapshot_id FROM lakehouse.{namespace}."{table}$snapshots" '
        "ORDER BY committed_at DESC, snapshot_id DESC LIMIT 1"
    )
    if len(rows) != 1:
        raise AssertionError(f"{table} current snapshot inventory is invalid")
    return str(rows[0]["snapshot_id"])


def _quality_properties(table: str) -> dict[str, str]:
    keys = ",".join(f"'{key}'" for key in QUALITY_PROPERTIES)
    rows = _trino(
        f'SELECT key, value FROM lakehouse.silver."{table}$properties" '
        f"WHERE key IN ({keys}) ORDER BY key"
    )
    return {str(row["key"]): str(row["value"]) for row in rows}


def _query_file(name: str) -> tuple[list[str], list[dict], str]:
    sql = (APP / "queries" / f"{name}.sql").read_text(encoding="utf-8")
    rows = _trino(sql)
    columns = list(rows[0]) if rows else []
    canonical = json.dumps(rows, sort_keys=True, separators=(",", ":")).encode()
    return columns, rows, hashlib.sha256(canonical).hexdigest()


def _publish_jar(client, app: Path, artifact: str) -> str:
    _run("mvn", "-q", "-B", "-f", str(app / "pom.xml"), "package")
    jar = app / "target" / f"{artifact}-0.1.0.jar"
    digest = hashlib.sha256(jar.read_bytes()).hexdigest()
    bucket = _env("MINIO_BUCKET_ICEBERG_JARS", "jars")
    key = f"{artifact}/0.1.0/app.jar"
    client.upload_file(str(jar), bucket, key)
    body = client.get_object(Bucket=bucket, Key=key)["Body"]
    try:
        published = body.read()
    finally:
        body.close()
    if hashlib.sha256(published).hexdigest() != digest:
        raise AssertionError(f"published {artifact} JAR does not match local artifact")
    return digest


@pytest.mark.skipif(os.environ.get("RUN_INFRA") != "1", reason="needs canonical live Atlas stack")
def test_nyc_taxi_data_quality_live_acceptance():
    with _owned_stack():
        _wait_for_dags()
        with _paused_dags() as initial_pause:
            baseline = {dag_id: {run["dag_run_id"] for run in _list_runs(_airflow, dag_id)} for dag_id in DAG_IDS}
            owned = {dag_id: set() for dag_id in DAG_IDS}
            all_drivers_before = _driver_ids()

            import boto3  # noqa: PLC0415

            minio = boto3.client(
                "s3",
                endpoint_url=f"http://127.0.0.1:{_env('MINIO_PORT', '20020')}",
                aws_access_key_id=_env("MINIO_ICEBERG_ACCESS_KEY"),
                aws_secret_access_key=_env("MINIO_ICEBERG_SECRET_KEY"),
                region_name=_env("MINIO_REGION", "us-east-1"),
            )
            etl_jar = _publish_jar(minio, ETL_APP, "nyc-taxi-etl")
            quality_jar = _publish_jar(minio, APP, "nyc-taxi-data-quality")
            pointer_before = _pointer_snapshot(minio)
            resolved = _resolve_existing_tiny()
            if resolved.get("dataset") != "nyc_taxi" or resolved.get("scale") != "tiny":
                raise AssertionError("resolver did not return the existing verified tiny NYC publication")
            if not resolved.get("objects") or any(item.get("size_bytes", 0) <= 0 for item in resolved["objects"]):
                raise AssertionError("resolver returned an empty or zero-byte NYC inventory")

            first_logical = datetime.now(timezone.utc).replace(microsecond=0)
            etl_run_1, etl_driver_1 = _execute_owned_run(
                "nyc_taxi_etl", first_logical, baseline=baseline["nyc_taxi_etl"], owned=owned["nyc_taxi_etl"]
            )
            owned["nyc_taxi_etl"].add(etl_run_1)
            _assert_bronze_schema(_snapshot_table("lakehouse.bronze.nyc_taxi_trips"))
            quality_run_1, quality_driver_1 = _execute_owned_run(
                "nyc_taxi_data_quality",
                first_logical,
                baseline=baseline["nyc_taxi_data_quality"],
                owned=owned["nyc_taxi_data_quality"],
            )
            owned["nyc_taxi_data_quality"].add(quality_run_1)
            quality_evidence_1 = _quality_run_evidence(quality_run_1, quality_driver_1, first_logical)
            first = {
                "bronze": _snapshot_table("lakehouse.bronze.nyc_taxi_trips"),
                "clean": _snapshot_table("lakehouse.silver.nyc_taxi_clean"),
                "quarantine": _snapshot_table("lakehouse.silver.nyc_taxi_quarantine"),
                "facts": _snapshot_table("lakehouse.gold.nyc_taxi_quality_facts"),
                "bronze_snapshot": _snapshot_id("bronze", "nyc_taxi_trips"),
                "clean_snapshot": _snapshot_id("silver", "nyc_taxi_clean"),
                "quarantine_snapshot": _snapshot_id("silver", "nyc_taxi_quarantine"),
                "clean_properties": _quality_properties("nyc_taxi_clean"),
                "quarantine_properties": _quality_properties("nyc_taxi_quarantine"),
            }
            retry_run, retry_driver, retry_evidence = _execute_same_date_replacement(
                first_logical,
                baseline=baseline["nyc_taxi_data_quality"],
                owned=owned["nyc_taxi_data_quality"],
                replaced_run_id=quality_run_1,
                first_evidence=quality_evidence_1,
            )
            owned["nyc_taxi_data_quality"].remove(quality_run_1)
            owned["nyc_taxi_data_quality"].add(retry_run)
            retry = {
                "clean": _snapshot_table("lakehouse.silver.nyc_taxi_clean"),
                "quarantine": _snapshot_table("lakehouse.silver.nyc_taxi_quarantine"),
                "facts": _snapshot_table("lakehouse.gold.nyc_taxi_quality_facts"),
                "clean_snapshot": _snapshot_id("silver", "nyc_taxi_clean"),
                "quarantine_snapshot": _snapshot_id("silver", "nyc_taxi_quarantine"),
            }
            if first["clean"] != retry["clean"] or first["quarantine"] != retry["quarantine"]:
                raise AssertionError("same-snapshot retry did not converge to identical Silver multisets")
            if first["facts"] != retry["facts"]:
                raise AssertionError("same-snapshot retry changed the governed fact set")
            if (
                first["clean_snapshot"] == retry["clean_snapshot"]
                or first["quarantine_snapshot"] == retry["quarantine_snapshot"]
            ):
                raise AssertionError("same-snapshot replacement did not create reviewed recovery snapshots")

            second_logical = first_logical + timedelta(seconds=1)
            etl_run_2, etl_driver_2 = _execute_owned_run(
                "nyc_taxi_etl", second_logical, baseline=baseline["nyc_taxi_etl"], owned=owned["nyc_taxi_etl"]
            )
            owned["nyc_taxi_etl"].add(etl_run_2)
            quality_run_2, quality_driver_2 = _execute_owned_run(
                "nyc_taxi_data_quality",
                second_logical,
                baseline=baseline["nyc_taxi_data_quality"],
                owned=owned["nyc_taxi_data_quality"],
            )
            owned["nyc_taxi_data_quality"].add(quality_run_2)
            second = {
                "bronze": _snapshot_table("lakehouse.bronze.nyc_taxi_trips"),
                "clean": _snapshot_table("lakehouse.silver.nyc_taxi_clean"),
                "quarantine": _snapshot_table("lakehouse.silver.nyc_taxi_quarantine"),
                "facts": _snapshot_table("lakehouse.gold.nyc_taxi_quality_facts"),
                "bronze_snapshot": _snapshot_id("bronze", "nyc_taxi_trips"),
                "clean_properties": _quality_properties("nyc_taxi_clean"),
                "quarantine_properties": _quality_properties("nyc_taxi_quarantine"),
            }
            if first["bronze_snapshot"] == second["bronze_snapshot"]:
                raise AssertionError("second matching ETL did not create a new Bronze snapshot")
            if second["clean"]["row_count"] + second["quarantine"]["row_count"] != second["bronze"]["row_count"]:
                raise AssertionError("Silver partitions do not conserve the Bronze row count")
            if (
                second["clean"]["schema"] != second["bronze"]["schema"]
                or second["quarantine"]["schema"] != second["bronze"]["schema"]
            ):
                raise AssertionError("Silver schema does not exactly match Bronze")
            if second["clean_properties"] != second["quarantine_properties"]:
                raise AssertionError("Silver provenance properties do not match")
            if second["clean_properties"].get("data_eng_lab.quality.source_snapshot_id") != second["bronze_snapshot"]:
                raise AssertionError("Silver provenance does not bind the current Bronze snapshot")
            facts = _trino(
                "SELECT quality_run_id, count(*) AS fact_count, count(DISTINCT rule_id) AS rule_count, "
                "count_if(status NOT IN ('pass','warn')) AS rejected "
                "FROM lakehouse.gold.nyc_taxi_quality_facts GROUP BY quality_run_id ORDER BY quality_run_id"
            )
            if len(facts) < 2 or any(
                int(row["fact_count"]) != 8 or int(row["rule_count"]) != 8 or int(row["rejected"]) != 0
                for row in facts[-2:]
            ):
                raise AssertionError("Gold facts do not contain two exact complete accepted runs")
            invalid_membership = _trino(
                "SELECT count(*) AS violations FROM lakehouse.silver.nyc_taxi_clean "
                "WHERE fare_amount IS NULL OR passenger_count IS NULL OR trip_distance IS NULL "
                "OR NOT is_finite(fare_amount) OR NOT is_finite(passenger_count) OR NOT is_finite(trip_distance) "
                "OR fare_amount < 0 OR passenger_count <= 0 OR trip_distance < 0"
            )
            if int(invalid_membership[0]["violations"]) != 0:
                raise AssertionError("clean Silver contains a rule-invalid row")
            query_evidence = {name: _query_file(name) for name in ("latest", "trend", "operator_attention")}
            if (
                len(query_evidence["latest"][1]) != 8
                or len(query_evidence["trend"][1]) > 90
                or len(query_evidence["operator_attention"][1]) > 100
            ):
                raise AssertionError("fixed Trino dashboard queries violate their row bounds")

            for dag_id in DAG_IDS:
                _validate_runs(_list_runs(_airflow, dag_id), baseline[dag_id], owned[dag_id])
            all_drivers = (etl_driver_1, quality_driver_1, retry_driver, etl_driver_2, quality_driver_2)
            if len(set(all_drivers)) != 5 or _driver_ids() - all_drivers_before != set(all_drivers):
                raise AssertionError("owned acceptance did not produce exactly five distinct Spark drivers")
            if _pointer_snapshot(minio) != pointer_before:
                raise AssertionError("NYC Taxi active pointer changed during acceptance")
            print(
                json.dumps(
                    {
                        "initial_pause": initial_pause,
                        "etl_jar_sha256": etl_jar,
                        "quality_jar_sha256": quality_jar,
                        "pointer_etag": pointer_before[1],
                        "resolution": {
                            key: resolved[key]
                            for key in ("plan_id", "publication_id", "manifest_sha256")
                        },
                        "runs": {
                            "etl": sorted(owned["nyc_taxi_etl"]),
                            "quality": sorted(owned["nyc_taxi_data_quality"]),
                        },
                        "drivers": all_drivers,
                        "same_date_retry": {
                            "replaced_run": quality_run_1,
                            "replacement_run": retry_run,
                            "first_driver": quality_evidence_1["driver_id"],
                            "replacement_driver": retry_evidence["driver_id"],
                            "sensor_log_proof": True,
                        },
                        "first": first,
                        "retry": retry,
                        "second": second,
                        "facts": facts,
                        "queries": {
                            name: {"columns": value[0], "rows": len(value[1]), "checksum": value[2]}
                            for name, value in query_evidence.items()
                        },
                    },
                    sort_keys=True,
                )
            )


def test_stack_ownership_rejects_preexisting_and_cleans_owned_failure():
    calls = []
    with pytest.raises(RuntimeError, match="containers exist"):
        with _owned_stack(runner=lambda *args: calls.append(args), probe=lambda: ("stopped",)):
            pass
    assert calls == []

    states = iter(((), ()))

    def runner(*args):
        calls.append(args)

    with pytest.raises(ValueError, match="primary"):
        with _owned_stack(runner=runner, probe=lambda: next(states)):
            raise ValueError("primary")
    assert calls[-2:] == [("./scripts/start-all.sh",), ("./scripts/stop-all.sh",)]


def test_pause_state_is_never_unpaused_during_body_and_is_restored():
    states = {dag_id: dag_id == "nyc_taxi_etl" for dag_id in DAG_IDS}
    patches = []

    def api(method, path, body=None):
        dag_id = path.split("/")[2]
        if method == "GET":
            return {"is_paused": states[dag_id]}
        patches.append((dag_id, body["is_paused"]))
        states[dag_id] = body["is_paused"]
        return {}

    with _paused_dags(api):
        assert all(states.values())
        assert not any(value is False for _, value in patches)
    assert states == {"nyc_taxi_etl": True, "nyc_taxi_data_quality": False}


def test_pause_setup_failure_restores_every_dag_already_mutated():
    states = {dag_id: False for dag_id in DAG_IDS}
    failed = False

    def api(method, path, body=None):
        nonlocal failed
        dag_id = path.split("/")[2]
        if method == "GET":
            return {"is_paused": states[dag_id]}
        if dag_id == "nyc_taxi_data_quality" and body["is_paused"] and not failed:
            failed = True
            raise RuntimeError("injected pause failure")
        states[dag_id] = body["is_paused"]
        return {}

    with pytest.raises(RuntimeError, match="injected pause failure"):
        with _paused_dags(api):
            pass
    assert states == {dag_id: False for dag_id in DAG_IDS}


def test_run_inventory_paginates_and_rejects_unexpected_active_run():
    runs = [
        {"dag_run_id": f"historical-{index}", "state": "success"}
        for index in range(101)
    ] + [{"dag_run_id": "unexpected", "state": "queued"}]

    def api(_method, path):
        offset = int(path.split("offset=")[1].split("&")[0])
        return {"total_entries": len(runs), "dag_runs": runs[offset : offset + _PAGE_LIMIT]}

    inventory = _list_runs(api, "nyc_taxi_data_quality")
    assert len(inventory) == 102
    baseline = {run["dag_run_id"] for run in inventory if run["dag_run_id"] != "unexpected"}
    with pytest.raises(AssertionError, match="owned run mismatch"):
        _validate_runs(inventory, baseline, set())


@pytest.mark.parametrize("dag_id", DAG_IDS)
def test_paused_dag_test_passes_exact_tiny_scale_conf(dag_id):
    calls = []
    logical_date = datetime(2026, 8, 13, 12, 34, 56, tzinfo=timezone.utc)

    def runner(*command, **kwargs):
        calls.append((command, kwargs))
        return subprocess.CompletedProcess(command, 0, stdout="", stderr="")

    _execute_dag_test(dag_id, logical_date, runner)

    command, kwargs = calls.pop()
    assert command[-4:] == (
        logical_date.isoformat(),
        "--use-executor",
        "--conf",
        '{"dataset_scale":"tiny"}',
    )
    assert json.loads(command[-1]) == {"dataset_scale": "tiny"}
    assert kwargs == {"timeout": 1200}


def test_etl_effective_scale_uses_the_exact_live_dag_run_conf(monkeypatch):
    source = (ETL_APP / "dag.py").read_text(encoding="utf-8")
    module = ast.parse(source)
    function = next(
        node
        for node in module.body
        if isinstance(node, ast.FunctionDef) and node.name == "_effective_scale"
    )
    scales = next(
        node
        for node in module.body
        if isinstance(node, ast.Assign)
        and any(isinstance(target, ast.Name) and target.id == "SCALES" for target in node.targets)
    )
    namespace = {"os": os}
    exec(compile(ast.Module(body=[scales, function], type_ignores=[]), str(ETL_APP / "dag.py"), "exec"), namespace)
    monkeypatch.setenv("DATASET_SCALE", "small")
    dag_run = type("DagRun", (), {"conf": {"dataset_scale": "tiny"}})()

    assert namespace["_effective_scale"]({"dag_run": dag_run}) == "tiny"


def test_owned_run_inventory_requires_exact_tiny_conf():
    _validate_tiny_run_conf({"conf": {"dataset_scale": "tiny"}})
    for conf in ({}, None, {"dataset_scale": "small"}, {"dataset_scale": "tiny", "extra": True}):
        with pytest.raises(AssertionError, match="exact tiny"):
            _validate_tiny_run_conf({"conf": conf})


def test_same_logical_date_retry_replaces_exactly_the_first_owned_run():
    baseline = {"historical"}
    owned = {"first"}
    before = [
        {"dag_run_id": "historical", "state": "success"},
        {"dag_run_id": "first", "state": "success", "conf": {"dataset_scale": "tiny"}},
    ]
    after = [
        {"dag_run_id": "historical", "state": "success"},
        {"dag_run_id": "replacement", "state": "success", "conf": {"dataset_scale": "tiny"}},
    ]

    replacement = _validate_same_date_replacement(before, after, baseline, owned, "first")

    assert replacement["dag_run_id"] == "replacement"


@pytest.mark.parametrize(
    ("after", "message"),
    (
        (
            [
                {"dag_run_id": "historical", "state": "success"},
                {"dag_run_id": "first", "state": "success"},
                {"dag_run_id": "replacement", "state": "success", "conf": {"dataset_scale": "tiny"}},
            ],
            "did not remove",
        ),
        (
            [
                {"dag_run_id": "replacement", "state": "success", "conf": {"dataset_scale": "tiny"}},
            ],
            "unrelated run inventory",
        ),
        (
            [
                {"dag_run_id": "historical", "state": "success"},
                {"dag_run_id": "replacement", "state": "success", "conf": {"dataset_scale": "tiny"}},
                {"dag_run_id": "unexpected", "state": "success"},
            ],
            "one replacement",
        ),
        (
            [
                {"dag_run_id": "historical", "state": "success"},
                {"dag_run_id": "replacement", "state": "failed", "conf": {"dataset_scale": "tiny"}},
            ],
            "did not succeed",
        ),
    ),
)
def test_same_logical_date_retry_rejects_unsafe_inventory_changes(after, message):
    before = [
        {"dag_run_id": "historical", "state": "success"},
        {"dag_run_id": "first", "state": "success", "conf": {"dataset_scale": "tiny"}},
    ]
    with pytest.raises(AssertionError, match=message):
        _validate_same_date_replacement(before, after, {"historical"}, {"first"}, "first")


def test_retry_evidence_requires_two_distinct_terminal_drivers_and_exact_sensor_logs():
    first = {
        "run_id": "first",
        "run": {
            "dag_run_id": "first",
            "state": "success",
            "logical_date": "2026-08-13T08:25:20Z",
            "conf": {"dataset_scale": "tiny"},
        },
        "driver_id": "driver-first",
        "driver": {"driverState": "FINISHED", "success": True},
        "sensor_log": (
            "Poking for tasks ['submit_nyc_taxi_etl'] in dag nyc_taxi_etl "
            "on 2026-08-13T08:25:20+00:00\nSuccess criteria met. Exiting."
        ),
        "spark_log": "Driver successfully submitted as driver-first\nState of driver driver-first is FINISHED",
    }
    second = {
        **first,
        "run_id": "replacement",
        "run": {**first["run"], "dag_run_id": "replacement"},
        "driver_id": "driver-second",
        "spark_log": "Driver successfully submitted as driver-second\nState of driver driver-second is FINISHED",
    }
    assert _validate_retry_evidence(first, second, "2026-08-13T08:25:20+00:00") == (
        "driver-first",
        "driver-second",
    )

    for changed in (
        {**second, "driver_id": "driver-first"},
        {**second, "driver": {"driverState": "FAILED", "success": False}},
        {**second, "sensor_log": "Success criteria met. Exiting."},
        {**second, "spark_log": "State of driver driver-second is FINISHED"},
    ):
        with pytest.raises(AssertionError):
            _validate_retry_evidence(first, changed, "2026-08-13T08:25:20+00:00")
    for changed_first in (
        {**first, "run": {**first["run"], "state": "failed"}},
        {**first, "run": {**first["run"], "conf": {"dataset_scale": "small"}}},
        {**first, "run": {**first["run"], "logical_date": "2026-08-13T08:25:21Z"}},
    ):
        with pytest.raises(AssertionError):
            _validate_retry_evidence(changed_first, second, "2026-08-13T08:25:20+00:00")


def test_retry_task_log_reader_is_attempt_and_byte_bounded():
    calls = []

    def runner(*command, **kwargs):
        calls.append((command, kwargs))
        return subprocess.CompletedProcess(command, 0, stdout="bounded", stderr="")

    assert _task_log("owned", "wait_for_nyc_taxi_etl", runner) == "bounded"
    command, kwargs = calls.pop()
    script = command[5]
    assert f"len(files) > {_MAX_TASK_LOG_ATTEMPTS}" in script
    assert f"remaining = {_MAX_TASK_LOG_BYTES}" in script
    assert "stream.read(min(16384, remaining + 1))" in script
    assert "read_bytes" not in script and "read_text" not in script
    assert kwargs == {"timeout": 30}

    def oversized(*command, **_kwargs):
        return subprocess.CompletedProcess(command, 0, stdout="x" * (_MAX_TASK_LOG_BYTES + 1), stderr="")

    with pytest.raises(AssertionError, match="byte bound"):
        _task_log("owned", "submit_nyc_taxi_data_quality", oversized)


def test_live_catalog_contract_requires_exact_ntz_producer_schema():
    _assert_bronze_schema({"schema": BRONZE_SCHEMA})
    legacy_utc = [
        "tpep_pickup_datetime:timestamptz" if value == "tpep_pickup_datetime:timestamp" else value
        for value in BRONZE_SCHEMA
    ]
    with pytest.raises(AssertionError, match="exact producer/quality contract"):
        _assert_bronze_schema({"schema": legacy_utc})


def test_failed_dag_test_terminalizes_only_its_exact_stopped_test_owned_run():
    run_id = "manual__2026-08-13T07:32:25.647385+00:00"
    run = {
        "dag_run_id": run_id,
        "state": "running",
        "triggered_by": "test",
        "triggering_user_name": "dag_test",
        "conf": {"dataset_scale": "tiny"},
    }
    calls = []
    current_state = "running"

    def api(method, path, body=None):
        nonlocal current_state
        calls.append((method, path, body))
        if path.endswith("/taskInstances"):
            return {"task_instances": [{"state": "success"}, {"state": "up_for_retry"}]}
        if method == "PATCH":
            assert body == {"state": "failed"}
            current_state = "failed"
        return {**run, "state": current_state, "end_date": "2026-08-13T07:50:55Z"}

    _terminalize_failed_dag_test(api, "nyc_taxi_data_quality", run)
    assert [call[0] for call in calls] == ["GET", "PATCH", "GET"]


def test_failed_dag_test_rejects_foreign_or_still_executing_runs_without_mutation():
    base = {
        "dag_run_id": "manual__owned",
        "state": "running",
        "triggered_by": "test",
        "triggering_user_name": "dag_test",
        "conf": {"dataset_scale": "tiny"},
    }
    patches = []

    def api_for(run, task_state):
        def api(method, path, body=None):
            if method == "PATCH":
                patches.append((path, body))
            if path.endswith("/taskInstances"):
                return {"task_instances": [{"state": task_state}]}
            return run

        return api

    foreign = {**base, "triggering_user_name": "operator"}
    with pytest.raises(AssertionError, match="not test-owned"):
        _terminalize_failed_dag_test(api_for(foreign, "failed"), "nyc_taxi_data_quality", foreign)
    with pytest.raises(AssertionError, match="still active"):
        _terminalize_failed_dag_test(api_for(base, "running"), "nyc_taxi_data_quality", base)
    assert patches == []


def test_resolver_failure_never_refreshes_or_attempts_a_second_command():
    calls = []

    def runner(*command):
        calls.append(command)
        raise subprocess.CalledProcessError(1, command)

    with pytest.raises(subprocess.CalledProcessError):
        _resolve_existing_tiny(runner)
    assert calls == [("uv", "run", "python", "scripts/resolve_dataset.py", "nyc_taxi", "--scale", "tiny")]
    assert all("--refresh" not in command for command in calls)


def test_existing_resolution_is_verified_without_refresh_and_must_remain_identical():
    document = {"dataset": "nyc_taxi", "scale": "tiny", "objects": [{"size_bytes": 1}]}
    calls = []

    def runner(*command):
        calls.append(command)
        return subprocess.CompletedProcess(command, 0, stdout=json.dumps(document), stderr="")

    assert _resolve_existing_tiny(runner) == document
    assert len(calls) == 3
    assert calls[1][-1] == "--verify-only"
    assert all("--refresh" not in command for command in calls)

    replies = iter((document, {**document, "scale": "small"}))

    def changed(*command):
        if "--verify-only" in command:
            return subprocess.CompletedProcess(command, 0, stdout="", stderr="")
        return subprocess.CompletedProcess(command, 0, stdout=json.dumps(next(replies)), stderr="")

    with pytest.raises(AssertionError, match="changed during verify-only"):
        _resolve_existing_tiny(changed)


def test_pointer_read_is_bounded_and_closes_body():
    class Body:
        closed = 0
        requested = None

        def read(self, size):
            self.requested = size
            return b"{}"

        def close(self):
            self.closed += 1

    body = Body()
    client = type("Client", (), {"get_object": lambda *_args, **_kwargs: {"Body": body, "ETag": "etag"}})()
    assert _pointer_snapshot(client) == (b"{}", "etag")
    assert body.requested == _MAX_POINTER_BYTES + 1 and body.closed == 1

    body.read = lambda _size: b"x" * (_MAX_POINTER_BYTES + 1)
    with pytest.raises(AssertionError, match="over bound"):
        _pointer_snapshot(client)
    assert body.closed == 2
