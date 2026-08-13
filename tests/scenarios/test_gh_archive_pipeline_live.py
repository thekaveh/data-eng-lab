from __future__ import annotations

import gzip
import hashlib
import importlib.util
import io
import json
import os
import subprocess
import time
import urllib.error
import urllib.request
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import NamedTuple
from urllib.parse import urlparse

import pytest

ROOT = Path(__file__).resolve().parents[2]
APP = ROOT / "spark-apps/gh-archive-pipeline"
INFRA_ENV = ROOT / "infra/.env"
DAG_ID = "gh_archive_flatten_sessionization"
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
_MAX_SOURCE_LINE_BYTES = 1 << 20
EXPECTED_LIVE_IDENTITY = {
    "jar_sha256": "5d2459e4dc9cebe96c16715db027b21333307e6cb2fae39b0c67d395535d52d1",
    "plan_id": "8ab812c3621cc3dae68989d9f24134351ea9683453133b31feaff579d0fa3e7f",
    "publication_id": "e53a481df5d54c6eabc645838fb2f2ba",
    "manifest_sha256": "998ec39bc61dca1b460e4b851d718a5347b8c7e575b96dd1e3ec62fd0b791678",
    "source_size_bytes": 59_785_519,
    "source_sha256": "2b0c0cc3b067f61c0f39d7623517904d95d22ef9d5c998953050a0b78adb6258",
    "row_count": 101_917,
    "distinct_ids": 101_916,
    "exact_duplicate_rows": 1,
    "distinct_actors": 16_331,
    "session_starts": 16_767,
    "events_checksum": "7ea82e3d0b5bad96",
    "sessions_checksum": "36136a1cab232348",
}

pytestmark = pytest.mark.infra


class SourceEvidence(NamedTuple):
    row_count: int
    distinct_ids: int
    exact_duplicate_rows: int
    distinct_actors: int


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
            remaining = tuple(inspect())
            if remaining:
                raise RuntimeError(
                    f"volume-preserving cleanup left project containers: {remaining}"
                )
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
    resolve = ("uv", "run", "python", "scripts/resolve_dataset.py", "gh_archive", "--scale", "tiny")
    before = json.loads(execute(*resolve).stdout)
    execute(
        "uv", "run", "python", "scripts/download_datasets.py",
        "--scale", "tiny", "--only", "gh_archive", "--verify-only",
    )
    after = json.loads(execute(*resolve).stdout)
    if before != after:
        raise AssertionError("GitHub Archive resolution changed during verify-only")
    return after


def _pointer_snapshot(client) -> tuple[bytes, str]:
    response = client.get_object(
        Bucket=_env("MINIO_BUCKET_LANDING", "landing"),
        Key="_data-eng-locks/current/gh_archive.json",
    )
    body = response["Body"].read()
    etag = response.get("ETag")
    if not isinstance(body, bytes) or not body:
        raise AssertionError("GitHub Archive active pointer body must be nonempty bytes")
    if not isinstance(etag, str) or not etag:
        raise AssertionError("GitHub Archive active pointer ETag must be nonempty")
    return body, etag


def _source_inventory(client, resolved: dict) -> SourceEvidence:
    matches = [
        item for item in resolved["objects"]
        if item["object_name"] == "2023-01-01-0.json.gz"
    ]
    if len(matches) != 1:
        raise AssertionError("resolved publication must contain exactly one tiny GH Archive object")
    source = matches[0]
    parsed = urlparse(source["uri"])
    if parsed.scheme != "s3" or not parsed.netloc or not parsed.path.startswith("/"):
        raise AssertionError("GH Archive source must have a canonical s3 URI")
    payload = client.get_object(Bucket=parsed.netloc, Key=parsed.path[1:])["Body"].read()
    if not isinstance(payload, bytes) or len(payload) != source["size_bytes"]:
        raise AssertionError("GH Archive bytes do not match the verified resolver size")
    if hashlib.sha256(payload).hexdigest() != source["sha256"]:
        raise AssertionError("GH Archive bytes do not match the verified resolver digest")

    def unique_object(pairs):
        result = {}
        for key, value in pairs:
            if key in result:
                raise AssertionError("GH Archive source contains a duplicate JSON key")
            result[key] = value
        return result

    def required_string(document, *path):
        value = document
        for part in path:
            if not isinstance(value, dict) or part not in value:
                raise AssertionError(f"GH Archive source is missing {'.'.join(path)}")
            value = value[part]
        if not isinstance(value, str) or not value.strip():
            raise AssertionError(f"GH Archive source has invalid {'.'.join(path)}")
        return value

    count = 0
    event_values: dict[str, tuple[str, str, str, str]] = {}
    actors: set[str] = set()
    exact_duplicate_rows = 0
    try:
        with gzip.GzipFile(fileobj=io.BytesIO(payload), mode="rb") as stream:
            while True:
                line = stream.readline(_MAX_SOURCE_LINE_BYTES + 1)
                if not line:
                    break
                if len(line) > _MAX_SOURCE_LINE_BYTES:
                    raise AssertionError("GH Archive source contains an overlong JSON record")
                try:
                    document = json.loads(
                        line.decode("utf-8"), object_pairs_hook=unique_object,
                    )
                except (UnicodeDecodeError, json.JSONDecodeError) as error:
                    raise AssertionError("GH Archive source contains invalid JSON") from error
                if not isinstance(document, dict):
                    raise AssertionError("GH Archive source record must be an object")
                event_id = required_string(document, "id")
                event_type = required_string(document, "type")
                actor_login = required_string(document, "actor", "login")
                actors.add(actor_login)
                repo_name = required_string(document, "repo", "name")
                created_at = required_string(document, "created_at")
                try:
                    parsed_timestamp = datetime.strptime(created_at, "%Y-%m-%dT%H:%M:%SZ")
                except ValueError as error:
                    raise AssertionError(
                        "GH Archive created_at must be exact whole-second UTC"
                    ) from error
                if parsed_timestamp.strftime("%Y-%m-%dT%H:%M:%SZ") != created_at:
                    raise AssertionError("GH Archive created_at must be exact whole-second UTC")
                value = (event_type, actor_login, repo_name, created_at)
                previous = event_values.get(event_id)
                if previous is not None and previous != value:
                    raise AssertionError("GH Archive source contains a conflicting event ID")
                if previous == value:
                    exact_duplicate_rows += 1
                event_values[event_id] = value
                count += 1
    except (gzip.BadGzipFile, EOFError) as error:
        raise AssertionError("GH Archive source is not a valid gzip stream") from error
    if count <= 0:
        raise AssertionError("GH Archive source must contain at least one event")
    return SourceEvidence(count, len(event_values), exact_duplicate_rows, len(actors))


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
    spec = importlib.util.spec_from_file_location("gh_archive_live_exec", ROOT / "tests/scenarios/live_exec.py")
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
        f'SELECT key, value FROM lakehouse.silver."{table}$properties" '
        f"WHERE key IN ({keys}) ORDER BY key"
    )
    return {str(key): str(value) for key, value in rows}


def _session_oracle(query=None) -> int:
    """Return the number of multiplicity mismatches against an independent session derivation."""
    execute = query or _trino
    rows = execute(
        "WITH ordered AS ("
        "SELECT id, type, actor_login, repo_name, created_at, "
        "lag(created_at) OVER (PARTITION BY actor_login ORDER BY created_at, id) "
        "AS previous_created_at "
        "FROM lakehouse.silver.gh_events), "
        "annotated AS ("
        "SELECT *, CASE WHEN previous_created_at IS NULL OR "
        "date_diff('second', previous_created_at, created_at) > 1800 THEN 1 ELSE 0 END "
        "AS new_session FROM ordered), "
        "expected_rows AS ("
        "SELECT id, type, actor_login, repo_name, created_at, previous_created_at, new_session, "
        "sum(new_session) OVER (PARTITION BY actor_login ORDER BY created_at, id "
        "ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id FROM annotated), "
        "expected_counts AS ("
        "SELECT id, type, actor_login, repo_name, created_at, previous_created_at, new_session, "
        "session_id, count(*) AS expected_multiplicity FROM expected_rows "
        "GROUP BY id, type, actor_login, repo_name, created_at, previous_created_at, "
        "new_session, session_id), "
        "actual_counts AS ("
        "SELECT id, type, actor_login, repo_name, created_at, previous_created_at, new_session, "
        "session_id, count(*) AS actual_multiplicity FROM lakehouse.silver.gh_sessions "
        "GROUP BY id, type, actor_login, repo_name, created_at, previous_created_at, "
        "new_session, session_id) "
        "SELECT count(*) FROM expected_counts e FULL OUTER JOIN actual_counts a ON "
        "e.id = a.id AND e.type = a.type AND e.actor_login = a.actor_login AND "
        "e.repo_name = a.repo_name AND e.created_at = a.created_at AND "
        "e.previous_created_at IS NOT DISTINCT FROM a.previous_created_at AND "
        "e.new_session = a.new_session AND e.session_id = a.session_id "
        "WHERE coalesce(e.expected_multiplicity, 0) <> coalesce(a.actual_multiplicity, 0)"
    )
    if len(rows) != 1 or len(rows[0]) != 1:
        raise AssertionError("session oracle must return exactly one mismatch count")
    return int(rows[0][0])


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
) -> tuple[dict, tuple[str, str]]:
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
    assert len(new_drivers) == 2, f"expected exactly two new Spark drivers, got {sorted(new_drivers)}"
    driver_ids = tuple(sorted(new_drivers))
    for driver_id in driver_ids:
        status = terminal(driver_id)
        assert status["driverState"] == "FINISHED" and status["success"] is True
    return in_window[run_id], driver_ids


def _snapshot_id(table: str) -> str:
    rows = _trino(
        f'SELECT snapshot_id FROM lakehouse.silver."{table}$snapshots" '
        "ORDER BY committed_at DESC, snapshot_id DESC LIMIT 1"
    )
    if len(rows) != 1 or len(rows[0]) != 1:
        raise AssertionError(f"{table} must expose exactly one current snapshot row")
    return str(rows[0][0])


@pytest.mark.skipif(os.environ.get("RUN_INFRA") != "1", reason="needs the canonical live Atlas stack")
def test_gh_archive_flatten_sessionization_live_acceptance():
    """Exercise build/publication, resolver, serialized Airflow/Spark, Iceberg/Trino, and safe cleanup."""
    with _owned_stack():
        _wait_for_dag()
        with _paused_dag():
            window_start = datetime.now(timezone.utc).isoformat()
            first_logical_date = datetime.now(timezone.utc).replace(microsecond=0)
            baseline = {run["dag_run_id"] for run in _list_runs(_airflow)}
            _assert_owned_runs(_airflow, window_start, set(), baseline=baseline)

            _run("mvn", "-q", "-B", "-f", str(APP / "pom.xml"), "package")
            jar = APP / "target/gh-archive-pipeline-0.1.0.jar"
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
            key = "gh-archive-pipeline/0.1.0/app.jar"
            minio.upload_file(str(jar), bucket, key)
            published = minio.get_object(Bucket=bucket, Key=key)["Body"].read()
            assert hashlib.sha256(published).hexdigest() == jar_sha256
            assert jar_sha256 == EXPECTED_LIVE_IDENTITY["jar_sha256"]

            pointer_before = _pointer_snapshot(minio)
            resolved = _resolve_or_publish_tiny()
            assert resolved["dataset"] == "gh_archive" and resolved["scale"] == "tiny"
            assert resolved["plan_id"] == EXPECTED_LIVE_IDENTITY["plan_id"]
            assert resolved["publication_id"] == EXPECTED_LIVE_IDENTITY["publication_id"]
            assert resolved["manifest_sha256"] == EXPECTED_LIVE_IDENTITY["manifest_sha256"]
            assert [
                (item["object_name"], item["schema_id"])
                for item in resolved["objects"]
            ] == [
                ("2023-01-01-0.json.gz", "gh_archive_consumed_fields"),
            ]
            assert all(item["size_bytes"] > 0 for item in resolved["objects"])
            assert resolved["objects"][0]["size_bytes"] == EXPECTED_LIVE_IDENTITY["source_size_bytes"]
            assert resolved["objects"][0]["sha256"] == EXPECTED_LIVE_IDENTITY["source_sha256"]
            source = _source_inventory(minio, resolved)
            assert source == SourceEvidence(
                EXPECTED_LIVE_IDENTITY["row_count"],
                EXPECTED_LIVE_IDENTITY["distinct_ids"],
                EXPECTED_LIVE_IDENTITY["exact_duplicate_rows"],
                EXPECTED_LIVE_IDENTITY["distinct_actors"],
            )

            _assert_owned_runs(_airflow, window_start, set(), baseline=baseline)
            first_run, first_drivers = _execute_paused_test_run(
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
                "events": _snapshot_table("lakehouse.silver.gh_events"),
                "sessions": _snapshot_table("lakehouse.silver.gh_sessions"),
                "event_properties": _properties("gh_events"),
                "session_properties": _properties("gh_sessions"),
            }
            first_snapshot_ids = {
                "events": _snapshot_id("gh_events"),
                "sessions": _snapshot_id("gh_sessions"),
            }
            _assert_owned_runs(_airflow, window_start, {first_run_id}, baseline=baseline)
            second_logical_date = max(
                datetime.now(timezone.utc).replace(microsecond=0),
                first_logical_date + timedelta(seconds=1),
            )
            second_run, second_drivers = _execute_paused_test_run(
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
                "events": _snapshot_table("lakehouse.silver.gh_events"),
                "sessions": _snapshot_table("lakehouse.silver.gh_sessions"),
                "event_properties": _properties("gh_events"),
                "session_properties": _properties("gh_sessions"),
            }
            second_snapshot_ids = {
                "events": _snapshot_id("gh_events"),
                "sessions": _snapshot_id("gh_sessions"),
            }

            assert first == second
            assert first_snapshot_ids["events"] != second_snapshot_ids["events"]
            assert first_snapshot_ids["sessions"] != second_snapshot_ids["sessions"]
            assert first["events"]["schema"] == sorted(
                ["id:string", "type:string", "actor_login:string", "repo_name:string", "created_at:timestamptz"]
            )
            assert first["sessions"]["schema"] == sorted(
                ["id:string", "type:string", "actor_login:string", "repo_name:string", "created_at:timestamptz",
                 "previous_created_at:timestamptz", "new_session:int", "session_id:long"]
            )
            assert first["events"]["row_count"] == first["sessions"]["row_count"] == source.row_count
            assert first["events"]["checksum"] == EXPECTED_LIVE_IDENTITY["events_checksum"]
            assert first["sessions"]["checksum"] == EXPECTED_LIVE_IDENTITY["sessions_checksum"]
            expected_properties = {
                "data_eng_lab.dataset": "gh_archive",
                "data_eng_lab.dataset.scale": "tiny",
                "data_eng_lab.dataset.plan_id": resolved["plan_id"],
                "data_eng_lab.dataset.publication_id": resolved["publication_id"],
                "data_eng_lab.dataset.manifest_sha256": resolved["manifest_sha256"],
            }
            assert first["event_properties"] == first["session_properties"] == expected_properties
            event_measures = _trino(
                "SELECT count(*), count(DISTINCT id), count(DISTINCT actor_login), "
                "min(created_at), max(created_at) FROM lakehouse.silver.gh_events"
            )[0]
            session_measures = _trino(
                "SELECT count(*), count(DISTINCT id), "
                "sum(CASE WHEN previous_created_at IS NULL THEN 1 ELSE 0 END), sum(new_session), "
                "sum(CASE WHEN previous_created_at IS NOT NULL AND "
                "date_diff('second', previous_created_at, created_at) > 1800 AND new_session <> 1 "
                "THEN 1 ELSE 0 END), "
                "sum(CASE WHEN previous_created_at IS NOT NULL AND "
                "date_diff('second', previous_created_at, created_at) <= 1800 AND new_session <> 0 "
                "THEN 1 ELSE 0 END) FROM lakehouse.silver.gh_sessions"
            )[0]
            assert int(event_measures[0]) == source.row_count
            assert int(event_measures[1]) == source.distinct_ids
            assert int(event_measures[0]) - int(event_measures[1]) == source.exact_duplicate_rows
            assert int(event_measures[2]) == source.distinct_actors
            assert event_measures[3] <= event_measures[4]
            assert int(session_measures[0]) == source.row_count
            assert int(session_measures[1]) == source.distinct_ids
            assert int(session_measures[2]) == EXPECTED_LIVE_IDENTITY["distinct_actors"]
            assert int(session_measures[3]) == EXPECTED_LIVE_IDENTITY["session_starts"]
            assert int(session_measures[4]) == int(session_measures[5]) == 0
            assert _session_oracle() == 0
            assert len(set(first_drivers + second_drivers)) == 4
            assert datetime.fromisoformat(second_run["start_date"]) >= datetime.fromisoformat(first_run["end_date"])
            _assert_owned_runs(
                _airflow,
                window_start,
                {first_run_id, second_run_id},
                require_terminal=True,
                baseline=baseline,
            )
            assert _pointer_snapshot(minio) == pointer_before
            print(json.dumps({
                "first_run_id": first_run_id,
                "second_run_id": second_run_id,
                "first_drivers": first_drivers,
                "second_drivers": second_drivers,
                "first_snapshot_ids": first_snapshot_ids,
                "second_snapshot_ids": second_snapshot_ids,
                "events_checksum": first["events"]["checksum"],
                "sessions_checksum": first["sessions"]["checksum"],
                "session_oracle_mismatches": 0,
                "pointer_etag": pointer_before[1],
            }, sort_keys=True))
