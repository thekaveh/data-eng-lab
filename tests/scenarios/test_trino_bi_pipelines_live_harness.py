from __future__ import annotations

import importlib.util
import json
import subprocess
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import parse_qs, urlparse

import pytest

ROOT = Path(__file__).resolve().parents[2]
SPEC = importlib.util.spec_from_file_location(
    "trino_bi_pipelines_live_harness",
    ROOT / "tests/scenarios/test_trino_bi_pipelines_live.py",
)
live = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(live)


def _paged_api(inventory):
    calls = []

    def api(_method, path, _body=None):
        parsed = urlparse(path)
        dag_id = parsed.path.split("/")[2]
        query = parse_qs(parsed.query)
        offset = int(query.get("offset", [0])[0])
        limit = int(query.get("limit", [100])[0])
        runs = inventory[dag_id]
        calls.append((dag_id, offset, limit))
        return {"dag_runs": runs[offset : offset + limit], "total_entries": len(runs)}

    return api, calls


def test_owned_stack_rejects_running_or_stopped_project_before_mutation():
    commands = []
    with pytest.raises(RuntimeError, match="already exists"):
        with live._owned_stack(runner=lambda *command, **_kwargs: commands.append(command), probe=lambda: ("x",)):
            raise AssertionError("must not enter")
    assert commands == []


def test_owned_stack_cleans_owned_failure_and_preserves_primary():
    commands = []

    def runner(*command, **_kwargs):
        commands.append(command)
        if command == ("./scripts/stop-all.sh",):
            raise RuntimeError("cleanup detail")

    with pytest.raises(ValueError, match="primary") as failure:
        with live._owned_stack(runner=runner, probe=tuple):
            raise ValueError("primary")
    assert commands == [("./scripts/start-all.sh",), ("./scripts/stop-all.sh",)]
    assert any("cleanup detail" in note for note in failure.value.__notes__)


def test_owned_stack_rejects_lingering_all_state_container():
    probes = iter([(), ("stopped-container",)])
    with pytest.raises(RuntimeError, match="cleanup left"):
        with live._owned_stack(
            runner=lambda *_args, **_kwargs: subprocess.CompletedProcess([], 0, "", ""),
            probe=lambda: next(probes),
        ):
            pass


def test_both_dags_remain_paused_and_restore_initial_states():
    states = {"tpch_bi_query": False, "nyc_taxi_trino_daily": True}
    calls = []

    def api(method, path, body=None):
        dag_id = path.split("/")[2]
        calls.append((method, dag_id, body))
        if method == "GET":
            return {"is_paused": states[dag_id]}
        states[dag_id] = body["is_paused"]
        return {}

    with live._paused_dags(api=api) as initial:
        assert initial == {"tpch_bi_query": False, "nyc_taxi_trino_daily": True}
        assert states == {"tpch_bi_query": True, "nyc_taxi_trino_daily": True}
        assert not any(body == {"is_paused": False} for _method, _dag, body in calls)
    assert states == {"tpch_bi_query": False, "nyc_taxi_trino_daily": True}


def test_complete_run_pagination_finds_unexpected_second_page_run():
    historical = [
        {"dag_run_id": f"old-{index}", "state": "success"} for index in range(100)
    ]
    inventory = {
        "tpch_bi_query": historical + [{"dag_run_id": "unexpected", "state": "queued"}],
    }
    api, calls = _paged_api(inventory)
    with pytest.raises(AssertionError, match="unexpected"):
        live._assert_owned_runs(api, "tpch_bi_query", set(h["dag_run_id"] for h in historical), set())
    assert calls == [("tpch_bi_query", 0, 100), ("tpch_bi_query", 100, 100)]


def test_complete_valid_multipage_inventory():
    runs = [{"dag_run_id": f"run-{index}", "state": "success"} for index in range(205)]
    api, calls = _paged_api({"tpch_bi_query": runs})
    assert live._list_runs(api, "tpch_bi_query") == runs
    assert calls == [
        ("tpch_bi_query", 0, 100),
        ("tpch_bi_query", 100, 100),
        ("tpch_bi_query", 200, 100),
    ]


@pytest.mark.parametrize(
    ("pages", "message"),
    [
        ([{"dag_runs": []}], "total_entries"),
        ([{"dag_runs": "bad", "total_entries": 1}], "dag_runs"),
        ([{"dag_runs": [], "total_entries": 1}], "progress"),
        ([{"dag_runs": [{"dag_run_id": "same"}], "total_entries": 2}] * 2, "duplicate"),
        ([{"dag_runs": [], "total_entries": 1001}], "safe maximum"),
    ],
)
def test_run_inventory_fails_closed_on_malformed_nonprogress_duplicate_or_bound(pages, message):
    documents = iter(pages)
    with pytest.raises(AssertionError, match=message):
        live._list_runs(lambda *_args, **_kwargs: next(documents), "tpch_bi_query")


def test_resolver_verifies_existing_pointer_without_refresh_and_requires_identity():
    calls = []
    document = {"dataset": "tpch", "scale": "tiny", "objects": [{"size_bytes": 1}]}

    def runner(*command, **_kwargs):
        calls.append(command)
        return subprocess.CompletedProcess(command, 0, json.dumps(document), "")

    assert live._verify_existing_tiny("tpch", runner=runner) == document
    assert calls == [
        ("uv", "run", "python", "scripts/resolve_dataset.py", "tpch", "--scale", "tiny"),
        (
            "uv",
            "run",
            "python",
            "scripts/download_datasets.py",
            "--scale",
            "tiny",
            "--only",
            "tpch",
            "--verify-only",
        ),
        ("uv", "run", "python", "scripts/resolve_dataset.py", "tpch", "--scale", "tiny"),
    ]
    assert not any("--refresh" in command for command in calls)


def test_generic_resolver_failure_never_verifies_refreshes_or_retries():
    calls = []

    def runner(*command, **_kwargs):
        calls.append(command)
        raise subprocess.CalledProcessError(1, command, output="ambiguous failure")

    with pytest.raises(subprocess.CalledProcessError):
        live._verify_existing_tiny("nyc_taxi", runner=runner)
    assert calls == [
        ("uv", "run", "python", "scripts/resolve_dataset.py", "nyc_taxi", "--scale", "tiny")
    ]


def test_resolver_rejects_identity_change_across_verify_only():
    before = {"dataset": "tpch", "scale": "tiny", "publication_id": "before", "objects": []}
    after = {**before, "publication_id": "after"}
    responses = iter([json.dumps(before), "verified", json.dumps(after)])

    def runner(*command, **_kwargs):
        return subprocess.CompletedProcess(command, 0, next(responses), "")

    with pytest.raises(AssertionError, match="changed"):
        live._verify_existing_tiny("tpch", runner=runner)


class _Body:
    def __init__(self, payload: bytes):
        self.payload = payload
        self.closed = False
        self.close_count = 0
        self.read_sizes = []

    def read(self, size=-1):
        self.read_sizes.append(size)
        return self.payload if size < 0 else self.payload[:size]

    def close(self):
        self.closed = True
        self.close_count += 1


class _S3Error(Exception):
    def __init__(self, code: str):
        self.response = {"Error": {"Code": code}}


class _S3:
    def __init__(self, response=None, error=None):
        self.response = response
        self.error = error
        self.calls = []

    def get_object(self, **kwargs):
        self.calls.append(kwargs)
        if self.error:
            raise self.error
        return self.response


def test_nyc_pointer_negative_control_distinguishes_exact_absence_without_resolver_calls():
    client = _S3(error=_S3Error("NoSuchKey"))
    assert live._optional_pointer_snapshot(client, "nyc_taxi") == ("absent",)
    assert client.calls == [
        {"Bucket": "landing", "Key": "_data-eng-locks/current/nyc_taxi.json"}
    ]


def test_nyc_pointer_negative_control_captures_exact_present_body_and_etag():
    body = _Body(b'{"dataset":"nyc_taxi"}')
    client = _S3(response={"Body": body, "ETag": '"etag"'})
    assert live._optional_pointer_snapshot(client, "nyc_taxi") == (
        "present",
        b'{"dataset":"nyc_taxi"}',
        '"etag"',
    )
    assert body.closed is True


def test_mandatory_pointer_capture_is_bounded_validated_and_closed_once():
    body = _Body(b'{"dataset":"tpch"}')
    client = _S3(response={"Body": body, "ETag": '"etag"'})
    assert live._pointer_snapshot(client, "tpch") == (b'{"dataset":"tpch"}', '"etag"')
    assert body.read_sizes == [live._MAX_POINTER_BYTES + 1]
    assert body.close_count == 1

    overlong = _Body(b"{" + b"x" * live._MAX_POINTER_BYTES + b"}")
    with pytest.raises(AssertionError, match="malformed"):
        live._pointer_snapshot(_S3(response={"Body": overlong, "ETag": '"etag"'}), "tpch")
    assert overlong.close_count == 1


@pytest.mark.parametrize(
    ("client", "message"),
    [
        (_S3(error=_S3Error("AccessDenied")), "read failed"),
        (_S3(error=TimeoutError("network")), "read failed"),
        (_S3(response={}), "malformed"),
        (_S3(response={"Body": _Body(b""), "ETag": '"etag"'}), "malformed"),
        (_S3(response={"Body": _Body(b"not-json"), "ETag": '"etag"'}), "malformed"),
        (_S3(response={"Body": _Body(b"[]"), "ETag": '"etag"'}), "malformed"),
        (_S3(response={"Body": _Body(b"{}"), "ETag": ""}), "malformed"),
    ],
)
def test_nyc_pointer_negative_control_fails_closed_on_ambiguous_or_malformed_read(client, message):
    with pytest.raises(AssertionError, match=message):
        live._optional_pointer_snapshot(client, "nyc_taxi")


def test_nyc_acceptance_source_never_resolves_verifies_or_publishes_raw_dataset():
    source = (ROOT / "tests/scenarios/test_trino_bi_pipelines_live.py").read_text(encoding="utf-8")
    acceptance = source.split("def test_trino_bi_pipelines_live_acceptance", 1)[1]
    assert '_verify_existing_tiny("nyc_taxi")' not in acceptance
    assert "download_datasets.py" not in acceptance
    assert "resolve_dataset.py" not in acceptance


def test_owned_run_validation_requires_exact_two_terminal_runs_and_no_active_foreign():
    baseline = {"historical"}
    inventory = {
        "tpch_bi_query": [
            {"dag_run_id": "historical", "state": "failed"},
            {"dag_run_id": "first", "state": "success"},
            {"dag_run_id": "second", "state": "success"},
        ]
    }
    api, _calls = _paged_api(inventory)
    found = live._assert_owned_runs(
        api, "tpch_bi_query", baseline, {"first", "second"}, require_terminal=True
    )
    assert set(found) == {"first", "second"}
    inventory["tpch_bi_query"].append({"dag_run_id": "foreign", "state": "running"})
    with pytest.raises(AssertionError, match="foreign"):
        live._assert_owned_runs(api, "tpch_bi_query", baseline, {"first", "second"})


def test_xcom_decoder_requires_bounded_canonical_dictionary():
    artifact = {"artifact_version": 1, "pipeline": "tpch_bi_query", "rows": [], "result_sha256": "a" * 64}
    encoded = json.dumps(artifact, sort_keys=True, separators=(",", ":"))
    assert live._decode_xcom({"value": encoded}) == artifact
    with pytest.raises(AssertionError, match="canonical"):
        live._decode_xcom({"value": json.dumps(artifact)})
    with pytest.raises(AssertionError, match="byte bound"):
        live._decode_xcom({"value": "x" * (live._MAX_XCOM_BYTES + 1)})


def test_artifact_inventory_is_exact_and_has_no_spark_driver_delta():
    first = {
        "columns": [{"name": "value", "type": "varchar"}],
        "query_ids": ["q1", "q2"],
        "result_sha256": "a" * 64,
        "rows": [["x"]],
    }
    second = {**first, "query_ids": ["q3", "q4"]}
    assert live._artifact_result_bytes(first) == live._artifact_result_bytes(second)
    with pytest.raises(AssertionError, match="Spark driver delta"):
        live._assert_no_spark_driver_delta({"driver-1"}, {"driver-1", "driver-2"})


def test_runtime_query_inventory_requires_exact_owned_finished_queries_without_errors():
    rows = [
        ["q1", "FINISHED", None, None],
        ["q2", "FINISHED", None, None],
    ]
    assert live._assert_runtime_queries(rows, baseline=set(), expected={"q1", "q2"}) == {
        "q1": {"state": "FINISHED", "error_type": None, "error_code": None},
        "q2": {"state": "FINISHED", "error_type": None, "error_code": None},
    }
    with pytest.raises(AssertionError, match="unexpected"):
        live._assert_runtime_queries(
            rows + [["foreign", "RUNNING", None, None]],
            baseline=set(),
            expected={"q1", "q2"},
        )
    with pytest.raises(AssertionError, match="terminal FINISHED"):
        live._assert_runtime_queries(
            [["q1", "FAILED", "USER_ERROR", "1"]], baseline=set(), expected={"q1"}
        )
    with pytest.raises(AssertionError, match="duplicate"):
        live._assert_runtime_queries([rows[0], rows[0]], baseline=set(), expected={"q1"})


def test_pinned_runtime_probe_requires_http_stack_and_forbids_trino_clients():
    expected = {
        "http_hook": True,
        "requests": True,
        "trino_client": False,
        "trino_provider": False,
    }

    def valid_runner(*_command, **_kwargs):
        return subprocess.CompletedProcess([], 0, json.dumps(expected), "")

    assert live._runtime_import_probe(runner=valid_runner) == expected

    def invalid_runner(*_command, **_kwargs):
        return subprocess.CompletedProcess([], 0, json.dumps({**expected, "trino_client": True}), "")

    with pytest.raises(AssertionError, match="runtime import contract"):
        live._runtime_import_probe(runner=invalid_runner)


def test_task_log_scan_rejects_endpoint_headers_sql_secrets_and_tracebacks(monkeypatch):
    monkeypatch.setenv("AIRFLOW_ADMIN_PASSWORD", "airflow-secret")
    clean = "Task exited with return code 0\nMarking task as SUCCESS"
    live._assert_task_log_clean(clean)
    for leaked in (
        "http://trino:8080/v1/statement",
        "X-Trino-User",
        "data_eng_lab_bi",
        "SELECT source_table FROM lakehouse.gold",
        "airflow-secret",
        "Traceback (most recent call last):",
    ):
        with pytest.raises(AssertionError, match="sensitive|failure"):
            live._assert_task_log_clean(clean + "\n" + leaked)


def test_task_log_reader_is_bounded_and_uses_exact_owned_task_path():
    calls = []

    def runner(*command, **_kwargs):
        calls.append((command, _kwargs))
        return subprocess.CompletedProcess(command, 0, "clean log", "")

    assert live._task_log("tpch_bi_query", "owned-run", runner=runner) == "clean log"
    command, kwargs = calls[0]
    assert command[-3:] == ("tpch_bi_query", "owned-run", live.TASK_ID)
    assert kwargs["timeout"] <= 30


def test_task_log_bounded_reader_streams_multiple_attempts_without_unbounded_read():
    with tempfile.TemporaryDirectory() as temp:
        paths = []
        total = 0
        for index, size in enumerate((17, 31, 43)):
            path = Path(temp) / f"attempt={index + 1}.log"
            path.write_bytes(bytes([65 + index]) * size)
            paths.append(path)
            total += size
        assert live._read_bounded_logs(paths, limit=total) == b"A" * 17 + b"B" * 31 + b"C" * 43


def test_task_log_bounded_reader_rejects_single_huge_and_too_many_attempts_early():
    class GuardedBody:
        def __init__(self, chunks):
            self.chunks = list(chunks)
            self.requests = []
            self.closed = False

        def read(self, size=-1):
            assert size >= 0, "unbounded read is forbidden"
            self.requests.append(size)
            return self.chunks.pop(0) if self.chunks else b""

        def close(self):
            self.closed = True

    huge = GuardedBody([b"x" * 65, b""])
    with pytest.raises(AssertionError, match="byte bound"):
        live._read_bounded_log_streams([huge], limit=64)
    assert huge.requests and max(huge.requests) <= 65
    assert huge.closed is True

    streams = [GuardedBody([b"x", b""]) for _ in range(live._MAX_TASK_LOG_ATTEMPTS + 1)]
    with pytest.raises(AssertionError, match="attempt bound"):
        live._read_bounded_log_streams(streams, limit=64)
    assert all(not stream.requests for stream in streams)


def test_fixed_live_source_contains_no_refresh_or_arbitrary_sql_inputs():
    source = (ROOT / "tests/scenarios/test_trino_bi_pipelines_live.py").read_text(encoding="utf-8")
    assert "--refresh" not in source
    assert "dag_run.conf" not in source
    assert "input(" not in source
    assert "RUN_INFRA" in source


def test_dag_test_failure_is_bounded_and_redacts_runtime_secrets(monkeypatch):
    monkeypatch.setenv("AIRFLOW_ADMIN_PASSWORD", "airflow-secret")
    monkeypatch.setenv("MINIO_ICEBERG_SECRET_KEY", "minio-secret")

    def runner(*command, **_kwargs):
        raise subprocess.CalledProcessError(
            1,
            command,
            output="airflow-secret " + "x" * 6000,
            stderr="minio-secret task failed",
        )

    with pytest.raises(AssertionError, match="paused Airflow test execution failed") as failure:
        live._execute_dag_test(
            "tpch_bi_query", datetime(2026, 8, 13, tzinfo=timezone.utc), runner=runner
        )
    text = str(failure.value)
    assert "airflow-secret" not in text and "minio-secret" not in text
    assert "<redacted>" in text and "<truncated>" in text
    assert len(text) < 5000
