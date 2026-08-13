from __future__ import annotations

import ast
import json
import threading
from pathlib import Path

import pytest
import yaml

from scripts.checkpoints import notebook_lease

ROOT = Path(__file__).resolve().parents[2]
SCENARIOS = {
    "streaming_ingest-events-spark-iceberg": (
        "streaming-events-v1",
        '"events/"',
        '"stopped"',
    ),
    "streaming_windows-events-spark-iceberg": (
        "streaming-event-windows-v1",
        '"event_windows/"',
        '"stopped"',
    ),
    "cdc_streaming-online_retail-spark-iceberg": (
        "streaming-online-retail-cdc-v1",
        '"online_retail_cdc/"',
        '"stopped"',
    ),
    "streaming_ingest-gh_archive-spark-iceberg": (
        "streaming-gh-archive-file-v1",
        'f"gh_events_file/{dataset_scale}/{publication_id}/{manifest_sha256}/"',
        '"completed"',
    ),
}


def _texts(scenario: str) -> tuple[str, str, str]:
    root = ROOT / "scenarios" / scenario
    jupyter = json.loads((root / "jupyter/notebook.ipynb").read_text(encoding="utf-8"))
    python = "\n".join("".join(cell.get("source", [])) for cell in jupyter["cells"])
    zeppelin = json.loads((root / "zeppelin/notebook.zpln").read_text(encoding="utf-8"))
    scala = "\n".join(paragraph.get("text", "") for paragraph in zeppelin["paragraphs"])
    helpers = [
        paragraph["text"]
        for paragraph in zeppelin["paragraphs"]
        if "final class CheckpointLease" in paragraph.get("text", "")
    ]
    assert len(helpers) == 1
    return python, scala, helpers[0]


def test_every_streaming_notebook_wraps_start_and_wait_in_exact_lease_lifecycle():
    scala_helpers = set()
    for scenario, (checkpoint_id, python_prefix, terminal_state) in SCENARIOS.items():
        python, scala, helper = _texts(scenario)
        scala_helpers.add(helper)

        assert "from scripts.checkpoints.notebook_lease import StreamingLease" in python
        assert f'checkpoint_id="{checkpoint_id}"' in python
        assert f"prefix={python_prefix}" in python
        assert f"terminal_state={terminal_state}" in python
        assert "with StreamingLease(" in python
        assert python.index("with StreamingLease(") < python.index(".writeStream")
        assert python.index("lease.bind_query(query)") > python.index(".writeStream")
        assert python.index("query.awaitTermination()") > python.index("lease.bind_query(query)")

        assert f'checkpointId = "{checkpoint_id}"' in scala
        assert "val lease = new CheckpointLease(" in scala
        assert scala.index("val lease = new CheckpointLease(") < scala.index(".writeStream")
        assert scala.index("lease.bindQuery(query)") > scala.index(".writeStream")
        assert scala.index("query.awaitTermination()") > scala.index("lease.bindQuery(query)")
        assert scala.index("lease.close()") > scala.index("query.awaitTermination()")

    assert len(scala_helpers) == 1, "Zeppelin lease helper must be one canonical projection"


def test_gh_archive_lease_binds_the_exact_resolved_generation():
    python, scala, _helper = _texts("streaming_ingest-gh_archive-spark-iceberg")
    for name in ("dataset_scale", "publication_id", "manifest_sha256"):
        assert name in python
    assert '"generation": {' in python
    assert '"scale": dataset_scale' in python
    assert '"publication_id": publication_id' in python
    assert '"manifest_sha256": manifest_sha256' in python
    for name in ("datasetScale", "publicationId", "manifestSha256"):
        assert name in scala
    assert '"generation"' in scala


def test_runtime_mounts_only_the_fixed_internal_lease_api_contract_into_notebooks():
    compose = yaml.safe_load((ROOT / "compose/data-eng-lab.yml").read_text(encoding="utf-8"))
    for service in ("jupyterhub", "zeppelin"):
        value = compose["services"][service]
        assert value["environment"]["CHECKPOINT_RETENTION_URI"] == "http://checkpoint-retention:8080"
        assert value["environment"]["CHECKPOINT_RETENTION_LEASE_TOKEN"] == (
            "${CHECKPOINT_RETENTION_LEASE_TOKEN:?CHECKPOINT_RETENTION_LEASE_TOKEN is required}"
        )
        assert "../scripts/checkpoints:/opt/data-eng-lab/scripts/checkpoints:ro" in value["volumes"]
        assert value["environment"]["PYTHONPATH"] == "/opt/data-eng-lab"
        assert value["depends_on"]["checkpoint-retention"] == {"condition": "service_healthy"}
        assert "MINIO_ROOT_USER" not in value["environment"]
        assert "MINIO_ROOT_PASSWORD" not in value["environment"]


def test_reproducibility_harness_preserves_streaming_state_and_has_no_root_delete_path():
    live_exec = (ROOT / "tests/scenarios/live_exec.py").read_text(encoding="utf-8")
    harness = (ROOT / "tests/scenarios/test_notebook_reproducibility_live.py").read_text(encoding="utf-8")
    tree = ast.parse(harness)
    assigned = {
        target.id
        for node in tree.body
        if isinstance(node, ast.Assign)
        for target in node.targets
        if isinstance(target, ast.Name)
    }

    assert "clear_checkpoint" not in live_exec
    assert "delete_objects" not in live_exec
    assert '"MINIO_ROOT_USER"' not in live_exec
    assert '"MINIO_ROOT_PASSWORD"' not in live_exec
    assert "CHECKPOINTS" not in assigned
    assert "CHECKPOINT_RESET_POLICY" not in assigned
    assert "if scenario not in STREAMING_SCENARIOS" in harness
    assert "live_exec.drop_table(table)" in harness


def test_bounded_live_execution_replaces_wait_and_preserves_finally_cleanup():
    live_exec = (ROOT / "tests/scenarios/live_exec.py").read_text(encoding="utf-8")
    assert 'replace("query.awaitTermination()", "query.processAllAvailable()")' in live_exec
    assert 'replace("query.awaitTermination()", "query.processAllAvailable()")' in live_exec
    assert 'candidates[0]["source"].extend' not in live_exec
    assert 'candidates[0]["text"] +=' not in live_exec


class _Query:
    def __init__(self):
        self.isActive = True
        self.stop_count = 0

    def stop(self):
        self.stop_count += 1
        self.isActive = False


def test_python_streaming_lease_acquires_heartbeats_and_terminalizes_in_order():
    calls = []

    def post(action, payload):
        calls.append((action, payload))
        return {"epoch": "550e8400-e29b-41d4-a716-446655440000", "state": "accepted"}

    query = _Query()
    lease = notebook_lease.StreamingLease(
        checkpoint_id="streaming-events-v1",
        prefix="events/",
        workload="streaming_ingest-events-spark-iceberg",
        owner_id="jupyter-notebook",
        session_id="550e8400-e29b-41d4-a716-446655440001",
        terminal_state="stopped",
        terminal_evidence={"generation": {}},
        post=post,
        heartbeat_seconds=60,
    )

    with lease:
        lease.bind_query(query)

    assert query.stop_count == 1
    assert [action for action, _payload in calls] == ["acquire", "heartbeat", "terminal"]
    assert calls[0][1]["prefix"] == "events/"
    assert calls[-1][1]["evidence"] == {"generation": {}}


def test_python_streaming_lease_stops_query_and_surfaces_heartbeat_ownership_loss():
    heartbeat_ready = threading.Event()
    release = threading.Event()

    def post(action, _payload):
        if action == "acquire":
            return {"epoch": "550e8400-e29b-41d4-a716-446655440000", "state": "accepted"}
        if action == "heartbeat" and not heartbeat_ready.is_set():
            heartbeat_ready.set()
            release.wait(timeout=1)
            raise RuntimeError("credential=must-not-escape")
        return {"state": "accepted"}

    query = _Query()
    lease = notebook_lease.StreamingLease(
        checkpoint_id="streaming-events-v1",
        prefix="events/",
        workload="streaming_ingest-events-spark-iceberg",
        owner_id="jupyter-notebook",
        session_id="550e8400-e29b-41d4-a716-446655440001",
        terminal_state="stopped",
        terminal_evidence={"generation": {}},
        post=post,
        heartbeat_seconds=0.01,
    )

    with pytest.raises(notebook_lease.NotebookLeaseFailure, match="heartbeat_failed") as failure:
        with lease:
            lease.bind_query(query)
            assert heartbeat_ready.wait(timeout=1)
            release.set()

    assert failure.value.__cause__ is None
    assert query.stop_count >= 1
