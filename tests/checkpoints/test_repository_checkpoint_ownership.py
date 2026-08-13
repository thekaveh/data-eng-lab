from __future__ import annotations

import ast
import json
import re
import subprocess
from pathlib import Path

from scripts.checkpoints.policy import load_policy

ROOT = Path(__file__).resolve().parents[2]
POLICY = load_policy(ROOT / "checkpoints" / "retention-policy.yaml")
SCENARIO_ROOT = ROOT / "scenarios"

EXPECTED_SCENARIOS = {
    "streaming_ingest-events-spark-iceberg": (
        "streaming-events-v1",
        "events/",
        "lakehouse.bronze.events",
    ),
    "streaming_windows-events-spark-iceberg": (
        "streaming-event-windows-v1",
        "event_windows/",
        "lakehouse.gold.event_windows",
    ),
    "cdc_streaming-online_retail-spark-iceberg": (
        "streaming-online-retail-cdc-v1",
        "online_retail_cdc/",
        "lakehouse.silver.online_retail_cdc",
    ),
    "streaming_ingest-gh_archive-spark-iceberg": (
        "streaming-gh-archive-file-v1",
        "gh_events_file/{scale}/{publication_id}/{manifest_sha256}/",
        "lakehouse.bronze.gh_events_stream",
    ),
}

EXPECTED_EXECUTABLE_LOCATIONS = {
    "docs/go-live.md": ("s3a://checkpoints/streaming_test",),
    "scenarios/cdc_streaming-online_retail-spark-iceberg/jupyter/notebook.ipynb": (
        "s3a://checkpoints/online_retail_cdc",
    ),
    "scenarios/cdc_streaming-online_retail-spark-iceberg/zeppelin/notebook.zpln": (
        "s3a://checkpoints/online_retail_cdc",
    ),
    "scenarios/streaming_ingest-events-spark-iceberg/jupyter/notebook.ipynb": ("s3a://checkpoints/events",),
    "scenarios/streaming_ingest-events-spark-iceberg/zeppelin/notebook.zpln": ("s3a://checkpoints/events",),
    "scenarios/streaming_ingest-gh_archive-spark-iceberg/jupyter/notebook.ipynb": (
        "s3a://checkpoints/gh_events_file/{scale}/{publication_id}/{manifest_sha256}",
    ),
    "scenarios/streaming_ingest-gh_archive-spark-iceberg/zeppelin/notebook.zpln": (
        "s3a://checkpoints/gh_events_file/{scale}/{publication_id}/{manifest_sha256}",
    ),
    "scenarios/streaming_windows-events-spark-iceberg/jupyter/notebook.ipynb": ("s3a://checkpoints/event_windows",),
    "scenarios/streaming_windows-events-spark-iceberg/zeppelin/notebook.zpln": ("s3a://checkpoints/event_windows",),
}

DYNAMIC_LOCATION_NORMALIZATION = {
    "s3a://checkpoints/gh_events_file/{dataset_scale}/{publication_id}/{manifest_sha256}": (
        "s3a://checkpoints/gh_events_file/{scale}/{publication_id}/{manifest_sha256}"
    ),
    "s3a://checkpoints/gh_events_file/$datasetScale/$publicationId/$manifestSha256": (
        "s3a://checkpoints/gh_events_file/{scale}/{publication_id}/{manifest_sha256}"
    ),
}


def _tracked_executable_checkpoint_locations() -> dict[str, tuple[str, ...]]:
    tracked = (
        subprocess.run(["git", "ls-files", "-z"], cwd=ROOT, check=True, capture_output=True)
        .stdout.decode("utf-8")
        .split("\0")
    )
    found: dict[str, tuple[str, ...]] = {}
    for relative in tracked:
        if not relative or relative.startswith(("tests/", "docs/notebooks/", "docs/superpowers/")):
            continue
        path = ROOT / relative
        if not path.is_file():
            continue
        try:
            text = path.read_text(encoding="utf-8", errors="strict")
        except UnicodeDecodeError:
            continue
        if "checkpointLocation" not in text:
            continue
        raw_locations = re.findall(r"s3a://checkpoints/[A-Za-z0-9_/{\}$]+", text)
        locations = tuple(DYNAMIC_LOCATION_NORMALIZATION.get(value, value) for value in raw_locations)
        found[relative] = locations
    return found


def test_checkpoint_location_inventory_is_exhaustively_discovered_from_tracked_executables():
    assert _tracked_executable_checkpoint_locations() == EXPECTED_EXECUTABLE_LOCATIONS


def _literal_assignment(path: Path, name: str):
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    for node in tree.body:
        if isinstance(node, ast.Assign) and any(
            isinstance(target, ast.Name) and target.id == name for target in node.targets
        ):
            return ast.literal_eval(node.value)
    raise AssertionError(f"missing literal assignment {name} in {path}")


def _notebook_texts(scenario: str) -> tuple[str, str]:
    root = SCENARIO_ROOT / scenario
    jupyter = json.loads((root / "jupyter/notebook.ipynb").read_text(encoding="utf-8"))
    jupyter_text = "\n".join("".join(cell.get("source", [])) for cell in jupyter.get("cells", []))
    zeppelin = json.loads((root / "zeppelin/notebook.zpln").read_text(encoding="utf-8"))
    zeppelin_text = "\n".join(paragraph.get("text", "") for paragraph in zeppelin["paragraphs"])
    return jupyter_text, zeppelin_text


def test_every_executable_streaming_notebook_maps_to_one_exact_owner_and_sink():
    for scenario, (checkpoint_id, template, sink) in EXPECTED_SCENARIOS.items():
        entry = POLICY.entries[checkpoint_id]
        assert entry.workload == scenario
        assert entry.prefix == template
        assert entry.sink == sink
        jupyter, zeppelin = _notebook_texts(scenario)
        assert sink in jupyter
        assert sink in zeppelin
        for required_segment in template.replace("{", "").replace("}", "").split("/"):
            if required_segment:
                assert required_segment in jupyter
                assert required_segment in zeppelin


def test_go_live_scratch_path_maps_to_disposable_acceptance_owner():
    entry = POLICY.entries["go-live-streaming-test-v1"]
    go_live = (ROOT / "docs/go-live.md").read_text(encoding="utf-8")

    assert entry.prefix == "streaming_test/"
    assert entry.durability == "disposable_acceptance"
    assert entry.sink == "s3a://lakehouse/bronze/streaming_test"
    assert "s3a://checkpoints/streaming_test" in go_live
    assert entry.sink in go_live


def test_exhaustive_gate_declares_unsafe_root_reset_and_disabled_replacement():
    harness = ROOT / "tests/scenarios/test_notebook_reproducibility_live.py"
    checkpoints = _literal_assignment(harness, "CHECKPOINTS")
    reset_policy = _literal_assignment(harness, "CHECKPOINT_RESET_POLICY")

    assert reset_policy == {
        "mode": "exclusive_disposable_stack_only",
        "unsafe_roots": ("gh_events_file",),
        "networked_replacement_issue": 86,
        "schedule": "disabled",
    }
    assert checkpoints == {
        "streaming_ingest-events-spark-iceberg": "events",
        "streaming_ingest-gh_archive-spark-iceberg": "gh_events_file",
        "streaming_windows-events-spark-iceberg": "event_windows",
        "cdc_streaming-online_retail-spark-iceberg": "online_retail_cdc",
    }
    for prefix in set(checkpoints.values()) - set(reset_policy["unsafe_roots"]):
        POLICY.match_prefix(f"{prefix}/")


def test_unknown_stale_upstream_and_control_prefixes_remain_unowned():
    for prefix in (
        "events_stream/",
        "redpanda/atlas_stream_events/",
        "gh_events_file/",
        "_retention/",
    ):
        try:
            POLICY.match_prefix(prefix)
        except ValueError:
            pass
        else:
            raise AssertionError(f"unexpected owned checkpoint prefix: {prefix}")
