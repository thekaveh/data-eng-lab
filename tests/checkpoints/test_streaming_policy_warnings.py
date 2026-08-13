from __future__ import annotations

import json
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]

SCENARIOS = {
    "streaming_ingest-events-spark-iceberg": (
        "streaming-events-v1",
        "durable stream",
        "fresh checkpoint can duplicate append output",
    ),
    "streaming_windows-events-spark-iceberg": (
        "streaming-event-windows-v1",
        "durable stream",
        "fresh checkpoint can duplicate append output",
    ),
    "cdc_streaming-online_retail-spark-iceberg": (
        "streaming-online-retail-cdc-v1",
        "durable stream",
        "CDC recovery is not assumed safe",
    ),
    "streaming_ingest-gh_archive-spark-iceberg": (
        "streaming-gh-archive-file-v1",
        "generation reproducibility",
        "reset `lakehouse.bronze.gh_events_stream`",
    ),
}


def _surfaces(scenario: str) -> tuple[str, str, str]:
    root = ROOT / "scenarios" / scenario
    readme = (root / "README.md").read_text(encoding="utf-8")
    jupyter = json.loads((root / "jupyter/notebook.ipynb").read_text(encoding="utf-8"))
    jupyter_text = "\n".join("".join(cell.get("source", [])) for cell in jupyter.get("cells", []))
    zeppelin = json.loads((root / "zeppelin/notebook.zpln").read_text(encoding="utf-8"))
    zeppelin_text = "\n".join(paragraph.get("text", "") for paragraph in zeppelin["paragraphs"])
    return readme, jupyter_text, zeppelin_text


@pytest.mark.parametrize(("scenario", "contract"), SCENARIOS.items())
def test_all_streaming_surfaces_warn_about_exact_checkpoint_policy(scenario, contract):
    checkpoint_id, durability, recovery = contract

    for text in _surfaces(scenario):
        assert "Checkpoint policy (#85)" in text
        assert checkpoint_id in text
        assert durability in text
        assert "Streaming Data Engineering" in text
        assert "active or uncertain" in text
        assert "Automated deletion remains disabled" in text
        assert "issue #86" in text
        assert recovery in text


def test_generation_warning_binds_exact_identity_and_sink_reset():
    for text in _surfaces("streaming_ingest-gh_archive-spark-iceberg"):
        assert "scale, publication ID, and manifest SHA-256" in text
        assert "exact resolver generation" in text
        assert "14 days" in text


@pytest.mark.parametrize(
    "scenario",
    [
        "streaming_ingest-events-spark-iceberg",
        "streaming_windows-events-spark-iceberg",
        "cdc_streaming-online_retail-spark-iceberg",
    ],
)
def test_durable_warning_requires_reviewed_retirement_and_thirty_days(scenario):
    for text in _surfaces(scenario):
        assert "reviewed retirement" in text
        assert "30-day quarantine" in text
