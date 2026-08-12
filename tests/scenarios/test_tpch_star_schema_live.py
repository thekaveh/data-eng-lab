from __future__ import annotations

import os

import pytest

pytestmark = pytest.mark.infra


@pytest.mark.skipif(os.environ.get("RUN_INFRA") != "1", reason="needs the live Atlas stack")
def test_tpch_star_schema_live_acceptance_is_recorded():
    """The repeatable live procedure and its latest evidence are maintained in the report.

    The runtime harness intentionally remains operator-driven because it builds/publishes a reviewed
    branch JAR, triggers Airflow twice, inspects Spark REST, and preserves the shared stack volumes.
    """
    from pathlib import Path

    report = Path(__file__).resolve().parents[2] / "docs/superpowers/reports/2026-08-12-tpch-star-schema-live-acceptance.md"
    text = report.read_text(encoding="utf-8")
    assert "driver-20260812190708-0000" in text
    assert "driver-20260812190835-0001" in text
    assert text.count("success=true") >= 2
    assert "8b024198f91d197b" in text and "8ce8521bbc607f2e" in text
