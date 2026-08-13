from __future__ import annotations

from pathlib import Path

import yaml

from scripts.docs.build_docs import render_site, render_wiki
from scripts.docs.manifest import load_manifest

ROOT = Path(__file__).resolve().parents[2]
RUNBOOK = ROOT / "docs/checkpoint-retention.md"

EXACT_IDS = (
    "streaming-events-v1",
    "streaming-event-windows-v1",
    "streaming-online-retail-cdc-v1",
    "streaming-gh-archive-file-v1",
    "go-live-streaming-test-v1",
)

REQUIRED_POLICY_TEXT = (
    "30-day quarantine",
    "14 days",
    "24 hours",
    "60 seconds",
    "10-minute TTL",
    "five minutes",
    "15-minute quiescence",
    "100 listing pages",
    "100,000 objects",
    "10 GiB",
    "1,000 keys",
    "64 KiB",
    "1 MiB",
    "dedicated checkpoint-maintenance service account",
    "scheduling remains disabled",
    "issue #86",
)


def test_canonical_runbook_contains_exact_policy_recovery_and_non_mutation_contract():
    text = RUNBOOK.read_text(encoding="utf-8")
    normalized = " ".join(text.split())

    for checkpoint_id in EXACT_IDS:
        assert checkpoint_id in text
    for phrase in REQUIRED_POLICY_TEXT:
        assert phrase in text
    assert "maximum of the terminal or retirement time, final lease heartbeat, and newest object" in normalized
    assert "object newer than the terminal record" in text
    assert "expiry alone never proves stopped" in text.lower()
    assert "zero S3 writes" in text
    assert "cannot contact MinIO" in text
    assert "does not delete checkpoints" in text
    assert "break glass" in text.lower()


def test_manifest_projects_checkpoint_runbook_to_site_and_wiki(tmp_path: Path):
    manifest = load_manifest(ROOT / "docs/manifest.yaml", ROOT)
    site = tmp_path / "site"
    wiki = tmp_path / "wiki"

    render_site(manifest, ROOT, site)
    render_wiki(manifest, ROOT, wiki)

    site_page = site / "checkpoint-retention.md"
    wiki_page = wiki / "Checkpoint-Retention.md"
    assert site_page.is_file()
    assert wiki_page.is_file()
    for checkpoint_id in EXACT_IDS:
        assert checkpoint_id in site_page.read_text(encoding="utf-8")
        assert checkpoint_id in wiki_page.read_text(encoding="utf-8")


def test_public_overviews_and_lakehouse_link_the_canonical_policy():
    for relative in (
        "README.md",
        "docs/index.md",
        "docs/scenarios/index.md",
        "docs/lakehouse.md",
    ):
        text = (ROOT / relative).read_text(encoding="utf-8")
        assert "checkpoint retention" in text.lower()
        assert "checkpoint-retention.md" in text
        assert "issue #86" in text.lower()
        assert "scheduling" in text.lower() and "disabled" in text.lower()


def test_execution_matrix_keeps_streams_unscheduled_and_binds_policy_issue():
    data = yaml.safe_load((ROOT / "scenarios/execution-modes.yaml").read_text())
    by_id = {entry["scenario_id"]: entry for entry in data["scenarios"]}
    for scenario in (
        "streaming_ingest-events-spark-iceberg",
        "streaming_ingest-gh_archive-spark-iceberg",
        "streaming_windows-events-spark-iceberg",
        "cdc_streaming-online_retail-spark-iceberg",
    ):
        entry = by_id[scenario]
        assert entry["classification"] in {
            "intentionally unscheduled long-running streaming",
            "intentionally notebook-only",
        }
        assert "unscheduled" in entry["schedule_policy"].lower()
        assert any("#85" in dependency for dependency in entry["dependencies"])
        assert any("issue #86" in contract.lower() for contract in entry["acceptance_contract"])


def test_go_live_calls_legacy_reset_exclusive_and_never_policy_eligible():
    text = (ROOT / "docs/go-live.md").read_text(encoding="utf-8")

    assert "exclusive-test-only" in text
    assert "never a retention eligibility decision" in text
    assert "`gh_events_file/` is a family-root deletion" in text
    assert "must not run on a shared environment" in text
    assert "issue #86" in text.lower()
    assert "scheduling remains disabled" in text
