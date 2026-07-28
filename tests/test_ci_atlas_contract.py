from __future__ import annotations

import re
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
WORKFLOW = ROOT / ".github/workflows/ci.yml"


def _atlas_contract_job(workflow: dict) -> dict:
    return workflow["jobs"]["atlas-consumer-contract"]


def _run_commands(job: dict) -> str:
    return "\n".join(step.get("run", "") for step in job["steps"])


def _assert_non_live_contract(job: dict) -> None:
    commands = _run_commands(job)
    assert "endpoints assert" not in commands
    assert not re.search(
        r"(?:\bdocker\s+compose\b|\bcompose\b|\./start\.sh\b)[^\n]*\bup\b", commands
    )


def test_ci_has_a_pinned_non_live_atlas_consumer_contract_job():
    workflow = yaml.safe_load(WORKFLOW.read_text(encoding="utf-8"))
    job = _atlas_contract_job(workflow)
    checkout = next(step for step in job["steps"] if step.get("uses", "").startswith("actions/checkout@"))
    assert checkout["with"]["submodules"] == "recursive"

    commands = _run_commands(job)
    for required in (
        "cp infra/.env.example infra/.env",
        "./start.sh env backfill",
        "--consumer ../atlas.consumer.yml compose validate",
        "--consumer ../atlas.consumer.yml doctor --format json",
    ):
        assert required in commands
    _assert_non_live_contract(job)


def test_non_live_ci_guard_rejects_live_operations():
    with_live_endpoint_assert = {"steps": [{"run": "./start.sh endpoints assert"}]}
    with_live_start = {"steps": [{"run": "docker compose up -d"}]}
    with_live_atlas_start = {"steps": [{"run": "./start.sh --consumer ../atlas.consumer.yml up -d"}]}
    for job in (with_live_endpoint_assert, with_live_start, with_live_atlas_start):
        try:
            _assert_non_live_contract(job)
        except AssertionError:
            pass
        else:
            raise AssertionError("live operation unexpectedly passed the CI contract")
