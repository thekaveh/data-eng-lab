from __future__ import annotations

import re
import shlex
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
WORKFLOW = ROOT / ".github/workflows/ci.yml"


def _atlas_contract_job(workflow: dict) -> dict:
    return workflow["jobs"]["atlas-consumer-contract"]


def _run_commands(job: dict) -> str:
    return "\n".join(step.get("run", "") for step in job["steps"])


def _executable_commands(job: dict) -> list[tuple[str, ...]]:
    """Return shell command fragments, excluding comments and echoed text."""
    commands: list[tuple[str, ...]] = []
    for line in _run_commands(job).splitlines():
        lexer = shlex.shlex(line, posix=True, punctuation_chars=";&|")
        lexer.whitespace_split = True
        lexer.commenters = "#"
        fragment: list[str] = []
        for token in lexer:
            if token in {";", "&&", "||", "|"}:
                if fragment:
                    commands.append(tuple(fragment))
                    fragment = []
            else:
                fragment.append(token)
        if fragment:
            commands.append(tuple(fragment))
    return commands


def _command_name(command: tuple[str, ...]) -> str:
    index = 0
    while index < len(command) and re.match(r"^[A-Za-z_][A-Za-z0-9_]*=", command[index]):
        index += 1
    return command[index] if index < len(command) else ""


def _assert_required_commands(job: dict) -> None:
    commands = _executable_commands(job)
    for required in (
        ("cp", "infra/.env.example", "infra/.env"),
        ("./start.sh", "env", "backfill"),
        ("./start.sh", "--consumer", "../atlas.consumer.yml", "compose", "validate"),
        ("./start.sh", "--consumer", "../atlas.consumer.yml", "doctor", "--format", "json"),
    ):
        assert required in commands


def _is_live_operation(command: tuple[str, ...]) -> bool:
    command_name = _command_name(command)
    live_actions = {"up", "start", "run"}
    if command_name == "docker" and "compose" in command:
        return bool(live_actions.intersection(command[command.index("compose") + 1 :]))
    if command_name in {"compose", "docker-compose"}:
        return bool(live_actions.intersection(command[1:]))
    if Path(command_name).name == "start.sh":
        return bool(live_actions.intersection(command[1:]))
    return False


def _assert_non_live_contract(job: dict) -> None:
    for command in _executable_commands(job):
        assert not (Path(_command_name(command)).name == "start.sh" and "endpoints" in command and "assert" in command)
        assert not _is_live_operation(command)


def test_ci_has_a_pinned_non_live_atlas_consumer_contract_job():
    workflow = yaml.safe_load(WORKFLOW.read_text(encoding="utf-8"))
    job = _atlas_contract_job(workflow)
    checkout = next(step for step in job["steps"] if step.get("uses", "").startswith("actions/checkout@"))
    assert checkout["with"]["submodules"] == "recursive"

    _assert_required_commands(job)
    _assert_non_live_contract(job)


def test_non_live_ci_guard_rejects_live_operations():
    with_live_endpoint_assert = {"steps": [{"run": "./start.sh endpoints assert"}]}
    with_live_start = {"steps": [{"run": "docker compose up -d"}]}
    with_live_atlas_start = {"steps": [{"run": "./start.sh --consumer ../atlas.consumer.yml up -d"}]}
    with_docker_compose_start = {"steps": [{"run": "docker compose start"}]}
    with_docker_compose_run = {"steps": [{"run": "docker compose run worker"}]}
    with_compose_start = {"steps": [{"run": "compose start"}]}
    with_start_script_start = {"steps": [{"run": "./start.sh --consumer ../atlas.consumer.yml start"}]}
    with_start_script_run = {"steps": [{"run": "./start.sh run consumer"}]}
    for job in (
        with_live_endpoint_assert,
        with_live_start,
        with_live_atlas_start,
        with_docker_compose_start,
        with_docker_compose_run,
        with_compose_start,
        with_start_script_start,
        with_start_script_run,
    ):
        try:
            _assert_non_live_contract(job)
        except AssertionError:
            pass
        else:
            raise AssertionError("live operation unexpectedly passed the CI contract")


def test_required_ci_command_guard_rejects_comments_and_echoes():
    required_lines = (
        "cp infra/.env.example infra/.env",
        "./start.sh env backfill",
        "./start.sh --consumer ../atlas.consumer.yml compose validate",
        "./start.sh --consumer ../atlas.consumer.yml doctor --format json",
    )
    for prefix in ("# ", "echo "):
        job = {"steps": [{"run": "\n".join(f"{prefix}{line}" for line in required_lines)}]}
        try:
            _assert_required_commands(job)
        except AssertionError:
            pass
        else:
            raise AssertionError(f"{prefix.strip()} unexpectedly satisfied required CI commands")
