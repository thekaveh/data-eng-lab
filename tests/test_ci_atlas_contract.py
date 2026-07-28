from __future__ import annotations

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
    live_actions = {"up", "start", "run"}

    def has_live_action_after(index: int) -> bool:
        return bool(live_actions.intersection(command[index + 1 :]))

    # Shell wrappers (for example ``env``, ``command``, ``if``, and ``(``)
    # change the first token without making the nested command any less live.
    # Scan the whole executable fragment instead of relying on command_name.
    for index, token in enumerate(command):
        if Path(token).name == "start.sh" and has_live_action_after(index):
            return True
        if token in {"compose", "docker-compose"} and has_live_action_after(index):
            return True
    return False


def _is_endpoint_assert(command: tuple[str, ...]) -> bool:
    """Return whether a ``start.sh ... endpoints assert`` invocation appears."""
    for index, token in enumerate(command):
        if Path(token).name != "start.sh":
            continue
        trailing_tokens = command[index + 1 :]
        try:
            endpoints_index = trailing_tokens.index("endpoints")
        except ValueError:
            continue
        if "assert" in trailing_tokens[endpoints_index + 1 :]:
            return True
    return False


def _assert_non_live_contract(job: dict) -> None:
    for command in _executable_commands(job):
        assert not _is_endpoint_assert(command)
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
    with_env_wrapped_compose = {"steps": [{"run": "env COMPOSE_PROJECT_NAME=x docker compose up -d"}]}
    with_command_wrapped_compose = {"steps": [{"run": "command docker compose start"}]}
    with_conditional_compose = {"steps": [{"run": "if docker compose run worker; then :; fi"}]}
    with_subshell_compose = {"steps": [{"run": "( docker compose up -d )"}]}
    for job in (
        with_live_endpoint_assert,
        with_live_start,
        with_live_atlas_start,
        with_docker_compose_start,
        with_docker_compose_run,
        with_compose_start,
        with_start_script_start,
        with_start_script_run,
        with_env_wrapped_compose,
        with_command_wrapped_compose,
        with_conditional_compose,
        with_subshell_compose,
    ):
        try:
            _assert_non_live_contract(job)
        except AssertionError:
            pass
        else:
            raise AssertionError("live operation unexpectedly passed the CI contract")


def test_non_live_ci_guard_rejects_wrapped_endpoint_assertions():
    with_env_wrapped_endpoint_assert = {"steps": [{"run": "env X=1 ./start.sh endpoints assert"}]}
    with_conditional_endpoint_assert = {
        "steps": [
            {
                "run": (
                    "if command ./start.sh --consumer ../atlas.consumer.yml "
                    "endpoints assert; then :; fi"
                )
            }
        ]
    }
    for job in (with_env_wrapped_endpoint_assert, with_conditional_endpoint_assert):
        try:
            _assert_non_live_contract(job)
        except AssertionError:
            pass
        else:
            raise AssertionError("wrapped endpoint assertion unexpectedly passed the CI contract")


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
