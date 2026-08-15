from __future__ import annotations

import shlex
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[1]
WORKFLOW = ROOT / ".github/workflows/ci.yml"


def _load_workflow() -> dict:
    return yaml.load(WORKFLOW.read_text(encoding="utf-8"), Loader=yaml.BaseLoader)


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
        ("cp", "atlas.env.user.example", "atlas.env.user"),
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


def _is_network_only_pytest(command: tuple[str, ...]) -> bool:
    if not any(Path(token).name == "pytest" for token in command):
        return False
    for index, token in enumerate(command):
        if token == "-m" and index + 1 < len(command):
            return command[index + 1].strip() == "network"
        if token.startswith("-m="):
            return token.partition("=")[2].strip() == "network"
    return False


def _assert_no_indirect_shell_execution(job: dict) -> None:
    """Reject constructs that can conceal a live command from fragment parsing."""
    shell_names = {"bash", "dash", "fish", "ksh", "powershell", "pwsh", "sh", "zsh"}
    for run_block in _run_commands(job).splitlines():
        assert "$(" not in run_block
        assert "`" not in run_block

        command = tuple(token for token in shlex.split(run_block, comments=True) if token)
        for index, token in enumerate(command):
            is_shell_command_flag = token.startswith("-") and not token.startswith("--") and "c" in token[1:]
            if not is_shell_command_flag:
                continue
            assert not any(Path(candidate).name in shell_names for candidate in command[:index])


def _assert_non_live_contract(job: dict) -> None:
    _assert_no_indirect_shell_execution(job)
    for command in _executable_commands(job):
        assert not _is_endpoint_assert(command)
        assert not _is_live_operation(command)


def test_ci_has_a_pinned_non_live_atlas_consumer_contract_job():
    workflow = _load_workflow()
    job = _atlas_contract_job(workflow)
    checkout = next(step for step in job["steps"] if step.get("uses", "").startswith("actions/checkout@"))
    assert checkout["with"]["submodules"] == "recursive"

    _assert_required_commands(job)
    _assert_non_live_contract(job)


def test_ci_contract_jobs_initialize_atlas_and_scope_validation_only_credentials():
    workflow = _load_workflow()
    expected_environment = {
        "MINIO_ROOT_USER": "ci-placeholder-user",
        "MINIO_ROOT_PASSWORD": "ci-placeholder-password",
    }
    atlas_job = workflow["jobs"]["atlas-consumer-contract"]
    static_job = workflow["jobs"]["static-and-unit"]
    for job in (atlas_job, static_job):
        checkout = next(step for step in job["steps"] if step.get("uses", "").startswith("actions/checkout@"))
        assert checkout["with"]["submodules"] == "recursive"
        assert "env" not in job

    validation = next(
        step for step in atlas_job["steps"] if step.get("name") == "Validate the pinned Atlas consumer contract"
    )
    assert validation["env"] == expected_environment
    assert all("env" not in step for step in static_job["steps"])


def test_static_ci_does_not_run_obsolete_host_tpch_network_install():
    static_job = _load_workflow()["jobs"]["static-and-unit"]
    commands = _run_commands(static_job)
    assert 'uv run pytest -m "not infra and not network" -q' in commands
    assert "INSTALL tpch" not in commands
    assert not any(_is_network_only_pytest(command) for command in _executable_commands(static_job))


@pytest.mark.parametrize(
    "command",
    [
        ("uv", "run", "pytest", "-m", "network", "-q"),
        ("uv", "run", "pytest", "-q", "-m=network"),
        ("pytest", "--maxfail=1", "-m", "network"),
    ],
)
def test_network_only_pytest_guard_rejects_marker_order_and_spelling(command):
    assert _is_network_only_pytest(command)


def test_network_only_pytest_guard_allows_canonical_offline_marker():
    assert not _is_network_only_pytest(("uv", "run", "pytest", "-m", "not infra and not network", "-q"))


def test_ci_covers_main_and_develop_pushes_and_pull_requests():
    workflow = _load_workflow()

    assert workflow["on"]["push"]["branches"] == ["main", "develop"]
    assert workflow["on"]["pull_request"]["branches"] == ["main", "develop"]


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
        "steps": [{"run": ("if command ./start.sh --consumer ../atlas.consumer.yml endpoints assert; then :; fi")}]
    }
    for job in (with_env_wrapped_endpoint_assert, with_conditional_endpoint_assert):
        try:
            _assert_non_live_contract(job)
        except AssertionError:
            pass
        else:
            raise AssertionError("wrapped endpoint assertion unexpectedly passed the CI contract")


def test_non_live_ci_guard_rejects_indirect_shell_execution():
    with_bash_compose = {"steps": [{"run": 'bash -c "docker compose up -d"'}]}
    with_bash_start = {"steps": [{"run": 'bash -c "./start.sh up -d"'}]}
    with_bash_endpoint_assert = {"steps": [{"run": 'bash -c "./start.sh endpoints assert"'}]}
    with_command_substitution = {"steps": [{"run": "$(./start.sh endpoints assert)"}]}
    with_backtick_substitution = {"steps": [{"run": "`./start.sh endpoints assert`"}]}
    for job in (
        with_bash_compose,
        with_bash_start,
        with_bash_endpoint_assert,
        with_command_substitution,
        with_backtick_substitution,
    ):
        try:
            _assert_non_live_contract(job)
        except AssertionError:
            pass
        else:
            raise AssertionError("indirect shell execution unexpectedly passed the CI contract")


def test_required_ci_command_guard_rejects_comments_and_echoes():
    required_lines = (
        "cp infra/.env.example infra/.env",
        "cp atlas.env.user.example atlas.env.user",
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
