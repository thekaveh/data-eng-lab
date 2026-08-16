from __future__ import annotations

import shutil
from pathlib import Path

import pytest

from scripts.security.contract import (
    ContractFailure,
    DependencyInventory,
    discover_inventory,
    load_yaml_exact,
    main,
    validate_codeql,
    validate_dependabot,
    validate_osv_workflow,
)

REPO_ROOT = Path(__file__).resolve().parents[2]
EXPECTED_MAVEN_DIRECTORIES = (
    "/spark-apps/gh-archive-pipeline",
    "/spark-apps/movielens-feature-pipeline",
    "/spark-apps/nyc-taxi-data-quality",
    "/spark-apps/nyc-taxi-etl",
    "/spark-apps/nyc-taxi-medallion",
    "/spark-apps/tpch-star-schema",
)


def test_inventory_is_exactly_the_parent_owned_dependency_surfaces() -> None:
    inventory = discover_inventory(REPO_ROOT)
    assert inventory.uv_lock == "/uv.lock"
    assert inventory.pip_lockfiles == ("/datasets/tpch-lock-requirements.txt",)
    assert inventory.action_directory == "/"
    assert inventory.maven_directories == EXPECTED_MAVEN_DIRECTORIES


def test_inventory_rejects_a_symlinked_maven_manifest(tmp_path: Path) -> None:
    (tmp_path / "uv.lock").write_text("version = 1\n", encoding="utf-8")
    app = tmp_path / "spark-apps" / "example"
    app.mkdir(parents=True)
    target = tmp_path / "foreign.xml"
    target.write_text("<project/>\n", encoding="utf-8")
    (app / "pom.xml").symlink_to(target)

    with pytest.raises(ContractFailure, match="inventory_manifest_invalid"):
        discover_inventory(tmp_path)


def test_yaml_loader_rejects_duplicate_keys(tmp_path: Path) -> None:
    config = tmp_path / "duplicate.yml"
    config.write_text("version: 2\nversion: 3\n", encoding="utf-8")

    with pytest.raises(ContractFailure, match="yaml_duplicate_key"):
        load_yaml_exact(config)


def test_yaml_loader_rejects_aliases(tmp_path: Path) -> None:
    config = tmp_path / "alias.yml"
    config.write_text("base: &base {read: true}\ncopy: *base\n", encoding="utf-8")

    with pytest.raises(ContractFailure, match="yaml_alias_forbidden"):
        load_yaml_exact(config)


@pytest.mark.parametrize(
    ("payload", "error"),
    [
        ("- item\n", "yaml_root_invalid"),
        (
            "\n".join(f"{'  ' * depth}level{depth}:" for depth in range(17)) + "\n" + "  " * 17 + "value: true\n",
            "yaml_depth_exceeded",
        ),
    ],
)
def test_yaml_loader_rejects_invalid_shape(tmp_path: Path, payload: str, error: str) -> None:
    config = tmp_path / "invalid.yml"
    config.write_text(payload, encoding="utf-8")

    with pytest.raises(ContractFailure, match=error):
        load_yaml_exact(config)


def test_yaml_loader_rejects_oversized_input(tmp_path: Path) -> None:
    config = tmp_path / "large.yml"
    config.write_bytes(b"key: " + b"x" * 262_145)

    with pytest.raises(ContractFailure, match="yaml_too_large"):
        load_yaml_exact(config)


def test_yaml_loader_closes_extreme_parser_depth(tmp_path: Path) -> None:
    config = tmp_path / "recursive.yml"
    config.write_text(
        "\n".join(f"{'  ' * depth}level{depth}:" for depth in range(500)) + "\n" + "  " * 500 + "value: true\n",
        encoding="utf-8",
    )

    with pytest.raises(ContractFailure, match="yaml_depth_exceeded"):
        load_yaml_exact(config)


def test_dependabot_covers_the_discovered_inventory_exactly() -> None:
    validate_dependabot(REPO_ROOT, discover_inventory(REPO_ROOT))


def test_dependabot_and_osv_cover_the_hashed_tpch_requirements() -> None:
    dependabot = load_yaml_exact(REPO_ROOT / ".github" / "dependabot.yml")
    updates = dependabot["updates"]
    assert isinstance(updates, list)
    assert any(
        update.get("package-ecosystem") == "pip" and update.get("directory") == "/datasets"
        for update in updates
        if isinstance(update, dict)
    )

    workflow = load_yaml_exact(REPO_ROOT / ".github" / "workflows" / "dependency-security.yml")
    scanner = next(
        step
        for step in workflow["jobs"]["pull-request-scan"]["steps"]
        if step.get("name") == "Scan exact proposed dependency manifests"
    )
    arguments = scanner["with"]["scan-args"]
    assert "--lockfile=requirements.txt:datasets/tpch-lock-requirements.txt" in arguments.splitlines()


def test_dependabot_rejects_a_missing_or_extra_maven_directory(tmp_path: Path) -> None:
    config = tmp_path / ".github" / "dependabot.yml"
    config.parent.mkdir()
    config.write_text(
        """\
version: 2
updates:
  - package-ecosystem: github-actions
    directory: /
    target-branch: develop
    schedule: {interval: weekly, day: monday, time: '04:10', timezone: UTC}
    open-pull-requests-limit: 2
    groups: {actions: {patterns: ['*']}}
  - package-ecosystem: uv
    directory: /
    target-branch: develop
    schedule: {interval: weekly, day: tuesday, time: '04:20', timezone: UTC}
    open-pull-requests-limit: 2
    groups: {python: {patterns: ['*']}}
  - package-ecosystem: maven
    directory: /spark-apps/unowned
    target-branch: develop
    schedule: {interval: weekly, day: wednesday, time: '04:30', timezone: UTC}
    open-pull-requests-limit: 2
    groups: {jvm: {patterns: ['*']}}
""",
        encoding="utf-8",
    )
    inventory = DependencyInventory("/uv.lock", "/", ("/spark-apps/owned",))

    with pytest.raises(ContractFailure, match="dependabot_maven_inventory_mismatch"):
        validate_dependabot(tmp_path, inventory)


def test_dependabot_rejects_non_develop_or_unbounded_updates(tmp_path: Path) -> None:
    config = tmp_path / ".github" / "dependabot.yml"
    config.parent.mkdir()
    config.write_text(
        """\
version: 2
updates:
  - package-ecosystem: github-actions
    directory: /
    target-branch: main
    schedule: {interval: daily}
    open-pull-requests-limit: 99
""",
        encoding="utf-8",
    )

    with pytest.raises(ContractFailure, match="dependabot_update_invalid"):
        validate_dependabot(tmp_path, DependencyInventory("/uv.lock", "/", ()))


@pytest.mark.parametrize(
    "schedule",
    [
        "{interval: weekly, day: monday, time: '24:00', timezone: UTC}",
        "{interval: weekly, day: monday, time: '99:99', timezone: UTC}",
        "{interval: weekly, day: monday, time: '1::23', timezone: UTC}",
        "{interval: weekly, day: monday, time: '１２:３４', timezone: UTC}",
        "{interval: weekly, day: [monday], time: '04:10', timezone: UTC}",
    ],
)
def test_dependabot_rejects_invalid_schedule_values(tmp_path: Path, schedule: str) -> None:
    config = tmp_path / ".github" / "dependabot.yml"
    config.parent.mkdir()
    config.write_text(
        f"""\
version: 2
updates:
  - package-ecosystem: github-actions
    directory: /
    target-branch: develop
    schedule: {schedule}
    open-pull-requests-limit: 2
    groups: {{actions: {{patterns: ['*']}}}}
""",
        encoding="utf-8",
    )

    with pytest.raises(ContractFailure, match="dependabot_update_invalid"):
        validate_dependabot(tmp_path, DependencyInventory("/uv.lock", "/", ()))


def test_yaml_loader_uses_github_yaml_boolean_semantics(tmp_path: Path) -> None:
    config = tmp_path / "workflow.yml"
    config.write_text("on: {workflow_dispatch: null}\nenabled: true\n", encoding="utf-8")

    assert load_yaml_exact(config) == {
        "on": {"workflow_dispatch": None},
        "enabled": True,
    }


def test_osv_workflow_scans_only_the_discovered_manifests() -> None:
    validate_osv_workflow(REPO_ROOT, discover_inventory(REPO_ROOT))


def test_osv_pull_request_scan_does_not_call_a_permission_elevating_workflow() -> None:
    workflow = load_yaml_exact(REPO_ROOT / ".github" / "workflows" / "dependency-security.yml")
    job = workflow["jobs"]["pull-request-scan"]

    assert "uses" not in job
    assert job["permissions"] == {"contents": "read"}
    assert [step["uses"] for step in job["steps"] if "uses" in step] == [
        "actions/checkout@8e8c483db84b4bee98b60c0593521ed34d9990e8",
        "google/osv-scanner-action/osv-scanner-action@06b2ab4348248b456ee06c9e953637f55e03504f",
        "actions/checkout@8e8c483db84b4bee98b60c0593521ed34d9990e8",
        "google/osv-scanner-action/osv-scanner-action@06b2ab4348248b456ee06c9e953637f55e03504f",
        "google/osv-scanner-action/osv-reporter-action@06b2ab4348248b456ee06c9e953637f55e03504f",
    ]


def test_osv_pull_request_scan_compares_base_and_head_without_failing_on_baseline() -> None:
    workflow = load_yaml_exact(REPO_ROOT / ".github" / "workflows" / "dependency-security.yml")
    steps = workflow["jobs"]["pull-request-scan"]["steps"]

    base_checkout = steps[0]
    assert base_checkout["name"] == "Checkout target revision"
    assert base_checkout["with"]["ref"] == "${{ github.event.pull_request.base.sha }}"
    assert base_checkout["with"]["fetch-depth"] == 1
    head_checkout = next(step for step in steps if step.get("name") == "Checkout proposed revision")
    assert head_checkout["with"]["ref"] == "${{ github.sha }}"
    assert head_checkout["with"]["clean"] is False
    binding = next(step for step in steps if step.get("name") == "Bind target scanner output")
    assert binding["id"] == "bind-target"
    assert "sha256sum" in binding["run"]
    boundary = next(step for step in steps if step.get("name") == "Revalidate scanner output boundary")
    assert boundary["env"]["EXPECTED_OLD_SHA256"] == "${{ steps.bind-target.outputs.sha256 }}"
    assert 'test ! -L "${OLD_RESULTS_FILE}"' in boundary["run"]
    assert 'test "${actual}" = "${EXPECTED_OLD_SHA256}"' in boundary["run"]
    assert 'test ! -e "${NEW_RESULTS_FILE}"' in boundary["run"]
    assert 'test ! -L "${NEW_RESULTS_FILE}"' in boundary["run"]
    assert 'test ! -e "${SARIF_FILE}"' in boundary["run"]
    assert 'test ! -L "${SARIF_FILE}"' in boundary["run"]

    scanners = [step for step in steps if step.get("name", "").startswith("Scan exact")]
    assert [step["name"] for step in scanners] == [
        "Scan exact target dependency manifests",
        "Scan exact proposed dependency manifests",
    ]
    reporter = next(step for step in steps if step.get("name") == "Fail on a newly introduced vulnerability")
    arguments = reporter["with"]["scan-args"]
    assert "--old=/github/workspace/.osv-old-" in arguments
    assert "--new=/github/workspace/.osv-new-" in arguments
    assert "--fail-on-vuln=true" in arguments

    full_reporter = next(
        step
        for step in workflow["jobs"]["full-scan"]["steps"]
        if step.get("name") == "Report complete vulnerability baseline"
    )
    assert "--fail-on-vuln=false" in full_reporter["with"]["scan-args"]


def test_osv_jobs_fail_closed_on_missing_or_malformed_scanner_output() -> None:
    workflow = load_yaml_exact(REPO_ROOT / ".github" / "workflows" / "dependency-security.yml")
    for name in ("pull-request-scan", "full-scan"):
        job = workflow["jobs"][name]
        assert "uses" not in job
        steps = job["steps"]
        prefix = "${{ github.workspace }}/.osv-results-${{ github.run_id }}-${{ github.run_attempt }}"
        prepare = next(step for step in steps if step.get("name") == "Prepare scanner output")
        assert prepare["env"]["SARIF_FILE"] == f"{prefix}.sarif"
        assert "rm -f --" in prepare["run"]
        validators = [step for step in steps if step.get("name", "").startswith("Validate ")]
        expected_count = 2 if name == "pull-request-scan" else 1
        assert len(validators) == expected_count
        for validator in validators:
            assert validator["if"] == "always() && !cancelled()"
            assert "python3 -m json.tool" in validator["run"]
        scanners = [step for step in steps if step.get("name", "").startswith("Scan exact")]
        assert len(scanners) == expected_count
        for scanner in scanners:
            assert "/github/workspace/.osv-" in scanner["with"]["scan-args"]
            assert "${{ github.workspace }}" not in scanner["with"]["scan-args"]
        reporter = next(step for step in steps if "vulnerability" in step.get("name", ""))
        assert "/github/workspace/.osv-results-" in reporter["with"]["scan-args"]
        assert "${{ github.workspace }}" not in reporter["with"]["scan-args"]


@pytest.mark.parametrize(
    "unsafe_operand",
    ["--recursive", "./", "infra/", "graphify-out/"],
)
def test_osv_workflow_rejects_recursive_or_unowned_scan_operands(tmp_path: Path, unsafe_operand: str) -> None:
    workflow = tmp_path / ".github" / "workflows" / "dependency-security.yml"
    workflow.parent.mkdir(parents=True)
    workflow.write_text(
        f"""\
name: Dependency vulnerability audit
on: {{pull_request: {{branches: [develop, main]}}}}
permissions: {{}}
jobs:
  pull-request-scan:
    if: github.event_name == 'pull_request'
    permissions: {{contents: read}}
    uses: google/osv-scanner-action/.github/workflows/osv-scanner-reusable.yml@8deb546fdb875b9996d27d4950be7312dac076a1
    with:
      scan-args: |-
        {unsafe_operand}
      upload-sarif: false
      fail-on-vuln: true
""",
        encoding="utf-8",
    )

    with pytest.raises(ContractFailure, match="osv_workflow_invalid"):
        validate_osv_workflow(tmp_path, DependencyInventory("/uv.lock", "/", ()))


def test_codeql_analyzes_only_supported_parent_source_languages() -> None:
    validate_codeql(REPO_ROOT)


def test_codeql_rejects_scala_claims_and_missing_exclusions(tmp_path: Path) -> None:
    github = tmp_path / ".github"
    shutil.copytree(REPO_ROOT / ".github", github)
    workflow = github / "workflows" / "codeql.yml"
    workflow.write_text(
        workflow.read_text(encoding="utf-8").replace("language: python", "language: scala"),
        encoding="utf-8",
    )

    with pytest.raises(ContractFailure, match="codeql_workflow_invalid"):
        validate_codeql(tmp_path)


def test_codeql_rejects_mutable_actions_or_excess_permissions(tmp_path: Path) -> None:
    github = tmp_path / ".github"
    shutil.copytree(REPO_ROOT / ".github", github)
    workflow = github / "workflows" / "codeql.yml"
    body = workflow.read_text(encoding="utf-8")
    body = body.replace(
        "github/codeql-action/analyze@ff2f1c621b7f889edc0d3c761ac2e6a3f8cdb0dd",
        "github/codeql-action/analyze@v4",
    ).replace("contents: read", "contents: write")
    workflow.write_text(body, encoding="utf-8")

    with pytest.raises(ContractFailure, match="codeql_workflow_invalid"):
        validate_codeql(tmp_path)


def test_contract_cli_validates_the_committed_repository(capsys: pytest.CaptureFixture[str]) -> None:
    assert main(["--root", str(REPO_ROOT)]) == 0
    assert capsys.readouterr() == ("security_contract_ok\n", "")
