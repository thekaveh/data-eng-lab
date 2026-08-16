from __future__ import annotations

import json
from pathlib import Path

import pytest

from scripts.security.repository_protections import (
    EvidenceFailure,
    main,
    validate_evidence,
)


def valid_evidence() -> dict[str, object]:
    commit = "a" * 40
    return {
        "schema_version": 1,
        "repository": "thekaveh/data-eng-lab",
        "captured_at": "2026-08-16T12:34:56Z",
        "commit_sha": commit,
        "settings": {
            "dependabot_security_updates": "enabled",
            "secret_scanning": "enabled",
            "secret_scanning_non_provider_patterns": "unsupported",
            "secret_scanning_push_protection": "enabled",
            "secret_scanning_validity_checks": "unsupported",
        },
        "dependabot": {
            "alerts_endpoint": "readable",
            "automated_security_fixes": "enabled",
            "security_update_pull_requests": "observed",
            "version_update_pull_requests": "observed",
            "vulnerability_alerts": "enabled",
        },
        "secret_scanning": {
            "alerts_endpoint": "readable",
            "probe_fixture": "github_official_dummy",
            "probe_remote_ref": "absent",
            "push_protection_probe": "blocked",
        },
        "code_scanning": {
            "analysis_commit_sha": commit,
            "categories": ["actions", "python"],
        },
        "limitations": [
            {
                "feature": "secret_scanning_non_provider_patterns",
                "reason_code": "github_secret_protection_required",
                "required_authority": "GitHub Team or Enterprise with Secret Protection",
                "settings_url": "https://github.com/thekaveh/data-eng-lab/settings/security_analysis",
            },
            {
                "feature": "secret_scanning_validity_checks",
                "reason_code": "github_secret_protection_required",
                "required_authority": "GitHub Team or Enterprise with Secret Protection",
                "settings_url": "https://github.com/thekaveh/data-eng-lab/settings/security_analysis",
            },
        ],
    }


def encoded(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode("ascii")


def test_valid_evidence_is_accepted_and_normalized() -> None:
    value = valid_evidence()
    assert validate_evidence(encoded(value)) == value
    assert validate_evidence(encoded(value) + b"\n") == value


@pytest.mark.parametrize(
    ("mutation", "code"),
    [
        (lambda value: value.update(schema_version=2), "schema_version_invalid"),
        (lambda value: value.update(repository="other/repo"), "repository_invalid"),
        (lambda value: value.update(captured_at="2026-08-16T12:34:56.1Z"), "captured_at_invalid"),
        (lambda value: value.update(captured_at="2026-99-16T12:34:56Z"), "captured_at_invalid"),
        (lambda value: value.update(commit_sha="A" * 40), "commit_sha_invalid"),
        (
            lambda value: value["settings"].update(secret_scanning="disabled"),
            "required_setting_disabled",
        ),
        (
            lambda value: value["settings"].update(unexpected="enabled"),
            "settings_shape_invalid",
        ),
        (
            lambda value: value["dependabot"].update(vulnerability_alerts="disabled"),
            "dependabot_not_enabled",
        ),
        (
            lambda value: value["secret_scanning"].update(push_protection_probe="bypassed"),
            "push_probe_invalid",
        ),
        (
            lambda value: value["code_scanning"].update(categories=["python"]),
            "code_scanning_invalid",
        ),
        (
            lambda value: value["code_scanning"].update(analysis_commit_sha="b" * 40),
            "code_scanning_invalid",
        ),
    ],
)
def test_evidence_refuses_invalid_contract(mutation, code: str) -> None:
    value = valid_evidence()
    mutation(value)
    with pytest.raises(EvidenceFailure, match=f"^{code}$"):
        validate_evidence(encoded(value))


def test_optional_enabled_settings_require_no_limitation() -> None:
    value = valid_evidence()
    value["settings"]["secret_scanning_non_provider_patterns"] = "enabled"
    value["settings"]["secret_scanning_validity_checks"] = "enabled"
    value["limitations"] = []
    assert validate_evidence(encoded(value))["limitations"] == []


def test_optional_unsupported_settings_require_exact_limitation() -> None:
    value = valid_evidence()
    value["limitations"].pop()
    with pytest.raises(EvidenceFailure, match="^limitations_invalid$"):
        validate_evidence(encoded(value))


@pytest.mark.parametrize(
    ("body", "code"),
    [
        (b'{"schema_version":1,"schema_version":1}', "json_duplicate_key"),
        (b"[]", "evidence_shape_invalid"),
        (b"{", "json_invalid"),
        (b"\xff", "json_invalid"),
        (b'{"x":NaN}', "json_number_invalid"),
        (b'{ "schema_version": 1 }', "evidence_not_canonical"),
        (b" " * 65_537, "evidence_too_large"),
    ],
)
def test_json_boundary_is_strict(body: bytes, code: str) -> None:
    with pytest.raises(EvidenceFailure, match=f"^{code}$"):
        validate_evidence(body)


def test_json_depth_and_nodes_are_bounded() -> None:
    deep: object = 0
    for _ in range(18):
        deep = [deep]
    with pytest.raises(EvidenceFailure, match="^json_depth_exceeded$"):
        validate_evidence(encoded(deep))

    many = {str(index): index for index in range(4_097)}
    with pytest.raises(EvidenceFailure, match="^json_nodes_exceeded$"):
        validate_evidence(encoded(many))


def test_cli_reads_regular_file_and_emits_only_constant_success(tmp_path: Path, capsys) -> None:
    evidence = tmp_path / "evidence.json"
    evidence.write_bytes(encoded(valid_evidence()))
    assert main(["--evidence", str(evidence)]) == 0
    assert capsys.readouterr().out == "repository_security_ok\n"


def test_cli_refuses_symlink_and_sanitizes_failure(tmp_path: Path, capsys) -> None:
    target = tmp_path / "target.json"
    target.write_text('{"secret":"must-not-leak"}', encoding="utf-8")
    evidence = tmp_path / "evidence.json"
    evidence.symlink_to(target)
    assert main(["--evidence", str(evidence)]) == 2
    output = capsys.readouterr()
    assert output.out == ""
    assert output.err == "evidence_file_invalid\n"
    assert "must-not-leak" not in output.err
