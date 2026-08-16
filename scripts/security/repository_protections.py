"""Validate bounded, secret-free evidence for repository security protections."""

from __future__ import annotations

import argparse
import json
import re
import sys
from datetime import datetime
from pathlib import Path
from typing import NoReturn

MAX_EVIDENCE_BYTES = 65_536
MAX_JSON_DEPTH = 16
MAX_JSON_NODES = 4_096
REPOSITORY = "thekaveh/data-eng-lab"
SETTINGS_URL = "https://github.com/thekaveh/data-eng-lab/settings/security_analysis"

_TOP_LEVEL_FIELDS = {
    "schema_version",
    "repository",
    "captured_at",
    "commit_sha",
    "settings",
    "dependabot",
    "secret_scanning",
    "code_scanning",
    "limitations",
}
_SETTING_FIELDS = {
    "dependabot_security_updates",
    "secret_scanning",
    "secret_scanning_non_provider_patterns",
    "secret_scanning_push_protection",
    "secret_scanning_validity_checks",
}
_REQUIRED_SETTINGS = {
    "dependabot_security_updates",
    "secret_scanning",
    "secret_scanning_push_protection",
}
_OPTIONAL_SETTINGS = {
    "secret_scanning_non_provider_patterns",
    "secret_scanning_validity_checks",
}
_LIMITATION_FIELDS = {
    "feature",
    "reason_code",
    "required_authority",
    "settings_url",
}
_LIMITATION_REASON = "github_secret_protection_required"
_LIMITATION_AUTHORITY = "GitHub Team or Enterprise with Secret Protection"


class EvidenceFailure(ValueError):
    """One closed repository-security evidence contract was not satisfied."""


def _fail(code: str) -> NoReturn:
    raise EvidenceFailure(code)


def _unique_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    value: dict[str, object] = {}
    for key, item in pairs:
        if key in value:
            _fail("json_duplicate_key")
        value[key] = item
    return value


def _reject_constant(_value: str) -> NoReturn:
    _fail("json_number_invalid")


def _validate_bounds(value: object) -> None:
    pending: list[tuple[object, int]] = [(value, 1)]
    nodes = 0
    while pending:
        item, depth = pending.pop()
        nodes += 1
        if nodes > MAX_JSON_NODES:
            _fail("json_nodes_exceeded")
        if depth > MAX_JSON_DEPTH:
            _fail("json_depth_exceeded")
        if isinstance(item, dict):
            pending.extend((key, depth + 1) for key in item)
            pending.extend((child, depth + 1) for child in item.values())
        elif isinstance(item, list):
            pending.extend((child, depth + 1) for child in item)


def _decode(body: bytes) -> dict[str, object]:
    if type(body) is not bytes or len(body) > MAX_EVIDENCE_BYTES:
        _fail("evidence_too_large")
    document = body[:-1] if body.endswith(b"\n") else body
    try:
        value = json.loads(
            document.decode("utf-8", errors="strict"),
            object_pairs_hook=_unique_object,
            parse_constant=_reject_constant,
        )
    except EvidenceFailure:
        raise
    except (UnicodeDecodeError, json.JSONDecodeError, RecursionError, ValueError):
        _fail("json_invalid")
    _validate_bounds(value)
    if not isinstance(value, dict):
        _fail("evidence_shape_invalid")
    canonical = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode("ascii")
    if document != canonical:
        _fail("evidence_not_canonical")
    return value


def _validate_limitations(settings: dict[str, object], limitations: object) -> None:
    if not isinstance(limitations, list) or len(limitations) > len(_OPTIONAL_SETTINGS):
        _fail("limitations_invalid")
    by_feature: dict[str, dict[str, object]] = {}
    for limitation in limitations:
        if (
            not isinstance(limitation, dict)
            or set(limitation) != _LIMITATION_FIELDS
            or not isinstance(limitation.get("feature"), str)
        ):
            _fail("limitations_invalid")
        feature = limitation["feature"]
        if feature in by_feature or feature not in _OPTIONAL_SETTINGS:
            _fail("limitations_invalid")
        if limitation != {
            "feature": feature,
            "reason_code": _LIMITATION_REASON,
            "required_authority": _LIMITATION_AUTHORITY,
            "settings_url": SETTINGS_URL,
        }:
            _fail("limitations_invalid")
        by_feature[feature] = limitation
    expected = {feature for feature in _OPTIONAL_SETTINGS if settings[feature] == "unsupported"}
    if set(by_feature) != expected:
        _fail("limitations_invalid")


def _valid_pr_numbers(value: object) -> bool:
    return (
        isinstance(value, list)
        and bool(value)
        and all(type(number) is int and number > 0 for number in value)
        and value == sorted(set(value))
    )


def validate_evidence(body: bytes) -> dict[str, object]:
    """Validate one canonical repository-security evidence document."""

    value = _decode(body)
    if set(value) != _TOP_LEVEL_FIELDS:
        _fail("evidence_shape_invalid")
    if value["schema_version"] != 1 or type(value["schema_version"]) is not int:
        _fail("schema_version_invalid")
    if value["repository"] != REPOSITORY:
        _fail("repository_invalid")
    captured_at = value["captured_at"]
    timestamp_pattern = r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z"
    if not isinstance(captured_at, str) or re.fullmatch(timestamp_pattern, captured_at) is None:
        _fail("captured_at_invalid")
    try:
        datetime.strptime(captured_at, "%Y-%m-%dT%H:%M:%SZ")
    except ValueError:
        _fail("captured_at_invalid")
    commit_sha = value["commit_sha"]
    if not isinstance(commit_sha, str) or re.fullmatch(r"[0-9a-f]{40}", commit_sha) is None:
        _fail("commit_sha_invalid")

    settings = value["settings"]
    if not isinstance(settings, dict) or set(settings) != _SETTING_FIELDS:
        _fail("settings_shape_invalid")
    if any(settings[name] != "enabled" for name in _REQUIRED_SETTINGS):
        _fail("required_setting_disabled")
    if any(settings[name] not in {"enabled", "unsupported"} for name in _OPTIONAL_SETTINGS):
        _fail("settings_shape_invalid")

    dependabot = value["dependabot"]
    if not isinstance(dependabot, dict) or set(dependabot) != {
        "alerts_endpoint",
        "automated_security_fixes",
        "security_update_pull_requests",
        "version_update_pull_requests",
        "vulnerability_alerts",
    }:
        _fail("dependabot_not_enabled")
    if (
        dependabot["alerts_endpoint"] != "readable"
        or dependabot["automated_security_fixes"] != "enabled"
        or dependabot["vulnerability_alerts"] != "enabled"
        or not _valid_pr_numbers(dependabot["security_update_pull_requests"])
        or not _valid_pr_numbers(dependabot["version_update_pull_requests"])
        or set(dependabot["security_update_pull_requests"]) & set(dependabot["version_update_pull_requests"])
    ):
        _fail("dependabot_not_enabled")

    secret_scanning = value["secret_scanning"]
    if not isinstance(secret_scanning, dict) or set(secret_scanning) != {
        "alerts_endpoint",
        "probe_commit_sha",
        "probe_fixture",
        "probe_ref",
        "probe_remote_ref",
        "push_protection_probe",
    }:
        _fail("push_probe_invalid")
    probe_sha = secret_scanning["probe_commit_sha"]
    probe_ref = secret_scanning["probe_ref"]
    if (
        secret_scanning["alerts_endpoint"] != "readable"
        or secret_scanning["probe_fixture"] != "github_official_dummy"
        or secret_scanning["probe_remote_ref"] != "absent"
        or secret_scanning["push_protection_probe"] != "blocked"
        or not isinstance(probe_sha, str)
        or re.fullmatch(r"[0-9a-f]{40}", probe_sha) is None
        or probe_sha == commit_sha
        or not isinstance(probe_ref, str)
        or re.fullmatch(r"refs/heads/codex/93-push-protection-probe-[0-9]{8}T[0-9]{4}Z", probe_ref) is None
    ):
        _fail("push_probe_invalid")

    code_scanning = value["code_scanning"]
    if not isinstance(code_scanning, dict) or set(code_scanning) != {
        "analysis_commit_sha",
        "analysis_ids",
        "categories",
    }:
        _fail("code_scanning_invalid")
    analysis_ids = code_scanning["analysis_ids"]
    if (
        code_scanning["analysis_commit_sha"] != commit_sha
        or code_scanning["categories"] != ["actions", "python"]
        or not isinstance(analysis_ids, dict)
        or set(analysis_ids) != {"actions", "python"}
        or any(type(identifier) is not int or identifier < 1 for identifier in analysis_ids.values())
        or len(set(analysis_ids.values())) != 2
    ):
        _fail("code_scanning_invalid")
    _validate_limitations(settings, value["limitations"])
    return value


def _read_evidence(path: Path) -> bytes:
    try:
        if path.is_symlink() or not path.is_file():
            _fail("evidence_file_invalid")
        with path.open("rb") as stream:
            body = stream.read(MAX_EVIDENCE_BYTES + 1)
    except EvidenceFailure:
        raise
    except OSError:
        _fail("evidence_file_invalid")
    if len(body) > MAX_EVIDENCE_BYTES:
        _fail("evidence_too_large")
    return body


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--evidence", required=True, type=Path)
    try:
        args = parser.parse_args(argv)
        validate_evidence(_read_evidence(args.evidence))
    except EvidenceFailure as exc:
        print(str(exc), file=sys.stderr)
        return 2
    print("repository_security_ok")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
