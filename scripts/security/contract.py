from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

MAX_YAML_BYTES = 262_144
MAX_YAML_DEPTH = 16
MAX_YAML_NODES = 16_384


class ContractFailure(ValueError):
    """A bounded repository security contract was not satisfied."""


@dataclass(frozen=True)
class DependencyInventory:
    uv_lock: str
    action_directory: str
    maven_directories: tuple[str, ...]


def _require_owned_file(root: Path, path: Path) -> None:
    try:
        resolved_root = root.resolve(strict=True)
        resolved = path.resolve(strict=True)
    except OSError:
        raise ContractFailure("inventory_manifest_invalid") from None
    if path.is_symlink() or not path.is_file() or not resolved.is_relative_to(resolved_root):
        raise ContractFailure("inventory_manifest_invalid")


def discover_inventory(root: Path) -> DependencyInventory:
    _require_owned_file(root, root / "uv.lock")
    manifests = sorted((root / "spark-apps").glob("*/pom.xml"))
    if not manifests:
        raise ContractFailure("inventory_manifest_invalid")
    for manifest in manifests:
        _require_owned_file(root, manifest)
    return DependencyInventory(
        uv_lock="/uv.lock",
        action_directory="/",
        maven_directories=tuple(f"/{manifest.parent.relative_to(root).as_posix()}" for manifest in manifests),
    )


class _ExactSafeLoader(yaml.SafeLoader):
    def compose_node(self, parent: Any, index: Any) -> yaml.Node:
        if self.check_event(yaml.AliasEvent):
            raise ContractFailure("yaml_alias_forbidden")
        return super().compose_node(parent, index)

    def construct_mapping(self, node: yaml.MappingNode, deep: bool = False) -> dict[Any, Any]:
        mapping: dict[Any, Any] = {}
        for key_node, value_node in node.value:
            key = self.construct_object(key_node, deep=deep)
            try:
                duplicate = key in mapping
            except TypeError:
                raise ContractFailure("yaml_key_invalid") from None
            if duplicate:
                raise ContractFailure("yaml_duplicate_key")
            mapping[key] = self.construct_object(value_node, deep=deep)
        return mapping


_ExactSafeLoader.yaml_implicit_resolvers = {
    key: [(tag, expression) for tag, expression in resolvers if tag != "tag:yaml.org,2002:bool"]
    for key, resolvers in yaml.SafeLoader.yaml_implicit_resolvers.items()
}
_ExactSafeLoader.add_implicit_resolver(
    "tag:yaml.org,2002:bool",
    re.compile(r"^(?:true|True|TRUE|false|False|FALSE)$"),
    list("tTfF"),
)


def _validate_tree(value: object) -> None:
    count = 0
    stack: list[tuple[object, int]] = [(value, 1)]
    while stack:
        item, depth = stack.pop()
        count += 1
        if count > MAX_YAML_NODES:
            raise ContractFailure("yaml_nodes_exceeded")
        if depth > MAX_YAML_DEPTH:
            raise ContractFailure("yaml_depth_exceeded")
        if isinstance(item, dict):
            stack.extend((key, depth + 1) for key in item)
            stack.extend((child, depth + 1) for child in item.values())
        elif isinstance(item, list):
            stack.extend((child, depth + 1) for child in item)


def load_yaml_exact(path: Path) -> dict[str, object]:
    try:
        body = path.read_bytes()
    except OSError:
        raise ContractFailure("yaml_read_failed") from None
    if len(body) > MAX_YAML_BYTES:
        raise ContractFailure("yaml_too_large")
    try:
        text = body.decode("utf-8")
        value = yaml.load(text, Loader=_ExactSafeLoader)
    except ContractFailure:
        raise
    except (UnicodeDecodeError, yaml.YAMLError):
        raise ContractFailure("yaml_malformed") from None
    if not isinstance(value, dict) or not all(isinstance(key, str) for key in value):
        raise ContractFailure("yaml_root_invalid")
    _validate_tree(value)
    return value


def _valid_schedule(value: object) -> bool:
    if not isinstance(value, dict) or set(value) != {
        "interval",
        "day",
        "time",
        "timezone",
    }:
        return False
    return (
        value["interval"] == "weekly"
        and value["day"] in {"monday", "tuesday", "wednesday", "thursday", "friday"}
        and isinstance(value["time"], str)
        and len(value["time"]) == 5
        and value["time"][2] == ":"
        and value["time"].replace(":", "").isdigit()
        and value["timezone"] == "UTC"
    )


def _valid_groups(value: object) -> bool:
    if not isinstance(value, dict) or len(value) != 1:
        return False
    group = next(iter(value.values()))
    return isinstance(group, dict) and group == {"patterns": ["*"]}


def validate_dependabot(root: Path, inventory: DependencyInventory) -> None:
    document = load_yaml_exact(root / ".github" / "dependabot.yml")
    if set(document) != {"version", "updates"} or document["version"] != 2:
        raise ContractFailure("dependabot_root_invalid")
    updates = document["updates"]
    if not isinstance(updates, list) or not updates:
        raise ContractFailure("dependabot_updates_invalid")
    allowed_keys = {
        "package-ecosystem",
        "directory",
        "target-branch",
        "schedule",
        "open-pull-requests-limit",
        "groups",
    }
    by_ecosystem: dict[str, list[str]] = {}
    for update in updates:
        if not isinstance(update, dict) or set(update) != allowed_keys:
            raise ContractFailure("dependabot_update_invalid")
        ecosystem = update["package-ecosystem"]
        directory = update["directory"]
        limit = update["open-pull-requests-limit"]
        if (
            not isinstance(ecosystem, str)
            or ecosystem not in {"github-actions", "uv", "maven"}
            or not isinstance(directory, str)
            or update["target-branch"] != "develop"
            or not _valid_schedule(update["schedule"])
            or not isinstance(limit, int)
            or isinstance(limit, bool)
            or not 1 <= limit <= 5
            or not _valid_groups(update["groups"])
        ):
            raise ContractFailure("dependabot_update_invalid")
        by_ecosystem.setdefault(ecosystem, []).append(directory)
    if by_ecosystem.get("github-actions") != [inventory.action_directory]:
        raise ContractFailure("dependabot_actions_inventory_mismatch")
    if by_ecosystem.get("uv") != ["/"]:
        raise ContractFailure("dependabot_uv_inventory_mismatch")
    if tuple(sorted(by_ecosystem.get("maven", []))) != inventory.maven_directories:
        raise ContractFailure("dependabot_maven_inventory_mismatch")


def _osv_operands(inventory: DependencyInventory) -> tuple[str, ...]:
    return (f"--lockfile={inventory.uv_lock.removeprefix('/')}",) + tuple(
        f"--lockfile={directory.removeprefix('/')}/pom.xml" for directory in inventory.maven_directories
    )


def validate_osv_workflow(root: Path, inventory: DependencyInventory) -> None:
    workflow = load_yaml_exact(root / ".github" / "workflows" / "dependency-security.yml")
    if set(workflow) != {"name", "on", "permissions", "concurrency", "jobs"}:
        raise ContractFailure("osv_workflow_invalid")
    expected_branches = {"branches": ["develop", "main"]}
    if workflow["on"] != {
        "pull_request": expected_branches,
        "push": expected_branches,
        "workflow_dispatch": None,
    }:
        raise ContractFailure("osv_workflow_invalid")
    if workflow["permissions"] != {} or workflow["concurrency"] != {
        "group": "dependency-security-${{ github.workflow }}-${{ github.ref }}",
        "cancel-in-progress": False,
    }:
        raise ContractFailure("osv_workflow_invalid")
    jobs = workflow["jobs"]
    if not isinstance(jobs, dict) or set(jobs) != {
        "pull-request-scan",
        "full-scan",
    }:
        raise ContractFailure("osv_workflow_invalid")
    expected_uses = (
        "google/osv-scanner-action/.github/workflows/osv-scanner-reusable.yml@8deb546fdb875b9996d27d4950be7312dac076a1"
    )
    expected_args = "\n".join(_osv_operands(inventory))
    expected_jobs = {
        "pull-request-scan": {
            "if": "github.event_name == 'pull_request'",
            "permissions": {"contents": "read"},
            "uses": expected_uses,
            "with": {
                "scan-args": expected_args,
                "upload-sarif": False,
                "fail-on-vuln": True,
            },
        },
        "full-scan": {
            "if": "github.event_name != 'pull_request'",
            "permissions": {
                "actions": "read",
                "contents": "read",
                "security-events": "write",
            },
            "uses": expected_uses,
            "with": {
                "scan-args": expected_args,
                "upload-sarif": True,
                "fail-on-vuln": True,
            },
        },
    }
    if jobs != expected_jobs:
        raise ContractFailure("osv_workflow_invalid")
