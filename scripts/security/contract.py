from __future__ import annotations

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
