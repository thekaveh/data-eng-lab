from __future__ import annotations

from pathlib import Path

import pytest

from scripts.security.contract import (
    ContractFailure,
    DependencyInventory,
    discover_inventory,
    load_yaml_exact,
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
    assert discover_inventory(REPO_ROOT) == DependencyInventory(
        uv_lock="/uv.lock",
        action_directory="/",
        maven_directories=EXPECTED_MAVEN_DIRECTORIES,
    )


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
