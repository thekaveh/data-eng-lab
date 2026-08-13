#!/usr/bin/env python3
"""Validate and project the canonical scenario execution-mode matrix."""

from __future__ import annotations

import argparse
import dataclasses
import sys
from pathlib import Path
from typing import Any

import yaml
from yaml.constructor import ConstructorError

CLASSIFICATIONS = (
    "existing production DAG",
    "approved new production DAG",
    "intentionally notebook-only",
    "intentionally unscheduled long-running streaming",
    "deprecated or superseded",
)

_TOP_LEVEL_FIELDS = {"version", "scenarios"}
_ROW_FIELDS = {
    "scenario_id",
    "classification",
    "justification",
    "owner",
    "runtime",
    "schedule_policy",
    "execution_entrypoint",
    "dependencies",
    "acceptance_contract",
    "child_issue",
}


class ExecutionModeError(ValueError):
    """Raised when the execution-mode contract is malformed or inconsistent."""


class _UniqueKeySafeLoader(yaml.SafeLoader):
    """Safe YAML loader that rejects duplicate keys instead of overwriting them."""


def _construct_unique_mapping(
    loader: _UniqueKeySafeLoader, node: yaml.MappingNode, deep: bool = False
) -> dict[Any, Any]:
    mapping: dict[Any, Any] = {}
    for key_node, value_node in node.value:
        key = loader.construct_object(key_node, deep=deep)
        if key in mapping:
            raise ConstructorError(
                "while constructing a mapping",
                node.start_mark,
                f"duplicate mapping key: {key!r}",
                key_node.start_mark,
            )
        mapping[key] = loader.construct_object(value_node, deep=deep)
    return mapping


_UniqueKeySafeLoader.add_constructor(
    yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG,
    _construct_unique_mapping,
)


@dataclasses.dataclass(frozen=True)
class ExecutionMode:
    scenario_id: str
    classification: str
    justification: str
    owner: str
    runtime: str
    schedule_policy: str
    execution_entrypoint: str | None
    dependencies: tuple[str, ...]
    acceptance_contract: tuple[str, ...]
    child_issue: int | None


def parse_execution_modes(text: str) -> tuple[ExecutionMode, ...]:
    """Parse the closed execution-mode YAML schema without repository I/O."""
    try:
        document = yaml.load(text, Loader=_UniqueKeySafeLoader)
    except yaml.YAMLError as error:
        raise ExecutionModeError(f"invalid YAML: {error}") from error
    if not isinstance(document, dict):
        raise ExecutionModeError("matrix must be a mapping")
    actual_top = set(document)
    if unknown := actual_top - _TOP_LEVEL_FIELDS:
        raise ExecutionModeError(f"unknown top-level fields: {sorted(unknown)}")
    if missing := _TOP_LEVEL_FIELDS - actual_top:
        raise ExecutionModeError(f"missing top-level fields: {sorted(missing)}")
    if isinstance(document["version"], bool) or type(document["version"]) is not int or document["version"] != 1:
        raise ExecutionModeError("version must be integer 1")
    rows = document["scenarios"]
    if not isinstance(rows, list):
        raise ExecutionModeError("scenarios must be a list")

    modes: list[ExecutionMode] = []
    seen: set[str] = set()
    for index, row in enumerate(rows):
        prefix = f"scenarios[{index}]"
        if not isinstance(row, dict):
            raise ExecutionModeError(f"{prefix} must be a mapping")
        actual = set(row)
        if unknown := actual - _ROW_FIELDS:
            raise ExecutionModeError(f"{prefix} unknown row fields: {sorted(unknown)}")
        if missing := _ROW_FIELDS - actual:
            raise ExecutionModeError(f"{prefix} missing row fields: {sorted(missing)}")
        for field in (
            "scenario_id",
            "classification",
            "justification",
            "owner",
            "runtime",
            "schedule_policy",
        ):
            if not isinstance(row[field], str) or not row[field].strip():
                raise ExecutionModeError(f"{prefix} {field} must be a non-empty string")
        scenario_id = row["scenario_id"]
        if scenario_id in seen:
            raise ExecutionModeError(f"duplicate scenario_id: {scenario_id}")
        seen.add(scenario_id)
        if row["classification"] not in CLASSIFICATIONS:
            raise ExecutionModeError(f"{prefix} unknown classification: {row['classification']}")
        entrypoint = row["execution_entrypoint"]
        if entrypoint is not None and (not isinstance(entrypoint, str) or not entrypoint.strip()):
            raise ExecutionModeError(f"{prefix} execution_entrypoint must be null or a non-empty string")
        child_issue = row["child_issue"]
        if child_issue is not None and (
            isinstance(child_issue, bool)
            or not isinstance(child_issue, int)
            or child_issue < 1
        ):
            raise ExecutionModeError(f"{prefix} child_issue must be null or a positive integer")
        dependencies = _string_tuple(row["dependencies"], prefix, "dependencies")
        acceptance = _string_tuple(row["acceptance_contract"], prefix, "acceptance_contract")
        modes.append(
            ExecutionMode(
                scenario_id=scenario_id,
                classification=row["classification"],
                justification=row["justification"].strip(),
                owner=row["owner"].strip(),
                runtime=row["runtime"].strip(),
                schedule_policy=row["schedule_policy"].strip(),
                execution_entrypoint=entrypoint.strip() if entrypoint is not None else None,
                dependencies=dependencies,
                acceptance_contract=acceptance,
                child_issue=child_issue,
            )
        )
    if tuple(mode.scenario_id for mode in modes) != tuple(sorted(seen)):
        raise ExecutionModeError("scenario rows must be sorted by scenario_id")
    return tuple(modes)


def _string_tuple(value: Any, prefix: str, field: str) -> tuple[str, ...]:
    if not isinstance(value, list) or not value:
        raise ExecutionModeError(f"{prefix} {field} must be a non-empty list")
    if any(not isinstance(item, str) or not item.strip() for item in value):
        raise ExecutionModeError(f"{prefix} {field} entries must be non-empty strings")
    return tuple(item.strip() for item in value)


def load_execution_modes(path: Path, root: Path) -> tuple[ExecutionMode, ...]:
    """Load and semantically validate the canonical matrix."""
    try:
        modes = parse_execution_modes(path.read_text(encoding="utf-8"))
    except OSError as error:
        raise ExecutionModeError(f"unable to read execution matrix {path}: {error}") from error
    validate_execution_modes(modes, root)
    return modes


def validate_execution_modes(modes: tuple[ExecutionMode, ...], root: Path) -> None:
    """Validate matrix meaning against the repository's executable inventory."""
    root = root.resolve()
    scenario_root = root / "scenarios"
    discovered = {
        path.name
        for path in scenario_root.iterdir()
        if path.is_dir()
        and (path / "jupyter/notebook.ipynb").is_file()
        and (path / "zeppelin/notebook.zpln").is_file()
    }
    declared = {mode.scenario_id for mode in modes}
    if declared != discovered:
        missing = sorted(discovered - declared)
        extra = sorted(declared - discovered)
        raise ExecutionModeError(f"scenario inventory mismatch: missing={missing}, extra={extra}")

    declared_entrypoints: set[str] = set()
    for mode in modes:
        entrypoint = mode.execution_entrypoint
        if mode.classification == "existing production DAG":
            if entrypoint is None:
                raise ExecutionModeError(f"{mode.scenario_id}: existing production DAG requires an entrypoint")
            path = Path(entrypoint)
            resolved = (root / path).resolve()
            if path.is_absolute() or not resolved.is_relative_to(root) or not resolved.is_file():
                raise ExecutionModeError(f"{mode.scenario_id}: invalid production entrypoint {entrypoint}")
            if path.name != "dag.py" or path.parts[:1] not in (("spark-apps",), ("airflow-dags",)):
                raise ExecutionModeError(
                    f"{mode.scenario_id}: production entrypoint must be a spark-apps or airflow-dags DAG"
                )
            if mode.child_issue is not None:
                raise ExecutionModeError(f"{mode.scenario_id}: existing production DAG cannot have a child issue")
            declared_entrypoints.add(path.as_posix())
        elif mode.classification == "approved new production DAG":
            if entrypoint is not None or mode.child_issue is None:
                raise ExecutionModeError(
                    f"{mode.scenario_id}: approved new production DAG requires a child issue and no entrypoint"
                )
        elif entrypoint is not None or mode.child_issue is not None:
            raise ExecutionModeError(
                f"{mode.scenario_id}: {mode.classification} cannot have an entrypoint or child issue"
            )
        if (
            mode.classification == "intentionally unscheduled long-running streaming"
            and "unscheduled" not in mode.schedule_policy.casefold()
        ):
            raise ExecutionModeError(f"{mode.scenario_id}: long-running stream must be explicitly unscheduled")

    actual_entrypoints = {
        path.relative_to(root).as_posix()
        for parent in (root / "spark-apps", root / "airflow-dags")
        for path in parent.rglob("dag.py")
    }
    if declared_entrypoints != actual_entrypoints:
        raise ExecutionModeError(
            "production DAG inventory mismatch: "
            f"declared={sorted(declared_entrypoints)}, actual={sorted(actual_entrypoints)}"
        )
    scenario_dags = sorted(path.relative_to(root).as_posix() for path in scenario_root.rglob("dag.py"))
    if scenario_dags:
        raise ExecutionModeError(f"scenario-local DAGs are prohibited: {scenario_dags}")


def render_markdown(modes: tuple[ExecutionMode, ...]) -> str:
    """Render the manifest-owned public matrix deterministically."""
    lines = [
        "# 5.21. Execution Modes",
        "",
        "This table is generated from `scenarios/execution-modes.yaml`. It is the "
        "reviewed execution contract for all 19 paired-notebook scenarios; edit the "
        "YAML and run `uv run python -m scripts.scenario_execution --render`.",
        "",
        "`nyc_taxi_etl`, `nyc_taxi_medallion`, `tpch_star_schema`, "
        "`movielens_feature_pipeline`, `gh_archive_flatten_sessionization`, `tpch_bi_query`, "
        "and `nyc_taxi_trino_daily` are seven "
        "production DAGs today. An "
        "approved child issue is a delivery boundary, not a runnable DAG. Notebook-only "
        "and continuous-stream scenarios run from their paired Zeppelin or Jupyter notebooks.",
        "",
        "| Scenario | Classification | Owner | Runtime and schedule | Entrypoint / child | "
        "Dependencies | Justification | Acceptance contract |",
        "|---|---|---|---|---|---|---|---|",
    ]
    for mode in modes:
        execution = (
            f"`{mode.execution_entrypoint}`"
            if mode.execution_entrypoint is not None
            else f"[#{mode.child_issue}](https://github.com/thekaveh/data-eng-lab/issues/{mode.child_issue})"
            if mode.child_issue is not None
            else "None"
        )
        cells = (
            f"`{mode.scenario_id}`",
            mode.classification,
            mode.owner,
            f"{mode.runtime}; {mode.schedule_policy}",
            execution,
            _join_contract_items(mode.dependencies),
            mode.justification,
            _join_contract_items(mode.acceptance_contract),
        )
        lines.append("| " + " | ".join(_markdown_cell(cell) for cell in cells) + " |")
    lines.extend(
        [
            "",
            "## 1. Classification Rules",
            "",
            "- **existing production DAG:** the entrypoint exists under `spark-apps/` or "
            "`airflow-dags/`, is "
            "mounted into Airflow, and performs reviewed work.",
            "- **approved new production DAG:** no production entrypoint exists yet; the "
            "linked child owns implementation and live acceptance.",
            "- **intentionally notebook-only:** the scenario is an educational, "
            "experimental, or operator-sensitive notebook workflow with no schedule.",
            "- **intentionally unscheduled long-running streaming:** an operator starts "
            "and supervises the continuous notebook query; Airflow does not treat it as a batch task.",
            "- **deprecated or superseded:** a row uses this only when the scenario itself "
            "is retired. Deleted no-op DAG artifacts do not create extra scenario rows.",
            "",
            "## 2. Review Boundary",
            "",
            "A child may begin implementation only after this matrix is merged to both "
            "protected branches. Productionization must replace the approved row with an "
            "existing production entrypoint only after its code, focused tests, documentation, "
            "and live Airflow/Spark or Trino acceptance pass.",
        ]
    )
    return "\n".join(lines) + "\n"


def _markdown_cell(value: str) -> str:
    return value.replace("|", "\\|").replace("\n", " ").strip()


def _join_contract_items(items: tuple[str, ...]) -> str:
    return "; ".join(item.rstrip(".;") for item in items) + "."


def check_projection(root: Path) -> tuple[str, ...]:
    """Return drift findings for the committed public projection."""
    root = root.resolve()
    modes = load_execution_modes(root / "scenarios/execution-modes.yaml", root)
    expected = render_markdown(modes)
    path = root / "docs/scenarios/execution-modes.md"
    try:
        actual = path.read_text(encoding="utf-8")
    except OSError as error:
        return (f"unable to read {path.relative_to(root)}: {error}",)
    if actual != expected:
        return ("docs/scenarios/execution-modes.md is not the canonical matrix projection",)
    return ()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path.cwd())
    actions = parser.add_mutually_exclusive_group(required=True)
    actions.add_argument("--check", action="store_true")
    actions.add_argument("--render", action="store_true")
    args = parser.parse_args(argv)
    root = args.root.resolve()
    try:
        modes = load_execution_modes(root / "scenarios/execution-modes.yaml", root)
        if args.render:
            destination = root / "docs/scenarios/execution-modes.md"
            destination.write_text(render_markdown(modes), encoding="utf-8")
            print(f"rendered {destination.relative_to(root)}")
            return 0
        findings = check_projection(root)
    except ExecutionModeError as error:
        print(f"execution-mode error: {error}", file=sys.stderr)
        return 1
    for finding in findings:
        print(finding, file=sys.stderr)
    return 1 if findings else 0


if __name__ == "__main__":
    raise SystemExit(main())
