#!/usr/bin/env python3
"""Config-driven repo verifier (skeleton). Exit 0 iff no error-severity findings."""
from __future__ import annotations

import argparse
import json
import re
import sys
from collections import namedtuple
from pathlib import Path

import yaml

Finding = namedtuple("Finding", "check severity message")


def _discover_scenarios(root: Path) -> list[str]:
    base = root / "scenarios"
    if not base.is_dir():
        return []
    return sorted(
        p.name for p in base.iterdir()
        if p.is_dir() and not p.name.startswith((".", "_"))
    )


def _check_scenarios(root: Path, cfg: dict) -> list[Finding]:
    import json as _json

    findings: list[Finding] = []
    regex = re.compile(cfg["scenario_name_regex"])
    required = cfg.get("scenario_required_files", [])
    sections = cfg.get("scenario_readme_sections", [])
    for name in _discover_scenarios(root):
        sdir = root / "scenarios" / name
        if not regex.fullmatch(name):
            findings.append(Finding("scenario.naming", "error",
                                    f"scenario '{name}' violates the naming convention"))
        for rel in required:
            if not (sdir / rel).exists():
                findings.append(Finding("scenario.files", "error",
                                        f"scenario '{name}' is missing '{rel}'"))
        readme = sdir / "README.md"
        if readme.exists():
            text = readme.read_text(encoding="utf-8")
            for sec in sections:
                if f"## {sec}" not in text:
                    findings.append(Finding("scenario.readme", "error",
                                            f"scenario '{name}' README missing section '## {sec}'"))
        for rel in required:
            if rel.endswith((".zpln", ".ipynb")) and (sdir / rel).exists():
                try:
                    _json.loads((sdir / rel).read_text(encoding="utf-8"))
                except Exception as exc:  # noqa: BLE001
                    findings.append(Finding("scenario.notebook_json", "error",
                                            f"scenario '{name}' file '{rel}' is not valid JSON: {exc}"))
    return findings


def _check_dataset_registry(root: Path, cfg: dict) -> list[Finding]:
    import importlib.util  # noqa: PLC0415

    reg = root / "datasets" / "registry.yaml"
    if not reg.exists():
        return []  # registry is optional until Phase 1a lands
    # Load this repo's schema.py BY PATH relative to verify_repo.py's own location, so it
    # works no matter how the verifier is invoked (sys.path[0] is scripts/, not the repo root).
    schema_path = Path(__file__).resolve().parent.parent / "datasets" / "schema.py"
    if not schema_path.exists():
        return [Finding("dataset.registry", "error", "registry.yaml present but datasets/schema.py missing")]
    spec = importlib.util.spec_from_file_location("_dataset_schema", schema_path)
    schema = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(schema)
    try:
        doc = yaml.safe_load(reg.read_text(encoding="utf-8"))
    except Exception as exc:  # noqa: BLE001
        return [Finding("dataset.registry", "error", f"registry.yaml is not valid YAML: {exc}")]
    return [Finding("dataset.registry", "error", msg) for msg in schema.validate_registry(doc)]


CHECKS = [_check_scenarios, _check_dataset_registry]


def run_checks(root: Path, cfg: dict) -> list[Finding]:
    findings: list[Finding] = []
    for check in CHECKS:
        findings.extend(check(root, cfg))
    return findings


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", default=".", type=Path)
    ap.add_argument("--config", default=None, type=Path)
    ap.add_argument("--json", action="store_true")
    args = ap.parse_args(argv)

    root = args.root.resolve()
    cfg_path = args.config or (root / "scripts" / "verify_repo_config.yaml")
    cfg = yaml.safe_load(cfg_path.read_text(encoding="utf-8"))

    findings = run_checks(root, cfg)
    errors = [f for f in findings if f.severity == "error"]

    if args.json:
        print(json.dumps([f._asdict() for f in findings], indent=2))
    else:
        for f in findings:
            print(f"[{f.severity.upper()}] {f.check}: {f.message}")
        print(f"\n{len(findings)} finding(s), {len(errors)} error(s)")
    return 1 if errors else 0


if __name__ == "__main__":
    sys.exit(main())
