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


def _check_naming(root: Path, cfg: dict) -> list[Finding]:
    findings: list[Finding] = []
    regex = re.compile(cfg["scenario_name_regex"])
    for name in cfg.get("active_scenario_dirs", []):
        if not regex.match(name):
            findings.append(Finding("scenario.naming", "error",
                                    f"scenario dir '{name}' violates naming convention"))
    return findings


def _check_declared_dirs_exist(root: Path, cfg: dict) -> list[Finding]:
    findings: list[Finding] = []
    for name in cfg.get("active_scenario_dirs", []):
        if not (root / "scenarios" / name).is_dir():
            findings.append(Finding("scenario.exists", "error",
                                    f"declared scenario '{name}' has no folder under scenarios/"))
    return findings


CHECKS = [_check_naming, _check_declared_dirs_exist]


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
    cfg = yaml.safe_load(cfg_path.read_text())

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
