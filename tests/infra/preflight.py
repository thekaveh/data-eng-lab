#!/usr/bin/env python3
"""Infra preflight — Layer 1 (existence & initialization). Fail-loud stack doctor."""
from __future__ import annotations

import sys
from collections import namedtuple
from pathlib import Path

Result = namedtuple("Result", "name status detail")


def run_layer1(services, docker_ok: bool) -> list[Result]:
    results: list[Result] = []
    for svc in services:
        if not svc.enabled:
            results.append(Result(svc.name, "skipped", "disabled by manifest (A-item pending)"))
            continue
        if not docker_ok:
            results.append(Result(svc.name, "blocked", "docker unavailable — root cause upstream"))
            continue
        ok, detail = svc.init_check()
        if not ok and detail in ("<docker-error>", "<docker-timeout>"):
            results.append(Result(svc.name, "blocked", detail))
        else:
            results.append(Result(svc.name, "pass" if ok else "fail", detail))
    return results


def render_matrix(results: list[Result]) -> str:
    icon = {"pass": "✅", "fail": "❌", "blocked": "⛔", "skipped": "⏭️"}
    width = max((len(r.name) for r in results), default=4)
    lines = ["Infra preflight — Layer 1 (existence & initialization)", "-" * 60]
    for r in results:
        lines.append(f"{icon.get(r.status,'?')} {r.name.ljust(width)}  {r.status.upper():8} {r.detail}")
    fails = [r for r in results if r.status == "fail"]
    lines.append("-" * 60)
    lines.append(f"{len(results)} services · {len(fails)} FAIL")
    return "\n".join(lines)


def main() -> int:
    # Make this file's directory importable whether run as a script or imported.
    sys.path.insert(0, str(Path(__file__).resolve().parent))
    from manifest import EXPECTED_SERVICES, daemon_ok

    docker_ok = daemon_ok()
    results = run_layer1(EXPECTED_SERVICES, docker_ok)
    print(render_matrix(results))
    return 1 if any(r.status == "fail" for r in results) else 0


if __name__ == "__main__":
    sys.exit(main())
