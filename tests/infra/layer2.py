#!/usr/bin/env python3
"""Infra preflight — Layer 2 (service<->service integration matrix).

Each Edge's probe performs a real round-trip via `exec_fn` (default: `docker exec`
into a container that has in-network access + the right client libs). Unit tests
inject a fake exec_fn; live runs use the real one.
"""
from __future__ import annotations

import subprocess
import sys
from collections import namedtuple
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from preflight import Result, render_matrix as _render  # noqa: E402

Edge = namedtuple("Edge", "name enabled probe")


def default_exec(container: str, argv: list[str]) -> tuple[int, str]:
    """Run `docker exec <container> <argv...>`; return (rc, combined stdout+stderr)."""
    import os

    project = os.environ.get("PROJECT_NAME", "data-eng-lab")
    full = ["docker", "exec", f"{project}-{container}", *argv]
    try:
        out = subprocess.run(full, capture_output=True, text=True, timeout=120)
    except subprocess.TimeoutExpired:
        return 124, "<timeout>"
    except FileNotFoundError:
        return 127, "<docker-missing>"
    return out.returncode, (out.stdout + out.stderr)


def run_layer2(edges, exec_fn, docker_ok) -> list[Result]:
    results: list[Result] = []
    for e in edges:
        if not e.enabled:
            results.append(Result(e.name, "skipped", "disabled by manifest (A-item pending)"))
            continue
        if not docker_ok:
            results.append(Result(e.name, "blocked", "docker/stack unavailable"))
            continue
        try:
            ok, detail = e.probe(exec_fn)
        except Exception as exc:  # noqa: BLE001
            ok, detail = False, f"probe error: {exc}"
        results.append(Result(e.name, "pass" if ok else "fail", detail))
    return results


def render_matrix(results) -> str:
    return _render(results).replace("Layer 1 (existence & initialization)",
                                    "Layer 2 (integration matrix)")
