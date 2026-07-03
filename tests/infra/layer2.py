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


def _truthy(var: str) -> bool:
    import os

    return os.environ.get(var, "").lower() in ("1", "true", "yes", "on")


def _run_in(container: str, script_path: str):
    def probe(exec_fn):
        rc, out = exec_fn(container, ["python", script_path])
        tail = out.strip().splitlines()[-1] if out.strip() else f"rc={rc}"
        return rc == 0, tail
    return probe


def _zeppelin_probe(exec_fn):
    """Zeppelin is probed from the host via its REST API (published port)."""
    import os

    import requests

    infra_env = Path(__file__).resolve().parents[2] / "infra" / ".env"
    port = ""
    if infra_env.exists():
        for line in infra_env.read_text(encoding="utf-8").splitlines():
            if line.startswith("ZEPPELIN_PORT="):
                port = line.split("=", 1)[1].strip()
    port = os.environ.get("ZEPPELIN_PORT", port)
    if not port:
        return False, "ZEPPELIN_PORT not found"
    r = requests.get(f"http://localhost:{port}/api/interpreter/setting", timeout=30)
    ok = r.status_code == 200 and "spark" in r.text
    return ok, f"interpreter API status={r.status_code}, spark-present={'spark' in r.text}"


ICEBERG_ON = _truthy("ICEBERG_REST_ENABLED")

EDGES = [
    Edge("spark->minio+iceberg", ICEBERG_ON, _run_in("jupyterhub", "/opt/probes/probe_spark.py")),
    Edge("jupyter->pyiceberg", ICEBERG_ON, _run_in("jupyterhub", "/opt/probes/probe_pyiceberg.py")),
    Edge("airflow->minio+spark", True, _run_in("airflow-scheduler", "/opt/probes/probe_airflow.py")),
    Edge("zeppelin->spark", True, _zeppelin_probe),
]


def main() -> int:
    from manifest import daemon_ok  # reuse Layer 1's real daemon check

    results = run_layer2(EDGES, default_exec, daemon_ok())
    print(render_matrix(results))
    return 1 if any(r.status == "fail" for r in results) else 0


if __name__ == "__main__":
    sys.exit(main())
