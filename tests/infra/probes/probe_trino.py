"""Probe: Trino queries the Iceberg REST catalog (lakehouse) via the Trino CLI.

Importable-by-package: call ``probe(exec_fn)`` to get ``(ok: bool, message: str)``.
The exec_fn should target the ``trino`` container where the ``trino`` CLI is available.
"""
from __future__ import annotations

import sys

_TRINO_CMD = ["trino", "--execute", "SHOW SCHEMAS FROM lakehouse"]


def probe(exec_fn):
    """Run SHOW SCHEMAS FROM lakehouse via the Trino CLI in the trino container.

    Proves Trino <-> Iceberg REST <-> MinIO by querying the ``lakehouse`` catalog.
    Returns (ok: bool, message: str).
    """
    rc, out = exec_fn("trino", _TRINO_CMD)
    if rc != 0:
        tail = out.strip().splitlines()[-1] if out.strip() else f"exit {rc}"
        return False, f"trino CLI exit {rc}: {tail[:200]}"
    schemas = [line for line in out.splitlines() if line.strip() and line.strip() != "Schema"]
    if not schemas:
        return False, "trino CLI returned no output (no schemas found in lakehouse)"
    return True, f"trino->lakehouse OK: schemas={schemas[:5]}"


if __name__ == "__main__":
    # Standalone: run from inside the trino container (``trino`` CLI present there).
    import os
    import subprocess

    if os.environ.get("RUN_INFRA") != "1":
        print("Set RUN_INFRA=1 to run this probe standalone.", file=sys.stderr)
        sys.exit(0)
    res = subprocess.run(_TRINO_CMD, capture_output=True, text=True, timeout=60)
    _ok = bool(res.returncode == 0 and res.stdout.strip())
    _msg = (
        f"trino->lakehouse OK: {res.stdout.strip()[:80]}"
        if _ok
        else f"trino CLI exit {res.returncode}: {res.stderr.strip()[:200]}"
    )
    print(_msg, file=sys.stdout if _ok else sys.stderr)
    sys.exit(0 if _ok else 1)
