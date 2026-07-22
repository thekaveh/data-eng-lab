"""Harness for live Scala/PySpark parity testing.

Heavy deps (pyiceberg, requests) are imported INSIDE functions so this module
is safe to import in offline/CI environments where those packages may not be
available or the live stack is not running.

Container name convention: ``{PROJECT_NAME}-{service}`` (mirrors manifest.py).
"""
from __future__ import annotations

import hashlib
import os
import subprocess
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
INFRA_ENV = ROOT / "infra" / ".env"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _env_val(key: str, default: str = "") -> str:
    """Read a key from infra/.env; env-var takes precedence."""
    val = os.environ.get(key)
    if val is not None:
        return val
    if INFRA_ENV.exists():
        found = ""
        for line in INFRA_ENV.read_text(encoding="utf-8").splitlines():
            if line.startswith(f"{key}="):
                found = line.split("=", 1)[1].strip()  # last assignment wins
        if found:
            return found
    return default


def _resolve_project() -> str:
    """PROJECT_NAME: env var > infra/.env > default (mirrors manifest.py)."""
    return _env_val("PROJECT_NAME", "data-eng-lab")


def _rest_catalog_kwargs() -> dict:
    """Build pyiceberg RestCatalog kwargs from infra/.env / env vars."""
    port = _env_val("ICEBERG_REST_PORT")
    minio_port = _env_val("MINIO_PORT")
    user = _env_val("MINIO_ROOT_USER")
    password = _env_val("MINIO_ROOT_PASSWORD")
    if not port:
        raise RuntimeError("ICEBERG_REST_PORT not set — is the live Iceberg stack up?")
    return {
        "uri": f"http://localhost:{port}",
        "warehouse": "s3a://lakehouse/",
        "s3.endpoint": f"http://localhost:{minio_port}",
        "s3.access-key-id": user,
        "s3.secret-access-key": password,
        "s3.path-style-access": "true",
    }


# ---------------------------------------------------------------------------
# Identifier normalization (issue #44)
# ---------------------------------------------------------------------------

def _catalog_identifier(table: str, catalog: str = "lakehouse") -> str:
    """Normalize a table identifier for pyiceberg's ``RestCatalog``.

    Spark addresses tables with a 3-part ``<catalog>.<namespace>.<table>``
    identifier, but pyiceberg's ``RestCatalog`` (constructed with ``name=catalog``)
    addresses by ``<namespace>.<table>`` — the catalog is the connection, not part
    of the wire identifier. Passing the Spark form makes pyiceberg read
    ``lakehouse`` as the first namespace level → ``NoSuchTableError``.

    Strip a leading ``<catalog>.`` selector so callers may pass either form; a
    3-part identifier under a *different* catalog is left intact so the mismatch
    surfaces rather than being silently mangled.
    """
    prefix = f"{catalog}."
    if table.startswith(prefix) and table.count(".") >= 2:
        return table[len(prefix):]
    return table


# ---------------------------------------------------------------------------
# snapshot_table
# ---------------------------------------------------------------------------

def snapshot_table(table: str) -> dict:
    """Snapshot an Iceberg table via pyiceberg + pyarrow.

    Returns ``{"schema": [sorted "name:type"], "row_count": int, "checksum": str}``.
    Heavy imports (pyiceberg, pyarrow) are inside this function — safe to import
    at module level without the live stack.

    The checksum is the first 16 hex chars of SHA-256 of the row data sorted by all
    columns, giving a deterministic fingerprint independent of insertion order.
    """
    from pyiceberg.catalog.rest import RestCatalog  # noqa: PLC0415

    catalog = RestCatalog(name="lakehouse", **_rest_catalog_kwargs())
    tbl = catalog.load_table(_catalog_identifier(table))
    arrow_tbl = tbl.scan().to_arrow()

    # Schema: sorted "name:type" strings
    schema = sorted(
        f"{field.name}:{field.field_type}"
        for field in tbl.schema().fields
    )

    row_count = len(arrow_tbl)

    # Deterministic checksum: SHA-256 of sorted-rows repr
    if row_count > 0:
        sort_keys = [(col, "ascending") for col in arrow_tbl.column_names]
        sorted_tbl = arrow_tbl.sort_by(sort_keys)
        rows_bytes = repr(sorted_tbl.to_pydict()).encode()
    else:
        rows_bytes = b""
    checksum = hashlib.sha256(rows_bytes).hexdigest()[:16]

    return {"schema": schema, "row_count": row_count, "checksum": checksum}


# ---------------------------------------------------------------------------
# drop_table  (so PySpark writes fresh after Scala run)
# ---------------------------------------------------------------------------

def drop_table(table: str) -> None:
    """Drop an Iceberg table via pyiceberg REST catalog (best-effort; no-op if absent)."""
    from pyiceberg.catalog.rest import RestCatalog  # noqa: PLC0415
    from pyiceberg.exceptions import NoSuchTableError  # noqa: PLC0415

    catalog = RestCatalog(name="lakehouse", **_rest_catalog_kwargs())
    try:
        catalog.drop_table(_catalog_identifier(table))
    except NoSuchTableError:
        pass  # table may not exist yet on first run


# ---------------------------------------------------------------------------
# run_zeppelin_note
# ---------------------------------------------------------------------------

def run_zeppelin_note(zpln_path: str, port_env: str = "ZEPPELIN_PORT") -> None:
    """Import a .zpln notebook via Zeppelin REST API, run all paragraphs, then delete.

    Zeppelin REST endpoints used:
      POST   /api/notebook/import     — import JSON notebook; returns {"body": "<note-id>"}
      POST   /api/notebook/job/{id}   — run all paragraphs; returns 200/201 on acceptance
      GET    /api/notebook/job/{id}   — poll paragraph statuses
      DELETE /api/notebook/{id}       — clean up after execution

    Paragraph statuses that indicate completion: FINISHED, READY.
    Any ERROR status raises RuntimeError.
    Timeout: 10 minutes (600 s), polled every 5 s.

    Heavy import (requests) is inside this function.
    """
    import requests  # noqa: PLC0415

    port = _env_val(port_env)
    if not port:
        raise RuntimeError(f"{port_env} not set — is the live Zeppelin stack up?")

    base = f"http://localhost:{port}/api"
    note_content = Path(zpln_path).read_text(encoding="utf-8")

    # Import note
    resp = requests.post(
        f"{base}/notebook/import",
        headers={"Content-Type": "application/json"},
        data=note_content,
        timeout=30,
    )
    resp.raise_for_status()
    body = resp.json().get("body")
    if not body:
        raise RuntimeError(f"Zeppelin import returned no note id: {resp.text}")
    note_id = body

    try:
        # Kick off run of all paragraphs
        run_resp = requests.post(f"{base}/notebook/job/{note_id}", timeout=30)
        run_resp.raise_for_status()

        # Poll for completion (max 10 minutes)
        deadline = time.time() + 600
        while time.time() < deadline:
            time.sleep(5)
            status_resp = requests.get(f"{base}/notebook/job/{note_id}", timeout=30)
            status_resp.raise_for_status()
            body = status_resp.json().get("body", [])
            if isinstance(body, dict):
                body = body.get("paragraphs", list(body.values()))
            paragraphs = [p for p in body if isinstance(p, dict)]
            if not paragraphs:
                continue
            statuses = {p.get("status") for p in paragraphs}
            if "ERROR" in statuses or "ABORT" in statuses:
                raise RuntimeError(f"Zeppelin paragraph error/abort in note {note_id}; statuses={statuses}")
            # All paragraphs in a terminal state
            if statuses <= {"FINISHED", "READY"}:
                return
        raise RuntimeError(f"Zeppelin note {note_id} timed out after 10 minutes")
    finally:
        # Best-effort cleanup — don't mask original exception
        try:
            requests.delete(f"{base}/notebook/{note_id}", timeout=15)
        except Exception:  # noqa: BLE001
            pass


# ---------------------------------------------------------------------------
# run_jupyter_note
# ---------------------------------------------------------------------------

def run_jupyter_note(ipynb_path: str, project: str | None = None) -> None:
    """Execute a Jupyter notebook inside the jupyterhub container.

    The scenarios directory is NOT mounted into jupyterhub
    (compose/data-eng-lab.yml mounts only ``datasets`` and ``probes``).
    We therefore ``docker cp`` the notebook into the container first, then execute
    it via papermill (or nbconvert as fallback if papermill is absent).

    Execution path inside container: /tmp/<notebook-basename>
    Output path inside container:    /tmp/out_<notebook-stem>.ipynb

    Args:
        ipynb_path: host path to the .ipynb file (absolute or relative to repo root).
        project: Docker Compose project name; defaults to ``_resolve_project()``.

    Raises:
        RuntimeError: if docker cp fails or notebook execution exits non-zero.
    """
    if project is None:
        project = _resolve_project()

    container = f"{project}-jupyterhub"
    nb_host = Path(ipynb_path) if Path(ipynb_path).is_absolute() else ROOT / ipynb_path
    nb_container = f"/tmp/{nb_host.name}"
    out_container = f"/tmp/out_{nb_host.stem}.ipynb"

    # Copy notebook into container
    cp = subprocess.run(
        ["docker", "cp", str(nb_host), f"{container}:{nb_container}"],
        capture_output=True, text=True, timeout=30,
    )
    if cp.returncode != 0:
        raise RuntimeError(
            f"docker cp to {container} failed (rc={cp.returncode}): {cp.stderr.strip()}"
        )

    # Try papermill; fall back to nbconvert if papermill binary not found
    cmd = (
        f"if command -v papermill >/dev/null 2>&1; then "
        f"  papermill {nb_container} {out_container} -k python3 --no-progress-bar 2>&1; "
        f"else "
        f"  jupyter nbconvert --to notebook --execute --inplace {nb_container} 2>&1; "
        f"fi"
    )

    result = subprocess.run(
        ["docker", "exec", container, "sh", "-c", cmd],
        capture_output=True, text=True, timeout=600,
    )
    combined = (result.stdout + result.stderr).strip()
    if result.returncode != 0:
        raise RuntimeError(
            f"Notebook execution failed in {container} (rc={result.returncode}):\n{combined}"
        )
