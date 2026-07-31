"""Resolve host-side endpoints from explicit Atlas consumer configuration."""
from __future__ import annotations

import os
from collections.abc import Mapping
from pathlib import Path


def read_env_file(path: Path) -> dict[str, str]:
    """Return assignments in an env file, with the last assignment taking precedence."""
    values: dict[str, str] = {}
    if not path.exists():
        return values
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        values[key] = value.strip()
    return values


def env_value(key: str, *, env: Mapping[str, str], env_file: Path) -> str:
    """Read an environment value, falling back to the generated Atlas env file."""
    if key in env and env[key].strip():
        return env[key].strip()
    return read_env_file(env_file).get(key, "").strip()


def resolve_http_endpoint(
    override_key: str,
    port_key: str,
    *,
    env: Mapping[str, str] | None = None,
    env_file: Path,
    export_key: str | None = None,
    export_file: Path | None = None,
) -> str:
    """Resolve an endpoint from an override, supported export, or Atlas port."""
    values = os.environ if env is None else env
    override = values.get(override_key, "").strip()
    if override:
        return override
    if export_key and export_file:
        exported = read_env_file(export_file).get(export_key, "").strip()
        if exported:
            return exported
    port = env_value(port_key, env=values, env_file=env_file)
    if port:
        return f"http://localhost:{port}"
    raise RuntimeError(
        f"{override_key} or {port_key} is required; start Atlas and resolve its endpoint first"
    )
