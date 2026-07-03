"""Validation for datasets/registry.yaml. Pure functions, no I/O."""
from __future__ import annotations

VALID_KINDS = {"http", "tpch"}


def validate_registry(doc: dict) -> list[str]:
    """Return a list of human-readable error strings; empty means valid."""
    errors: list[str] = []
    if doc.get("version") != 1:
        errors.append("registry: 'version' must be 1")
    datasets = doc.get("datasets")
    if not isinstance(datasets, dict) or not datasets:
        errors.append("registry: 'datasets' must be a non-empty mapping")
        return errors

    for name, ds in datasets.items():
        p = f"datasets.{name}"
        for field in ("description", "format", "license", "landing_prefix", "fetch", "scales"):
            if field not in ds:
                errors.append(f"{p}: missing '{field}'")
        if "fetch" in ds:
            kind = (ds.get("fetch") or {}).get("kind")
            if kind not in VALID_KINDS:
                errors.append(f"{p}: fetch.kind '{kind}' not in {sorted(VALID_KINDS)}")
        else:
            kind = None
        if "scales" in ds:
            scales = ds.get("scales") or {}
            if not scales:
                errors.append(f"{p}: 'scales' must define at least one tier")
        else:
            scales = {}
        for tier, spec in scales.items():
            if kind == "http" and not spec.get("urls"):
                errors.append(f"{p}.scales.{tier}: http datasets require a non-empty 'urls' list")
            if kind == "tpch" and "sf" not in spec:
                errors.append(f"{p}.scales.{tier}: tpch datasets require 'sf'")
    return errors
