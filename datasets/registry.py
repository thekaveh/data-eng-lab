"""Load and resolve datasets/registry.yaml."""
from __future__ import annotations

import importlib.util
import sys
import types
from dataclasses import dataclass
from pathlib import Path

import yaml

# Workaround for importlib.util.exec_module compatibility with @dataclass decorators
# When loaded via importlib with a custom module name, sys.modules might not have the
# module registered before class definitions. Create a placeholder module entry first.
if __name__ not in sys.modules:
    _placeholder = types.ModuleType(__name__)
    _placeholder.__file__ = __file__
    sys.modules[__name__] = _placeholder

# Load schema by path to support importlib-loaded test harness
_schema_path = Path(__file__).resolve().parent / "schema.py"
_spec = importlib.util.spec_from_file_location("_schema_local", _schema_path)
_schema_module = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_schema_module)
validate_registry = _schema_module.validate_registry


@dataclass(frozen=True)
class Dataset:
    name: str
    description: str
    format: str
    license: str
    landing_prefix: str
    kind: str
    unzip: bool
    scales: dict


@dataclass(frozen=True)
class ScalePlan:
    dataset: Dataset
    scale: str
    urls: list[str]
    sf: float | None


def load_registry(path: Path) -> dict[str, Dataset]:
    doc = yaml.safe_load(Path(path).read_text(encoding="utf-8"))
    errors = validate_registry(doc)
    if errors:
        raise ValueError("invalid registry:\n  - " + "\n  - ".join(errors))
    out: dict[str, Dataset] = {}
    for name, ds in doc["datasets"].items():
        fetch = ds["fetch"]
        out[name] = Dataset(
            name=name,
            description=ds["description"],
            format=ds["format"],
            license=ds["license"],
            landing_prefix=ds["landing_prefix"],
            kind=fetch["kind"],
            unzip=bool(fetch.get("unzip", False)),
            scales=ds["scales"],
        )
    return out


def resolve_scale(ds: Dataset, scale: str) -> ScalePlan:
    if scale not in ds.scales:
        raise KeyError(f"dataset '{ds.name}' has no scale '{scale}' (have: {sorted(ds.scales)})")
    spec = ds.scales[scale]
    return ScalePlan(
        dataset=ds,
        scale=scale,
        urls=list(spec.get("urls", [])),
        sf=spec.get("sf"),
    )


# Sync module definitions with sys.modules entry (for importlib.util.exec_module compat)
if __name__ in sys.modules:
    sys.modules[__name__].__dict__.update(globals())
