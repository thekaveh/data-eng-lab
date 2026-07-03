import tomllib
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def test_ruff_extend_exclude_is_anchored_to_the_atlas_submodule():
    cfg = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    excludes = cfg["tool"]["ruff"].get("extend-exclude", [])
    # A bare 'infra' also swallows tests/infra/ — the exclude MUST be anchored.
    assert "infra" not in excludes, (
        "extend-exclude has a bare 'infra' that also excludes tests/infra/; anchor it (e.g. ./infra/**)"
    )
