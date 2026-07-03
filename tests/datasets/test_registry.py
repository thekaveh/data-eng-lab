from pathlib import Path

import pytest

from datasets import registry as reg

ROOT = Path(__file__).resolve().parents[2]
REAL = ROOT / "datasets" / "registry.yaml"


def test_load_real_registry_has_core_datasets():
    ds = reg.load_registry(REAL)
    assert {"nyc_taxi", "gh_archive", "movielens", "tpch"} <= set(ds)
    assert ds["nyc_taxi"].kind == "http"
    assert ds["tpch"].kind == "tpch"
    assert ds["movielens"].unzip is True


def test_resolve_http_scale_returns_urls():
    ds = reg.load_registry(REAL)["nyc_taxi"]
    plan = reg.resolve_scale(ds, "tiny")
    assert plan.urls and plan.sf is None
    assert all(u.startswith("http") for u in plan.urls)


def test_resolve_tpch_scale_returns_sf():
    ds = reg.load_registry(REAL)["tpch"]
    plan = reg.resolve_scale(ds, "small")
    assert plan.sf == 1 and plan.urls == []


def test_unknown_scale_raises():
    ds = reg.load_registry(REAL)["nyc_taxi"]
    with pytest.raises(KeyError):
        reg.resolve_scale(ds, "gigantic")


def test_invalid_registry_raises(tmp_path: Path):
    bad = tmp_path / "r.yaml"
    bad.write_text("version: 1\ndatasets: {}\n")
    with pytest.raises(ValueError):
        reg.load_registry(bad)
