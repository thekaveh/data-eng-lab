import importlib.util
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
_spec = importlib.util.spec_from_file_location("schema", ROOT / "datasets" / "schema.py")
schema = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(schema)


def _valid_doc():
    return {
        "version": 1,
        "datasets": {
            "nyc_taxi": {
                "description": "d", "format": "parquet", "license": "public",
                "landing_prefix": "nyc_taxi", "fetch": {"kind": "http"},
                "scales": {"tiny": {"urls": ["https://x/a.parquet"]}},
            }
        },
    }


def test_valid_registry_has_no_errors():
    assert schema.validate_registry(_valid_doc()) == []


def test_missing_landing_prefix_is_error():
    doc = _valid_doc()
    del doc["datasets"]["nyc_taxi"]["landing_prefix"]
    errs = schema.validate_registry(doc)
    assert any("landing_prefix" in e for e in errs), errs


def test_unknown_fetch_kind_is_error():
    doc = _valid_doc()
    doc["datasets"]["nyc_taxi"]["fetch"]["kind"] = "ftp"
    errs = schema.validate_registry(doc)
    assert any("fetch.kind" in e for e in errs), errs


def test_http_scale_requires_urls():
    doc = _valid_doc()
    doc["datasets"]["nyc_taxi"]["scales"]["tiny"] = {}
    errs = schema.validate_registry(doc)
    assert any("urls" in e for e in errs), errs
