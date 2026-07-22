"""Offline unit tests for live_exec pure-Python helpers.

Issue #44: pyiceberg's RestCatalog (already named 'lakehouse') addresses tables
by `namespace.table`, NOT the Spark-style 3-part `lakehouse.namespace.table`.
`_catalog_identifier` strips a leading catalog selector so callers can pass
either form and pyiceberg receives the 2-part identifier it expects.
"""
import importlib.util
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def _live_exec():
    spec = importlib.util.spec_from_file_location(
        "live_exec", ROOT / "tests" / "scenarios" / "live_exec.py"
    )
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    return m


le = _live_exec()


def test_strips_lakehouse_catalog_prefix():
    """The Spark 3-part identifier drops its 'lakehouse.' catalog selector."""
    assert le._catalog_identifier("lakehouse.bronze.nyc_taxi_trips") == "bronze.nyc_taxi_trips"


def test_two_part_identifier_unchanged():
    """A pyiceberg-native 2-part identifier passes through untouched."""
    assert le._catalog_identifier("bronze.nyc_taxi_trips") == "bronze.nyc_taxi_trips"


def test_strips_only_the_matching_catalog_name():
    """Only the catalog whose name matches the RestCatalog ('lakehouse') is stripped;
    an unrelated 3-part identifier is left intact (surfaced, not silently mangled)."""
    assert le._catalog_identifier("other.bronze.tbl") == "other.bronze.tbl"


def test_custom_catalog_name():
    """The catalog name to strip is parameterizable."""
    assert le._catalog_identifier("warehouse.silver.t", catalog="warehouse") == "silver.t"


def test_silver_and_gold_namespaces():
    assert le._catalog_identifier("lakehouse.silver.orders") == "silver.orders"
    assert le._catalog_identifier("lakehouse.gold.metrics") == "gold.metrics"
