"""The `live` dependency group declares the client libs the RUN_INFRA suite needs.

Issue #43: `tests/lakehouse/test_bronze_smoke.py` (pyspark),
`tests/scenarios/test_streaming_live.py` (kafka-python), and
`tests/scenarios/test_trino_query_live.py` (trino) do gated imports inside
infra-marked tests; without these declared, `RUN_INFRA=1` runs are not
reproducible from a clean checkout.
"""
import tomllib
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def _groups() -> dict:
    cfg = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    return cfg.get("dependency-groups", {})


def test_live_group_exists():
    assert "live" in _groups(), "pyproject.toml must declare a [dependency-groups] 'live' group"


def test_live_group_declares_client_libs():
    deps = " ".join(_groups().get("live", []))
    assert "pyspark" in deps, "live group must declare pyspark"
    assert "kafka-python" in deps, "live group must declare kafka-python"
    assert "trino" in deps, "live group must declare trino"


def test_live_group_not_in_dev():
    """Live client libs stay OUT of the dev group so offline CI (which installs
    dev) never pulls them — keeps the offline install lean and CI unaffected."""
    dev = " ".join(_groups().get("dev", []))
    assert "pyspark" not in dev
    assert "kafka-python" not in dev
    assert "trino" not in dev
