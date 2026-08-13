"""Exhaustively re-execute both notebook surfaces for every scenario."""

from __future__ import annotations

import importlib.util
import os
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
pytestmark = pytest.mark.infra

SCENARIOS = (
    "batch_ingest-nyc_taxi-spark-iceberg",
    "medallion-nyc_taxi-spark-iceberg",
    "data_quality-nyc_taxi-spark-iceberg",
    "schema_evolution-gh_archive-spark-iceberg",
    "time_travel-nyc_taxi-spark-iceberg",
    "table_maintenance-nyc_taxi-spark-iceberg",
    "streaming_ingest-events-spark-iceberg",
    "streaming_ingest-gh_archive-spark-iceberg",
    "streaming_windows-events-spark-iceberg",
    "cdc_streaming-online_retail-spark-iceberg",
    "federated_query-nyc_taxi-trino-iceberg",
    "star_schema-tpch-spark-iceberg",
    "bi_query-tpch-trino-iceberg",
    "join_optimization-tpch-spark-iceberg",
    "feature_engineering-movielens-spark-iceberg",
    "incremental_upsert-online_retail-spark-iceberg",
    "scd2-online_retail-spark-iceberg",
    "json_flatten-gh_archive-spark-iceberg",
    "sessionization-gh_archive-spark-iceberg",
)

OUTPUT_TABLES = {
    "batch_ingest-nyc_taxi-spark-iceberg": ("lakehouse.bronze.nyc_taxi_trips",),
    "medallion-nyc_taxi-spark-iceberg": (
        "lakehouse.silver.nyc_taxi_trips",
        "lakehouse.gold.nyc_taxi_daily",
    ),
    "data_quality-nyc_taxi-spark-iceberg": (
        "lakehouse.silver.nyc_taxi_clean",
        "lakehouse.silver.nyc_taxi_quarantine",
    ),
    "schema_evolution-gh_archive-spark-iceberg": ("lakehouse.silver.gh_events_se",),
    "time_travel-nyc_taxi-spark-iceberg": ("lakehouse.silver.nyc_taxi_tt",),
    "table_maintenance-nyc_taxi-spark-iceberg": ("lakehouse.silver.nyc_taxi_tm",),
    "streaming_ingest-events-spark-iceberg": ("lakehouse.bronze.events",),
    "streaming_ingest-gh_archive-spark-iceberg": ("lakehouse.bronze.gh_events_stream",),
    "streaming_windows-events-spark-iceberg": ("lakehouse.gold.event_windows",),
    "cdc_streaming-online_retail-spark-iceberg": ("lakehouse.silver.online_retail_cdc",),
    "federated_query-nyc_taxi-trino-iceberg": ("lakehouse.gold.nyc_taxi_daily_trino",),
    "star_schema-tpch-spark-iceberg": (
        "lakehouse.gold.dim_customer",
        "lakehouse.gold.fct_orders",
    ),
    "bi_query-tpch-trino-iceberg": ("lakehouse.gold.bi_segment_revenue",),
    "join_optimization-tpch-spark-iceberg": ("lakehouse.gold.tpch_segment_revenue",),
    "feature_engineering-movielens-spark-iceberg": (
        "lakehouse.gold.ml_user_features",
        "lakehouse.gold.ml_movie_features",
    ),
    "incremental_upsert-online_retail-spark-iceberg": ("lakehouse.silver.online_retail",),
    "scd2-online_retail-spark-iceberg": ("lakehouse.gold.dim_customer_scd2",),
    "json_flatten-gh_archive-spark-iceberg": ("lakehouse.silver.gh_events",),
    "sessionization-gh_archive-spark-iceberg": ("lakehouse.silver.gh_sessions",),
}

CHECKPOINTS = {
    "streaming_ingest-events-spark-iceberg": "events",
    "streaming_ingest-gh_archive-spark-iceberg": "gh_events_file",
    "streaming_windows-events-spark-iceberg": "event_windows",
    "cdc_streaming-online_retail-spark-iceberg": "online_retail_cdc",
}

# This legacy reset exists only to give paired notebooks equivalent state on an
# exclusive disposable stack. The GH Archive value is deliberately identified as
# an unsafe family-root reset: issue #86 must replace it with the approved exact-
# generation, lease/tombstone-controlled implementation before any schedule exists.
CHECKPOINT_RESET_POLICY = {
    "mode": "exclusive_disposable_stack_only",
    "unsafe_roots": ("gh_events_file",),
    "networked_replacement_issue": 86,
    "schedule": "disabled",
}

DISCOVERED = {
    path.name
    for path in (ROOT / "scenarios").iterdir()
    if path.is_dir() and (path / "zeppelin/notebook.zpln").is_file() and (path / "jupyter/notebook.ipynb").is_file()
}
assert len(SCENARIOS) == 19
assert len(set(SCENARIOS)) == 19
assert set(SCENARIOS) == DISCOVERED
assert set(OUTPUT_TABLES) == set(SCENARIOS)
assert set(CHECKPOINTS) < set(SCENARIOS)


def _live_exec():
    path = ROOT / "tests/scenarios/live_exec.py"
    spec = importlib.util.spec_from_file_location("notebook_reproducibility_live_exec", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _reset_scenario(live_exec, scenario: str) -> None:
    """Remove only the output state owned by one scenario execution."""
    for table in OUTPUT_TABLES[scenario]:
        live_exec.drop_table(table)
    if checkpoint := CHECKPOINTS.get(scenario):
        live_exec.clear_checkpoint(checkpoint)


@pytest.mark.skipif(
    os.environ.get("RUN_INFRA") != "1",
    reason="set RUN_INFRA=1 with a prepared Atlas data-eng stack",
)
@pytest.mark.parametrize("scenario", SCENARIOS)
def test_paired_notebooks_reexecute_to_completion(scenario: str):
    """Run both peers from equivalent scenario-local output state."""
    live_exec = _live_exec()
    root = ROOT / "scenarios" / scenario
    bounded_stream = scenario in CHECKPOINTS
    _reset_scenario(live_exec, scenario)
    live_exec.run_zeppelin_note(str(root / "zeppelin/notebook.zpln"), bounded_stream=bounded_stream)
    _reset_scenario(live_exec, scenario)
    live_exec.run_jupyter_note(str(root / "jupyter/notebook.ipynb"), bounded_stream=bounded_stream)
