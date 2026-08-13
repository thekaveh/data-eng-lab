from __future__ import annotations

import re
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
QUERY_DIR = ROOT / "spark-apps/nyc-taxi-data-quality/queries"
QUERY_NAMES = ("latest", "trend", "operator_attention")
FACTS = "lakehouse.gold.nyc_taxi_quality_facts"


def _sql(name: str) -> str:
    return (QUERY_DIR / f"{name}.sql").read_text(encoding="utf-8")


def _compact(sql: str) -> str:
    return re.sub(r"\s+", " ", sql.strip()).lower()


@pytest.mark.parametrize("name", QUERY_NAMES)
def test_registry_is_fixed_single_read_only_bounded_statement(name):
    raw = _sql(name)
    sql = _compact(raw)
    assert sql.startswith(("select ", "with "))
    table_names = set(re.findall(r"lakehouse\.[a-z0-9_]+\.[a-z0-9_]+", sql))
    assert table_names == {FACTS}
    assert ";" not in raw and "--" not in raw and "/*" not in raw and "*/" not in raw
    assert "{" not in raw and "}" not in raw and "?" not in raw
    assert re.search(r"\b(insert|update|delete|merge|create|drop|alter|call|set|grant|revoke)\b", sql) is None
    assert "select *" not in sql and " order by " in sql and " limit " in sql
    assert "current_" not in sql and "random(" not in sql and "now(" not in sql


def test_latest_returns_the_latest_complete_accepted_eight_fact_set():
    sql = _compact(_sql("latest"))
    for fragment in (
        "having count(*) = 8",
        "count_if(f.status not in ('pass', 'warn')) = 0",
        "order by logical_date desc, source_snapshot_id desc, quality_run_id desc",
        "limit 1",
        "order by f.layer, f.rule_id",
        "cast(f.metric_value as decimal(38, 9)) as metric_value",
        "logical_date_utc",
    ):
        assert fragment in sql
    assert "limit 8" in sql
    for rule in (
        "bronze.source_available.v1",
        "bronze.schema.v1",
        "bronze.snapshot_freshness.v1",
        "bronze.invalid_ratio.v1",
        "silver.partition_conservation.v1",
        "silver.clean_nonempty.v1",
        "silver.quarantine_ratio.v1",
        "silver.output_readback.v1",
    ):
        assert f"('{rule}', 'nyc_taxi_quality_v1')" in sql
    for fragment in (
        "count(distinct f.dataset_id) = 1",
        "min(f.dataset_id) = 'nyc_taxi'",
        "count(distinct f.binding_type) = 1",
        "min(f.binding_type) = 'iceberg_snapshot'",
        "count(distinct f.source_snapshot_id) = 1",
        "count(distinct f.rule_id) = 8",
    ):
        assert fragment in sql


def test_trend_returns_at_most_ninety_complete_runs_with_exact_measures():
    sql = _compact(_sql("trend"))
    for fragment in (
        "having count(*) = 8",
        "count_if(f.status not in ('pass', 'warn')) = 0",
        "limit 90",
        "order by logical_date desc, source_snapshot_id desc, quality_run_id desc",
        "as source_row_count",
        "as invalid_row_count",
        "as invalid_ratio",
        "as clean_row_count",
        "as quarantine_row_count",
        "as quarantine_ratio",
        "as overall_status",
    ):
        assert fragment in sql
    assert sql.count("decimal(38, 9)") == 2
    assert "count(distinct f.rule_id) = 8" in sql
    assert "count(distinct f.source_snapshot_id) = 1" in sql


def test_operator_attention_is_exact_and_status_ordered():
    sql = _compact(_sql("operator_attention"))
    assert "where f.status in ('warn', 'fail', 'missing', 'stale')" in sql
    assert "limit 100" in sql
    for column in (
        "diagnostic_code",
        "owner",
        "source_snapshot_id",
        "metric_numerator",
        "metric_denominator",
        "warn_threshold",
        "fail_threshold",
    ):
        assert column in sql
    assert (
        "case f.status when 'missing' then 4 when 'stale' then 3 when 'fail' then 2 when 'warn' then 1 end desc"
        in sql
    )
    assert "order by f.logical_date desc" in sql and "f.layer, f.rule_id" in sql


@pytest.mark.parametrize("name", QUERY_NAMES)
def test_timestamp_with_time_zone_is_formatted_directly_as_canonical_utc(name):
    sql = _compact(_sql(name))
    assert "with_timezone(" not in sql
    assert re.search(
        r"format_datetime\(\s*(?:f\.)?logical_date, 'yyyy-mm-dd''t''hh:mm:ss''z'''\)",
        sql,
    )
