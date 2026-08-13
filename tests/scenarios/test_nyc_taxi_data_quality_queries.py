from __future__ import annotations

import hashlib
import re
from datetime import datetime
from pathlib import Path

import duckdb
import pytest

ROOT = Path(__file__).resolve().parents[2]
QUERY_DIR = ROOT / "spark-apps/nyc-taxi-data-quality/queries"
QUERY_NAMES = ("latest", "trend", "operator_attention")
FACTS = "lakehouse.gold.nyc_taxi_quality_facts"
SCHEMA_SHA256 = "5a8d2916cc5967c0eeb8318136c1262156cd616105dad67a713f1cb1cc872fc5"

RULES = (
    ("bronze.source_available.v1", "Bronze", "Data Engineering", "source_row_count", None, "rows=0"),
    ("bronze.schema.v1", "Bronze", "Data Engineering", "schema_match_ratio", None, "ratio<1.000000000"),
    (
        "bronze.snapshot_freshness.v1",
        "Bronze",
        "Data Engineering",
        "snapshot_age_seconds",
        None,
        "seconds>21600",
    ),
    (
        "bronze.invalid_ratio.v1",
        "Bronze",
        "Data Quality Engineering",
        "invalid_row_ratio",
        "ratio>0.010000000",
        "ratio>0.050000000",
    ),
    (
        "silver.partition_conservation.v1",
        "Silver",
        "Data Quality Engineering",
        "partition_row_ratio",
        None,
        "ratio!=1.000000000",
    ),
    ("silver.clean_nonempty.v1", "Silver", "Data Quality Engineering", "clean_row_count", None, "rows=0"),
    (
        "silver.quarantine_ratio.v1",
        "Silver",
        "Data Quality Engineering",
        "quarantine_row_ratio",
        "ratio>0.010000000",
        "ratio>0.050000000",
    ),
    (
        "silver.output_readback.v1",
        "Silver",
        "Data Platform Engineering",
        "readback_check_ratio",
        None,
        "ratio<1.000000000",
    ),
)


def _sql(name: str) -> str:
    return (QUERY_DIR / f"{name}.sql").read_text(encoding="utf-8")


def _compact(sql: str) -> str:
    return re.sub(r"\s+", " ", sql.strip()).lower()


def _facts_connection(rows):
    connection = duckdb.connect(":memory:")
    connection.execute(
        """CREATE TABLE facts (
            quality_run_id VARCHAR, logical_date TIMESTAMP, data_interval_end TIMESTAMP,
            dataset_id VARCHAR, binding_type VARCHAR, upstream_dag_id VARCHAR,
            source_table VARCHAR, source_snapshot_id BIGINT, source_snapshot_committed_at TIMESTAMP,
            source_schema_sha256 VARCHAR, layer VARCHAR, rule_id VARCHAR, rule_version VARCHAR,
            owner VARCHAR, metric_name VARCHAR, metric_numerator BIGINT, metric_denominator BIGINT,
            metric_value DECIMAL(38, 9), warn_threshold VARCHAR, fail_threshold VARCHAR,
            severity VARCHAR, status VARCHAR, diagnostic_code VARCHAR
        )"""
    )
    connection.executemany("INSERT INTO facts VALUES (" + ",".join("?" for _ in range(23)) + ")", rows)
    return connection


def _accepted_rows():
    logical_date = datetime(2026, 8, 13, 10, 0, 0)
    interval_end = logical_date
    committed_at = datetime(2026, 8, 13, 10, 0, 21, 731000)
    run_id = hashlib.sha256(
        f"nyc_taxi\n{logical_date.strftime('%Y-%m-%dT%H:%M:%SZ')}\n123\nnyc_taxi_quality_v1".encode()
    ).hexdigest()
    metrics = {
        "bronze.source_available.v1": (100, None, "100.000000000"),
        "bronze.schema.v1": (20, 20, "1.000000000"),
        "bronze.snapshot_freshness.v1": (-22, 21600, "-22.000000000"),
        "bronze.invalid_ratio.v1": (1, 100, "0.010000000"),
        "silver.partition_conservation.v1": (100, 100, "1.000000000"),
        "silver.clean_nonempty.v1": (99, 100, "99.000000000"),
        "silver.quarantine_ratio.v1": (1, 100, "0.010000000"),
        "silver.output_readback.v1": (8, 8, "1.000000000"),
    }
    return [
        (
            run_id,
            logical_date,
            interval_end,
            "nyc_taxi",
            "iceberg_snapshot",
            "nyc_taxi_etl",
            "lakehouse.bronze.nyc_taxi_trips",
            123,
            committed_at,
            SCHEMA_SHA256,
            layer,
            rule_id,
            "nyc_taxi_quality_v1",
            owner,
            metric_name,
            metrics[rule_id][0],
            metrics[rule_id][1],
            metrics[rule_id][2],
            warn,
            fail,
            "info",
            "pass",
            "ok",
        )
        for rule_id, layer, owner, metric_name, warn, fail in RULES
    ]


def _complete_run_query():
    prefix = _sql("latest").split("),\nlatest_run AS (", maxsplit=1)[0]
    return (
        (prefix + ")\nSELECT quality_run_id FROM complete_runs")
        .replace(FACTS, "facts")
        .replace("lower(to_hex(sha256(to_utf8(concat(", "sha256(concat(")
        .replace("'nyc_taxi_quality_v1'\n       )))))", "'nyc_taxi_quality_v1'\n       ))")
        .replace(
            "format_datetime(max(f.logical_date), 'yyyy-MM-dd''T''HH:mm:ss''Z''')",
            "strftime(max(f.logical_date), '%Y-%m-%dT%H:%M:%SZ')",
        )
    )


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
        "f.quality_run_id = lower(to_hex(sha256(to_utf8(concat(",
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
        assert f"('{rule}', 'nyc_taxi_quality_v1'," in sql
    for fragment in (
        "count(distinct f.dataset_id) = 1",
        "min(f.dataset_id) = 'nyc_taxi'",
        "count(distinct f.binding_type) = 1",
        "min(f.binding_type) = 'iceberg_snapshot'",
        "count(distinct f.source_snapshot_id) = 1",
        "count(f.source_snapshot_id) = 8",
        "min(f.source_snapshot_id) > 0",
        "count(f.source_schema_sha256) = 8",
        "min(f.source_schema_sha256) = '5a8d2916cc5967c0eeb8318136c1262156cd616105dad67a713f1cb1cc872fc5'",
        "count(distinct f.logical_date) = 1",
        "count(distinct f.data_interval_end) = 1",
        "count(distinct f.upstream_dag_id) = 1",
        "min(f.upstream_dag_id) = 'nyc_taxi_etl'",
        "count(distinct f.source_snapshot_committed_at) = 1",
        "count(distinct f.rule_id) = 8",
    ):
        assert fragment in sql


def test_trend_returns_at_most_ninety_complete_runs_with_exact_measures():
    sql = _compact(_sql("trend"))
    for fragment in (
        "having count(*) = 8",
        "f.quality_run_id = lower(to_hex(sha256(to_utf8(concat(",
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
    assert sql.count("decimal(38, 9)") >= 2
    assert "count(distinct f.rule_id) = 8" in sql
    assert "count(distinct f.source_snapshot_id) = 1" in sql


def test_operator_attention_is_exact_and_status_ordered():
    sql = _compact(_sql("operator_attention"))
    assert "f.status in ('warn', 'fail', 'missing', 'stale')" in sql
    for diagnostic in (
        "threshold_warn", "threshold_fail", "source_missing", "source_stale",
        "schema_mismatch", "partition_mismatch", "output_empty", "readback_mismatch",
    ):
        assert f"'{diagnostic}'" in sql
    assert "f.rule_id = 'bronze.source_available.v1'" in sql
    assert "f.status = 'missing' and f.severity = 'error' and f.diagnostic_code = 'source_missing'" in sql
    assert "complete_runs" not in sql and "join complete_runs" not in sql
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


@pytest.mark.parametrize(
    ("column", "value"),
    (
        (1, None),
        (2, None),
        (3, None),
        (4, None),
        (5, None),
        (6, None),
        (7, None),
        (8, None),
        (9, None),
        (9, "f" * 64),
        (5, "wrong_etl"),
        (13, "wrong owner"),
        (14, "wrong_metric"),
        (19, "wrong threshold"),
    ),
)
def test_complete_run_cte_rejects_null_wrong_lineage_and_rule_metadata(column, value):
    rows = _accepted_rows()
    changed = list(rows[0])
    changed[column] = value
    rows[0] = tuple(changed)
    connection = _facts_connection(rows)
    assert connection.execute(_complete_run_query()).fetchall() == []


@pytest.mark.parametrize("mutation", ("missing", "duplicate", "foreign"))
def test_complete_run_cte_rejects_missing_duplicate_and_foreign_rows(mutation):
    rows = _accepted_rows()
    if mutation == "missing":
        rows.pop()
    elif mutation == "duplicate":
        rows.append(rows[0])
    else:
        changed = list(rows[0])
        changed[11] = "foreign.rule.v1"
        rows.append(tuple(changed))
    connection = _facts_connection(rows)
    assert connection.execute(_complete_run_query()).fetchall() == []


def test_complete_run_cte_admits_only_the_exact_accepted_fact_set():
    rows = _accepted_rows()
    connection = _facts_connection(rows)
    assert connection.execute(_complete_run_query()).fetchall() == [(rows[0][0],)]


def test_complete_run_cte_admits_the_exact_warning_band_and_conserved_partition():
    rows = _accepted_rows()
    updates = {
        "bronze.invalid_ratio.v1": (2, 100, "0.020000000", "warning", "warn", "threshold_warn"),
        "silver.clean_nonempty.v1": (98, 100, "98.000000000", "info", "pass", "ok"),
        "silver.quarantine_ratio.v1": (2, 100, "0.020000000", "warning", "warn", "threshold_warn"),
    }
    for index, row in enumerate(rows):
        if row[11] in updates:
            changed = list(row)
            numerator, denominator, value, severity, status, diagnostic = updates[row[11]]
            changed[15:18] = [numerator, denominator, value]
            changed[20:23] = [severity, status, diagnostic]
            rows[index] = tuple(changed)
    assert _facts_connection(rows).execute(_complete_run_query()).fetchall() == [(rows[0][0],)]


def test_complete_run_cte_rejects_stale_or_non_floor_freshness_values():
    for numerator, committed_at in (
        (21601, datetime(2026, 8, 13, 3, 59, 59)),
        (-21, datetime(2026, 8, 13, 10, 0, 21, 731000)),
    ):
        rows = _accepted_rows()
        rows = [tuple(committed_at if column == 8 else value for column, value in enumerate(row)) for row in rows]
        index = next(index for index, row in enumerate(rows) if row[11] == "bronze.snapshot_freshness.v1")
        changed = list(rows[index])
        changed[15] = numerator
        changed[17] = f"{numerator}.000000000"
        rows[index] = tuple(changed)
        assert _facts_connection(rows).execute(_complete_run_query()).fetchall() == []


@pytest.mark.parametrize(
    ("rule_id", "column", "value"),
    (
        ("silver.output_readback.v1", 15, 1),
        ("silver.output_readback.v1", 16, 1),
        ("silver.output_readback.v1", 17, "0.125000000"),
        ("bronze.source_available.v1", 15, 99),
        ("bronze.schema.v1", 15, 19),
        ("bronze.snapshot_freshness.v1", 16, 1),
        ("bronze.invalid_ratio.v1", 17, "0.020000000"),
        ("silver.partition_conservation.v1", 15, 99),
        ("silver.clean_nonempty.v1", 16, 99),
        ("silver.quarantine_ratio.v1", 15, 2),
        ("bronze.schema.v1", 20, "error"),
        ("bronze.schema.v1", 21, "warn"),
        ("bronze.schema.v1", 22, "threshold_warn"),
    ),
)
def test_complete_run_cte_rejects_wrong_rule_signal_values(rule_id, column, value):
    rows = _accepted_rows()
    index = next(index for index, row in enumerate(rows) if row[11] == rule_id)
    changed = list(rows[index])
    changed[column] = value
    rows[index] = tuple(changed)
    assert _facts_connection(rows).execute(_complete_run_query()).fetchall() == []


def test_complete_run_cte_rejects_run_id_not_bound_to_logical_date_snapshot_and_version():
    rows = [tuple(("f" * 64) if index == 0 else value for index, value in enumerate(row))
            for row in _accepted_rows()]
    assert _facts_connection(rows).execute(_complete_run_query()).fetchall() == []


def test_operator_attention_surfaces_a_partial_missing_source_diagnostic():
    row = list(_accepted_rows()[0])
    row[7] = None
    row[8] = None
    row[9] = None
    row[15] = None
    row[16] = None
    row[17] = None
    row[20] = "error"
    row[21] = "missing"
    row[22] = "source_missing"
    connection = _facts_connection([tuple(row)])
    query = _sql("operator_attention").replace(FACTS, "facts").replace(
        "format_datetime(f.logical_date, 'yyyy-MM-dd''T''HH:mm:ss''Z''')",
        "strftime(f.logical_date, '%Y-%m-%dT%H:%M:%SZ')",
    )
    observed = connection.execute(query).fetchall()
    assert len(observed) == 1
    assert observed[0][4:8] == ("bronze.source_available.v1", "missing", "error", "source_missing")


def test_operator_attention_rejects_impossible_rule_status_diagnostic_combinations():
    row = list(_accepted_rows()[0])
    row[20] = "warning"
    row[21] = "warn"
    row[22] = "threshold_warn"
    connection = _facts_connection([tuple(row)])
    query = _sql("operator_attention").replace(FACTS, "facts").replace(
        "format_datetime(f.logical_date, 'yyyy-MM-dd''T''HH:mm:ss''Z''')",
        "strftime(f.logical_date, '%Y-%m-%dT%H:%M:%SZ')",
    )
    assert connection.execute(query).fetchall() == []


def test_spark_failure_injections_never_embed_secret_like_values():
    source = (ROOT / "spark-apps/nyc-taxi-data-quality/src/test/scala/com/thekaveh/dataeng/quality/"
              "NycTaxiDataQualitySpec.scala").read_text(encoding="utf-8")
    assert "secret-" not in source


@pytest.mark.parametrize("name", QUERY_NAMES)
def test_timestamp_with_time_zone_is_formatted_directly_as_canonical_utc(name):
    sql = _compact(_sql(name))
    assert "with_timezone(" not in sql
    assert re.search(
        r"format_datetime\(\s*(?:f\.)?logical_date, 'yyyy-mm-dd''t''hh:mm:ss''z'''\)",
        sql,
    )
