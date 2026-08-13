from __future__ import annotations

import hashlib
import importlib.util
import json
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
MODULE = ROOT / "airflow-dags" / "trino_bi" / "contracts.py"


def _load_contracts():
    assert MODULE.is_file(), "Trino BI contracts module has not been implemented"
    spec = importlib.util.spec_from_file_location("trino_bi_contracts", MODULE)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


EXPECTED_KEYS = (
    "data_eng_lab.dataset",
    "data_eng_lab.dataset.scale",
    "data_eng_lab.dataset.plan_id",
    "data_eng_lab.dataset.publication_id",
    "data_eng_lab.dataset.manifest_sha256",
)
PROVENANCE = {
    "data_eng_lab.dataset": "tpch",
    "data_eng_lab.dataset.scale": "tiny",
    "data_eng_lab.dataset.plan_id": "1" * 64,
    "data_eng_lab.dataset.publication_id": "a" * 12 + "4" + "b" * 3 + "8" + "c" * 15,
    "data_eng_lab.dataset.manifest_sha256": "2" * 64,
}


def _property_rows(values=PROVENANCE):
    return [[key, values[key]] for key in EXPECTED_KEYS]


def test_registry_freezes_actual_trino_482_declared_type_spelling():
    c = _load_contracts()
    assert c.QUERIES[c.QueryName.TPCH_PROPERTIES].columns == (
        ("source_table", "varchar(12)"),
        ("key", "varchar"),
        ("value", "varchar"),
    )
    assert c.QUERIES[c.QueryName.TPCH_SNAPSHOTS].columns[0] == (
        "source_table",
        "varchar(12)",
    )
    assert c.QUERIES[c.QueryName.TPCH_SOURCE_TOTALS].columns[1] == (
        "fact_revenue",
        "decimal(38, 2)",
    )
    assert c.QUERIES[c.QueryName.TPCH_SEGMENT_REVENUE].columns[1] == (
        "total_revenue",
        "decimal(38, 2)",
    )
    assert c.TPCH_FACT_SCHEMA[3] == ("revenue", "decimal(25,2)")


DIM_SCHEMA = [
    ["c_custkey", "bigint"],
    ["c_name", "varchar"],
    ["c_nationkey", "integer"],
    ["c_mktsegment", "varchar"],
]
FACT_SCHEMA = [
    ["o_orderkey", "bigint"],
    ["o_custkey", "bigint"],
    ["o_orderdate", "date"],
    ["revenue", "decimal(25,2)"],
    ["line_count", "bigint"],
]
TPC_ROWS = [
    ["AUTOMOBILE", "100.00", 4, 2],
    ["BUILDING", "200.00", 5, 4],
    ["FURNITURE", "300.00", 7, 5],
    ["HOUSEHOLD", "400.00", 8, 6],
    ["MACHINERY", "500.00", 9, 7],
]
TPC_SOURCE = {
    "fact_order_count": 24,
    "fact_revenue": "1500.00",
    "fact_line_count": 33,
    "unmatched_orders": 0,
}
TPC_SNAPSHOTS = {"dim_customer": 101, "fct_orders": 202}


def test_registry_contains_only_the_exact_reviewed_read_queries() -> None:
    c = _load_contracts()
    assert tuple(c.QUERIES) == (
        c.QueryName.TPCH_PROPERTIES,
        c.QueryName.TPCH_SNAPSHOTS,
        c.QueryName.TPCH_SCHEMAS,
        c.QueryName.TPCH_SOURCE_TOTALS,
        c.QueryName.TPCH_SEGMENT_REVENUE,
        c.QueryName.NYC_SNAPSHOT,
        c.QueryName.NYC_SCHEMA,
        c.QueryName.NYC_SOURCE_COUNT,
        c.QueryName.NYC_DAILY_FARES,
    )
    for name, query in c.QUERIES.items():
        assert query.name is name
        c.validate_read_only_sql(query.sql)
        assert "{" not in query.sql and "}" not in query.sql


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT 1;",
        "SELECT 1 -- comment",
        "SELECT /* comment */ 1",
        "SELECT 1; SELECT 2",
        "CREATE TABLE x AS SELECT 1",
        "WITH x AS (DELETE FROM y) SELECT * FROM x",
        "INSERT INTO x VALUES (1)",
        "UPDATE x SET y = 1",
        "MERGE INTO x USING y ON true WHEN MATCHED THEN DELETE",
        "CALL system.flush_metadata_cache()",
        "START TRANSACTION",
        "SET SESSION x = 1",
        "GRANT SELECT ON x TO y",
        "SELECT * FROM {table}",
        "SELECT 'unterminated",
        "WITH x AS (SELECT 1 SELECT * FROM x",
    ],
)
def test_read_only_validator_rejects_registry_drift(sql: str) -> None:
    c = _load_contracts()
    with pytest.raises(c.ContractError, match="read-only query registry"):
        c.validate_read_only_sql(sql)


@pytest.mark.parametrize("sql", ["SELECT 1", "WITH x AS (SELECT 1) SELECT * FROM x"])
def test_read_only_validator_accepts_one_select_statement(sql: str) -> None:
    _load_contracts().validate_read_only_sql(sql)


def test_tpch_provenance_accepts_exact_equal_five_key_maps() -> None:
    c = _load_contracts()
    assert c.validate_tpch_provenance(_property_rows(), _property_rows()) == PROVENANCE


@pytest.mark.parametrize(
    ("mutator", "message"),
    [
        (lambda rows: rows[:-1], "exactly five"),
        (lambda rows: rows + [rows[0]], "duplicate"),
        (lambda rows: rows + [["data_eng_lab.dataset.unexpected", "x"]], "exactly five"),
        (lambda rows: [[key, ""] if key == EXPECTED_KEYS[0] else [key, value] for key, value in rows], "blank"),
        (lambda rows: [["wrong", value] if key == EXPECTED_KEYS[0] else [key, value] for key, value in rows], "key"),
    ],
)
def test_tpch_provenance_rejects_incomplete_or_ambiguous_rows(mutator, message: str) -> None:
    c = _load_contracts()
    with pytest.raises(c.ContractError, match=message):
        c.validate_tpch_provenance(mutator(_property_rows()), _property_rows())


@pytest.mark.parametrize(
    ("key", "value"),
    [
        ("data_eng_lab.dataset", "nyc_taxi"),
        ("data_eng_lab.dataset.scale", "large"),
        ("data_eng_lab.dataset.plan_id", "A" * 64),
        ("data_eng_lab.dataset.publication_id", "0" * 32),
        ("data_eng_lab.dataset.manifest_sha256", "2" * 63),
    ],
)
def test_tpch_provenance_rejects_malformed_identity(key: str, value: str) -> None:
    c = _load_contracts()
    changed = {**PROVENANCE, key: value}
    with pytest.raises(c.ContractError, match="TPC-H provenance"):
        c.validate_tpch_provenance(_property_rows(changed), _property_rows(changed))


def test_tpch_provenance_rejects_cross_table_mismatch() -> None:
    c = _load_contracts()
    fact = {**PROVENANCE, "data_eng_lab.dataset.manifest_sha256": "3" * 64}
    with pytest.raises(c.ContractError, match="do not match"):
        c.validate_tpch_provenance(_property_rows(), _property_rows(fact))


def test_tpch_schemas_match_the_producer_contract_exactly() -> None:
    c = _load_contracts()
    c.validate_tpch_schemas(DIM_SCHEMA, FACT_SCHEMA)
    with pytest.raises(c.ContractError, match="dim_customer schema"):
        c.validate_tpch_schemas(list(reversed(DIM_SCHEMA)), FACT_SCHEMA)
    with pytest.raises(c.ContractError, match="fct_orders schema"):
        c.validate_tpch_schemas(DIM_SCHEMA, FACT_SCHEMA[:-1] + [["line_count", "integer"]])


def test_tpch_artifact_is_canonical_reconciled_and_snapshot_bound() -> None:
    c = _load_contracts()
    artifact = c.build_tpch_artifact(
        provenance=PROVENANCE,
        schemas={"dim_customer": DIM_SCHEMA, "fct_orders": FACT_SCHEMA},
        rows=list(reversed(TPC_ROWS)),
        source_totals=TPC_SOURCE,
        snapshots_before=TPC_SNAPSHOTS,
        snapshots_after=TPC_SNAPSHOTS,
        provenance_after=PROVENANCE,
        query_ids=[f"20260812_00000{i}_00001_x" for i in range(5)],
    )
    assert artifact["pipeline"] == "tpch_bi_query"
    assert artifact["row_count"] == 5
    assert [row[0] for row in artifact["rows"]] == sorted(row[0] for row in TPC_ROWS)
    assert artifact["rows"][0][1] == "100.00"
    assert artifact["source"]["provenance"] == PROVENANCE
    assert artifact["source"]["snapshots"] == TPC_SNAPSHOTS
    assert c.canonical_json_bytes(json.loads(c.canonical_json_bytes(artifact))) == c.canonical_json_bytes(artifact)
    payload = {"columns": artifact["columns"], "rows": artifact["rows"]}
    assert artifact["result_sha256"] == hashlib.sha256(c.canonical_json_bytes(payload)).hexdigest()
    assert len(c.canonical_json_bytes(artifact)) <= c.TPCH_ARTIFACT_MAX_BYTES


@pytest.mark.parametrize(
    ("change", "message"),
    [
        ({"rows": TPC_ROWS[:-1]}, "five market segments"),
        ({"rows": TPC_ROWS[:-1] + [["BUILDING", "500.00", 9, 7]]}, "unique"),
        ({"rows": TPC_ROWS[:-1] + [["MACHINERY", "500.001", 9, 7]]}, "scale"),
        ({"rows": TPC_ROWS[:-1] + [["MACHINERY", "-1.00", 9, 7]]}, "positive"),
        ({"rows": TPC_ROWS[:-1] + [["MACHINERY", "500.00", 2, 7]]}, "line_count"),
        ({"source_totals": {**TPC_SOURCE, "unmatched_orders": 1}}, "join"),
        ({"source_totals": {**TPC_SOURCE, "fact_revenue": "1500.01"}}, "revenue"),
        ({"snapshots_after": {**TPC_SNAPSHOTS, "fct_orders": 303}}, "snapshot"),
        ({"provenance_after": {**PROVENANCE, EXPECTED_KEYS[-1]: "3" * 64}}, "provenance"),
    ],
)
def test_tpch_artifact_rejects_invalid_or_changed_results(change, message: str) -> None:
    c = _load_contracts()
    kwargs = {
        "provenance": PROVENANCE,
        "schemas": {"dim_customer": DIM_SCHEMA, "fct_orders": FACT_SCHEMA},
        "rows": TPC_ROWS,
        "source_totals": TPC_SOURCE,
        "snapshots_before": TPC_SNAPSHOTS,
        "snapshots_after": TPC_SNAPSHOTS,
        "provenance_after": PROVENANCE,
        "query_ids": ["query-1"],
    }
    kwargs.update(change)
    with pytest.raises(c.ContractError, match=message):
        c.build_tpch_artifact(**kwargs)


NYC_SCHEMA = [
    ["tpep_pickup_datetime", "timestamp(6)"],
    ["passenger_count", "double"],
    ["fare_amount", "double"],
    ["trip_date", "date"],
]
NYC_ROWS = [["2023-01-02", 3, 15.25], ["2023-01-01", 2, "12.5000"]]


def test_nyc_artifact_is_canonical_count_reconciled_and_snapshot_bound() -> None:
    c = _load_contracts()
    artifact = c.build_nyc_artifact(
        source_schema=NYC_SCHEMA,
        rows=NYC_ROWS,
        source_count=5,
        snapshot_before=404,
        snapshot_after=404,
        query_ids=["query-1", "query-2", "query-3"],
    )
    assert artifact["pipeline"] == "nyc_taxi_trino_daily"
    assert artifact["source"] == {"binding": "iceberg_snapshot", "row_count": 5, "snapshot_id": 404}
    assert artifact["rows"] == [["2023-01-01", 2, "12.5"], ["2023-01-02", 3, "15.25"]]
    assert len(c.canonical_json_bytes(artifact)) <= c.NYC_ARTIFACT_MAX_BYTES


@pytest.mark.parametrize(
    ("change", "message"),
    [
        ({"source_schema": NYC_SCHEMA[:-1]}, "trip_date"),
        ({"source_schema": NYC_SCHEMA[:-1] + [["trip_date", "varchar"]]}, "trip_date"),
        ({"rows": []}, "nonempty"),
        ({"rows": NYC_ROWS + [["2023-01-01", 1, 5.0]]}, "unique"),
        ({"rows": [["2023/01/01", 5, 1.0]]}, "ISO"),
        ({"rows": [["2023-01-01", 0, 1.0]]}, "positive"),
        ({"rows": [["2023-01-01", 5, float("inf")]]}, "finite"),
        ({"source_count": 6}, "source count"),
        ({"snapshot_after": 405}, "snapshot"),
    ],
)
def test_nyc_artifact_rejects_invalid_or_changed_results(change, message: str) -> None:
    c = _load_contracts()
    kwargs = {
        "source_schema": NYC_SCHEMA,
        "rows": NYC_ROWS,
        "source_count": 5,
        "snapshot_before": 404,
        "snapshot_after": 404,
        "query_ids": ["query-1"],
    }
    kwargs.update(change)
    with pytest.raises(c.ContractError, match=message):
        c.build_nyc_artifact(**kwargs)


def test_artifact_encoder_rejects_noncanonical_or_secret_values() -> None:
    c = _load_contracts()
    with pytest.raises(c.ContractError, match="finite"):
        c.canonical_json_bytes({"value": float("nan")})
    with pytest.raises(c.ContractError, match="allowed fields"):
        c.validate_artifact_fields({"pipeline": "x", "sql": "SELECT secret"})
