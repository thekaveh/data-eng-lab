from __future__ import annotations

import importlib
import sys
from copy import deepcopy
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
AIRFLOW_DAGS = ROOT / "airflow-dags"
TASKS = AIRFLOW_DAGS / "trino_bi" / "tasks.py"

PROVENANCE = {
    "data_eng_lab.dataset": "tpch",
    "data_eng_lab.dataset.scale": "tiny",
    "data_eng_lab.dataset.plan_id": "a" * 64,
    "data_eng_lab.dataset.publication_id": "123456789abc41238123123456789abc",
    "data_eng_lab.dataset.manifest_sha256": "b" * 64,
}


def _load():
    assert TASKS.is_file(), "Trino BI task attempts have not been implemented"
    sys.path.insert(0, str(AIRFLOW_DAGS))
    try:
        return importlib.import_module("trino_bi.tasks")
    finally:
        sys.path.remove(str(AIRFLOW_DAGS))


def _result(module, name: str, columns, rows, index: int):
    return module.QueryResult(f"20260812_00000{index}_00001_x", tuple(columns), tuple(map(tuple, rows)))


def _tpch_results(module):
    c = module.QueryName
    properties = [
        [table, key, value]
        for table in ("dim_customer", "fct_orders")
        for key, value in PROVENANCE.items()
    ]
    snapshots = [["dim_customer", 101], ["fct_orders", 202]]
    schemas = [
        ["dim_customer", "c_custkey", "bigint"],
        ["dim_customer", "c_name", "varchar"],
        ["dim_customer", "c_nationkey", "integer"],
        ["dim_customer", "c_mktsegment", "varchar"],
        ["fct_orders", "o_orderkey", "bigint"],
        ["fct_orders", "o_custkey", "bigint"],
        ["fct_orders", "o_orderdate", "date"],
        ["fct_orders", "revenue", "decimal(25,2)"],
        ["fct_orders", "line_count", "bigint"],
    ]
    totals = [[5, "150.00", 15, 0]]
    rows = [
        ["AUTOMOBILE", "10.00", 1, 1],
        ["BUILDING", "20.00", 2, 1],
        ["FURNITURE", "30.00", 3, 1],
        ["HOUSEHOLD", "40.00", 4, 1],
        ["MACHINERY", "50.00", 5, 1],
    ]
    return {
        c.TPCH_PROPERTIES: _result(module, "properties", module.QUERIES[c.TPCH_PROPERTIES].columns, properties, 1),
        c.TPCH_SNAPSHOTS: _result(module, "snapshots", module.QUERIES[c.TPCH_SNAPSHOTS].columns, snapshots, 2),
        c.TPCH_SCHEMAS: _result(module, "schemas", module.QUERIES[c.TPCH_SCHEMAS].columns, schemas, 3),
        c.TPCH_SOURCE_TOTALS: _result(
            module, "totals", module.QUERIES[c.TPCH_SOURCE_TOTALS].columns, totals, 4
        ),
        c.TPCH_SEGMENT_REVENUE: _result(
            module, "revenue", module.QUERIES[c.TPCH_SEGMENT_REVENUE].columns, rows, 5
        ),
    }


def _nyc_results(module):
    c = module.QueryName
    return {
        c.NYC_SNAPSHOT: _result(module, "snapshot", module.QUERIES[c.NYC_SNAPSHOT].columns, [[303]], 1),
        c.NYC_SCHEMA: _result(
            module,
            "schema",
            module.QUERIES[c.NYC_SCHEMA].columns,
            [["trip_date", "date"], ["fare_amount", "double"], ["vendor_id", "varchar"]],
            2,
        ),
        c.NYC_SOURCE_COUNT: _result(
            module, "count", module.QUERIES[c.NYC_SOURCE_COUNT].columns, [[3]], 3
        ),
        c.NYC_DAILY_FARES: _result(
            module,
            "fares",
            module.QUERIES[c.NYC_DAILY_FARES].columns,
            [["2026-08-11", 1, 10.5], ["2026-08-12", 2, 20.25]],
            4,
        ),
    }


class RecordingClient:
    def __init__(self, results, *, fail_at=None):
        self.results = results
        self.fail_at = fail_at
        self.calls = []

    def execute(self, name):
        self.calls.append(name)
        if len(self.calls) == self.fail_at:
            raise RuntimeError("injected transport failure")
        value = self.results[name]
        return value.pop(0) if isinstance(value, list) else value


def _factory(client):
    return lambda: client


def _twice(result, post_index: int):
    return [result, deepcopy(result)._replace(query_id=f"20260812_00000{post_index}_00001_x")]


def test_tpch_attempt_executes_preflight_bi_and_postflight_in_exact_order() -> None:
    module = _load()
    results = _tpch_results(module)
    results[module.QueryName.TPCH_PROPERTIES] = _twice(results[module.QueryName.TPCH_PROPERTIES], 6)
    results[module.QueryName.TPCH_SNAPSHOTS] = _twice(results[module.QueryName.TPCH_SNAPSHOTS], 7)
    client = RecordingClient(results)
    artifact = module.run_tpch_bi(client_factory=_factory(client))

    assert client.calls == [
        module.QueryName.TPCH_PROPERTIES,
        module.QueryName.TPCH_SNAPSHOTS,
        module.QueryName.TPCH_SCHEMAS,
        module.QueryName.TPCH_SOURCE_TOTALS,
        module.QueryName.TPCH_SEGMENT_REVENUE,
        module.QueryName.TPCH_PROPERTIES,
        module.QueryName.TPCH_SNAPSHOTS,
    ]
    assert artifact["pipeline"] == "tpch_bi_query"
    assert artifact["row_count"] == 5
    assert artifact["source"] == {
        "provenance": PROVENANCE,
        "snapshots": {"dim_customer": 101, "fct_orders": 202},
    }
    assert len(artifact["query_ids"]) == 7


@pytest.mark.parametrize("fail_at", range(1, 8))
def test_tpch_transport_failure_stops_attempt_at_exact_boundary(fail_at: int) -> None:
    module = _load()
    results = _tpch_results(module)
    results[module.QueryName.TPCH_PROPERTIES] = _twice(results[module.QueryName.TPCH_PROPERTIES], 6)
    results[module.QueryName.TPCH_SNAPSHOTS] = _twice(results[module.QueryName.TPCH_SNAPSHOTS], 7)
    client = RecordingClient(results, fail_at=fail_at)
    with pytest.raises(RuntimeError, match="injected transport failure"):
        module.run_tpch_bi(client_factory=_factory(client))
    assert len(client.calls) == fail_at


def test_tpch_bad_preflight_provenance_never_submits_bi_sql() -> None:
    module = _load()
    results = _tpch_results(module)
    bad = list(results[module.QueryName.TPCH_PROPERTIES].rows)
    bad[-1] = (bad[-1][0], bad[-1][1], "c" * 64)
    results[module.QueryName.TPCH_PROPERTIES] = results[module.QueryName.TPCH_PROPERTIES]._replace(rows=tuple(bad))
    client = RecordingClient(results)
    with pytest.raises(module.ContractError, match="do not match"):
        module.run_tpch_bi(client_factory=_factory(client))
    assert client.calls == [module.QueryName.TPCH_PROPERTIES]
    assert module.QueryName.TPCH_SEGMENT_REVENUE not in client.calls


@pytest.mark.parametrize(
    ("query", "rows", "message"),
    [
        ("TPCH_SNAPSHOTS", [["dim_customer", 101]], "snapshot"),
        ("TPCH_SCHEMAS", [["dim_customer", "c_custkey", "varchar"]], "schema"),
        ("TPCH_SOURCE_TOTALS", [[5, "150.00", 15, 1]], "join"),
        (
            "TPCH_SEGMENT_REVENUE",
            [["AUTOMOBILE", "150.00", 15, 5]],
            "five market segments",
        ),
    ],
)
def test_tpch_invalid_preflight_or_result_fails_closed(query: str, rows, message: str) -> None:
    module = _load()
    results = _tpch_results(module)
    name = getattr(module.QueryName, query)
    results[name] = results[name]._replace(rows=tuple(map(tuple, rows)))
    if name in {module.QueryName.TPCH_PROPERTIES, module.QueryName.TPCH_SNAPSHOTS}:
        results[name] = _twice(results[name], 7)
    else:
        for repeated in (module.QueryName.TPCH_PROPERTIES, module.QueryName.TPCH_SNAPSHOTS):
                results[repeated] = _twice(
                    results[repeated], 6 if repeated == module.QueryName.TPCH_PROPERTIES else 7
                )
    client = RecordingClient(results)
    with pytest.raises(module.ContractError, match=message):
        module.run_tpch_bi(client_factory=_factory(client))


@pytest.mark.parametrize("changed", ["properties", "snapshots"])
def test_tpch_postflight_change_rejects_artifact(changed: str) -> None:
    module = _load()
    results = _tpch_results(module)
    first_properties = results[module.QueryName.TPCH_PROPERTIES]
    second_properties = deepcopy(first_properties)._replace(query_id="20260812_000006_00001_x")
    first_snapshots = results[module.QueryName.TPCH_SNAPSHOTS]
    second_snapshots = deepcopy(first_snapshots)._replace(query_id="20260812_000007_00001_x")
    if changed == "properties":
        rows = list(second_properties.rows)
        rows[-1] = (rows[-1][0], rows[-1][1], "c" * 64)
        second_properties = second_properties._replace(rows=tuple(rows))
    else:
        rows = list(second_snapshots.rows)
        rows[-1] = (rows[-1][0], 999)
        second_snapshots = second_snapshots._replace(rows=tuple(rows))
    results[module.QueryName.TPCH_PROPERTIES] = [first_properties, second_properties]
    results[module.QueryName.TPCH_SNAPSHOTS] = [first_snapshots, second_snapshots]
    client = RecordingClient(results)
    with pytest.raises(module.ContractError, match="changed|do not match"):
        module.run_tpch_bi(client_factory=_factory(client))
    assert len(client.calls) == (6 if changed == "properties" else 7)


def test_nyc_attempt_executes_snapshot_bound_queries_in_exact_order() -> None:
    module = _load()
    results = _nyc_results(module)
    results[module.QueryName.NYC_SNAPSHOT] = _twice(results[module.QueryName.NYC_SNAPSHOT], 5)
    client = RecordingClient(results)
    artifact = module.run_nyc_bi(client_factory=_factory(client))
    assert client.calls == [
        module.QueryName.NYC_SNAPSHOT,
        module.QueryName.NYC_SCHEMA,
        module.QueryName.NYC_SOURCE_COUNT,
        module.QueryName.NYC_DAILY_FARES,
        module.QueryName.NYC_SNAPSHOT,
    ]
    assert artifact["pipeline"] == "nyc_taxi_trino_daily"
    assert artifact["source"] == {"binding": "iceberg_snapshot", "row_count": 3, "snapshot_id": 303}
    assert len(artifact["query_ids"]) == 5


@pytest.mark.parametrize("fail_at", range(1, 6))
def test_nyc_transport_failure_stops_attempt_at_exact_boundary(fail_at: int) -> None:
    module = _load()
    results = _nyc_results(module)
    results[module.QueryName.NYC_SNAPSHOT] = _twice(results[module.QueryName.NYC_SNAPSHOT], 5)
    client = RecordingClient(results, fail_at=fail_at)
    with pytest.raises(RuntimeError, match="injected transport failure"):
        module.run_nyc_bi(client_factory=_factory(client))
    assert len(client.calls) == fail_at


@pytest.mark.parametrize(
    ("query", "rows", "message"),
    [
        ("NYC_SNAPSHOT", [], "snapshot"),
        ("NYC_SCHEMA", [["trip_date", "varchar"]], "trip_date"),
        ("NYC_SOURCE_COUNT", [[0]], "positive"),
        ("NYC_DAILY_FARES", [["2026-08-12", 2, 20.0]], "reconcile"),
    ],
)
def test_nyc_invalid_source_or_result_fails_closed(query: str, rows, message: str) -> None:
    module = _load()
    results = _nyc_results(module)
    name = getattr(module.QueryName, query)
    results[name] = results[name]._replace(rows=tuple(map(tuple, rows)))
    if name == module.QueryName.NYC_SNAPSHOT:
        results[name] = _twice(results[name], 5)
    else:
        results[module.QueryName.NYC_SNAPSHOT] = _twice(results[module.QueryName.NYC_SNAPSHOT], 5)
    client = RecordingClient(results)
    with pytest.raises(module.ContractError, match=message):
        module.run_nyc_bi(client_factory=_factory(client))


def test_nyc_postflight_snapshot_change_rejects_artifact() -> None:
    module = _load()
    results = _nyc_results(module)
    first = results[module.QueryName.NYC_SNAPSHOT]
    second = first._replace(query_id="20260812_000005_00001_x", rows=((404,),))
    results[module.QueryName.NYC_SNAPSHOT] = [first, second]
    client = RecordingClient(results)
    with pytest.raises(module.ContractError, match="changed"):
        module.run_nyc_bi(client_factory=_factory(client))
    assert len(client.calls) == 5


def test_reordered_transport_rows_converge_to_same_artifact_result() -> None:
    module = _load()
    baseline = _tpch_results(module)
    for repeated in (module.QueryName.TPCH_PROPERTIES, module.QueryName.TPCH_SNAPSHOTS):
        baseline[repeated] = _twice(
            baseline[repeated], 6 if repeated == module.QueryName.TPCH_PROPERTIES else 7
        )
    first = module.run_tpch_bi(client_factory=_factory(RecordingClient(deepcopy(baseline))))

    shuffled = deepcopy(baseline)
    for name in (module.QueryName.TPCH_PROPERTIES, module.QueryName.TPCH_SNAPSHOTS):
        shuffled[name] = [item._replace(rows=tuple(reversed(item.rows))) for item in shuffled[name]]
    shuffled[module.QueryName.TPCH_SEGMENT_REVENUE] = shuffled[module.QueryName.TPCH_SEGMENT_REVENUE]._replace(
        rows=tuple(reversed(shuffled[module.QueryName.TPCH_SEGMENT_REVENUE].rows))
    )
    second = module.run_tpch_bi(client_factory=_factory(RecordingClient(shuffled)))
    assert first["rows"] == second["rows"]
    assert first["result_sha256"] == second["result_sha256"]
