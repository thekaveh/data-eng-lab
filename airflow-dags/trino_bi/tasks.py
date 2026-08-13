"""One-attempt read-only Trino BI task orchestration."""

from __future__ import annotations

from collections.abc import Callable, Sequence
from decimal import Decimal, InvalidOperation
from typing import Any

from .client import QUERIES, QueryName, QueryResult, TrinoHttpClient
from .contracts import (
    ContractError,
    build_nyc_artifact,
    build_tpch_artifact,
    validate_tpch_provenance,
    validate_tpch_schemas,
)


def _partition(
    rows: Sequence[Sequence[Any]], names: tuple[str, ...], *, width: int, label: str
) -> dict[str, list[tuple[Any, ...]]]:
    result = {name: [] for name in names}
    for row in rows:
        if not isinstance(row, (list, tuple)) or len(row) != width or row[0] not in result:
            raise ContractError(f"{label} row shape is invalid")
        result[row[0]].append(tuple(row[1:]))
    if any(not result[name] for name in names):
        raise ContractError(f"{label} is missing a required table")
    return result


def _snapshot_map(rows: Sequence[Sequence[Any]]) -> dict[str, int]:
    grouped = _partition(rows, ("dim_customer", "fct_orders"), width=2, label="TPC-H snapshot")
    result: dict[str, int] = {}
    for table, values in grouped.items():
        if len(values) != 1:
            raise ContractError("TPC-H snapshot must contain one row per table")
        value = values[0][0]
        if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
            raise ContractError("TPC-H snapshot must be a positive integer")
        result[table] = value
    return result


def _schema_map(rows: Sequence[Sequence[Any]]) -> dict[str, list[tuple[Any, ...]]]:
    return _partition(rows, ("dim_customer", "fct_orders"), width=3, label="TPC-H schema")


def _property_map(rows: Sequence[Sequence[Any]]) -> dict[str, list[tuple[Any, ...]]]:
    return _partition(rows, ("dim_customer", "fct_orders"), width=3, label="TPC-H provenance")


def _one_row(result: QueryResult, *, width: int, label: str) -> tuple[Any, ...]:
    if len(result.rows) != 1 or len(result.rows[0]) != width:
        raise ContractError(f"{label} must contain exactly one row")
    return result.rows[0]


def _positive_integer(value: Any, label: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ContractError(f"{label} must be positive")
    return value


def _validate_source_totals(result: QueryResult) -> dict[str, Any]:
    row = _one_row(result, width=4, label="TPC-H source totals")
    order_count = _positive_integer(row[0], "TPC-H source order count")
    line_count = _positive_integer(row[2], "TPC-H source line count")
    if line_count < order_count:
        raise ContractError("TPC-H source line count must be at least order count")
    try:
        revenue = Decimal(str(row[1]))
    except (InvalidOperation, ValueError):
        raise ContractError("TPC-H source revenue must be a finite decimal") from None
    if not revenue.is_finite() or revenue <= 0 or revenue.quantize(Decimal("0.01")) != revenue:
        raise ContractError("TPC-H source revenue must be a positive scale-2 decimal")
    unmatched = row[3]
    if isinstance(unmatched, bool) or not isinstance(unmatched, int) or unmatched != 0:
        raise ContractError("TPC-H customer join is incomplete")
    return {
        "fact_order_count": order_count,
        "fact_revenue": row[1],
        "fact_line_count": line_count,
        "unmatched_orders": unmatched,
    }


def _nyc_snapshot(result: QueryResult) -> int:
    return _positive_integer(_one_row(result, width=1, label="NYC snapshot")[0], "NYC snapshot")


def _nyc_schema(result: QueryResult) -> tuple[tuple[Any, ...], ...]:
    schema: dict[str, str] = {}
    for row in result.rows:
        if len(row) != 2 or not all(isinstance(value, str) for value in row):
            raise ContractError("NYC schema row is invalid")
        name, data_type = row
        if name in schema:
            raise ContractError("NYC schema contains a duplicate column")
        schema[name] = data_type.lower()
    if schema.get("trip_date") != "date":
        raise ContractError("NYC source trip_date must have type date")
    if schema.get("fare_amount") != "double":
        raise ContractError("NYC source fare_amount must have type double")
    return result.rows


def _nyc_count(result: QueryResult) -> int:
    return _positive_integer(_one_row(result, width=1, label="NYC source count")[0], "NYC source count")


def run_tpch_bi(*, client_factory: Callable[[], TrinoHttpClient] = TrinoHttpClient) -> dict[str, Any]:
    """Run all TPC-H preflight, BI, and postflight checks in one task attempt."""
    client = client_factory()
    query_ids: list[str] = []

    properties_before_result = client.execute(QueryName.TPCH_PROPERTIES)
    query_ids.append(properties_before_result.query_id)
    property_rows = _property_map(properties_before_result.rows)
    provenance = validate_tpch_provenance(
        property_rows["dim_customer"], property_rows["fct_orders"]
    )

    snapshots_before_result = client.execute(QueryName.TPCH_SNAPSHOTS)
    query_ids.append(snapshots_before_result.query_id)
    snapshots_before = _snapshot_map(snapshots_before_result.rows)

    schemas_result = client.execute(QueryName.TPCH_SCHEMAS)
    query_ids.append(schemas_result.query_id)
    schemas = _schema_map(schemas_result.rows)
    validate_tpch_schemas(schemas["dim_customer"], schemas["fct_orders"])

    totals_result = client.execute(QueryName.TPCH_SOURCE_TOTALS)
    query_ids.append(totals_result.query_id)
    source_totals = _validate_source_totals(totals_result)

    result = client.execute(QueryName.TPCH_SEGMENT_REVENUE)
    query_ids.append(result.query_id)

    properties_after_result = client.execute(QueryName.TPCH_PROPERTIES)
    query_ids.append(properties_after_result.query_id)
    property_rows_after = _property_map(properties_after_result.rows)
    provenance_after = validate_tpch_provenance(
        property_rows_after["dim_customer"], property_rows_after["fct_orders"]
    )

    snapshots_after_result = client.execute(QueryName.TPCH_SNAPSHOTS)
    query_ids.append(snapshots_after_result.query_id)
    snapshots_after = _snapshot_map(snapshots_after_result.rows)

    return build_tpch_artifact(
        provenance=provenance,
        schemas=schemas,
        rows=result.rows,
        source_totals=source_totals,
        snapshots_before=snapshots_before,
        snapshots_after=snapshots_after,
        provenance_after=provenance_after,
        query_ids=query_ids,
    )


def run_nyc_bi(*, client_factory: Callable[[], TrinoHttpClient] = TrinoHttpClient) -> dict[str, Any]:
    """Run all NYC snapshot-bound checks and BI SQL in one task attempt."""
    client = client_factory()
    query_ids: list[str] = []

    snapshot_before_result = client.execute(QueryName.NYC_SNAPSHOT)
    query_ids.append(snapshot_before_result.query_id)
    snapshot_before = _nyc_snapshot(snapshot_before_result)

    schema_result = client.execute(QueryName.NYC_SCHEMA)
    query_ids.append(schema_result.query_id)
    source_schema = _nyc_schema(schema_result)

    count_result = client.execute(QueryName.NYC_SOURCE_COUNT)
    query_ids.append(count_result.query_id)
    source_count = _nyc_count(count_result)

    result = client.execute(QueryName.NYC_DAILY_FARES)
    query_ids.append(result.query_id)

    snapshot_after_result = client.execute(QueryName.NYC_SNAPSHOT)
    query_ids.append(snapshot_after_result.query_id)
    snapshot_after = _nyc_snapshot(snapshot_after_result)

    return build_nyc_artifact(
        source_schema=source_schema,
        rows=result.rows,
        source_count=source_count,
        snapshot_before=snapshot_before,
        snapshot_after=snapshot_after,
        query_ids=query_ids,
    )


__all__ = [
    "ContractError",
    "QUERIES",
    "QueryName",
    "QueryResult",
    "run_nyc_bi",
    "run_tpch_bi",
]
