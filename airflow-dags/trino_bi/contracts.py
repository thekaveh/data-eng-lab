"""Fixed read-only SQL and pure result contracts for the Trino BI DAGs."""

from __future__ import annotations

import hashlib
import json
import math
import re
from decimal import Decimal, InvalidOperation
from enum import Enum
from typing import Any, NamedTuple, Sequence


class ContractError(ValueError):
    """A query or result violated the reviewed production contract."""


class QueryName(str, Enum):
    TPCH_PROPERTIES = "tpch_properties"
    TPCH_SNAPSHOTS = "tpch_snapshots"
    TPCH_SCHEMAS = "tpch_schemas"
    TPCH_SOURCE_TOTALS = "tpch_source_totals"
    TPCH_SEGMENT_REVENUE = "tpch_segment_revenue"
    NYC_SNAPSHOT = "nyc_snapshot"
    NYC_SCHEMA = "nyc_schema"
    NYC_SOURCE_COUNT = "nyc_source_count"
    NYC_DAILY_FARES = "nyc_daily_fares"


class QuerySpec(NamedTuple):
    name: QueryName
    sql: str
    columns: tuple[tuple[str, str], ...]
    max_rows: int
    schema: str | None = None


PROVENANCE_KEYS = (
    "data_eng_lab.dataset",
    "data_eng_lab.dataset.scale",
    "data_eng_lab.dataset.plan_id",
    "data_eng_lab.dataset.publication_id",
    "data_eng_lab.dataset.manifest_sha256",
)

TPCH_DIM_SCHEMA = (
    ("c_custkey", "bigint"),
    ("c_name", "varchar"),
    ("c_nationkey", "integer"),
    ("c_mktsegment", "varchar"),
)
TPCH_FACT_SCHEMA = (
    ("o_orderkey", "bigint"),
    ("o_custkey", "bigint"),
    ("o_orderdate", "date"),
    ("revenue", "decimal(25,2)"),
    ("line_count", "bigint"),
)
TPCH_RESULT_COLUMNS = (
    ("market_segment", "varchar"),
    ("total_revenue", "decimal(38, 2)"),
    ("line_count", "bigint"),
    ("order_count", "bigint"),
)
NYC_RESULT_COLUMNS = (
    ("trip_date", "date"),
    ("trip_count", "bigint"),
    ("avg_fare", "double"),
)

TPCH_ARTIFACT_MAX_BYTES = 64 * 1024
NYC_ARTIFACT_MAX_BYTES = 256 * 1024
NYC_MAX_ROWS = 4_000


QUERIES = {
    QueryName.TPCH_PROPERTIES: QuerySpec(
        QueryName.TPCH_PROPERTIES,
        """SELECT source_table, key, value
FROM (
  SELECT 'dim_customer' AS source_table, key, value
  FROM lakehouse.gold."dim_customer$properties"
  WHERE key IN (
    'data_eng_lab.dataset',
    'data_eng_lab.dataset.scale',
    'data_eng_lab.dataset.plan_id',
    'data_eng_lab.dataset.publication_id',
    'data_eng_lab.dataset.manifest_sha256'
  )
  UNION ALL
  SELECT 'fct_orders' AS source_table, key, value
  FROM lakehouse.gold."fct_orders$properties"
  WHERE key IN (
    'data_eng_lab.dataset',
    'data_eng_lab.dataset.scale',
    'data_eng_lab.dataset.plan_id',
    'data_eng_lab.dataset.publication_id',
    'data_eng_lab.dataset.manifest_sha256'
  )
)
ORDER BY source_table, key""",
        (("source_table", "varchar(12)"), ("key", "varchar"), ("value", "varchar")),
        10,
        "gold",
    ),
    QueryName.TPCH_SNAPSHOTS: QuerySpec(
        QueryName.TPCH_SNAPSHOTS,
        """SELECT source_table, snapshot_id
FROM (
  SELECT 'dim_customer' AS source_table, snapshot_id
  FROM lakehouse.gold."dim_customer$refs" WHERE name = 'main'
  UNION ALL
  SELECT 'fct_orders' AS source_table, snapshot_id
  FROM lakehouse.gold."fct_orders$refs" WHERE name = 'main'
)
ORDER BY source_table""",
        (("source_table", "varchar(12)"), ("snapshot_id", "bigint")),
        2,
        "gold",
    ),
    QueryName.TPCH_SCHEMAS: QuerySpec(
        QueryName.TPCH_SCHEMAS,
        """SELECT table_name, column_name, data_type
FROM lakehouse.information_schema.columns
WHERE table_schema = 'gold' AND table_name IN ('dim_customer', 'fct_orders')
ORDER BY table_name, ordinal_position""",
        (("table_name", "varchar"), ("column_name", "varchar"), ("data_type", "varchar")),
        9,
        "information_schema",
    ),
    QueryName.TPCH_SOURCE_TOTALS: QuerySpec(
        QueryName.TPCH_SOURCE_TOTALS,
        """SELECT count(*) AS fact_order_count,
       CAST(sum(f.revenue) AS decimal(38,2)) AS fact_revenue,
       CAST(sum(f.line_count) AS bigint) AS fact_line_count,
       count_if(c.c_custkey IS NULL) AS unmatched_orders
FROM lakehouse.gold.fct_orders f
LEFT JOIN lakehouse.gold.dim_customer c ON f.o_custkey = c.c_custkey""",
        (
            ("fact_order_count", "bigint"),
            ("fact_revenue", "decimal(38, 2)"),
            ("fact_line_count", "bigint"),
            ("unmatched_orders", "bigint"),
        ),
        1,
        "gold",
    ),
    QueryName.TPCH_SEGMENT_REVENUE: QuerySpec(
        QueryName.TPCH_SEGMENT_REVENUE,
        """SELECT c.c_mktsegment AS market_segment,
       CAST(sum(f.revenue) AS decimal(38,2)) AS total_revenue,
       CAST(sum(f.line_count) AS bigint) AS line_count,
       count(*) AS order_count
FROM lakehouse.gold.fct_orders f
JOIN lakehouse.gold.dim_customer c ON f.o_custkey = c.c_custkey
GROUP BY c.c_mktsegment
ORDER BY market_segment""",
        TPCH_RESULT_COLUMNS,
        5,
        "gold",
    ),
    QueryName.NYC_SNAPSHOT: QuerySpec(
        QueryName.NYC_SNAPSHOT,
        """SELECT snapshot_id
FROM lakehouse.bronze."nyc_taxi_trips$refs"
WHERE name = 'main'""",
        (("snapshot_id", "bigint"),),
        1,
        "bronze",
    ),
    QueryName.NYC_SCHEMA: QuerySpec(
        QueryName.NYC_SCHEMA,
        """SELECT column_name, data_type
FROM lakehouse.information_schema.columns
WHERE table_schema = 'bronze' AND table_name = 'nyc_taxi_trips'
ORDER BY ordinal_position""",
        (("column_name", "varchar"), ("data_type", "varchar")),
        256,
        "information_schema",
    ),
    QueryName.NYC_SOURCE_COUNT: QuerySpec(
        QueryName.NYC_SOURCE_COUNT,
        "SELECT count(*) AS source_count FROM lakehouse.bronze.nyc_taxi_trips",
        (("source_count", "bigint"),),
        1,
        "bronze",
    ),
    QueryName.NYC_DAILY_FARES: QuerySpec(
        QueryName.NYC_DAILY_FARES,
        """SELECT trip_date,
       count(*) AS trip_count,
       avg(fare_amount) AS avg_fare
FROM lakehouse.bronze.nyc_taxi_trips
GROUP BY trip_date
ORDER BY trip_date""",
        NYC_RESULT_COLUMNS,
        NYC_MAX_ROWS,
        "bronze",
    ),
}

_FORBIDDEN_WORDS = {
    "ALTER",
    "CALL",
    "COMMIT",
    "CREATE",
    "DELETE",
    "DROP",
    "GRANT",
    "INSERT",
    "MERGE",
    "RESET",
    "REVOKE",
    "ROLLBACK",
    "SET",
    "START",
    "TRUNCATE",
    "UPDATE",
}


def validate_read_only_sql(sql: str) -> None:
    """Reject registry drift outside one fixed SELECT/WITH statement."""
    if not isinstance(sql, str) or not sql.strip():
        raise ContractError("read-only query registry requires nonblank SQL")
    if any(token in sql for token in (";", "--", "/*", "*/", "{", "}")):
        raise ContractError("read-only query registry contains unsafe syntax")

    scrubbed: list[str] = []
    quote: str | None = None
    depth = 0
    index = 0
    while index < len(sql):
        char = sql[index]
        if quote is not None:
            if char == quote:
                if index + 1 < len(sql) and sql[index + 1] == quote:
                    index += 2
                    continue
                quote = None
            scrubbed.append(" ")
        elif char in ("'", '"'):
            quote = char
            scrubbed.append(" ")
        else:
            if char == "(":
                depth += 1
            elif char == ")":
                depth -= 1
                if depth < 0:
                    raise ContractError("read-only query registry has unbalanced tokens")
            scrubbed.append(char)
        index += 1
    if quote is not None or depth != 0:
        raise ContractError("read-only query registry has unbalanced tokens")

    words = re.findall(r"[A-Za-z_]+", "".join(scrubbed).upper())
    if not words or words[0] not in {"SELECT", "WITH"} or _FORBIDDEN_WORDS.intersection(words):
        raise ContractError("read-only query registry permits SELECT/WITH only")


def _property_map(rows: Sequence[Sequence[Any]]) -> dict[str, str]:
    observed_keys = [row[0] for row in rows if isinstance(row, (list, tuple)) and len(row) == 2]
    if len(observed_keys) != len(set(observed_keys)):
        raise ContractError("TPC-H provenance contains a duplicate key")
    if len(rows) != len(PROVENANCE_KEYS):
        raise ContractError("TPC-H provenance must contain exactly five rows")
    result: dict[str, str] = {}
    for row in rows:
        if not isinstance(row, (list, tuple)) or len(row) != 2:
            raise ContractError("TPC-H provenance row shape is invalid")
        key, value = row
        if not isinstance(key, str) or key not in PROVENANCE_KEYS:
            raise ContractError("TPC-H provenance key is not one of the exact five keys")
        if key in result:
            raise ContractError("TPC-H provenance contains a duplicate key")
        if not isinstance(value, str) or not value.strip():
            raise ContractError("TPC-H provenance contains a blank value")
        result[key] = value
    return {key: result[key] for key in PROVENANCE_KEYS}


def validate_tpch_provenance(
    dim_rows: Sequence[Sequence[Any]], fact_rows: Sequence[Sequence[Any]]
) -> dict[str, str]:
    dim = _property_map(dim_rows)
    fact = _property_map(fact_rows)
    if dim != fact:
        raise ContractError("TPC-H provenance maps do not match")
    publication = dim["data_eng_lab.dataset.publication_id"]
    valid = (
        dim["data_eng_lab.dataset"] == "tpch"
        and dim["data_eng_lab.dataset.scale"] in {"tiny", "small", "medium"}
        and re.fullmatch(r"[0-9a-f]{64}", dim["data_eng_lab.dataset.plan_id"])
        and re.fullmatch(r"[0-9a-f]{12}4[0-9a-f]{3}[89ab][0-9a-f]{15}", publication)
        and re.fullmatch(r"[0-9a-f]{64}", dim["data_eng_lab.dataset.manifest_sha256"])
    )
    if not valid:
        raise ContractError("TPC-H provenance identity is malformed")
    return dim


def _schema(rows: Sequence[Sequence[Any]]) -> tuple[tuple[str, str], ...]:
    result: list[tuple[str, str]] = []
    for row in rows:
        if not isinstance(row, (list, tuple)) or len(row) != 2 or not all(isinstance(v, str) for v in row):
            raise ContractError("table schema row is invalid")
        result.append((row[0], row[1].lower()))
    return tuple(result)


def validate_tpch_schemas(dim_rows: Sequence[Sequence[Any]], fact_rows: Sequence[Sequence[Any]]) -> None:
    if _schema(dim_rows) != TPCH_DIM_SCHEMA:
        raise ContractError("dim_customer schema does not match #107")
    if _schema(fact_rows) != TPCH_FACT_SCHEMA:
        raise ContractError("fct_orders schema does not match #107")


def _integer(value: Any, label: str, *, positive: bool = False) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ContractError(f"{label} must be an integer")
    if positive and value <= 0:
        raise ContractError(f"{label} must be positive")
    return value


def _decimal(value: Any, label: str, *, scale: int | None = None, positive: bool = False) -> Decimal:
    if isinstance(value, bool):
        raise ContractError(f"{label} must be a finite decimal")
    try:
        result = Decimal(str(value))
    except (InvalidOperation, ValueError):
        raise ContractError(f"{label} must be a finite decimal") from None
    if not result.is_finite():
        raise ContractError(f"{label} must be finite")
    if scale is not None:
        quantum = Decimal(1).scaleb(-scale)
        if result.quantize(quantum) != result:
            raise ContractError(f"{label} has invalid scale")
    if positive and result <= 0:
        raise ContractError(f"{label} must be positive")
    return result


def _fixed_decimal(value: Any, label: str, scale: int) -> str:
    return format(_decimal(value, label, scale=scale).quantize(Decimal(1).scaleb(-scale)), "f")


def _normalized_decimal(value: Any, label: str) -> str:
    result = _decimal(value, label)
    if result == 0:
        return "0"
    text = format(result.normalize(), "f")
    return text.rstrip("0").rstrip(".") if "." in text else text


def _snapshots(value: dict[str, Any]) -> dict[str, int]:
    if set(value) != {"dim_customer", "fct_orders"}:
        raise ContractError("TPC-H snapshot map is invalid")
    return {key: _integer(value[key], f"{key} snapshot", positive=True) for key in sorted(value)}


def _query_ids(values: Sequence[Any]) -> list[str]:
    if not values or any(not isinstance(value, str) or not value.strip() for value in values):
        raise ContractError("query IDs must be nonblank strings")
    if len(set(values)) != len(values):
        raise ContractError("query IDs must be unique")
    return list(values)


_ALLOWED_ARTIFACT_FIELDS = {
    "artifact_version",
    "columns",
    "pipeline",
    "query_ids",
    "result_sha256",
    "row_count",
    "rows",
    "source",
}


def validate_artifact_fields(value: dict[str, Any]) -> None:
    if not isinstance(value, dict) or not set(value).issubset(_ALLOWED_ARTIFACT_FIELDS):
        raise ContractError("artifact contains fields outside the allowed fields")


def _reject_nonfinite(value: Any) -> None:
    if isinstance(value, float) and not math.isfinite(value):
        raise ContractError("artifact numbers must be finite")
    if isinstance(value, dict):
        for key, child in value.items():
            if not isinstance(key, str):
                raise ContractError("artifact keys must be strings")
            _reject_nonfinite(child)
    elif isinstance(value, (list, tuple)):
        for child in value:
            _reject_nonfinite(child)


def canonical_json_bytes(value: Any) -> bytes:
    _reject_nonfinite(value)
    try:
        return json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        ).encode("utf-8")
    except (TypeError, ValueError) as error:
        raise ContractError("artifact is not canonical JSON") from error


def _finish_artifact(artifact: dict[str, Any], max_bytes: int) -> dict[str, Any]:
    validate_artifact_fields(artifact)
    result_payload = {"columns": artifact["columns"], "rows": artifact["rows"]}
    artifact["result_sha256"] = hashlib.sha256(canonical_json_bytes(result_payload)).hexdigest()
    if len(canonical_json_bytes(artifact)) > max_bytes:
        raise ContractError("artifact exceeds its byte bound")
    return artifact


def build_tpch_artifact(
    *,
    provenance: dict[str, str],
    schemas: dict[str, Sequence[Sequence[Any]]],
    rows: Sequence[Sequence[Any]],
    source_totals: dict[str, Any],
    snapshots_before: dict[str, Any],
    snapshots_after: dict[str, Any],
    provenance_after: dict[str, str],
    query_ids: Sequence[Any],
) -> dict[str, Any]:
    validate_tpch_schemas(schemas.get("dim_customer", ()), schemas.get("fct_orders", ()))
    if provenance_after != provenance:
        raise ContractError("TPC-H provenance changed during the task")
    before = _snapshots(snapshots_before)
    if _snapshots(snapshots_after) != before:
        raise ContractError("TPC-H snapshot changed during the task")
    if len(rows) != 5:
        raise ContractError("TPC-H result must contain five market segments")

    normalized: list[list[Any]] = []
    seen: set[str] = set()
    revenue_total = Decimal(0)
    line_total = 0
    order_total = 0
    for row in rows:
        if not isinstance(row, (list, tuple)) or len(row) != 4:
            raise ContractError("TPC-H result row shape is invalid")
        segment = row[0]
        if not isinstance(segment, str) or not segment.strip():
            raise ContractError("market segment must be nonblank")
        if segment in seen:
            raise ContractError("market segments must be unique")
        seen.add(segment)
        revenue = _decimal(row[1], "total revenue", scale=2, positive=True)
        line_count = _integer(row[2], "line_count", positive=True)
        order_count = _integer(row[3], "order_count", positive=True)
        if line_count < order_count:
            raise ContractError("line_count must be at least order_count")
        normalized.append([segment, format(revenue.quantize(Decimal("0.01")), "f"), line_count, order_count])
        revenue_total += revenue
        line_total += line_count
        order_total += order_count
    normalized.sort(key=lambda row: row[0])

    required_totals = {"fact_order_count", "fact_revenue", "fact_line_count", "unmatched_orders"}
    if set(source_totals) != required_totals:
        raise ContractError("TPC-H source totals shape is invalid")
    if _integer(source_totals["unmatched_orders"], "unmatched orders") != 0:
        raise ContractError("TPC-H customer join is incomplete")
    source_revenue = _decimal(source_totals["fact_revenue"], "source revenue", scale=2, positive=True)
    if revenue_total != source_revenue:
        raise ContractError("TPC-H result revenue does not reconcile")
    if line_total != _integer(source_totals["fact_line_count"], "source line count", positive=True):
        raise ContractError("TPC-H result line_count does not reconcile")
    if order_total != _integer(source_totals["fact_order_count"], "source order count", positive=True):
        raise ContractError("TPC-H result order count does not reconcile")

    artifact = {
        "artifact_version": 1,
        "pipeline": "tpch_bi_query",
        "columns": [{"name": name, "type": data_type} for name, data_type in TPCH_RESULT_COLUMNS],
        "rows": normalized,
        "row_count": len(normalized),
        "source": {"provenance": dict(provenance), "snapshots": before},
        "query_ids": _query_ids(query_ids),
    }
    return _finish_artifact(artifact, TPCH_ARTIFACT_MAX_BYTES)


def _validate_nyc_schema(rows: Sequence[Sequence[Any]]) -> None:
    schema = dict(_schema(rows))
    required = {"trip_date": "date", "fare_amount": "double"}
    for name, data_type in required.items():
        if schema.get(name) != data_type:
            raise ContractError(f"NYC source {name} must have type {data_type}")


def build_nyc_artifact(
    *,
    source_schema: Sequence[Sequence[Any]],
    rows: Sequence[Sequence[Any]],
    source_count: Any,
    snapshot_before: Any,
    snapshot_after: Any,
    query_ids: Sequence[Any],
) -> dict[str, Any]:
    _validate_nyc_schema(source_schema)
    snapshot = _integer(snapshot_before, "NYC snapshot", positive=True)
    if _integer(snapshot_after, "NYC snapshot", positive=True) != snapshot:
        raise ContractError("NYC source snapshot changed during the task")
    expected_count = _integer(source_count, "NYC source count", positive=True)
    if not rows:
        raise ContractError("NYC daily result must be nonempty")
    if len(rows) > NYC_MAX_ROWS:
        raise ContractError("NYC daily result exceeds its row bound")

    normalized: list[list[Any]] = []
    seen: set[str] = set()
    result_count = 0
    for row in rows:
        if not isinstance(row, (list, tuple)) or len(row) != 3:
            raise ContractError("NYC result row shape is invalid")
        trip_date = row[0]
        if not isinstance(trip_date, str) or re.fullmatch(r"\d{4}-\d{2}-\d{2}", trip_date) is None:
            raise ContractError("NYC trip_date must be an ISO date")
        if trip_date in seen:
            raise ContractError("NYC trip_date values must be unique")
        seen.add(trip_date)
        trip_count = _integer(row[1], "NYC trip_count", positive=True)
        avg_fare = _normalized_decimal(row[2], "NYC avg_fare")
        normalized.append([trip_date, trip_count, avg_fare])
        result_count += trip_count
    normalized.sort(key=lambda row: row[0])
    if result_count != expected_count:
        raise ContractError("NYC result does not reconcile with source count")

    artifact = {
        "artifact_version": 1,
        "pipeline": "nyc_taxi_trino_daily",
        "columns": [{"name": name, "type": data_type} for name, data_type in NYC_RESULT_COLUMNS],
        "rows": normalized,
        "row_count": len(normalized),
        "source": {"binding": "iceberg_snapshot", "row_count": expected_count, "snapshot_id": snapshot},
        "query_ids": _query_ids(query_ids),
    }
    return _finish_artifact(artifact, NYC_ARTIFACT_MAX_BYTES)


for _query in QUERIES.values():
    validate_read_only_sql(_query.sql)
