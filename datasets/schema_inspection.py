"""Offline physical-schema inspection for locked dataset objects."""

from __future__ import annotations

import codecs
import csv
import gzip
import io
import json
import math
import posixpath
import re
import zipfile
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from decimal import Decimal, InvalidOperation
from pathlib import Path, PurePosixPath
from typing import BinaryIO, cast
from xml.etree import ElementTree

import duckdb

from datasets.locking import schema_fingerprint
from datasets.registry import SchemaContract, SchemaField
from datasets.verification import LockMismatch, VerificationContext, VerifiedFile

_DUCKDB_VERSION = "1.5.4"
_MIN_EXPANDED_BYTES = 64 << 20
_MAX_EXPANDED_BYTES = 8 << 30
_EXPANSION_FACTOR = 200
_MAX_JSON_DEPTH = 64
_MAX_RECORD_BYTES = 16 << 20
_MAX_STRING_BYTES = 16 << 20
_READ_SIZE = 1 << 20

_DECIMAL_TYPE_RE = re.compile(r"^DECIMAL\(([0-9]+),([0-9]+)\)$")
_PARQUET_DECIMAL_ANNOTATION_RE = re.compile(
    r"^DecimalType\(scale=([0-9]+), precision=([0-9]+)\)$"
)
_PARQUET_INTEGER_ANNOTATION_RE = re.compile(
    r"^IntType\(bitWidth=(8|16|32|64), isSigned=(0|1|true|false)\)$",
    re.IGNORECASE,
)
_PARQUET_TIMESTAMP_ANNOTATION_RE = re.compile(
    r"^TimestampType\(isAdjustedToUTC=(0|1), unit=TimeUnit\("
    r"MILLIS=(MilliSeconds\(\)|<null>), "
    r"MICROS=(MicroSeconds\(\)|<null>), "
    r"NANOS=(NanoSeconds\(\)|<null>)\)\)$"
)
_INTEGER_RE = re.compile(r"^[+-]?[0-9]+$")
_DECIMAL_VALUE_RE = re.compile(r"^[+-]?(?:[0-9]+(?:\.[0-9]*)?|\.[0-9]+)$")
_DATE_RE = re.compile(r"^[0-9]{4}-[0-9]{2}-[0-9]{2}$")
_TIMESTAMP_RE = re.compile(
    r"^(?P<date>[0-9]{4}-[0-9]{2}-[0-9]{2})T"
    r"(?P<time>[0-9]{2}:[0-9]{2}:[0-9]{2}(?:\.[0-9]{1,9})?)"
    r"(?P<offset>Z|[+-][0-9]{2}:[0-9]{2})?$"
)
_CELL_REFERENCE_RE = re.compile(r"^([A-Z]+)([1-9][0-9]*)$")
_INTEGER_RANGES = {
    "int8": (-(1 << 7), (1 << 7) - 1),
    "int16": (-(1 << 15), (1 << 15) - 1),
    "int32": (-(1 << 31), (1 << 31) - 1),
    "int64": (-(1 << 63), (1 << 63) - 1),
    "uint8": (0, (1 << 8) - 1),
    "uint16": (0, (1 << 16) - 1),
    "uint32": (0, (1 << 32) - 1),
    "uint64": (0, (1 << 64) - 1),
}
_PARQUET_DUCKDB_TYPES = {
    "BOOLEAN": "boolean",
    "TINYINT": "int8",
    "SMALLINT": "int16",
    "INTEGER": "int32",
    "BIGINT": "int64",
    "UTINYINT": "uint8",
    "USMALLINT": "uint16",
    "UINTEGER": "uint32",
    "UBIGINT": "uint64",
    "FLOAT": "float32",
    "REAL": "float32",
    "DOUBLE": "float64",
    "DATE": "date",
    "TIMESTAMP_S": "timestamp",
    "TIMESTAMP_MS": "timestamp",
    "TIMESTAMP": "timestamp",
    "TIMESTAMP_NS": "timestamp",
    "TIMESTAMP WITH TIME ZONE": "timestamp-tz",
    "VARCHAR": "string",
    "BLOB": "binary",
}
_PARQUET_INTEGER_ANNOTATIONS = {
    "INT_8": "int8",
    "INT_16": "int16",
    "INT_32": "int32",
    "INT_64": "int64",
    "UINT_8": "uint8",
    "UINT_16": "uint16",
    "UINT_32": "uint32",
    "UINT_64": "uint64",
}
_BUILTIN_DATE_FORMATS = frozenset({14, 15, 16, 17, 18, 19, 20, 21, 22, 45, 46, 47})


@dataclass(frozen=True)
class ObservedField:
    name: str
    logical_type: str
    nullable: bool


@dataclass(frozen=True)
class ObservedSchema:
    fields: tuple[ObservedField, ...]


def _plain(value: object) -> object:
    if isinstance(value, Mapping):
        return {str(key): _plain(item) for key, item in value.items()}
    if isinstance(value, tuple):
        return [_plain(item) for item in value]
    return value


def _contract_fingerprint(contract: SchemaContract) -> str:
    payload = {
        "format": contract.format,
        "mode": contract.mode,
        "fields": [
            {
                "name": field.name,
                "logical_type": field.logical_type,
                "nullable": field.nullable,
            }
            for field in contract.fields
        ],
        "options": _plain(contract.options),
    }
    return schema_fingerprint(payload)


def _expanded_limit(locked_size: int) -> int:
    return min(max(_MIN_EXPANDED_BYTES, _EXPANSION_FACTOR * locked_size), _MAX_EXPANDED_BYTES)


def _input_path(value: Path | VerifiedFile) -> tuple[Path, int]:
    if isinstance(value, VerifiedFile):
        return value.path, value.expected.size_bytes
    path = Path(value)
    return path, path.stat().st_size


def _expected_fields(contract: SchemaContract) -> tuple[ObservedField, ...]:
    return tuple(
        ObservedField(field.name, field.logical_type, field.nullable) for field in contract.fields
    )


def _field_value(row: object, field: str, index: int) -> object:
    if isinstance(row, Mapping):
        return row.get(field)
    if isinstance(row, Sequence) and not isinstance(row, (str, bytes, bytearray)):
        return row[index] if index < len(row) else None
    return getattr(row, field, None)


def _normalize_physical_parquet_type(row: object) -> str:
    duckdb_type_value = _field_value(row, "duckdb_type", 11)
    duckdb_type = str(duckdb_type_value).upper() if duckdb_type_value is not None else ""
    physical_value = _field_value(row, "type", 2)
    physical = str(physical_value).upper() if physical_value is not None else ""
    converted_value = _field_value(row, "converted_type", 6)
    converted = str(converted_value).upper() if converted_value is not None else ""
    logical_value = _field_value(row, "logical_type", 10)
    logical_annotation = str(logical_value) if logical_value is not None else ""
    precision_value = _field_value(row, "precision", 8)
    scale_value = _field_value(row, "scale", 7)

    physical_logical: str
    if physical in {"BOOLEAN", "FLOAT", "DOUBLE"}:
        if converted or logical_annotation:
            raise ValueError(f"unsupported annotation for Parquet {physical}")
        physical_logical = {
            "BOOLEAN": "boolean",
            "FLOAT": "float32",
            "DOUBLE": "float64",
        }[physical]
    elif converted == "DECIMAL" or logical_annotation.startswith("DecimalType("):
        if physical not in {"INT32", "INT64", "BYTE_ARRAY", "FIXED_LEN_BYTE_ARRAY"}:
            raise ValueError("unsupported Parquet decimal physical type")
        if converted not in {"", "DECIMAL"}:
            raise ValueError("inconsistent Parquet decimal converted annotation")
        if not isinstance(precision_value, int) or not isinstance(scale_value, int):
            raise ValueError("Parquet decimal is missing precision or scale")
        annotation_match = _PARQUET_DECIMAL_ANNOTATION_RE.fullmatch(logical_annotation)
        if logical_annotation and annotation_match is None:
            raise ValueError("unsupported Parquet decimal annotation")
        if annotation_match is not None:
            annotation_scale, annotation_precision = map(int, annotation_match.groups())
            if (annotation_precision, annotation_scale) != (precision_value, scale_value):
                raise ValueError("inconsistent Parquet decimal annotation")
        if not 1 <= precision_value <= 38 or not 0 <= scale_value <= precision_value:
            raise ValueError("unsupported Parquet decimal precision or scale")
        physical_logical = f"decimal({precision_value},{scale_value})"
    elif physical == "INT32" and (converted == "DATE" or logical_annotation == "DateType()"):
        if converted not in {"", "DATE"} or logical_annotation not in {"", "DateType()"}:
            raise ValueError("inconsistent Parquet date annotation")
        physical_logical = "date"
    elif physical == "INT64" and logical_annotation.startswith("TimestampType("):
        annotation_match = _PARQUET_TIMESTAMP_ANNOTATION_RE.fullmatch(logical_annotation)
        if annotation_match is None:
            raise ValueError("unsupported Parquet timestamp annotation")
        adjusted_to_utc, *units = annotation_match.groups()
        if sum(unit != "<null>" for unit in units) != 1:
            raise ValueError("unsupported Parquet timestamp unit")
        if adjusted_to_utc == "1":
            physical_logical = "timestamp-tz"
        else:
            if converted in {"TIMESTAMP_MILLIS", "TIMESTAMP_MICROS"}:
                raise ValueError("inconsistent Parquet timestamp UTC annotation")
            physical_logical = "timestamp"
        if converted not in {"", "TIMESTAMP_MILLIS", "TIMESTAMP_MICROS"}:
            raise ValueError("unsupported Parquet timestamp annotation")
    elif physical == "INT64" and converted in {"TIMESTAMP_MILLIS", "TIMESTAMP_MICROS"}:
        physical_logical = "timestamp-tz"
    elif physical in {"INT32", "INT64"}:
        converted_logical = _PARQUET_INTEGER_ANNOTATIONS.get(converted)
        annotation_match = _PARQUET_INTEGER_ANNOTATION_RE.fullmatch(logical_annotation)
        annotation_logical = None
        if annotation_match is not None:
            width, signed_value = annotation_match.groups()
            signed = signed_value.lower() in {"1", "true"}
            annotation_logical = f"{'int' if signed else 'uint'}{width}"
        elif logical_annotation:
            raise ValueError("unsupported Parquet integer annotation")
        physical_logical = converted_logical or annotation_logical or ""
        if not physical_logical:
            raise ValueError("ambiguous Parquet integer is missing its width annotation")
        if converted_logical and annotation_logical and converted_logical != annotation_logical:
            raise ValueError("inconsistent Parquet integer annotations")
        expected_physical = "INT64" if physical_logical.endswith("64") else "INT32"
        if physical != expected_physical:
            raise ValueError("inconsistent Parquet integer physical width")
    elif physical in {"BYTE_ARRAY", "FIXED_LEN_BYTE_ARRAY"}:
        string_annotation = converted in {"UTF8", "STRING"} or logical_annotation == "StringType()"
        if string_annotation:
            if converted not in {"", "UTF8", "STRING"} or logical_annotation not in {
                "",
                "StringType()",
            }:
                raise ValueError("inconsistent Parquet string annotation")
            physical_logical = "string"
        elif not converted and not logical_annotation:
            physical_logical = "binary"
        else:
            raise ValueError("unsupported Parquet byte-array annotation")
    else:
        raise ValueError(f"unsupported or ambiguous Parquet physical type {physical!r}")

    decimal_match = _DECIMAL_TYPE_RE.fullmatch(duckdb_type)
    if decimal_match is not None:
        duckdb_precision, duckdb_scale = map(int, decimal_match.groups())
        duckdb_logical = f"decimal({duckdb_precision},{duckdb_scale})"
    else:
        duckdb_logical = _PARQUET_DUCKDB_TYPES.get(duckdb_type)
    if duckdb_logical is None:
        raise ValueError(f"unsupported DuckDB Parquet type {duckdb_type!r}")
    if duckdb_logical != physical_logical:
        raise ValueError(
            f"inconsistent Parquet metadata: physical {physical_logical}, DuckDB {duckdb_logical}"
        )
    return physical_logical


def normalize_parquet_schema(rows: Iterable[object]) -> tuple[ObservedField, ...]:
    """Freeze root Parquet order, type and repetition from DuckDB metadata rows."""
    fields: list[ObservedField] = []
    names: set[str] = set()
    saw_schema_root = False
    for row in rows:
        name_value = _field_value(row, "name", 1)
        physical_type = _field_value(row, "type", 2)
        children = _field_value(row, "num_children", 5)
        if physical_type is None and children is not None:
            if saw_schema_root or fields:
                raise ValueError("nested Parquet group is unsupported")
            saw_schema_root = True
            continue
        if not isinstance(name_value, str) or not name_value:
            raise ValueError("Parquet field name is missing")
        if name_value in names:
            raise ValueError(f"duplicate Parquet field {name_value!r}")
        if children not in (None, 0):
            raise ValueError("nested Parquet fields are unsupported")
        repetition_value = _field_value(row, "repetition_type", 4)
        repetition = str(repetition_value).upper() if repetition_value is not None else ""
        if repetition not in {"REQUIRED", "OPTIONAL"}:
            raise ValueError("repeated or unknown Parquet repetition is unsupported")
        logical_type = _normalize_physical_parquet_type(row)
        fields.append(ObservedField(name_value, logical_type, repetition == "OPTIONAL"))
        names.add(name_value)
    if not fields:
        raise ValueError("Parquet schema has no root fields")
    return tuple(fields)


def _raise_value_mismatch(
    context: VerificationContext,
    field: SchemaField,
    reason: str,
    record_number: int,
) -> None:
    raise LockMismatch(
        context,
        f"{field.name} {reason}",
        f"{field.logical_type}; nullable={field.nullable}",
        f"invalid value at record {record_number}",
    )


def _parse_datetime(value: str, logical_type: str) -> bool:
    if logical_type == "date":
        if _DATE_RE.fullmatch(value) is None:
            return False
        try:
            return date.fromisoformat(value).isoformat() == value
        except ValueError:
            return False
    match = _TIMESTAMP_RE.fullmatch(value)
    if match is None:
        return False
    candidate = value[:-1] + "+00:00" if value.endswith("Z") else value
    try:
        parsed = datetime.fromisoformat(candidate)
    except ValueError:
        return False
    has_offset = match.group("offset") is not None and parsed.utcoffset() is not None
    if logical_type == "timestamp-tz":
        return has_offset
    return not has_offset


def _decimal_shape(value: Decimal) -> tuple[int, int]:
    _, digits, exponent = value.as_tuple()
    scale = max(-exponent, 0)
    integer_digits = max(len(digits) + exponent, 0)
    return integer_digits + scale, scale


def _validate_value(value: object, logical_type: str, source: str, *, date_formatted: bool = False) -> bool:
    if logical_type == "string":
        return isinstance(value, str)
    if logical_type == "binary":
        return isinstance(value, (bytes, bytearray))
    if logical_type == "json":
        if source == "csv" and isinstance(value, str):
            try:
                _strict_json_loads(value)
            except (json.JSONDecodeError, RecursionError, UnicodeError, ValueError):
                return False
        return source == "json" or source == "csv"
    if logical_type == "boolean":
        if source == "json":
            return type(value) is bool
        if source == "xlsx" and type(value) is bool:
            return True
        return isinstance(value, str) and value.lower() in {"true", "false"}
    if logical_type in _INTEGER_RANGES:
        if date_formatted:
            return False
        if source == "json":
            if type(value) is not int:
                return False
            integer = cast(int, value)
        elif source == "xlsx":
            if not isinstance(value, Decimal) or value != value.to_integral_value():
                return False
            integer = int(value)
        else:
            if not isinstance(value, str) or _INTEGER_RE.fullmatch(value) is None:
                return False
            integer = int(value, 10)
        lower, upper = _INTEGER_RANGES[logical_type]
        return lower <= integer <= upper
    if logical_type in {"float32", "float64"}:
        if date_formatted or type(value) is bool:
            return False
        try:
            if source == "csv":
                if not isinstance(value, str) or _DECIMAL_VALUE_RE.fullmatch(value) is None:
                    return False
                number = float(value)
            else:
                if not isinstance(value, (int, float, Decimal)):
                    return False
                number = float(value)
        except (OverflowError, ValueError):
            return False
        limit = 3.4028234663852886e38 if logical_type == "float32" else 1.7976931348623157e308
        return math.isfinite(number) and abs(number) <= limit
    decimal_type = re.fullmatch(r"decimal\(([0-9]+),([0-9]+)\)", logical_type)
    if decimal_type is not None:
        if date_formatted or type(value) is bool:
            return False
        if source == "csv" and (
            not isinstance(value, str) or _DECIMAL_VALUE_RE.fullmatch(value) is None
        ):
            return False
        if source == "json" and not isinstance(value, (int, Decimal)):
            return False
        if source == "xlsx" and not isinstance(value, Decimal):
            return False
        try:
            number = Decimal(value)
        except (InvalidOperation, TypeError, ValueError):
            return False
        if not number.is_finite():
            return False
        precision, scale = map(int, decimal_type.groups())
        actual_precision, actual_scale = _decimal_shape(number)
        return actual_precision <= precision and actual_scale <= scale
    if logical_type in {"date", "timestamp", "timestamp-tz"}:
        if source == "xlsx" and date_formatted:
            if not isinstance(value, (date, datetime)):
                return False
            if logical_type == "date":
                return type(value) is date
            return isinstance(value, datetime) and logical_type == "timestamp"
        return isinstance(value, str) and _parse_datetime(value, logical_type)
    return False


def _validate_record_values(
    values: Sequence[object | None],
    contract: SchemaContract,
    context: VerificationContext,
    record_number: int,
    source: str,
    date_fields: frozenset[int] = frozenset(),
) -> None:
    for index, (field, value) in enumerate(zip(contract.fields, values, strict=True)):
        if value is None or (source == "csv" and value == ""):
            if not field.nullable:
                _raise_value_mismatch(context, field, "null", record_number)
            continue
        if not _validate_value(
            value,
            field.logical_type,
            source,
            date_formatted=index in date_fields,
        ):
            _raise_value_mismatch(context, field, field.logical_type, record_number)


def inspect_parquet(
    path: Path,
    contract: SchemaContract,
    context: VerificationContext,
    locked_size: int,
) -> ObservedSchema:
    del contract, locked_size
    if duckdb.__version__ != _DUCKDB_VERSION:
        raise LockMismatch(context, "duckdb_version", _DUCKDB_VERSION, duckdb.__version__)
    try:
        connection = duckdb.connect(":memory:")
        try:
            connection.execute("SET autoinstall_known_extensions = false")
            connection.execute("SET autoload_known_extensions = false")
            result = connection.execute("SELECT * FROM parquet_schema(?) ORDER BY column_id", [str(path)])
            columns = [description[0] for description in result.description]
            rows = [dict(zip(columns, row, strict=True)) for row in result.fetchall()]
        finally:
            connection.close()
        return ObservedSchema(normalize_parquet_schema(rows))
    except LockMismatch:
        raise
    except Exception as error:
        raise LockMismatch(context, "parquet_schema", "supported flat Parquet schema", type(error).__name__) from error


def inspect_csv(
    path: Path,
    contract: SchemaContract,
    context: VerificationContext,
    locked_size: int,
) -> ObservedSchema:
    del locked_size
    delimiter = contract.options.get("delimiter")
    has_header = contract.options.get("header")
    if not isinstance(delimiter, str) or len(delimiter) != 1 or not isinstance(has_header, bool):
        raise LockMismatch(context, "csv_options", "validated CSV options", "invalid options")
    try:
        with path.open("rb") as raw:
            if raw.read(3) == codecs.BOM_UTF8:
                raise LockMismatch(context, "CSV BOM", "UTF-8 without BOM", "BOM present")
            raw.seek(0)
            text = io.TextIOWrapper(raw, encoding="utf-8", errors="strict", newline="")
            reader = csv.reader(_strict_csv_lines(text, delimiter), delimiter=delimiter, strict=True)
            record_number = 0
            if has_header:
                try:
                    header = next(reader)
                except StopIteration as error:
                    raise LockMismatch(context, "header", tuple(field.name for field in contract.fields), ()) from error
                if tuple(header) != tuple(field.name for field in contract.fields):
                    raise LockMismatch(
                        context,
                        "header",
                        tuple(field.name for field in contract.fields),
                        tuple(header),
                    )
            for record_number, row in enumerate(reader, start=1):
                if len(row) != len(contract.fields):
                    raise LockMismatch(context, "CSV width", len(contract.fields), len(row))
                _validate_record_values(row, contract, context, record_number, "csv")
    except LockMismatch:
        raise
    except UnicodeError as error:
        raise LockMismatch(context, "UTF-8", "valid UTF-8", "decode error") from error
    except csv.Error as error:
        raise LockMismatch(context, "CSV quoting", "strict CSV", "malformed CSV") from error
    return ObservedSchema(_expected_fields(contract))


def _strict_csv_lines(text: Iterable[str], delimiter: str) -> Iterable[str]:
    state = "unquoted"
    field_start = True
    for line in text:
        for character in line:
            if character == "\x00":
                raise csv.Error("NUL is forbidden in CSV")
            if state == "quoted":
                if character == '"':
                    state = "after_quote"
                continue
            if state == "after_quote":
                if character == '"':
                    state = "quoted"
                elif character == delimiter:
                    state = "unquoted"
                    field_start = True
                elif character in "\r\n":
                    state = "unquoted"
                    field_start = True
                else:
                    raise csv.Error("characters after closing quote")
                continue
            if character == '"':
                if not field_start:
                    raise csv.Error("quote inside unquoted field")
                state = "quoted"
                field_start = False
            elif character == delimiter or character in "\r\n":
                field_start = True
            else:
                field_start = False
        yield line
    if state == "quoted":
        raise csv.Error("unterminated quoted field")


def _json_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"duplicate JSON member {key!r}")
        result[key] = value
    return result


def _reject_json_constant(value: str) -> object:
    raise ValueError(f"non-standard JSON constant {value!r}")


def _strict_json_loads(value: str | bytes) -> object:
    return json.loads(
        value,
        parse_float=Decimal,
        parse_int=int,
        parse_constant=_reject_json_constant,
        object_pairs_hook=_json_object,
    )


def _check_json_lexical_depth(value: bytes) -> None:
    text = value.decode("utf-8", errors="strict")
    depth = 0
    in_string = False
    escaped = False
    for character in text:
        if in_string:
            if escaped:
                escaped = False
            elif character == "\\":
                escaped = True
            elif character == '"':
                in_string = False
            continue
        if character == '"':
            in_string = True
        elif character in "[{":
            depth += 1
            if depth > _MAX_JSON_DEPTH:
                raise ValueError("JSON depth bound exceeded")
        elif character in "]}":
            depth -= 1


def _check_json_bounds(root: object) -> None:
    stack: list[tuple[object, int]] = [(root, 1)]
    while stack:
        value, depth = stack.pop()
        if isinstance(value, str) and len(value.encode("utf-8")) > _MAX_STRING_BYTES:
            raise ValueError("JSON string bound exceeded")
        if isinstance(value, Mapping):
            if depth > _MAX_JSON_DEPTH:
                raise ValueError("JSON depth bound exceeded")
            for key, item in value.items():
                if len(key.encode("utf-8")) > _MAX_STRING_BYTES:
                    raise ValueError("JSON string bound exceeded")
                stack.append((item, depth + 1))
        elif isinstance(value, list):
            if depth > _MAX_JSON_DEPTH:
                raise ValueError("JSON depth bound exceeded")
            stack.extend((item, depth + 1) for item in value)


_MISSING = object()


def _dotted_value(record: Mapping[str, object], path: str) -> object:
    value: object = record
    for part in path.split("."):
        if not isinstance(value, Mapping) or part not in value:
            return _MISSING
        value = value[part]
    return value


def _json_paths(
    record: Mapping[str, object], terminal_paths: frozenset[tuple[str, ...]]
) -> frozenset[tuple[str, ...]]:
    paths: set[tuple[str, ...]] = set()
    stack: list[tuple[tuple[str, ...], object]] = [((), record)]
    while stack:
        prefix, value = stack.pop()
        if prefix in terminal_paths:
            paths.add(prefix)
        elif isinstance(value, Mapping) and value:
            stack.extend(((*prefix, key), item) for key, item in value.items())
        elif prefix:
            paths.add(prefix)
    return frozenset(paths)


def inspect_jsonl_gzip(
    path: Path,
    contract: SchemaContract,
    context: VerificationContext,
    locked_size: int,
) -> ObservedSchema:
    expanded = 0
    records = 0
    expanded_limit = _expanded_limit(locked_size)
    declared_paths = frozenset(tuple(field.name.split(".")) for field in contract.fields)
    try:
        with gzip.open(path, "rb") as stream:
            while True:
                line = stream.readline(_MAX_RECORD_BYTES + 2)
                if not line:
                    break
                expanded += len(line)
                if expanded > expanded_limit:
                    raise LockMismatch(context, "expanded bytes", expanded_limit, expanded)
                if len(line.rstrip(b"\r\n")) > _MAX_RECORD_BYTES:
                    raise LockMismatch(context, "JSON record bound", _MAX_RECORD_BYTES, len(line))
                if not line.strip():
                    continue
                records += 1
                try:
                    _check_json_lexical_depth(line)
                    record = _strict_json_loads(line)
                except RecursionError as error:
                    raise LockMismatch(
                        context,
                        "JSON depth",
                        f"at most {_MAX_JSON_DEPTH} levels",
                        "decoder recursion exceeded",
                    ) from error
                except (json.JSONDecodeError, ValueError, UnicodeError) as error:
                    reason = str(error)
                    if "depth" in reason:
                        field = "JSON depth"
                    elif "string bound" in reason:
                        field = "JSON string"
                    else:
                        field = "JSON record"
                    raise LockMismatch(context, field, "one JSON object", "invalid JSON") from error
                if not isinstance(record, dict):
                    raise LockMismatch(context, "JSON object", "object", type(record).__name__)
                try:
                    _check_json_bounds(record)
                except ValueError as error:
                    reason = str(error)
                    field = "JSON depth" if "depth" in reason else "JSON string"
                    raise LockMismatch(context, field, "within bound", "bound exceeded") from error
                if contract.mode == "exact":
                    actual_paths = _json_paths(record, declared_paths)
                    if actual_paths != declared_paths:
                        raise LockMismatch(
                            context,
                            "JSON fields",
                            tuple(sorted(declared_paths)),
                            tuple(sorted(actual_paths)),
                        )
                values: list[object | None] = []
                for field in contract.fields:
                    value = _dotted_value(record, field.name)
                    if value is _MISSING:
                        if not field.nullable:
                            _raise_value_mismatch(context, field, "missing", records)
                        values.append(None)
                    else:
                        values.append(cast(object | None, value))
                _validate_record_values(values, contract, context, records, "json")
        if records == 0:
            raise LockMismatch(context, "JSON records", "at least one record", 0)
    except LockMismatch:
        raise
    except (gzip.BadGzipFile, EOFError, OSError) as error:
        raise LockMismatch(context, "gzip integrity", "valid gzip stream", type(error).__name__) from error
    return ObservedSchema(_expected_fields(contract))


class _ArchiveBudget:
    def __init__(self, limit: int, context: VerificationContext):
        self.limit = limit
        self.context = context
        self.expanded = 0

    def add(self, amount: int) -> None:
        self.expanded += amount
        if self.expanded > self.limit:
            raise LockMismatch(self.context, "expanded bytes", self.limit, self.expanded)


class _CheckedXmlReader:
    def __init__(self, stream: BinaryIO, budget: _ArchiveBudget):
        self._stream = stream
        self._budget = budget
        self._raw_probe = bytearray()
        self._decoder: codecs.IncrementalDecoder | None = None
        self._decoded_tail = ""

    def _detect_encoding(self, *, final: bool) -> str | None:
        probe = bytes(self._raw_probe)
        if probe.startswith(b"\xff\xfe"):
            return "utf-16-le"
        if probe.startswith(b"\xfe\xff"):
            return "utf-16-be"
        if probe.startswith(codecs.BOM_UTF8):
            return "utf-8-sig"
        if len(probe) >= 4:
            if probe[:4] in {b"\x00\x00\x00<", b"<\x00\x00\x00"}:
                raise ValueError("unsupported wide XML encoding")
            if probe[0] == 0 and probe[1] == ord("<") and probe[2] == 0:
                return "utf-16-be"
            if probe[0] == ord("<") and probe[1] == 0 and probe[3] == 0:
                return "utf-16-le"
            if 0 in probe[:4]:
                raise ValueError("unsupported wide XML encoding")
            return "utf-8"
        return "utf-8" if final else None

    def _scan_declarations(self, data: bytes, *, final: bool) -> None:
        if self._decoder is None:
            self._raw_probe.extend(data)
            encoding = self._detect_encoding(final=final)
            if encoding is None:
                return
            decoder_type = codecs.getincrementaldecoder(encoding)
            self._decoder = decoder_type(errors="strict")
            decoded = self._decoder.decode(bytes(self._raw_probe), final=final)
            self._raw_probe.clear()
        else:
            decoded = self._decoder.decode(data, final=final)
        checked = (self._decoded_tail + decoded).upper()
        if "<!DOCTYPE" in checked:
            raise ValueError("DTD declaration is forbidden")
        if "<!ENTITY" in checked:
            raise ValueError("entity declaration is forbidden")
        self._decoded_tail = checked[-16:]

    def read(self, size: int = -1) -> bytes:
        data = self._stream.read(size)
        self._budget.add(len(data))
        self._scan_declarations(data, final=not data)
        return data


def _local_name(tag: str) -> str:
    return tag.rsplit("}", 1)[-1]


def _xml_events(
    archive: zipfile.ZipFile,
    member: str,
    budget: _ArchiveBudget,
) -> Iterable[tuple[str, ElementTree.Element]]:
    stream = archive.open(member, "r")
    try:
        yield from ElementTree.iterparse(_CheckedXmlReader(stream, budget), events=("start", "end"))
    finally:
        stream.close()


def _safe_xlsx_archive(
    path: Path,
    locked_size: int,
    context: VerificationContext,
) -> tuple[zipfile.ZipFile, _ArchiveBudget]:
    limit = _expanded_limit(locked_size)
    try:
        archive = zipfile.ZipFile(path)
        infos = archive.infolist()
    except (OSError, zipfile.BadZipFile) as error:
        raise LockMismatch(context, "XLSX ZIP", "valid OOXML ZIP", type(error).__name__) from error
    names: set[str] = set()
    total = 0
    for info in infos:
        candidate = PurePosixPath(info.filename)
        if (
            not info.filename
            or candidate.is_absolute()
            or "\\" in info.filename
            or any(part in {"", ".", ".."} for part in info.filename.split("/"))
            or info.filename in names
            or info.flag_bits & 0x1
            or info.compress_type not in {zipfile.ZIP_STORED, zipfile.ZIP_DEFLATED}
        ):
            archive.close()
            raise LockMismatch(context, "XLSX ZIP", "safe unique members", "unsafe member")
        names.add(info.filename)
        total += info.file_size
        if total > limit:
            archive.close()
            raise LockMismatch(context, "expanded bytes", limit, total)
    return archive, _ArchiveBudget(limit, context)


def _parse_workbook(
    archive: zipfile.ZipFile,
    budget: _ArchiveBudget,
) -> tuple[list[tuple[str, str, bool]], bool]:
    sheets: list[tuple[str, str, bool]] = []
    date_1904 = False
    for event, element in _xml_events(archive, "xl/workbook.xml", budget):
        if event != "end":
            continue
        tag = _local_name(element.tag)
        if tag == "workbookPr":
            date_1904 = element.attrib.get("date1904", "0").lower() in {"1", "true"}
        elif tag == "sheet":
            relationship_id = next(
                (value for key, value in element.attrib.items() if _local_name(key) == "id"),
                "",
            )
            sheets.append(
                (
                    element.attrib.get("name", ""),
                    relationship_id,
                    element.attrib.get("state", "visible") == "visible",
                )
            )
        element.clear()
    return sheets, date_1904


def _parse_relationships(
    archive: zipfile.ZipFile,
    budget: _ArchiveBudget,
) -> dict[str, str]:
    relationships: dict[str, str] = {}
    for event, element in _xml_events(archive, "xl/_rels/workbook.xml.rels", budget):
        if event == "end" and _local_name(element.tag) == "Relationship":
            if element.attrib.get("TargetMode") == "External":
                raise ValueError("external workbook relationship is forbidden")
            identifier = element.attrib.get("Id", "")
            target = element.attrib.get("Target", "")
            if target.startswith("/"):
                normalized = target.lstrip("/")
            else:
                normalized = posixpath.normpath(posixpath.join("xl", target))
            if not identifier or not normalized.startswith("xl/") or ".." in PurePosixPath(normalized).parts:
                raise ValueError("unsafe workbook relationship")
            relationships[identifier] = normalized
            element.clear()
    return relationships


def _parse_shared_strings(
    archive: zipfile.ZipFile,
    budget: _ArchiveBudget,
) -> tuple[str, ...]:
    if "xl/sharedStrings.xml" not in archive.namelist():
        return ()
    strings: list[str] = []
    for event, element in _xml_events(archive, "xl/sharedStrings.xml", budget):
        if event == "end" and _local_name(element.tag) == "si":
            strings.append(
                "".join(
                    child.text or "" for child in element.iter() if _local_name(child.tag) == "t"
                )
            )
            element.clear()
    return tuple(strings)


def _looks_like_date_format(format_code: str) -> bool:
    normalized = re.sub(r'"[^"\\]*(?:\\.[^"\\]*)*"', "", format_code.lower())
    normalized = re.sub(r"\[(?!h+\]|m+\]|s+\])[^]]*\]", "", normalized)
    normalized = re.sub(r"\\.|_.|\*.", "", normalized)
    return re.search(r"(?:y+|d+|h+|s+|m{1,4})", normalized) is not None


def _parse_date_styles(
    archive: zipfile.ZipFile,
    budget: _ArchiveBudget,
) -> tuple[bool, ...]:
    if "xl/styles.xml" not in archive.namelist():
        return (False,)
    custom_formats: dict[int, str] = {}
    styles: list[bool] = []
    in_cell_xfs = False
    for event, element in _xml_events(archive, "xl/styles.xml", budget):
        tag = _local_name(element.tag)
        if event == "start" and tag == "cellXfs":
            in_cell_xfs = True
        elif event == "end" and tag == "cellXfs":
            in_cell_xfs = False
            element.clear()
        elif event == "end" and tag == "numFmt":
            try:
                custom_formats[int(element.attrib["numFmtId"])] = element.attrib["formatCode"]
            except (KeyError, ValueError) as error:
                raise ValueError("invalid XLSX number format") from error
            element.clear()
        elif event == "end" and tag == "xf" and in_cell_xfs:
            try:
                format_id = int(element.attrib.get("numFmtId", "0"))
            except ValueError as error:
                raise ValueError("invalid XLSX style") from error
            styles.append(
                format_id in _BUILTIN_DATE_FORMATS
                or (format_id in custom_formats and _looks_like_date_format(custom_formats[format_id]))
            )
            element.clear()
    return tuple(styles) or (False,)


def _column_index(reference: str) -> int:
    match = _CELL_REFERENCE_RE.fullmatch(reference)
    if match is None:
        raise ValueError("invalid XLSX cell reference")
    column = 0
    for character in match.group(1):
        column = column * 26 + ord(character) - ord("A") + 1
    return column - 1


def _excel_value(value: Decimal, date_1904: bool, want_date: bool) -> date | datetime:
    if not value.is_finite():
        raise ValueError("non-finite XLSX date")
    whole_days = int(value.to_integral_value(rounding="ROUND_FLOOR"))
    fraction = value - whole_days
    if want_date and fraction != 0:
        raise ValueError("XLSX date has a time component")
    epoch = datetime(1904, 1, 1) if date_1904 else datetime(1899, 12, 30)
    microseconds = int((fraction * Decimal(86_400_000_000)).to_integral_value())
    converted = epoch + timedelta(days=whole_days, microseconds=microseconds)
    return converted.date() if want_date else converted


def _cell_value(
    cell: ElementTree.Element,
    shared_strings: tuple[str, ...],
    date_styles: tuple[bool, ...],
    logical_type: str,
    date_1904: bool,
) -> tuple[object | None, bool]:
    if any(_local_name(child.tag) == "f" for child in cell):
        raise RuntimeError("formula cell is forbidden")
    cell_type = cell.attrib.get("t", "n")
    value_element = next((child for child in cell if _local_name(child.tag) == "v"), None)
    inline_element = next((child for child in cell if _local_name(child.tag) == "is"), None)
    text = value_element.text if value_element is not None else None
    if inline_element is not None:
        text = "".join(
            child.text or "" for child in inline_element.iter() if _local_name(child.tag) == "t"
        )
    if text is None:
        return None, False
    if cell_type == "s":
        try:
            index = int(text)
            if not 0 <= index < len(shared_strings):
                raise ValueError("shared-string index is out of range")
            return shared_strings[index], False
        except (ValueError, IndexError) as error:
            raise ValueError("invalid XLSX shared-string reference") from error
    if cell_type in {"inlineStr", "str"}:
        return text, False
    if cell_type == "b":
        if text not in {"0", "1"}:
            raise ValueError("invalid XLSX boolean")
        return text == "1", False
    if cell_type == "d":
        return text, False
    if cell_type == "e":
        raise ValueError("XLSX error cell is unsupported")
    if cell_type != "n":
        raise ValueError("unsupported XLSX cell type")
    try:
        number = Decimal(text)
        style_index = int(cell.attrib.get("s", "0"))
        if not 0 <= style_index < len(date_styles):
            raise ValueError("style index is out of range")
        date_formatted = date_styles[style_index]
    except (InvalidOperation, ValueError, IndexError) as error:
        raise ValueError("invalid XLSX numeric cell or style") from error
    if date_formatted:
        return _excel_value(number, date_1904, logical_type == "date"), True
    return number, False


def _inspect_worksheet(
    archive: zipfile.ZipFile,
    member: str,
    budget: _ArchiveBudget,
    contract: SchemaContract,
    context: VerificationContext,
    shared_strings: tuple[str, ...],
    date_styles: tuple[bool, ...],
    date_1904: bool,
    *,
    validate_schema: bool,
) -> None:
    header_row = contract.options.get("header_row")
    if not isinstance(header_row, int):
        raise ValueError("invalid XLSX header row")
    row_number = 0
    header_found = False
    cells: dict[int, ElementTree.Element] = {}
    for event, element in _xml_events(archive, member, budget):
        tag = _local_name(element.tag)
        if event == "end" and tag == "c":
            if any(_local_name(child.tag) == "f" for child in element):
                raise RuntimeError("formula cell is forbidden")
            if validate_schema:
                index = _column_index(element.attrib.get("r", ""))
                if index in cells:
                    raise ValueError("duplicate XLSX cell")
                cells[index] = element
            else:
                element.clear()
        elif event == "end" and tag == "row":
            try:
                row_number = int(element.attrib.get("r", str(row_number + 1)))
            except ValueError as error:
                raise ValueError("invalid XLSX row number") from error
            if validate_schema and cells:
                if any(index >= len(contract.fields) for index in cells):
                    raise LockMismatch(context, "XLSX width", len(contract.fields), max(cells) + 1)
                values: list[object | None] = []
                date_fields: set[int] = set()
                for index, field in enumerate(contract.fields):
                    cell = cells.get(index)
                    if cell is None:
                        values.append(None)
                    else:
                        value, date_formatted = _cell_value(
                            cell,
                            shared_strings,
                            date_styles,
                            field.logical_type,
                            date_1904,
                        )
                        values.append(value)
                        if date_formatted:
                            date_fields.add(index)
                if row_number == header_row:
                    expected = tuple(field.name for field in contract.fields)
                    actual = tuple(values)
                    if actual != expected:
                        raise LockMismatch(context, "XLSX header", expected, actual)
                    header_found = True
                elif row_number > header_row:
                    _validate_record_values(
                        values,
                        contract,
                        context,
                        row_number,
                        "xlsx",
                        frozenset(date_fields),
                    )
            cells = {}
            element.clear()
    if validate_schema and not header_found:
        raise LockMismatch(
            context,
            "XLSX header",
            tuple(field.name for field in contract.fields),
            "missing",
        )


def inspect_xlsx(
    path: Path,
    contract: SchemaContract,
    context: VerificationContext,
    locked_size: int,
) -> ObservedSchema:
    archive, budget = _safe_xlsx_archive(path, locked_size, context)
    try:
        required_members = {"xl/workbook.xml", "xl/_rels/workbook.xml.rels"}
        if not required_members.issubset(archive.namelist()):
            raise ValueError("XLSX workbook metadata is missing")
        sheets, date_1904 = _parse_workbook(archive, budget)
        relationships = _parse_relationships(archive, budget)
        visible_names = tuple(name for name, _, visible in sheets if visible)
        expected_sheets = contract.options.get("sheets")
        if not isinstance(expected_sheets, (tuple, list)) or visible_names != tuple(expected_sheets):
            raise LockMismatch(context, "XLSX sheets", tuple(expected_sheets or ()), visible_names)
        shared_strings = _parse_shared_strings(archive, budget)
        date_styles = _parse_date_styles(archive, budget)
        for name, relationship_id, visible in sheets:
            member = relationships.get(relationship_id)
            if member is None or member not in archive.namelist():
                raise ValueError("XLSX worksheet relationship is missing")
            _inspect_worksheet(
                archive,
                member,
                budget,
                contract,
                context,
                shared_strings,
                date_styles,
                date_1904,
                validate_schema=visible and name in visible_names,
            )
    except LockMismatch:
        raise
    except RuntimeError as error:
        raise LockMismatch(context, "XLSX formula", "no formula cells", "formula present") from error
    except (ElementTree.ParseError, ValueError, KeyError, OSError, zipfile.BadZipFile) as error:
        reason = str(error)
        if "DTD" in reason:
            field = "XLSX DTD"
        elif "entity" in reason:
            field = "XLSX entity"
        else:
            field = "XLSX structure"
        raise LockMismatch(context, field, "valid bounded OOXML", type(error).__name__) from error
    finally:
        archive.close()
    return ObservedSchema(_expected_fields(contract))


def inspect_text(
    path: Path,
    contract: SchemaContract,
    context: VerificationContext,
    locked_size: int,
) -> ObservedSchema:
    del locked_size
    if contract.fields:
        raise LockMismatch(context, "text fields", (), tuple(field.name for field in contract.fields))
    decoder = codecs.getincrementaldecoder("utf-8")(errors="strict")
    try:
        with path.open("rb") as stream:
            while chunk := stream.read(_READ_SIZE):
                decoder.decode(chunk)
            decoder.decode(b"", final=True)
    except UnicodeError as error:
        raise LockMismatch(context, "UTF-8", "valid UTF-8", "decode error") from error
    return ObservedSchema(())


def compare_observed_schema(
    observed: ObservedSchema,
    contract: SchemaContract,
    context: VerificationContext,
) -> None:
    expected = _expected_fields(contract)
    if observed.fields != expected:
        raise LockMismatch(context, "physical schema", expected, observed.fields)


def verify_physical_schema(
    path: Path | VerifiedFile,
    contract: SchemaContract,
    context: VerificationContext,
) -> ObservedSchema:
    """Verify a locked object's schema without network access or type inference."""
    actual_fingerprint = _contract_fingerprint(contract)
    if actual_fingerprint != contract.fingerprint:
        raise LockMismatch(context, "schema fingerprint", contract.fingerprint, actual_fingerprint)
    resolved_path, locked_size = _input_path(path)
    inspectors = {
        "parquet": inspect_parquet,
        "csv": inspect_csv,
        "jsonl-gzip": inspect_jsonl_gzip,
        "xlsx": inspect_xlsx,
        "text": inspect_text,
    }
    inspector = inspectors.get(contract.format)
    if inspector is None:
        raise LockMismatch(context, "schema format", tuple(inspectors), contract.format)
    observed = inspector(resolved_path, contract, context, locked_size)
    compare_observed_schema(observed, contract, context)
    return observed
