from __future__ import annotations

import gzip
import json
import zipfile
from dataclasses import replace
from pathlib import Path

import duckdb
import pytest

import datasets.schema_inspection as inspection
from datasets.locking import schema_fingerprint
from datasets.registry import SchemaContract, SchemaField, load_registry
from datasets.schema_inspection import (
    ObservedField,
    ObservedSchema,
    normalize_parquet_schema,
    verify_physical_schema,
)
from datasets.verification import LockMismatch, VerificationContext

CONTEXT = VerificationContext("fixture", "tiny", "schema", "artifact", "object")


def contract(
    format_name: str,
    fields: tuple[SchemaField, ...],
    *,
    mode: str = "exact",
    options: dict[str, object] | None = None,
) -> SchemaContract:
    format_options = options or {}
    raw = {
        "format": format_name,
        "mode": mode,
        "fields": [
            {
                "name": field.name,
                "logical_type": field.logical_type,
                "nullable": field.nullable,
            }
            for field in fields
        ],
        "options": format_options,
    }
    return SchemaContract(
        "fixture",
        format_name,
        mode,
        fields,
        format_options,
        schema_fingerprint(raw),
    )


def parquet_row(
    name: str,
    duckdb_type: str,
    repetition: str = "OPTIONAL",
    **overrides: object,
) -> dict[str, object]:
    row: dict[str, object] = {
        "name": name,
        "type": overrides.pop("type", duckdb_type),
        "repetition_type": repetition,
        "num_children": None,
        "converted_type": None,
        "scale": None,
        "precision": None,
        "logical_type": None,
        "duckdb_type": duckdb_type,
    }
    row.update(overrides)
    return row


@pytest.mark.parametrize(
    ("logical", "duckdb_type"),
    [
        ("boolean", "BOOLEAN"),
        ("int8", "TINYINT"),
        ("int16", "SMALLINT"),
        ("int32", "INTEGER"),
        ("int64", "BIGINT"),
        ("uint8", "UTINYINT"),
        ("uint16", "USMALLINT"),
        ("uint32", "UINTEGER"),
        ("uint64", "UBIGINT"),
        ("float32", "FLOAT"),
        ("float32", "REAL"),
        ("float64", "DOUBLE"),
        ("date", "DATE"),
        ("timestamp", "TIMESTAMP_S"),
        ("timestamp", "TIMESTAMP_MS"),
        ("timestamp", "TIMESTAMP"),
        ("timestamp", "TIMESTAMP_NS"),
        ("timestamp-tz", "TIMESTAMP WITH TIME ZONE"),
        ("string", "VARCHAR"),
        ("binary", "BLOB"),
        ("decimal(12,2)", "DECIMAL(12,2)"),
        ("decimal(15,2)", "DECIMAL(15,2)"),
    ],
)
def test_parquet_normalization_is_frozen(logical: str, duckdb_type: str):
    assert normalize_parquet_schema([parquet_row("value", duckdb_type)]) == (
        ObservedField("value", logical, True),
    )


def test_parquet_normalization_preserves_order_and_required_nullability():
    rows = [
        parquet_row("first", "BIGINT", "REQUIRED"),
        parquet_row("second", "VARCHAR"),
    ]

    assert normalize_parquet_schema(rows) == (
        ObservedField("first", "int64", False),
        ObservedField("second", "string", True),
    )


@pytest.mark.parametrize(
    "rows",
    [
        [parquet_row("items", "INTEGER", "REPEATED")],
        [parquet_row("record", "STRUCT", num_children=1)],
        [parquet_row("value", "HUGEINT")],
        [parquet_row("value", "INTEGER"), parquet_row("value", "INTEGER")],
        [parquet_row("value", "", type="INT32", converted_type=None)],
    ],
)
def test_parquet_normalization_rejects_unsupported_or_ambiguous_fields(rows):
    with pytest.raises(ValueError):
        normalize_parquet_schema(rows)


def test_parquet_normalization_rejects_nested_group_before_its_leaf():
    rows = [
        {"name": "schema", "type": None, "repetition_type": "REQUIRED", "num_children": 1},
        {"name": "record", "type": None, "repetition_type": "OPTIONAL", "num_children": 1},
        parquet_row("value", "INTEGER"),
    ]

    with pytest.raises(ValueError, match="nested"):
        normalize_parquet_schema(rows)


def test_parquet_normalization_does_not_trust_inferred_integer_without_width_annotation():
    row = parquet_row("value", "INTEGER", type="INT32", converted_type=None)

    with pytest.raises(ValueError, match="ambiguous"):
        normalize_parquet_schema([row])


def test_parquet_inspection_uses_duckdb_metadata_without_value_inference(tmp_path: Path):
    path = tmp_path / "ordered.parquet"
    connection = duckdb.connect(":memory:")
    connection.execute(
        "COPY (SELECT CAST(1 AS BIGINT) AS id, CAST('01' AS VARCHAR) AS code) "
        f"TO '{path}' (FORMAT PARQUET)"
    )
    expected = contract(
        "parquet",
        (
            SchemaField("id", "int64", True),
            SchemaField("code", "string", True),
        ),
    )

    assert verify_physical_schema(path, expected, CONTEXT) == ObservedSchema(
        (
            ObservedField("id", "int64", True),
            ObservedField("code", "string", True),
        )
    )


def test_parquet_inspection_requires_exact_duckdb_version(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    path = tmp_path / "data.parquet"
    path.write_bytes(b"not inspected")
    expected = contract("parquet", (SchemaField("id", "int64", True),))
    monkeypatch.setattr(inspection.duckdb, "__version__", "1.5.3")

    with pytest.raises(LockMismatch, match="duckdb_version"):
        verify_physical_schema(path, expected, CONTEXT)


CSV_CONTRACT = contract(
    "csv",
    (
        SchemaField("id", "int8", False),
        SchemaField("score", "float64", True),
        SchemaField("label", "string", False),
    ),
    options={"header": True, "delimiter": ",", "encoding": "utf-8"},
)


def test_csv_accepts_strict_quoted_rows_and_nulls(tmp_path: Path):
    path = tmp_path / "data.csv"
    path.write_text('id,score,label\n1,2.5,"hello, world"\n2,,text\n', encoding="utf-8")

    assert verify_physical_schema(path, CSV_CONTRACT, CONTEXT) == ObservedSchema(
        tuple(ObservedField(field.name, field.logical_type, field.nullable) for field in CSV_CONTRACT.fields)
    )


@pytest.mark.parametrize(
    ("payload", "reason"),
    [
        (b"\xef\xbb\xbfid,score,label\n1,2,x\n", "BOM"),
        (b'id,score,label\n1,2,"unterminated\n', "CSV"),
        (b'id,score,label\n1,2,un"quoted\n', "CSV"),
        (b"score,id,label\n2,1,x\n", "header"),
        (b"id,score,label\n1,2\n", "width"),
        (b"id,score,label\n,2,x\n", "null"),
        (b"id,score,label\n128,2,x\n", "int8"),
        (b"id,score,label\n1,NaN,x\n", "float64"),
        (b"id,score,label\n1,2.0,x\n2,nope,y\n", "float64"),
        (b"id,score,label\n1,2,\xff\n", "UTF-8"),
    ],
)
def test_csv_rejects_invalid_encoding_shape_nullability_and_types(
    tmp_path: Path, payload: bytes, reason: str
):
    path = tmp_path / "data.csv"
    path.write_bytes(payload)

    with pytest.raises(LockMismatch, match=reason):
        verify_physical_schema(path, CSV_CONTRACT, CONTEXT)


def test_csv_requires_a_time_component_for_timestamp(tmp_path: Path):
    path = tmp_path / "timestamp.csv"
    path.write_text("created_at\n2026-08-11\n", encoding="utf-8")
    timestamp_contract = contract(
        "csv",
        (SchemaField("created_at", "timestamp", False),),
        options={"header": True, "delimiter": ",", "encoding": "utf-8"},
    )

    with pytest.raises(LockMismatch, match="timestamp"):
        verify_physical_schema(path, timestamp_contract, CONTEXT)


JSON_CONTRACT = contract(
    "jsonl-gzip",
    (
        SchemaField("id", "string", False),
        SchemaField("actor.login", "string", False),
        SchemaField("count", "int32", True),
    ),
    mode="minimum",
    options={"record_shape": "object", "compression": "gzip", "encoding": "utf-8"},
)


def write_json_gzip(path: Path, records: list[object]) -> None:
    with gzip.open(path, "wt", encoding="utf-8", newline="") as stream:
        for record in records:
            stream.write(json.dumps(record, ensure_ascii=False, separators=(",", ":")))
            stream.write("\n")


def test_jsonl_gzip_accepts_nested_paths_and_minimum_mode_extras(tmp_path: Path):
    path = tmp_path / "records.json.gz"
    write_json_gzip(
        path,
        [
            {"extra": True, "actor": {"login": "octo"}, "id": "1", "count": 2},
            {"id": "2", "actor": {"login": "hub"}, "count": None},
        ],
    )

    assert verify_physical_schema(path, JSON_CONTRACT, CONTEXT) == ObservedSchema(
        tuple(ObservedField(field.name, field.logical_type, field.nullable) for field in JSON_CONTRACT.fields)
    )


@pytest.mark.parametrize(
    ("records", "reason"),
    [
        ([{"id": "1", "actor": {}}], "actor.login"),
        ([{"id": None, "actor": {"login": "octo"}}], "null"),
        ([{"id": 1, "actor": {"login": "octo"}}], "string"),
        ([{"id": "1", "actor": {"login": "octo"}, "count": 1.5}], "int32"),
        ([[]], "object"),
    ],
)
def test_jsonl_gzip_rejects_missing_null_and_wrong_json_kinds(
    tmp_path: Path, records: list[object], reason: str
):
    path = tmp_path / "records.json.gz"
    write_json_gzip(path, records)

    with pytest.raises(LockMismatch, match=reason):
        verify_physical_schema(path, JSON_CONTRACT, CONTEXT)


def test_jsonl_gzip_exact_mode_rejects_undeclared_nested_fields(tmp_path: Path):
    path = tmp_path / "records.json.gz"
    write_json_gzip(path, [{"id": "1", "actor": {"login": "octo", "id": 7}}])
    exact = replace(JSON_CONTRACT, mode="exact")
    raw = {
        "format": exact.format,
        "mode": exact.mode,
        "fields": [
            {"name": field.name, "logical_type": field.logical_type, "nullable": field.nullable}
            for field in exact.fields
        ],
        "options": dict(exact.options),
    }
    exact = replace(exact, fingerprint=schema_fingerprint(raw))

    with pytest.raises(LockMismatch, match="fields"):
        verify_physical_schema(path, exact, CONTEXT)


def test_jsonl_gzip_rejects_corrupt_trailer(tmp_path: Path):
    path = tmp_path / "records.json.gz"
    write_json_gzip(path, [{"id": "1", "actor": {"login": "octo"}}])
    path.write_bytes(path.read_bytes()[:-4] + b"xxxx")

    with pytest.raises(LockMismatch, match="gzip"):
        verify_physical_schema(path, JSON_CONTRACT, CONTEXT)


def test_jsonl_gzip_enforces_record_string_depth_and_expansion_bounds(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    monkeypatch.setattr(inspection, "_MAX_RECORD_BYTES", 96)
    monkeypatch.setattr(inspection, "_MAX_STRING_BYTES", 8)
    monkeypatch.setattr(inspection, "_MAX_JSON_DEPTH", 3)
    monkeypatch.setattr(inspection, "_MIN_EXPANDED_BYTES", 128)
    monkeypatch.setattr(inspection, "_EXPANSION_FACTOR", 1)
    cases = [
        ({"id": "x" * 9, "actor": {"login": "ok"}}, "string"),
        ({"id": "1", "actor": {"login": "ok"}, "a": {"b": {"c": {}}}}, "depth"),
        (
            {
                "id": "1",
                "actor": {"login": "ok"},
                **{f"extra{index}": "12345678" for index in range(8)},
            },
            "record",
        ),
    ]
    for index, (record, reason) in enumerate(cases):
        path = tmp_path / f"bounded-{index}.json.gz"
        write_json_gzip(path, [record])
        with pytest.raises(LockMismatch, match=reason):
            verify_physical_schema(path, JSON_CONTRACT, CONTEXT)

    expansion = tmp_path / "expansion.json.gz"
    write_json_gzip(
        expansion,
        [
            {"id": str(index), "actor": {"login": "ok"}}
            for index in range(20)
        ],
    )
    with pytest.raises(LockMismatch, match="expanded"):
        verify_physical_schema(expansion, JSON_CONTRACT, CONTEXT)


def test_json_depth_counts_nested_containers_not_scalar_leaves(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    path = tmp_path / "depth.json.gz"
    write_json_gzip(path, [{"id": "1", "actor": {"login": "ok"}}])
    monkeypatch.setattr(inspection, "_MAX_JSON_DEPTH", 2)

    verify_physical_schema(path, JSON_CONTRACT, CONTEXT)


XLSX_CONTRACT = contract(
    "xlsx",
    (
        SchemaField("name", "string", False),
        SchemaField("count", "int64", False),
        SchemaField("when", "date", False),
    ),
    options={"sheets": ["Data"], "header_row": 1},
)


def xlsx_fixture(
    tmp_path: Path,
    *,
    data_cells: str,
    sheet_name: str = "Data",
    shared_strings: tuple[str, ...] = ("name", "count", "when", "widget"),
    styles_xml: str | None = None,
    worksheet_prefix: str = "",
) -> Path:
    path = tmp_path / "workbook.xlsx"
    strings = "".join(f"<si><t>{value}</t></si>" for value in shared_strings)
    shared_xml = (
        '<?xml version="1.0" encoding="UTF-8"?>'
        '<sst xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" '
        f'count="{len(shared_strings)}" uniqueCount="{len(shared_strings)}">{strings}</sst>'
    )
    workbook_xml = (
        '<?xml version="1.0" encoding="UTF-8"?>'
        '<workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" '
        'xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">'
        f'<sheets><sheet name="{sheet_name}" sheetId="1" r:id="rId1"/></sheets></workbook>'
    )
    relationships_xml = (
        '<?xml version="1.0" encoding="UTF-8"?>'
        '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">'
        '<Relationship Id="rId1" '
        'Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" '
        'Target="worksheets/sheet1.xml"/></Relationships>'
    )
    sheet_xml = (
        f'{worksheet_prefix}<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">'
        '<sheetData><row r="1">'
        '<c r="A1" t="s"><v>0</v></c><c r="B1" t="s"><v>1</v></c>'
        '<c r="C1" t="s"><v>2</v></c></row>'
        f'<row r="2">{data_cells}</row></sheetData></worksheet>'
    )
    default_styles = (
        '<?xml version="1.0" encoding="UTF-8"?>'
        '<styleSheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">'
        '<numFmts count="0"/><cellXfs count="2"><xf numFmtId="0"/><xf numFmtId="14"/></cellXfs>'
        '</styleSheet>'
    )
    with zipfile.ZipFile(path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr("xl/workbook.xml", workbook_xml)
        archive.writestr("xl/_rels/workbook.xml.rels", relationships_xml)
        archive.writestr("xl/sharedStrings.xml", shared_xml)
        archive.writestr("xl/styles.xml", styles_xml or default_styles)
        archive.writestr("xl/worksheets/sheet1.xml", sheet_xml)
    return path


def valid_xlsx(tmp_path: Path) -> Path:
    return xlsx_fixture(
        tmp_path,
        data_cells=(
            '<c r="A2" t="s"><v>3</v></c>'
            '<c r="B2" t="n"><v>2</v></c>'
            '<c r="C2" s="1" t="n"><v>43831</v></c>'
        ),
    )


def test_xlsx_streams_shared_strings_styles_dates_and_numeric_cells(tmp_path: Path):
    path = valid_xlsx(tmp_path)

    assert verify_physical_schema(path, XLSX_CONTRACT, CONTEXT) == ObservedSchema(
        tuple(ObservedField(field.name, field.logical_type, field.nullable) for field in XLSX_CONTRACT.fields)
    )


def test_xlsx_rejects_formula_cells_before_value_validation(tmp_path: Path):
    path = xlsx_fixture(
        tmp_path,
        data_cells=(
            '<c r="A2"><f>1+1</f><v>2</v></c>'
            '<c r="B2" t="n"><v>2</v></c><c r="C2" s="1"><v>43831</v></c>'
        ),
    )

    with pytest.raises(LockMismatch, match="formula"):
        verify_physical_schema(path, XLSX_CONTRACT, CONTEXT)


@pytest.mark.parametrize("declaration", ["<!DOCTYPE worksheet>", '<!ENTITY x "boom">'])
def test_xlsx_rejects_dtd_and_entity_declarations(tmp_path: Path, declaration: str):
    path = xlsx_fixture(
        tmp_path,
        worksheet_prefix=declaration,
        data_cells=(
            '<c r="A2" t="s"><v>3</v></c><c r="B2"><v>2</v></c>'
            '<c r="C2" s="1"><v>43831</v></c>'
        ),
    )

    with pytest.raises(LockMismatch, match="DTD|entity"):
        verify_physical_schema(path, XLSX_CONTRACT, CONTEXT)


def test_xlsx_requires_declared_visible_sheet_set_and_header_order(tmp_path: Path):
    wrong_sheet = xlsx_fixture(
        tmp_path,
        sheet_name="Other",
        data_cells=(
            '<c r="A2" t="s"><v>3</v></c><c r="B2"><v>2</v></c>'
            '<c r="C2" s="1"><v>43831</v></c>'
        ),
    )
    with pytest.raises(LockMismatch, match="sheets"):
        verify_physical_schema(wrong_sheet, XLSX_CONTRACT, CONTEXT)

    wrong_header = xlsx_fixture(
        tmp_path,
        shared_strings=("count", "name", "when", "widget"),
        data_cells=(
            '<c r="A2" t="s"><v>3</v></c><c r="B2"><v>2</v></c>'
            '<c r="C2" s="1"><v>43831</v></c>'
        ),
    )
    with pytest.raises(LockMismatch, match="header"):
        verify_physical_schema(wrong_header, XLSX_CONTRACT, CONTEXT)


def test_xlsx_rejects_blank_nonnullable_mixed_type_and_expansion(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    blank = xlsx_fixture(
        tmp_path,
        data_cells='<c r="A2" t="s"><v>3</v></c><c r="C2" s="1"><v>43831</v></c>',
    )
    with pytest.raises(LockMismatch, match="null"):
        verify_physical_schema(blank, XLSX_CONTRACT, CONTEXT)

    mixed = xlsx_fixture(
        tmp_path,
        data_cells=(
            '<c r="A2" t="s"><v>3</v></c><c r="B2" t="s"><v>3</v></c>'
            '<c r="C2" s="1"><v>43831</v></c>'
        ),
    )
    with pytest.raises(LockMismatch, match="int64"):
        verify_physical_schema(mixed, XLSX_CONTRACT, CONTEXT)

    path = valid_xlsx(tmp_path)
    monkeypatch.setattr(inspection, "_MIN_EXPANDED_BYTES", 1)
    monkeypatch.setattr(inspection, "_EXPANSION_FACTOR", 0)
    with pytest.raises(LockMismatch, match="expanded"):
        verify_physical_schema(path, XLSX_CONTRACT, CONTEXT)


TEXT_CONTRACT = contract("text", (), options={"encoding": "utf-8"})


def test_text_accepts_utf8_and_empty_observed_schema(tmp_path: Path):
    path = tmp_path / "README.txt"
    path.write_text("hello, café\n", encoding="utf-8")

    assert verify_physical_schema(path, TEXT_CONTRACT, CONTEXT) == ObservedSchema(())


def test_text_rejects_invalid_utf8(tmp_path: Path):
    path = tmp_path / "README.txt"
    path.write_bytes(b"hello\xff")

    with pytest.raises(LockMismatch, match="UTF-8"):
        verify_physical_schema(path, TEXT_CONTRACT, CONTEXT)


def test_verification_recomputes_contract_fingerprint_before_inspection(tmp_path: Path):
    path = tmp_path / "README.txt"
    path.write_text("hello", encoding="utf-8")
    corrupt = replace(TEXT_CONTRACT, fingerprint="0" * 64)

    with pytest.raises(LockMismatch, match="fingerprint"):
        verify_physical_schema(path, corrupt, CONTEXT)


def test_minimal_registry_covers_every_inspector_format():
    registry = load_registry(Path(__file__).parent / "fixtures" / "registry-v2-minimal.yaml")

    assert {
        schema.format
        for dataset in registry.values()
        for schema in dataset.schemas.values()
    } == {"parquet", "csv", "jsonl-gzip", "xlsx", "text"}
