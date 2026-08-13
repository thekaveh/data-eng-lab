# Parquet Schema Normalization Blocker Report

## Outcome

The supported NYC Taxi tiny publication failed before S3 publication because the physical-schema
normalizer rejected two valid metadata shapes emitted by Arrow-written Parquet and reported by
DuckDB 1.5.4. The fix is limited to `datasets/schema_inspection.py`; registry schemas, object locks,
dependency locks, S3 publication semantics, and fail-closed comparisons are unchanged.

## Reproduction and root cause

The exact locked January object is 47,673,370 bytes with SHA-256
`32df6f67578fa86c484a6b5ef23a5281992ff085521082340b0f9e5889e9a572`. The full call chain was:

```text
fetch_http
  -> _fetch_artifact
  -> _verify_bound_output
  -> verify_physical_schema
  -> inspect_parquet
  -> normalize_parquet_schema
  -> _normalize_physical_parquet_type
```

The first primary exception was `ValueError: ambiguous Parquet integer is missing its width
annotation` for `VendorID`, wrapped as the expected `LockMismatch: nyc_taxi/tiny output
parquet_schema mismatch`. DuckDB ordinary-path and `/dev/fd` metadata rows were identical except
for `file_name` on macOS arm64 and Linux arm64.

The locked file was created by `parquet-cpp-arrow version 8.0.0`. PyArrow independently reports:

- bare physical `INT64` for `VendorID`, `PULocationID`, `DOLocationID`, and `payment_type`;
- DuckDB's matching logical types are signed `BIGINT`;
- pickup/dropoff are physical `INT64` with authoritative logical
  `Timestamp(isAdjustedToUTC=false, microseconds)`; and
- DuckDB exposes both `converted_type=TIMESTAMP_MICROS` and the non-UTC logical annotation even
  though PyArrow records that the timestamp was not created from a legacy converted annotation.

The #81 hardening change rejected every bare integer even when the physical width and signed
DuckDB type agreed, and interpreted the duplicated timestamp field as a conflicting UTC claim.
Synthetic production-schema tests supplied idealized integer annotations and therefore missed the
locked Arrow shape.

## TDD evidence

RED was four intended failures with eleven guard cases already passing:

- bare `INT32` + signed DuckDB `INTEGER`;
- bare `INT64` + signed DuckDB `BIGINT`;
- a real PyArrow Parquet file read through `/dev/fd`; and
- `TIMESTAMP_MICROS` plus authoritative `isAdjustedToUTC=0` microsecond logical metadata.

GREEN is fifteen focused cases. The physical Parquet regression is stored as bounded fixture bytes
and therefore runs in the dev-only CI environment without relying on the optional live dependency
group. Bare signed integers are accepted only for exact
`INT32/INTEGER` or `INT64/BIGINT` agreement. Width mismatch, unsigned DuckDB types, missing physical
or DuckDB type, conflicting annotations, malformed timestamps, and converted/logical unit mismatch
continue to fail. A matching logical timestamp controls timezone semantics, while any duplicated
legacy unit must match exactly.

The exact locked January file now passes the reviewed registry schema through both an ordinary
pathname and a live `/dev/fd` capability, producing equal 19-field observations with `int64`
VendorID and non-UTC `timestamp` pickup/dropoff.

## Gates before independent review

```text
focused new cases: 15 passed
full schema-inspection suite: 130 passed
isolated no-live schema-inspection suite: 130 passed
full datasets suite: 2116 passed
Ruff (changed files): passed
isolated no-live Ruff (changed files): passed
git diff --check: passed
```

The branch-wide offline suite reaches `2894 passed, 47 deselected`; its thirteen remaining
failures, plus the single `make verify` inventory finding, are the already-planned #91
execution-matrix/Atlas projections for the new quality DAG. They do not import or exercise this
dataset normalizer and will be reconciled after the production live gate, before #91 review.
The same failures were present at this #91 implementation checkpoint independently of the
normalizer delta.

`ruff format --check` is not a repository gate and also reports both unmodified HEAD versions of
the touched files as needing a whole-file rewrite. This fix preserves their established formatting
to avoid mixing unrelated mechanical churn; `ruff check` and `git diff --check` are clean.

The exact bounded publication command is intentionally not retried until independent spec and
quality review find zero blocking issues.
