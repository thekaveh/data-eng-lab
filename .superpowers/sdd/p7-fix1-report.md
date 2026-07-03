# Phase 7 Fix Report: ruff per-file-ignore + time_travel Verify

## FIX 1 — pyproject.toml ruff config

**Change made to `/Users/kaveh/repos/data-eng-lab/pyproject.toml`:**

Before:
```toml
[tool.ruff]
extend-exclude = ["./infra/**", "scenarios/**/jupyter/**", "scenarios/**/zeppelin/**"]

[tool.ruff.lint]
select = ["E", "F", "W", "I"]
```

After:
```toml
[tool.ruff]
extend-exclude = ["./infra/**"]

[tool.ruff.lint]
select = ["E", "F", "W", "I"]

[tool.ruff.lint.per-file-ignores]
"**/*.ipynb" = ["E501"]
```

The two `scenarios/**/jupyter/**` and `scenarios/**/zeppelin/**` entries were removed. The `.zpln` files are JSON (not linted). A `[tool.ruff.lint.per-file-ignores]` table was added so notebooks keep F/W/I checks but skip E501 (long SQL lines).

## Notebook F/W/I issues surfaced by re-enabled linting

None. `uv run ruff check .` → **All checks passed!** after the config change, with no modifications to any notebook code cells required. The batch-B notebooks (`star_schema-tpch-spark-iceberg`, `json_flatten-gh_archive-spark-iceberg`) had clean imports and no undefined names.

## FIX 2 — time_travel Verify section regeneration

A throwaway script was written at `/private/tmp/.../scratchpad/regen_time_travel.py` using `tests/scenarios/build_notebooks.write_scenario`. The "6. Verify" section was extended with two comment lines (retaining the existing `.history` query as the first statement):

**PySpark (`.ipynb`)**:
```python
spark.sql("SELECT committed_at, snapshot_id FROM lakehouse.silver.nyc_taxi_tt.history ORDER BY committed_at").show(truncate=False)
# time-travel: spark.sql("SELECT count(*) FROM lakehouse.silver.nyc_taxi_tt VERSION AS OF <snapshot_id>").show()
# rollback:    spark.sql("CALL lakehouse.system.rollback_to_snapshot('lakehouse.silver.nyc_taxi_tt', <snapshot_id>)").show()
```

**Scala (`.zpln`, `%spark`)**:
```scala
spark.sql("SELECT committed_at, snapshot_id FROM lakehouse.silver.nyc_taxi_tt.history ORDER BY committed_at").show(false)
// time-travel: spark.sql("SELECT count(*) FROM lakehouse.silver.nyc_taxi_tt VERSION AS OF <snapshot_id>").show()
// rollback:    spark.sql("CALL lakehouse.system.rollback_to_snapshot('lakehouse.silver.nyc_taxi_tt', <snapshot_id>)").show()
```

All other cells (Setup, Read, Transform, Write), the README.md, and dag.py were preserved unchanged.

## Verification outputs

### ruff
```
uv run ruff check .
All checks passed!
```

### verify_repo.py
```
uv run python scripts/verify_repo.py --root .
0 finding(s), 0 error(s)
```

### pytest (not infra)
```
uv run pytest -m "not infra" -q
87 passed, 6 deselected in 1.62s
```

### test_dag_catalog_conf.py
```
uv run pytest tests/test_dag_catalog_conf.py -q
1 passed in 0.01s
```

### test_lint_config.py
```
uv run pytest tests/test_lint_config.py -q
1 passed in 0.00s
```
