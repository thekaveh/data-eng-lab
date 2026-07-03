# join_optimization-tpch-spark-iceberg

Optimize TPCH query performance using broadcast joins and Adaptive Query Execution (AQE).
Joins the `orders` and `customer` dimensions with a broadcast strategy, then aggregates revenue
by market segment. Demonstrates how Spark's AQE automatically selects efficient join strategies.

## 1. Scenario summary
Broadcast join on TPCH orders ↔ customer (on `o_custkey` = `c_custkey`), group by market segment,
aggregate total revenue and order count. Reads from S3 parquet, writes aggregated mart to Iceberg `lakehouse.gold.tpch_segment_revenue`.

## 2. Why this exists
Teaches broadcast vs sort-merge join strategies and shows AQE in action. The `.explain()` output
demonstrates Spark's physical plan optimization (BroadcastHashJoin), and the SCALE knob can tune
broadcast threshold behavior.

## 3. What's in the notebooks
`zeppelin/notebook.zpln` (Scala) and `jupyter/notebook.ipynb` (PySpark), sections Overview->Verify;
a `dag.py` placeholder.

## 4. How to run
Open either notebook on the Atlas stack (after ensuring TPCH datasets are available),
or trigger the `join_optimization_tpch` Airflow DAG.

## 5. Data & dependencies
Requires `s3a://landing/tpch/orders` and `s3a://landing/tpch/customer` parquet datasets;
Spark + Iceberg `lakehouse` catalog (Atlas A1-A4).

## 6. Known issues & caveats
Notebook execution + Scala/PySpark parity are live-gated on Atlas A1-A4. This scenario writes to
`lakehouse.gold.tpch_segment_revenue`, which requires the `gold` namespace to exist in the Iceberg
REST catalog. Run `scripts/register_iceberg.py` (creates `bronze`, `silver`, and `gold`) and
`make datasets` before executing this scenario standalone.
