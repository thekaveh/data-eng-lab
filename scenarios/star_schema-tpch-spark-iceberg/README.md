# star_schema-tpch-spark-iceberg

Build fact and dimension tables from TPC-H dataset: `s3a://landing/tpch/{orders,customer,lineitem}` →
`lakehouse.gold.{dim_customer,fct_orders}`. Demonstrates dimensional modeling with star schema.
Scala (Zeppelin) and PySpark (Jupyter) notebooks implement the same logic.

## 1. Scenario summary
Star schema transform: read orders, customer, lineitem tables from S3 → join orders with lineitem
on order key → aggregate by order/customer/date → create dimension and fact tables. Writes to
Iceberg `lakehouse.gold.*`.

## 2. Why this exists
Demonstrates dimensional modeling and star schema design, a foundational data warehouse pattern.

## 3. What's in the notebooks
`zeppelin/notebook.zpln` (Scala) and `jupyter/notebook.ipynb` (PySpark), sections Overview->Verify;
a `dag.py`.

## 4. How to run
Open either notebook on the Atlas stack (after running `make datasets` for TPC-H), or trigger the
`star_schema_tpch` Airflow DAG.

## 5. Data & dependencies
Requires TPC-H dataset in S3 (`s3a://landing/tpch/{orders,customer,lineitem}`); Spark + Iceberg
`lakehouse` catalog (Atlas A1-A4).

## 6. Known issues & caveats
Notebook execution + Scala/PySpark parity are live-gated on Atlas A1-A4. This scenario writes to
`lakehouse.gold.*`, which requires the `gold` namespace to exist in the Iceberg REST catalog. Run
`scripts/register_iceberg.py` (creates `bronze`, `silver`, and `gold`) before executing this
scenario standalone. Use `make datasets` to populate the TPC-H landing zone.
