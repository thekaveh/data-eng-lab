# 7.4. tpch-star-schema

The production Scala/Spark application deterministically replaces `lakehouse.gold.dim_customer` and `lakehouse.gold.fct_orders` from one resolver-verified immutable TPC-H publication. Jenkins publishes `s3a://jars/tpch-star-schema/0.1.0/app.jar`; Airflow runs `tpch_star_schema` daily or with an explicit manual `dataset_scale`.

## 1. Input Contract

The entrypoint accepts all eight canonical publication objects in registry order, followed by scale, plan, publication, and manifest arguments. It rejects flat, malformed, duplicate, missing, extra, reordered, or cross-generation inputs and converts only the validated `s3://` scheme to `s3a://`.

The transform reads customer, orders, and lineitem. Carrying all eight objects through the executable boundary binds the run to one complete verified publication.

## 2. Output Contract

`dim_customer` contains `c_custkey long`, `c_name string`, `c_nationkey integer`, and `c_mktsegment string`. `fct_orders` contains `o_orderkey long`, `o_custkey long`, `o_orderdate date`, `revenue decimal(25,2)`, and `line_count long`. Revenue is the sum of `l_extendedprice` per order and line count is `count(*)`.

Both tables carry equal Iceberg `data_eng_lab.dataset*` properties for dataset, scale, plan, publication, and manifest. The application reads those properties back before reporting success. Downstream #83 must compare the two Iceberg `$properties` metadata tables and reject mixed-generation inputs.

## 3. Failure and Recovery

Both frames are validated and materialized before the first write. Iceberg does not provide a cross-table atomic commit, so the dimension is replaced first and the fact second. A failure between writes fails Airflow; rerunning the same immutable generation deterministically converges both results and provenance.

## 4. Build and Run

```bash
mvn -q -B -f spark-apps/tpch-star-schema/pom.xml test
mvn -q -B -f spark-apps/tpch-star-schema/pom.xml package
```

Jenkins publishes the reviewed artifact. Airflow submits through `spark_default` in cluster mode and Atlas's REST-confirming adapter; success requires Spark `FINISHED` and `success=true`.

## 5. Live Evidence

On 2026-08-12, two manual tiny-scale runs succeeded. The rerun preserved `1500` dimension rows with checksum `8b024198f91d197b`, `15000` fact rows with checksum `8ce8521bbc607f2e`, exact schemas, meaningful segment revenue, and identical provenance on both tables.

## 6. See Also

- [Scenario](../scenarios/star_schema-tpch-spark-iceberg.md)
- [Notebook walkthrough](../notebooks/star_schema-tpch-spark-iceberg.md)
- [Execution-mode matrix](../scenarios/execution-modes.md)
- [Datasets](../datasets.md)
