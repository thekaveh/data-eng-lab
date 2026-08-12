# TPC-H Star-Schema Production Application

This Scala/Spark application deterministically replaces `lakehouse.gold.dim_customer` and
`lakehouse.gold.fct_orders` from one resolver-verified immutable TPC-H publication. Jenkins
publishes the reviewed JAR to `s3a://jars/tpch-star-schema/0.1.0/app.jar`; the `tpch_star_schema`
Airflow DAG runs it `@daily` or on demand with `{"dataset_scale":"tiny|small|medium"}`.

## Input contract

The entrypoint accepts exactly these canonical `s3://landing/tpch/_generations/<plan>/<publication>/...`
URIs in order: `customer.parquet`, `lineitem.parquet`, `nation.parquet`, `orders.parquet`,
`part.parquet`, `partsupp.parquet`, `region.parquet`, and `supplier.parquet`. They are followed by
`--dataset-scale`, `--plan-id`, `--publication-id`, and `--manifest-sha256`. The application rejects
flat, malformed, duplicate, missing, extra, reordered, or cross-generation arguments and validates
that explicit metadata matches the URI generation before converting only `s3://` to `s3a://`.

The transform reads customer, orders, and lineitem. All eight URIs cross the boundary so the run is
bound to one complete verified publication, not an ad hoc subset.

## Output contract

`dim_customer` has `c_custkey long`, `c_name string`, `c_nationkey integer`, and
`c_mktsegment string`. `fct_orders` groups the orders/lineitem inner join by `o_orderkey long`,
`o_custkey long`, and `o_orderdate date`; `revenue decimal(25,2)` is `sum(l_extendedprice)` and
`line_count long` is `count(*)`. Required schemas, non-null unique source keys, and customer/order
foreign-key integrity are checked before writes.

Each table carries equal Iceberg properties:

- `data_eng_lab.dataset=tpch`
- `data_eng_lab.dataset.scale`
- `data_eng_lab.dataset.plan_id`
- `data_eng_lab.dataset.publication_id`
- `data_eng_lab.dataset.manifest_sha256`

The application reads these properties back after both writes and fails if either table differs.
Downstream #83 must compare `lakehouse.gold."dim_customer$properties"` with
`lakehouse.gold."fct_orders$properties"` and reject mismatched provenance before BI queries.

## Failure and recovery

Both frames are validated and materialized before writing. Iceberg cannot atomically commit two
independent tables, so the dimension is replaced first and the fact second. This avoids publishing a
new fact before its matching dimension, but a failure between writes can leave mixed provenance.
Airflow reports failure. Rerun the same immutable generation; deterministic replacements converge
both logical results and provenance. New snapshot IDs on a rerun are expected.

## Build and publish

```bash
mvn -q -B -f spark-apps/tpch-star-schema/pom.xml test
mvn -q -B -f spark-apps/tpch-star-schema/pom.xml package
```

Jenkins tests, packages, and copies the exact JAR using injected MinIO credentials. Airflow uses
`spark_default`, cluster mode, and Atlas `RestConfirmingSparkHook`; success requires Spark
`FINISHED` with `success=true`. The Spark runtime, S3A, and Iceberg dependencies are supplied by Atlas.
