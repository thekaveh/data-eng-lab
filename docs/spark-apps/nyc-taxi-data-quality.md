# 7.7. nyc-taxi-data-quality

This Scala Spark application binds one NYC Taxi Bronze Iceberg snapshot, creates a null-safe Silver quality split, and persists eight governed Gold facts. Jenkins publishes `s3a://jars/nyc-taxi-data-quality/0.1.0/app.jar`; Airflow DAG `nyc_taxi_data_quality` submits it through Atlas's REST-confirming operator.

## 1. Trust Boundary

The input binding is the exact `lakehouse.bronze.nyc_taxi_trips` snapshot ID, commit timestamp, schema hash, count, and content checks. It is snapshot-bound evidence, not five-key generation provenance. Upstream five-key Bronze properties remain explicitly deferred producer hardening.

## 2. Outputs

- `lakehouse.silver.nyc_taxi_clean`
- `lakehouse.silver.nyc_taxi_quarantine`
- `lakehouse.gold.nyc_taxi_quality_facts`

Clean and quarantine preserve the exact nullable 20-column Bronze schema, duplicate multiplicity, and all rows with null rule operands. The deterministic Gold MERGE key is `(quality_run_id, rule_id)`.

## 3. Quality Contract

The application evaluates source availability, exact schema, snapshot freshness, invalid ratio, Silver partition conservation, nonempty clean output, quarantine ratio, and output readback. Invalid/quarantine ratios pass through 1%, warn through 5%, and fail above 5%. Facts record exact rule versions, owners, thresholds, severity, canonical decimal metrics, and diagnostic codes.

## 4. Orchestration and Recovery

`wait_for_nyc_taxi_etl` requires the successful same-logical-date `nyc_taxi_etl.submit_nyc_taxi_etl` task before `submit_nyc_taxi_data_quality` starts. The `@daily` DAG sets `max_active_runs=1`. Because the two Silver replacements and Gold MERGE are not cross-table atomic, direct concurrent JAR execution is unsupported; a deterministic same-date rerun is the supported recovery.

## 5. Dashboard

Three bounded, fixed SELECT-only Trino queries under `spark-apps/nyc-taxi-data-quality/queries/` expose latest accepted facts, 90-run trend history, and operator-attention rows. Operators retrieve these query results from Trino; the durable source is the Gold Iceberg table.

## 6. Build and Publish

```bash
mvn -q -B -f spark-apps/nyc-taxi-data-quality/pom.xml test
mvn -q -B -f spark-apps/nyc-taxi-data-quality/pom.xml package
```

The application uses `spark_default`, standalone cluster mode, Iceberg/S3A configuration, and Atlas `RestConfirmingSparkHook`. Accepted Airflow task success requires the Spark driver to finish with `success=true`.

## 7. Educational Boundary

The paired scenario notebooks directly replace the same Silver tables without production provenance, governed facts, or Airflow serialization. They are educational only; production writes must use the DAG/application.

## 8. See Also

- [NYC Taxi data-quality scenario](../scenarios/data_quality-nyc_taxi-spark-iceberg.md)
- [Execution-mode matrix](../scenarios/execution-modes.md)
- [Go-live runbook](../go-live.md)
