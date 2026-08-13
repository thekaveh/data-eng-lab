# 8.3. Atlas Go-Live Results

Detailed results from the go-live validation of the `data-eng-lab` platform.

## 1. 2026-07-31 Atlas Acceptance

The scoped acceptance run used Atlas pin
`985918ce8c805081947d53b1c48bb80610237a5b`.

| Check | Observed result |
|---|---|
| Representative Airflow feature-artifact task | The first and only attempt succeeded. |
| Spark standalone REST status | `FINISHED`; `success=true` |
| `lakehouse.bronze.nyc_taxi_trips` row count | `8,991,502` |
| Iceberg `passenger_count` type | `double` |

This evidence closes only the acceptance gate for the reviewed Atlas pin. The
consumer-modernization changes had already completed Gitflow promotion through
PRs #66, #67, and #68.

## 2. Preflight Results

```text
Layer 1 — Service existence:
  ✔ MinIO              : http://localhost:9000
  ✔ Postgres/Supabase  : localhost:5432
  ✔ Spark Connect      : sc://localhost:15002
  ✔ Spark Master       : localhost:7077
  ✔ JupyterHub         : http://localhost:8888
  ✔ Zeppelin           : http://localhost:8890
  ✔ Trino              : http://localhost:8080
  ✔ Airflow            : http://localhost:8090
  ✔ Jenkins            : http://localhost:8081
  ✔ Redpanda           : localhost:9092

Layer 2 — Round-trip probes:
  ✔ Spark ↔ MinIO ↔ Iceberg  (write + read Iceberg table)
  ✔ Jupyter ↔ PyIceberg      (direct table metadata read)
  ✔ Airflow ↔ MinIO/Spark    (mc CLI + spark-submit)
  ✔ Zeppelin ↔ Spark         (Scala notebook execution)
```

## 3. Bronze Smoke Test

```text
Writing to lakehouse.bronze.smoke_test_table (spark connect) ...
Read back rows: 100
Smoke test: PASS
```

## 4. Scenario Execution

The historical acceptance record reports all 19 scenarios passing and all 17 dual-language scenarios matching. The matrix below enumerates that recorded result; the two Trino-only scenarios have no Scala notebook counterpart.

| Scenario | PySpark | Scala Spark | Parity |
|---|---|---|---|
| batch_ingest-nyc_taxi | PASS | PASS | MATCH |
| medallion-nyc_taxi | PASS | PASS | MATCH |
| data_quality-nyc_taxi | PASS | PASS | MATCH |
| schema_evolution-gh_archive | PASS | PASS | MATCH |
| time_travel-nyc_taxi | PASS | PASS | MATCH |
| table_maintenance-nyc_taxi | PASS | PASS | MATCH |
| streaming_ingest-events | PASS | PASS | MATCH |
| streaming_ingest-gh_archive | PASS | PASS | MATCH |
| streaming_windows-events | PASS | PASS | MATCH |
| cdc_streaming-online_retail | PASS | PASS | MATCH |
| federated_query-nyc_taxi | PASS | N/A | — |
| bi_query-tpch | PASS | N/A | — |
| join_optimization-tpch | PASS | PASS | MATCH |
| star_schema-tpch | PASS | PASS | MATCH |
| feature_engineering-movielens | PASS | PASS | MATCH |
| incremental_upsert-online_retail | PASS | PASS | MATCH |
| scd2-online_retail | PASS | PASS | MATCH |
| json_flatten-gh_archive | PASS | PASS | MATCH |
| sessionization-gh_archive | PASS | PASS | MATCH |

**Summary: 19/19 scenarios passed. 17/17 dual-language scenarios show parity.**

## 5. Trino Validation

Issue #83 added a current production replay for `tpch_bi_query` and `nyc_taxi_trino_daily` on
2026-08-12/13. Two paused runs per DAG succeeded through the real Airflow/Trino path and produced
stable canonical metadata-DB XCom checksums. TPC-H validated the exact five-key table provenance;
NYC remained bound to one unchanged Bronze snapshot. No Iceberg snapshot, property, raw pointer, or
Spark driver changed. See the [tracked acceptance report](superpowers/reports/2026-08-12-trino-bi-pipelines-live-acceptance.md).

```sql
-- federated_query-nyc_taxi
SELECT COUNT(*) FROM lakehouse.bronze.nyc_taxi_trips;
-- Result: matches Spark count ✓

-- bi_query-tpch
CREATE TABLE lakehouse.gold.bi_segment_revenue AS
SELECT market_segment, SUM(totalprice) AS revenue
FROM lakehouse.bronze.orders o
JOIN lakehouse.bronze.customer c ON o.o_custkey = c.c_custkey
GROUP BY market_segment;
-- Result: 5 segments with revenue ✓
```

## 6. Streaming Validation

- `streaming_ingest-events`: 500 events produced to Redpanda `events` topic, consumed by Spark Structured Streaming, written to `lakehouse.bronze.events`. Count matches source. ✓
- `cdc_streaming-online_retail`: CDC events ingested via `foreachBatch`, `MERGE INTO` applied. Upsert result matches expected state. ✓

## 7. Jenkins CI

```text
mvn test ... SUCCESS
mvn package ... SUCCESS
mc cp target/nyc-taxi-*.jar s3://jars/ ... SUCCESS
```

## 8. Recommendations

- Consider adding a cleanup task for streaming checkpoint directories to prevent growth.
- Monitor MinIO disk usage as scenarios are re-run with larger dataset scales.
- TPC-H at `large` scale may require increasing Spark executor memory to avoid OOM.
