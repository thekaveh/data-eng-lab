WITH expected_rules(
    rule_id, rule_version, layer, owner, metric_name, warn_threshold, fail_threshold
) AS (
    VALUES
        ('bronze.source_available.v1', 'nyc_taxi_quality_v1', 'Bronze', 'Data Engineering',
            'source_row_count', CAST(NULL AS varchar), 'rows=0'),
        ('bronze.schema.v1', 'nyc_taxi_quality_v1', 'Bronze', 'Data Engineering',
            'schema_match_ratio', CAST(NULL AS varchar), 'ratio<1.000000000'),
        ('bronze.snapshot_freshness.v1', 'nyc_taxi_quality_v1', 'Bronze', 'Data Engineering',
            'snapshot_age_seconds', CAST(NULL AS varchar), 'seconds>21600'),
        ('bronze.invalid_ratio.v1', 'nyc_taxi_quality_v1', 'Bronze', 'Data Quality Engineering',
            'invalid_row_ratio', 'ratio>0.010000000', 'ratio>0.050000000'),
        ('silver.partition_conservation.v1', 'nyc_taxi_quality_v1', 'Silver', 'Data Quality Engineering',
            'partition_row_ratio', CAST(NULL AS varchar), 'ratio!=1.000000000'),
        ('silver.clean_nonempty.v1', 'nyc_taxi_quality_v1', 'Silver', 'Data Quality Engineering',
            'clean_row_count', CAST(NULL AS varchar), 'rows=0'),
        ('silver.quarantine_ratio.v1', 'nyc_taxi_quality_v1', 'Silver', 'Data Quality Engineering',
            'quarantine_row_ratio', 'ratio>0.010000000', 'ratio>0.050000000'),
        ('silver.output_readback.v1', 'nyc_taxi_quality_v1', 'Silver', 'Data Platform Engineering',
            'readback_check_ratio', CAST(NULL AS varchar), 'ratio<1.000000000')
)
SELECT
    f.quality_run_id,
    format_datetime(f.logical_date, 'yyyy-MM-dd''T''HH:mm:ss''Z''') AS logical_date_utc,
    f.source_snapshot_id,
    f.layer,
    f.rule_id,
    f.status,
    f.severity,
    f.diagnostic_code,
    f.owner,
    f.metric_name,
    f.metric_numerator,
    f.metric_denominator,
    CAST(f.metric_value AS decimal(38, 9)) AS metric_value,
    f.warn_threshold,
    f.fail_threshold
FROM lakehouse.gold.nyc_taxi_quality_facts AS f
JOIN expected_rules AS e
    ON f.rule_id = e.rule_id
   AND f.rule_version = e.rule_version
   AND f.layer = e.layer
   AND f.owner = e.owner
   AND f.metric_name = e.metric_name
   AND f.warn_threshold IS NOT DISTINCT FROM e.warn_threshold
   AND f.fail_threshold = e.fail_threshold
WHERE f.dataset_id = 'nyc_taxi'
  AND f.binding_type = 'iceberg_snapshot'
  AND f.upstream_dag_id = 'nyc_taxi_etl'
  AND f.source_table = concat('lakehouse.bronze.', 'nyc_taxi_trips')
  AND f.status IN ('warn', 'fail', 'missing', 'stale')
  AND f.diagnostic_code IN (
      'threshold_warn', 'threshold_fail', 'source_missing', 'source_stale',
      'schema_mismatch', 'partition_mismatch', 'output_empty', 'readback_mismatch'
  )
  AND f.severity = CASE WHEN f.status = 'warn' THEN 'warning' ELSE 'error' END
ORDER BY f.logical_date DESC,
    CASE f.status
        WHEN 'missing' THEN 4
        WHEN 'stale' THEN 3
        WHEN 'fail' THEN 2
        WHEN 'warn' THEN 1
    END DESC,
    f.layer,
    f.rule_id
LIMIT 100
