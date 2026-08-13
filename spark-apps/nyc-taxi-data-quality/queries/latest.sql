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
),
complete_runs AS (
    SELECT
        f.quality_run_id,
        max(f.logical_date) AS logical_date,
        max(f.source_snapshot_id) AS source_snapshot_id
    FROM lakehouse.gold.nyc_taxi_quality_facts AS f
    LEFT JOIN expected_rules AS e
        ON f.rule_id = e.rule_id
       AND f.rule_version = e.rule_version
       AND f.layer = e.layer
       AND f.owner = e.owner
       AND f.metric_name = e.metric_name
       AND f.warn_threshold IS NOT DISTINCT FROM e.warn_threshold
       AND f.fail_threshold = e.fail_threshold
    GROUP BY f.quality_run_id
    HAVING count(*) = 8
       AND count(e.rule_id) = 8
       AND count(DISTINCT f.rule_id) = 8
       AND count(f.dataset_id) = 8
       AND count(DISTINCT f.dataset_id) = 1
       AND min(f.dataset_id) = 'nyc_taxi'
       AND count(f.binding_type) = 8
       AND count(DISTINCT f.binding_type) = 1
       AND min(f.binding_type) = 'iceberg_snapshot'
       AND count(f.upstream_dag_id) = 8
       AND count(DISTINCT f.upstream_dag_id) = 1
       AND min(f.upstream_dag_id) = 'nyc_taxi_etl'
       AND count(f.source_table) = 8
       AND count(DISTINCT f.source_table) = 1
       AND min(f.source_table) = concat('lakehouse.bronze.', 'nyc_taxi_trips')
       AND count(f.source_snapshot_id) = 8
       AND count(DISTINCT f.source_snapshot_id) = 1
       AND min(f.source_snapshot_id) > 0
       AND count(f.source_snapshot_committed_at) = 8
       AND count(DISTINCT f.source_snapshot_committed_at) = 1
       AND count(f.source_schema_sha256) = 8
       AND count(DISTINCT f.source_schema_sha256) = 1
       AND min(f.source_schema_sha256) = '5a8d2916cc5967c0eeb8318136c1262156cd616105dad67a713f1cb1cc872fc5'
       AND count(f.logical_date) = 8
       AND count(DISTINCT f.logical_date) = 1
       AND count(f.data_interval_end) = 8
       AND count(DISTINCT f.data_interval_end) = 1
       AND max(f.data_interval_end) = max(f.logical_date) + INTERVAL '1' HOUR
       AND f.quality_run_id = lower(to_hex(sha256(to_utf8(concat(
           'nyc_taxi', chr(10),
           format_datetime(max(f.logical_date), 'yyyy-MM-dd''T''HH:mm:ss''Z'''), chr(10),
           cast(max(f.source_snapshot_id) AS varchar), chr(10),
           'nyc_taxi_quality_v1'
       )))))
       AND count_if(
           f.rule_id = 'bronze.source_available.v1'
           AND f.metric_numerator > 0
           AND f.metric_denominator IS NULL
           AND f.metric_value = cast(f.metric_numerator AS decimal(38, 9))
           AND f.status = 'pass' AND f.severity = 'info' AND f.diagnostic_code = 'ok'
       ) = 1
       AND count_if(
           f.rule_id = 'bronze.schema.v1'
           AND f.metric_numerator = 20 AND f.metric_denominator = 20
           AND f.metric_value = cast(1 AS decimal(38, 9))
           AND f.status = 'pass' AND f.severity = 'info' AND f.diagnostic_code = 'ok'
       ) = 1
       AND count_if(
           f.rule_id = 'bronze.snapshot_freshness.v1'
           AND f.metric_numerator BETWEEN 0 AND 21600 AND f.metric_denominator = 21600
           AND f.metric_numerator = date_diff(
               'second', f.source_snapshot_committed_at, f.data_interval_end
           )
           AND f.metric_value = cast(f.metric_numerator AS decimal(38, 9))
           AND f.status = 'pass' AND f.severity = 'info' AND f.diagnostic_code = 'ok'
       ) = 1
       AND count_if(
           f.rule_id = 'bronze.invalid_ratio.v1'
           AND f.metric_numerator BETWEEN 0 AND f.metric_denominator
           AND f.metric_denominator > 0
           AND f.metric_value = cast(
               cast(f.metric_numerator AS decimal(38, 9)) / f.metric_denominator AS decimal(38, 9)
           )
           AND (
               (f.metric_value <= cast(0.010000000 AS decimal(38, 9))
                   AND f.status = 'pass' AND f.severity = 'info' AND f.diagnostic_code = 'ok')
               OR
               (f.metric_value > cast(0.010000000 AS decimal(38, 9))
                   AND f.metric_value <= cast(0.050000000 AS decimal(38, 9))
                   AND f.status = 'warn' AND f.severity = 'warning'
                   AND f.diagnostic_code = 'threshold_warn')
           )
       ) = 1
       AND count_if(
           f.rule_id = 'silver.partition_conservation.v1'
           AND f.metric_numerator > 0 AND f.metric_numerator = f.metric_denominator
           AND f.metric_value = cast(1 AS decimal(38, 9))
           AND f.status = 'pass' AND f.severity = 'info' AND f.diagnostic_code = 'ok'
       ) = 1
       AND count_if(
           f.rule_id = 'silver.clean_nonempty.v1'
           AND f.metric_numerator > 0 AND f.metric_denominator > 0
           AND f.metric_value = cast(f.metric_numerator AS decimal(38, 9))
           AND f.status = 'pass' AND f.severity = 'info' AND f.diagnostic_code = 'ok'
       ) = 1
       AND count_if(
           f.rule_id = 'silver.quarantine_ratio.v1'
           AND f.metric_numerator BETWEEN 0 AND f.metric_denominator
           AND f.metric_denominator > 0
           AND f.metric_value = cast(
               cast(f.metric_numerator AS decimal(38, 9)) / f.metric_denominator AS decimal(38, 9)
           )
           AND (
               (f.metric_value <= cast(0.010000000 AS decimal(38, 9))
                   AND f.status = 'pass' AND f.severity = 'info' AND f.diagnostic_code = 'ok')
               OR
               (f.metric_value > cast(0.010000000 AS decimal(38, 9))
                   AND f.metric_value <= cast(0.050000000 AS decimal(38, 9))
                   AND f.status = 'warn' AND f.severity = 'warning'
                   AND f.diagnostic_code = 'threshold_warn')
           )
       ) = 1
       AND count_if(
           f.rule_id = 'silver.output_readback.v1'
           AND f.metric_numerator = 8 AND f.metric_denominator = 8
           AND f.metric_value = cast(1 AS decimal(38, 9))
           AND f.status = 'pass' AND f.severity = 'info' AND f.diagnostic_code = 'ok'
       ) = 1
       AND max(CASE WHEN f.rule_id = 'bronze.source_available.v1' THEN f.metric_numerator END)
           = max(CASE WHEN f.rule_id = 'bronze.invalid_ratio.v1' THEN f.metric_denominator END)
       AND max(CASE WHEN f.rule_id = 'bronze.source_available.v1' THEN f.metric_numerator END)
           = max(CASE WHEN f.rule_id = 'silver.partition_conservation.v1' THEN f.metric_numerator END)
       AND max(CASE WHEN f.rule_id = 'bronze.source_available.v1' THEN f.metric_numerator END)
           = max(CASE WHEN f.rule_id = 'silver.clean_nonempty.v1' THEN f.metric_denominator END)
       AND max(CASE WHEN f.rule_id = 'bronze.source_available.v1' THEN f.metric_numerator END)
           = max(CASE WHEN f.rule_id = 'silver.quarantine_ratio.v1' THEN f.metric_denominator END)
       AND max(CASE WHEN f.rule_id = 'bronze.invalid_ratio.v1' THEN f.metric_numerator END)
           = max(CASE WHEN f.rule_id = 'silver.quarantine_ratio.v1' THEN f.metric_numerator END)
       AND max(CASE WHEN f.rule_id = 'silver.clean_nonempty.v1' THEN f.metric_numerator END)
           + max(CASE WHEN f.rule_id = 'silver.quarantine_ratio.v1' THEN f.metric_numerator END)
           = max(CASE WHEN f.rule_id = 'bronze.source_available.v1' THEN f.metric_numerator END)
),
latest_run AS (
    SELECT quality_run_id
    FROM complete_runs
    ORDER BY logical_date DESC, source_snapshot_id DESC, quality_run_id DESC
    LIMIT 1
)
SELECT
    f.quality_run_id,
    format_datetime(f.logical_date, 'yyyy-MM-dd''T''HH:mm:ss''Z''') AS logical_date_utc,
    f.source_snapshot_id,
    f.layer,
    f.rule_id,
    f.owner,
    f.metric_name,
    f.metric_numerator,
    f.metric_denominator,
    CAST(f.metric_value AS decimal(38, 9)) AS metric_value,
    f.warn_threshold,
    f.fail_threshold,
    f.severity,
    f.status,
    f.diagnostic_code
FROM lakehouse.gold.nyc_taxi_quality_facts AS f
JOIN latest_run AS r ON f.quality_run_id = r.quality_run_id
ORDER BY f.layer, f.rule_id
LIMIT 8
