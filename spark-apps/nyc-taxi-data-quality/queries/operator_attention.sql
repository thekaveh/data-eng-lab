WITH expected_rules(rule_id, rule_version) AS (
    VALUES
        ('bronze.source_available.v1', 'nyc_taxi_quality_v1'),
        ('bronze.schema.v1', 'nyc_taxi_quality_v1'),
        ('bronze.snapshot_freshness.v1', 'nyc_taxi_quality_v1'),
        ('bronze.invalid_ratio.v1', 'nyc_taxi_quality_v1'),
        ('silver.partition_conservation.v1', 'nyc_taxi_quality_v1'),
        ('silver.clean_nonempty.v1', 'nyc_taxi_quality_v1'),
        ('silver.quarantine_ratio.v1', 'nyc_taxi_quality_v1'),
        ('silver.output_readback.v1', 'nyc_taxi_quality_v1')
),
complete_runs AS (
    SELECT f.quality_run_id
    FROM lakehouse.gold.nyc_taxi_quality_facts AS f
    LEFT JOIN expected_rules AS e
        ON f.rule_id = e.rule_id AND f.rule_version = e.rule_version
    GROUP BY f.quality_run_id
    HAVING count(*) = 8
       AND count(e.rule_id) = 8
       AND count(DISTINCT f.rule_id) = 8
       AND count(DISTINCT f.dataset_id) = 1
       AND min(f.dataset_id) = 'nyc_taxi'
       AND count(DISTINCT f.binding_type) = 1
       AND min(f.binding_type) = 'iceberg_snapshot'
       AND count(DISTINCT f.source_table) = 1
       AND min(f.source_table) = concat('lakehouse.bronze.', 'nyc_taxi_trips')
       AND count(DISTINCT f.source_snapshot_id) = 1
       AND count(DISTINCT f.source_schema_sha256) = 1
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
JOIN complete_runs AS r ON f.quality_run_id = r.quality_run_id
WHERE f.status IN ('warn', 'fail', 'missing', 'stale')
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
