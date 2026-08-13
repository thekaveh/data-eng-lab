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
    SELECT
        f.quality_run_id,
        max(f.logical_date) AS logical_date,
        max(f.source_snapshot_id) AS source_snapshot_id,
        max(CASE WHEN f.rule_id = 'bronze.source_available.v1' THEN f.metric_numerator END)
            AS source_row_count,
        max(CASE WHEN f.rule_id = 'bronze.invalid_ratio.v1' THEN f.metric_numerator END)
            AS invalid_row_count,
        CAST(max(CASE WHEN f.rule_id = 'bronze.invalid_ratio.v1' THEN f.metric_value END) AS decimal(38, 9))
            AS invalid_ratio,
        max(CASE WHEN f.rule_id = 'silver.clean_nonempty.v1' THEN f.metric_numerator END)
            AS clean_row_count,
        max(CASE WHEN f.rule_id = 'silver.quarantine_ratio.v1' THEN f.metric_numerator END)
            AS quarantine_row_count,
        CAST(max(CASE WHEN f.rule_id = 'silver.quarantine_ratio.v1' THEN f.metric_value END) AS decimal(38, 9))
            AS quarantine_ratio,
        CASE WHEN count_if(f.status = 'warn') > 0 THEN 'warn' ELSE 'pass' END
            AS overall_status
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
       AND count_if(f.status NOT IN ('pass', 'warn')) = 0
)
SELECT
    quality_run_id,
    format_datetime(logical_date, 'yyyy-MM-dd''T''HH:mm:ss''Z''') AS logical_date_utc,
    source_snapshot_id,
    source_row_count,
    invalid_row_count,
    invalid_ratio,
    clean_row_count,
    quarantine_row_count,
    quarantine_ratio,
    overall_status
FROM complete_runs
ORDER BY logical_date DESC, source_snapshot_id DESC, quality_run_id DESC
LIMIT 90
