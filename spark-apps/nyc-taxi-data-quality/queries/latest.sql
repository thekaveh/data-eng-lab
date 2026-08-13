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
        max(f.source_snapshot_id) AS source_snapshot_id
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
