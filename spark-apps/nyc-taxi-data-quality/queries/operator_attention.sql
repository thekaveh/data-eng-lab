SELECT
    quality_run_id,
    format_datetime(logical_date, 'yyyy-MM-dd''T''HH:mm:ss''Z''') AS logical_date_utc,
    source_snapshot_id,
    layer,
    rule_id,
    status,
    severity,
    diagnostic_code,
    owner,
    metric_name,
    metric_numerator,
    metric_denominator,
    CAST(metric_value AS decimal(38, 9)) AS metric_value,
    warn_threshold,
    fail_threshold
FROM lakehouse.gold.nyc_taxi_quality_facts
WHERE status IN ('warn', 'fail', 'missing', 'stale')
ORDER BY logical_date DESC,
    CASE status
        WHEN 'missing' THEN 4
        WHEN 'stale' THEN 3
        WHEN 'fail' THEN 2
        WHEN 'warn' THEN 1
    END DESC,
    layer,
    rule_id
LIMIT 100
