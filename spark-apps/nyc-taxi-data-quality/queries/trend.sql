WITH complete_runs AS (
    SELECT
        quality_run_id,
        max(logical_date) AS logical_date,
        max(source_snapshot_id) AS source_snapshot_id,
        max(CASE WHEN rule_id = 'bronze.source_available.v1' THEN metric_numerator END)
            AS source_row_count,
        max(CASE WHEN rule_id = 'bronze.invalid_ratio.v1' THEN metric_numerator END)
            AS invalid_row_count,
        CAST(max(CASE WHEN rule_id = 'bronze.invalid_ratio.v1' THEN metric_value END) AS decimal(38, 9))
            AS invalid_ratio,
        max(CASE WHEN rule_id = 'silver.clean_nonempty.v1' THEN metric_numerator END)
            AS clean_row_count,
        max(CASE WHEN rule_id = 'silver.quarantine_ratio.v1' THEN metric_numerator END)
            AS quarantine_row_count,
        CAST(max(CASE WHEN rule_id = 'silver.quarantine_ratio.v1' THEN metric_value END) AS decimal(38, 9))
            AS quarantine_ratio,
        CASE WHEN count_if(status = 'warn') > 0 THEN 'warn' ELSE 'pass' END
            AS overall_status
    FROM lakehouse.gold.nyc_taxi_quality_facts
    GROUP BY quality_run_id
    HAVING count(*) = 8
       AND count_if(status NOT IN ('pass', 'warn')) = 0
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
