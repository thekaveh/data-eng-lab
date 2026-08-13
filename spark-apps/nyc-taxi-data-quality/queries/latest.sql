WITH complete_runs AS (
    SELECT
        quality_run_id,
        max(logical_date) AS logical_date,
        max(source_snapshot_id) AS source_snapshot_id
    FROM lakehouse.gold.nyc_taxi_quality_facts
    GROUP BY quality_run_id
    HAVING count(*) = 8
       AND count_if(status NOT IN ('pass', 'warn')) = 0
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
