-- Expectation pass/fail counts per table and rule, from the pipeline event
-- log. Run on a SQL warehouse; point event_log() at any table in the pipeline.

WITH progress AS (
  SELECT
    timestamp,
    origin.flow_name AS flow_name,
    from_json(
      details:flow_progress:data_quality:expectations,
      'ARRAY<STRUCT<name: STRING, dataset: STRING, passed_records: BIGINT, failed_records: BIGINT>>'
    ) AS expectations
  FROM event_log(TABLE(clinical_dev.clinical.silver_observation_changes))
  WHERE event_type = 'flow_progress'
    AND details:flow_progress:data_quality:expectations IS NOT NULL
)
SELECT
  e.dataset,
  e.name AS expectation,
  sum(e.passed_records) AS passed,
  sum(e.failed_records) AS failed,
  round(100 * try_divide(sum(e.failed_records), sum(e.passed_records) + sum(e.failed_records)), 3) AS failed_pct
FROM progress
LATERAL VIEW explode(expectations) x AS e
GROUP BY e.dataset, e.name
ORDER BY failed DESC;
