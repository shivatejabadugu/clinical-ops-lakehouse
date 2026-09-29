-- Acute kidney injury by the KDIGO creatinine criteria:
--   rise of >= 0.3 mg/dL within 48 hours, or
--   >= 1.5x the lowest value in the prior 7 days (baseline)
-- RANGE frames over timestamps give "within the last 48 hours" by clock time,
-- regardless of how many results fall in the window.

CREATE OR REFRESH MATERIALIZED VIEW gold_fct_aki_events
COMMENT 'First AKI detection per visit, with KDIGO stage by creatinine ratio'
AS
WITH creatinine AS (
  SELECT o.patient_id, m.visit_id, o.effective_at, o.value
  FROM silver_observations AS o
  INNER JOIN gold_encounter_visit_map AS m ON m.encounter_id = o.encounter_id
  WHERE o.measure = 'creatinine' AND o.value IS NOT NULL
),
windowed AS (
  SELECT
    *,
    min(value) OVER (
      PARTITION BY patient_id ORDER BY effective_at
      RANGE BETWEEN INTERVAL 48 HOURS PRECEDING AND CURRENT ROW
    ) AS min_48h,
    min(value) OVER (
      PARTITION BY patient_id ORDER BY effective_at
      RANGE BETWEEN INTERVAL 7 DAYS PRECEDING AND CURRENT ROW
    ) AS baseline_7d
  FROM creatinine
),
flagged AS (
  SELECT
    *,
    value - min_48h >= 0.3 AS rise_48h,
    try_divide(value, baseline_7d) AS ratio_to_baseline
  FROM windowed
)
SELECT
  visit_id,
  patient_id,
  min(effective_at) FILTER (WHERE rise_48h OR ratio_to_baseline >= 1.5) AS aki_detected_at,
  min(baseline_7d) AS baseline_creatinine,
  max(value) AS peak_creatinine,
  round(max(ratio_to_baseline), 2) AS peak_ratio,
  CASE
    WHEN max(ratio_to_baseline) >= 3.0 OR max(value) >= 4.0 THEN 3
    WHEN max(ratio_to_baseline) >= 2.0 THEN 2
    ELSE 1
  END AS kdigo_stage
FROM flagged
GROUP BY visit_id, patient_id
HAVING count_if(rise_48h OR ratio_to_baseline >= 1.5) > 0;
