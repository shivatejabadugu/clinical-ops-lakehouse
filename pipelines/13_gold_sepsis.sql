-- Sepsis surveillance and 3-hour bundle compliance.
--
-- This follows the structure of the CMS SEP-1 measure but is simplified and
-- is not a certified implementation:
--   time zero  = first hour in which, within a 6-hour window either side:
--                  * the patient meets 2+ SIRS criteria (that hour),
--                  * a blood culture was collected (infection suspected), and
--                  * there is organ dysfunction (lactate > 2 or SBP < 90),
--                and never earlier than arrival. Suspected infection that
--                never shows organ dysfunction is a rule-out, not an episode.
--   bundle     = lactate measured (6h before to 3h after time zero)
--              + blood cultures drawn before the first antibiotic
--              + antibiotics within 3h of time zero
--              + repeat lactate by 6h if the first lactate was > 2 mmol/L

-- Step 1: an hour-by-hour picture of each visit, with the last known value of
-- every vital carried forward, but only while it is still fresh (6h for
-- vitals, 24h for WBC). Missing hours are filled from a complete hour spine.
CREATE OR REFRESH MATERIALIZED VIEW gold_fct_vitals_hourly
COMMENT 'Hourly vitals per visit with bounded carry-forward, SIRS and qSOFA counts'
AS
WITH obs AS (
  SELECT m.visit_id, o.measure, o.value, date_trunc('HOUR', o.effective_at) AS hour_start
  FROM silver_observations AS o
  INNER JOIN gold_encounter_visit_map AS m ON m.encounter_id = o.encounter_id
  WHERE o.value IS NOT NULL
),
hourly AS (
  SELECT
    visit_id,
    hour_start,
    max(CASE WHEN measure = 'heart_rate' THEN value END) AS hr_max,
    max(CASE WHEN measure = 'resp_rate' THEN value END) AS rr_max,
    min(CASE WHEN measure = 'systolic_bp' THEN value END) AS sbp_min,
    max(CASE WHEN measure = 'temperature' THEN value END) AS temp_max,
    min(CASE WHEN measure = 'temperature' THEN value END) AS temp_min,
    min(CASE WHEN measure = 'gcs' THEN value END) AS gcs_min,
    max(CASE WHEN measure = 'wbc' THEN value END) AS wbc_max,
    min(CASE WHEN measure = 'wbc' THEN value END) AS wbc_min
  FROM obs
  GROUP BY visit_id, hour_start
),
spine AS (
  SELECT
    visit_id,
    explode(sequence(date_trunc('HOUR', visit_start), date_trunc('HOUR', visit_end), INTERVAL 1 HOUR)) AS hour_start
  FROM gold_dim_visits
  WHERE visit_end >= visit_start
),
filled AS (
  SELECT s.visit_id, s.hour_start, h.hr_max, h.rr_max, h.sbp_min, h.temp_max, h.temp_min,
         h.gcs_min, h.wbc_max, h.wbc_min
  FROM spine AS s
  LEFT JOIN hourly AS h ON h.visit_id = s.visit_id AND h.hour_start = s.hour_start
),
-- carry forward (value, hour it was observed) so staleness can be checked
carried AS (
  SELECT
    visit_id,
    hour_start,
    last_value(CASE WHEN hr_max IS NOT NULL THEN named_struct('at', hour_start, 'v', hr_max) END, true) OVER w AS hr,
    last_value(CASE WHEN rr_max IS NOT NULL THEN named_struct('at', hour_start, 'v', rr_max) END, true) OVER w AS rr,
    last_value(CASE WHEN sbp_min IS NOT NULL THEN named_struct('at', hour_start, 'v', sbp_min) END, true) OVER w AS sbp,
    last_value(CASE WHEN temp_max IS NOT NULL THEN named_struct('at', hour_start, 'v', temp_max) END, true) OVER w AS tmax,
    last_value(CASE WHEN temp_min IS NOT NULL THEN named_struct('at', hour_start, 'v', temp_min) END, true) OVER w AS tmin,
    last_value(CASE WHEN gcs_min IS NOT NULL THEN named_struct('at', hour_start, 'v', gcs_min) END, true) OVER w AS gcs,
    last_value(CASE WHEN wbc_max IS NOT NULL THEN named_struct('at', hour_start, 'v', wbc_max) END, true) OVER w AS wmax,
    last_value(CASE WHEN wbc_min IS NOT NULL THEN named_struct('at', hour_start, 'v', wbc_min) END, true) OVER w AS wmin
  FROM filled
  WINDOW w AS (PARTITION BY visit_id ORDER BY hour_start ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
fresh AS (
  SELECT
    visit_id,
    hour_start,
    CASE WHEN hr.at >= hour_start - INTERVAL 6 HOURS THEN hr.v END AS heart_rate,
    CASE WHEN rr.at >= hour_start - INTERVAL 6 HOURS THEN rr.v END AS resp_rate,
    CASE WHEN sbp.at >= hour_start - INTERVAL 6 HOURS THEN sbp.v END AS systolic_bp,
    CASE WHEN tmax.at >= hour_start - INTERVAL 6 HOURS THEN tmax.v END AS temp_max,
    CASE WHEN tmin.at >= hour_start - INTERVAL 6 HOURS THEN tmin.v END AS temp_min,
    CASE WHEN gcs.at >= hour_start - INTERVAL 6 HOURS THEN gcs.v END AS gcs,
    CASE WHEN wmax.at >= hour_start - INTERVAL 24 HOURS THEN wmax.v END AS wbc_max,
    CASE WHEN wmin.at >= hour_start - INTERVAL 24 HOURS THEN wmin.v END AS wbc_min
  FROM carried
)
SELECT
  *,
  cast(coalesce(temp_max > 38.0 OR temp_min < 36.0, false) AS INT)
    + cast(coalesce(heart_rate > 90, false) AS INT)
    + cast(coalesce(resp_rate > 20, false) AS INT)
    + cast(coalesce(wbc_max > 12 OR wbc_min < 4, false) AS INT) AS sirs_count,
  cast(coalesce(resp_rate >= 22, false) AS INT)
    + cast(coalesce(systolic_bp <= 100, false) AS INT)
    + cast(coalesce(gcs < 15, false) AS INT) AS qsofa_count
FROM fresh;

-- Step 2: time zero and the bundle elements for each episode.
CREATE OR REFRESH MATERIALIZED VIEW gold_fct_sepsis_episodes
COMMENT 'One row per visit that met the sepsis screen, with SEP-1-style bundle elements'
AS
WITH cultures AS (
  SELECT m.visit_id, o.effective_at
  FROM silver_observations AS o
  INNER JOIN gold_encounter_visit_map AS m ON m.encounter_id = o.encounter_id
  WHERE o.measure = 'blood_culture'
),
lactates AS (
  SELECT m.visit_id, o.effective_at, o.value
  FROM silver_observations AS o
  INNER JOIN gold_encounter_visit_map AS m ON m.encounter_id = o.encounter_id
  WHERE o.measure = 'lactate'
),
antibiotics AS (
  SELECT m.visit_id, a.administered_at, a.medication
  FROM silver_medication_administrations AS a
  INNER JOIN gold_encounter_visit_map AS m ON m.encounter_id = a.encounter_id
  WHERE a.category = 'antibiotic' AND a.status = 'completed'
),
organ_dysfunction AS (
  SELECT visit_id, effective_at AS observed_at FROM lactates WHERE value > 2
  UNION ALL
  SELECT visit_id, hour_start FROM gold_fct_vitals_hourly WHERE systolic_bp < 90
),
candidate_hours AS (
  SELECT h.visit_id, h.hour_start
  FROM gold_fct_vitals_hourly AS h
  WHERE h.sirs_count >= 2
    AND EXISTS (
      SELECT 1 FROM cultures AS c
      WHERE c.visit_id = h.visit_id
        AND c.effective_at >= h.hour_start - INTERVAL 6 HOURS
        AND c.effective_at < h.hour_start + INTERVAL 7 HOURS
    )
    AND EXISTS (
      SELECT 1 FROM organ_dysfunction AS od
      WHERE od.visit_id = h.visit_id
        AND od.observed_at >= h.hour_start - INTERVAL 6 HOURS
        AND od.observed_at < h.hour_start + INTERVAL 7 HOURS
    )
),
time_zero AS (
  SELECT c.visit_id, greatest(min(c.hour_start), min(v.visit_start)) AS time_zero
  FROM candidate_hours AS c
  INNER JOIN gold_dim_visits AS v ON v.visit_id = c.visit_id
  GROUP BY c.visit_id
),
initial_lactate AS (
  SELECT t.visit_id, min(l.effective_at) AS measured_at, min_by(l.value, l.effective_at) AS value
  FROM time_zero AS t
  INNER JOIN lactates AS l
    ON l.visit_id = t.visit_id
   AND l.effective_at BETWEEN t.time_zero - INTERVAL 6 HOURS AND t.time_zero + INTERVAL 3 HOURS
  GROUP BY t.visit_id
),
repeat_lactate AS (
  SELECT i.visit_id, min(l.effective_at) AS measured_at
  FROM initial_lactate AS i
  INNER JOIN time_zero AS t ON t.visit_id = i.visit_id
  INNER JOIN lactates AS l
    ON l.visit_id = i.visit_id
   AND l.effective_at > i.measured_at
   AND l.effective_at <= t.time_zero + INTERVAL 6 HOURS
  GROUP BY i.visit_id
),
first_culture AS (
  SELECT t.visit_id, min(c.effective_at) AS collected_at
  FROM time_zero AS t
  INNER JOIN cultures AS c ON c.visit_id = t.visit_id AND c.effective_at >= t.time_zero - INTERVAL 6 HOURS
  GROUP BY t.visit_id
),
first_antibiotic AS (
  SELECT t.visit_id, min(a.administered_at) AS given_at, min_by(a.medication, a.administered_at) AS medication
  FROM time_zero AS t
  INNER JOIN antibiotics AS a ON a.visit_id = t.visit_id AND a.administered_at >= t.time_zero - INTERVAL 24 HOURS
  GROUP BY t.visit_id
),
qsofa AS (
  SELECT visit_id, min(hour_start) AS first_qsofa_positive_hour
  FROM gold_fct_vitals_hourly
  WHERE qsofa_count >= 2
  GROUP BY visit_id
),
coded AS (
  SELECT DISTINCT m.visit_id
  FROM silver_conditions AS c
  INNER JOIN gold_encounter_visit_map AS m ON m.encounter_id = c.encounter_id
  WHERE c.icd10_code LIKE 'A41%' OR c.icd10_code LIKE 'R65.2%'
),
elements AS (
  SELECT
    t.visit_id,
    v.patient_id,
    v.facility_id,
    t.time_zero,
    il.measured_at AS initial_lactate_at,
    il.value AS initial_lactate,
    rl.measured_at AS repeat_lactate_at,
    fc.collected_at AS first_culture_at,
    fa.given_at AS first_antibiotic_at,
    fa.medication AS first_antibiotic,
    timestampdiff(MINUTE, t.time_zero, fa.given_at) AS minutes_to_antibiotic,
    q.first_qsofa_positive_hour,
    il.measured_at IS NOT NULL AS lactate_measured,
    coalesce(fc.collected_at <= fa.given_at, fc.collected_at IS NOT NULL AND fa.given_at IS NULL)
      AS cultures_before_antibiotic,
    coalesce(fa.given_at <= t.time_zero + INTERVAL 3 HOURS, false) AS antibiotic_within_3h,
    NOT coalesce(il.value > 2, false) OR rl.measured_at IS NOT NULL AS repeat_lactate_if_needed,
    cd.visit_id IS NOT NULL AS sepsis_coded,
    v.used_icu,
    v.died
  FROM time_zero AS t
  INNER JOIN gold_dim_visits AS v ON v.visit_id = t.visit_id
  LEFT JOIN initial_lactate AS il ON il.visit_id = t.visit_id
  LEFT JOIN repeat_lactate AS rl ON rl.visit_id = t.visit_id
  LEFT JOIN first_culture AS fc ON fc.visit_id = t.visit_id
  LEFT JOIN first_antibiotic AS fa ON fa.visit_id = t.visit_id
  LEFT JOIN qsofa AS q ON q.visit_id = t.visit_id
  LEFT JOIN coded AS cd ON cd.visit_id = t.visit_id
)
SELECT
  *,
  lactate_measured AND cultures_before_antibiotic AND antibiotic_within_3h AND repeat_lactate_if_needed
    AS bundle_compliant,
  CASE
    WHEN NOT lactate_measured THEN 'no_initial_lactate'
    WHEN NOT cultures_before_antibiotic THEN 'antibiotic_before_culture'
    WHEN NOT antibiotic_within_3h THEN 'antibiotic_late_or_missing'
    WHEN NOT repeat_lactate_if_needed THEN 'no_repeat_lactate'
  END AS first_failed_element
FROM elements;

CREATE OR REFRESH MATERIALIZED VIEW gold_agg_sepsis_monthly
COMMENT 'Sepsis bundle compliance by facility and month'
AS SELECT
  facility_id,
  date_trunc('MONTH', from_utc_timestamp(time_zero, 'America/New_York')) AS month_local,
  count(*) AS episodes,
  round(100 * try_divide(count_if(bundle_compliant), count(*)), 1) AS bundle_compliance_pct,
  round(100 * try_divide(count_if(lactate_measured), count(*)), 1) AS lactate_measured_pct,
  round(100 * try_divide(count_if(cultures_before_antibiotic), count(*)), 1) AS cultures_first_pct,
  round(100 * try_divide(count_if(antibiotic_within_3h), count(*)), 1) AS antibiotic_3h_pct,
  round(100 * try_divide(count_if(repeat_lactate_if_needed), count(*)), 1) AS repeat_lactate_pct,
  percentile_approx(minutes_to_antibiotic, 0.5) AS median_minutes_to_antibiotic,
  round(100 * try_divide(count_if(sepsis_coded), count(*)), 1) AS coded_pct,
  round(100 * try_divide(count_if(died), count(*)), 1) AS mortality_pct
FROM gold_fct_sepsis_episodes
GROUP BY facility_id, date_trunc('MONTH', from_utc_timestamp(time_zero, 'America/New_York'));
