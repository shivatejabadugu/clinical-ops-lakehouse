-- Emergency department throughput. All intervals in minutes. Reporting dates
-- are in hospital local time, because "Tuesday's LWBS rate" means the local
-- Tuesday, not a UTC day.

CREATE OR REFRESH MATERIALIZED VIEW gold_fct_ed_visits
COMMENT 'One row per ED encounter with throughput intervals and outcome'
AS
WITH ed AS (
  SELECT * FROM silver_encounters WHERE encounter_class = 'EMER'
),
admissions AS (
  SELECT part_of_encounter_id, encounter_id, start_at AS admit_decision_at
  FROM silver_encounters
  WHERE encounter_class = 'IMP' AND part_of_encounter_id IS NOT NULL
)
SELECT
  ed.encounter_id AS ed_encounter_id,
  ed.patient_id,
  ed.facility_id,
  ed.start_at AS arrived_at,
  ed.triage_at,
  ed.provider_seen_at,
  ed.end_at AS departed_at,
  to_date(from_utc_timestamp(ed.start_at, 'America/New_York')) AS arrival_date_local,
  hour(from_utc_timestamp(ed.start_at, 'America/New_York')) AS arrival_hour_local,
  timestampdiff(MINUTE, ed.start_at, ed.triage_at) AS door_to_triage_min,
  timestampdiff(MINUTE, ed.start_at, ed.provider_seen_at) AS door_to_provider_min,
  timestampdiff(MINUTE, ed.start_at, ed.end_at) AS ed_length_of_stay_min,
  a.encounter_id AS inpatient_encounter_id,
  a.admit_decision_at,
  -- boarding: admitted, waiting in the ED for an inpatient bed
  CASE WHEN a.encounter_id IS NOT NULL
       THEN greatest(timestampdiff(MINUTE, a.admit_decision_at, ed.end_at), 0) END AS boarding_min,
  CASE
    WHEN ed.status <> 'finished' THEN 'still_in_ed'
    WHEN ed.provider_seen_at IS NULL THEN 'left_without_being_seen'
    WHEN a.encounter_id IS NOT NULL THEN 'admitted'
    ELSE 'discharged'
  END AS ed_outcome
FROM ed
LEFT JOIN admissions AS a ON a.part_of_encounter_id = ed.encounter_id;

CREATE OR REFRESH MATERIALIZED VIEW gold_agg_ed_daily
COMMENT 'Daily ED performance per facility (local dates)'
AS SELECT
  facility_id,
  arrival_date_local,
  count(*) AS arrivals,
  count_if(ed_outcome = 'left_without_being_seen') AS lwbs,
  round(100 * try_divide(count_if(ed_outcome = 'left_without_being_seen'), count(*)), 1) AS lwbs_pct,
  round(100 * try_divide(count_if(ed_outcome = 'admitted'), count(*)), 1) AS admit_pct,
  percentile_approx(door_to_provider_min, 0.5) AS median_door_to_provider_min,
  percentile_approx(door_to_provider_min, 0.9) AS p90_door_to_provider_min,
  percentile_approx(ed_length_of_stay_min, 0.5) FILTER (WHERE ed_outcome = 'discharged')
    AS median_los_discharged_min,
  round(percentile_approx(boarding_min, 0.5) / 60.0, 1) AS median_boarding_hours,
  count_if(boarding_min > 240) AS boarded_over_4h
FROM gold_fct_ed_visits
WHERE ed_outcome <> 'still_in_ed'
GROUP BY facility_id, arrival_date_local;
