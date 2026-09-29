-- A "visit" is one trip to the hospital: the ED encounter plus the inpatient
-- encounter it turned into (linked through Encounter.partOf). Most clinical
-- measures (sepsis, AKI) have to follow the patient across that boundary.

CREATE OR REFRESH MATERIALIZED VIEW gold_encounter_visit_map
COMMENT 'Maps every encounter to its visit (the root ED encounter, or itself)'
AS SELECT
  encounter_id,
  coalesce(part_of_encounter_id, encounter_id) AS visit_id,
  encounter_class,
  patient_id,
  facility_id
FROM silver_encounters;

CREATE OR REFRESH MATERIALIZED VIEW gold_dim_visits
COMMENT 'One row per visit: arrival to final departure, with ICU use and outcome'
AS
WITH data_as_of AS (
  SELECT max(last_updated) AS as_of FROM silver_encounters
),
enc AS (
  SELECT
    coalesce(e.part_of_encounter_id, e.encounter_id) AS visit_id,
    e.*
  FROM silver_encounters AS e
)
SELECT
  enc.visit_id,
  min_by(enc.patient_id, enc.start_at) AS patient_id,
  min_by(enc.facility_id, enc.start_at) AS facility_id,
  min(enc.start_at) AS visit_start,
  -- open encounters run to the end of the data
  max(coalesce(enc.end_at, a.as_of)) AS visit_end,
  max(enc.encounter_class = 'IMP') AS was_admitted,
  max_by(CASE WHEN enc.encounter_class = 'IMP' THEN enc.encounter_id END, enc.encounter_class = 'IMP')
    AS inpatient_encounter_id,
  exists(flatten(collect_list(enc.locations)), l -> l.location_id LIKE '%-ICU') AS used_icu,
  max_by(enc.disposition, enc.start_at) AS final_disposition,
  coalesce(max_by(enc.disposition, enc.start_at) = 'exp', false) AS died,
  bool_and(enc.status = 'finished') AS is_complete
FROM enc
CROSS JOIN data_as_of AS a
GROUP BY enc.visit_id;

CREATE OR REFRESH MATERIALIZED VIEW gold_dim_patient
COMMENT 'Current patient demographics, de-identified (no names). Full history lives in silver_patients_history.'
AS SELECT
  patient_id,
  mrn,
  gender,
  birth_date,
  floor(months_between(current_date(), birth_date) / 12) AS age_years,
  postal_code,
  __START_AT AS version_valid_from
FROM silver_patients_history
WHERE __END_AT IS NULL;
