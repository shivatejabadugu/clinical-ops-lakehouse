-- Unity Catalog governance for analyst access. Runs on a SQL warehouse after
-- each pipeline refresh (job task apply_governance). Idempotent.
--
-- Analysts never query the pipeline schema (clinical) directly. They get the
-- clinical_secure views, which:
--   * mask MRNs unless the user is in phi_full_access
--   * show only the facilities the user's groups allow
--   * never expose names, birth dates, or ZIP codes at full precision
--
-- Groups (create in the account console): clinical_analysts, phi_full_access,
-- facility_atl_main, facility_atl_east, facility_all.

-- Only the catalog is parameterised (dev/prod). View bodies can't contain
-- parameter markers, so schema names are fixed and resolve against the
-- catalog in effect when each view is created.
USE CATALOG IDENTIFIER(:catalog);
USE SCHEMA clinical_secure;

CREATE OR REPLACE FUNCTION mask_mrn(mrn STRING)
RETURNS STRING
COMMENT 'Full MRN for phi_full_access, last 3 characters otherwise'
RETURN CASE
  WHEN is_account_group_member('phi_full_access') THEN mrn
  ELSE concat('***', right(mrn, 3))
END;

CREATE OR REPLACE FUNCTION can_see_facility(facility_id STRING)
RETURNS BOOLEAN
COMMENT 'Row-level access by facility group membership'
RETURN is_account_group_member('facility_all')
  OR is_account_group_member(concat('facility_', lower(replace(facility_id, '-', '_'))));

CREATE OR REPLACE VIEW ed_visits
COMMENT 'ED visits, facility-filtered, de-identified'
AS SELECT
  v.ed_encounter_id,
  clinical_secure.mask_mrn(p.mrn) AS mrn,
  p.gender,
  -- HIPAA Safe Harbor: ages over 89 are grouped
  CASE WHEN p.age_years > 89 THEN '90+' ELSE cast(p.age_years AS STRING) END AS age,
  left(p.postal_code, 3) AS zip3,
  v.facility_id, v.arrival_date_local, v.arrival_hour_local,
  v.door_to_triage_min, v.door_to_provider_min, v.ed_length_of_stay_min, v.boarding_min, v.ed_outcome
FROM clinical.gold_fct_ed_visits AS v
LEFT JOIN clinical.gold_dim_patient AS p ON p.patient_id = v.patient_id
WHERE clinical_secure.can_see_facility(v.facility_id);

CREATE OR REPLACE VIEW sepsis_episodes
COMMENT 'Sepsis bundle episodes, facility-filtered, de-identified'
AS SELECT
  s.visit_id, clinical_secure.mask_mrn(p.mrn) AS mrn, s.facility_id, s.time_zero, s.minutes_to_antibiotic,
  s.lactate_measured, s.cultures_before_antibiotic, s.antibiotic_within_3h, s.repeat_lactate_if_needed,
  s.bundle_compliant, s.first_failed_element, s.sepsis_coded, s.used_icu, s.died
FROM clinical.gold_fct_sepsis_episodes AS s
LEFT JOIN clinical.gold_dim_patient AS p ON p.patient_id = s.patient_id
WHERE clinical_secure.can_see_facility(s.facility_id);

-- Aggregates carry no patient-level data: filter by facility only.
CREATE OR REPLACE VIEW ed_daily AS
SELECT * FROM clinical.gold_agg_ed_daily WHERE clinical_secure.can_see_facility(facility_id);

CREATE OR REPLACE VIEW sepsis_monthly AS
SELECT * FROM clinical.gold_agg_sepsis_monthly WHERE clinical_secure.can_see_facility(facility_id);

CREATE OR REPLACE VIEW unit_occupancy_daily AS
SELECT * FROM clinical.gold_agg_unit_occupancy_daily WHERE clinical_secure.can_see_facility(facility_id);

GRANT USE SCHEMA ON SCHEMA clinical_secure TO `clinical_analysts`;
GRANT SELECT ON SCHEMA clinical_secure TO `clinical_analysts`;
GRANT EXECUTE ON SCHEMA clinical_secure TO `clinical_analysts`;
