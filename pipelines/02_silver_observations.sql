-- Observations: vitals and labs.
--
-- 1. parsed:     flatten FHIR JSON, normalise units (degF -> Cel, lactate mg/dL -> mmol/L)
-- 2. changes:    every version that passes quality rules (expectations drop the rest)
-- 3. quarantine: every version that fails, with the reason
-- 4. current:    APPLY CHANGES keeps the latest version per id and deletes
--                results later marked entered-in-error, even when versions
--                arrive out of order across export batches.

CREATE OR REFRESH STREAMING TABLE silver_observation_parsed
COMMENT 'Observation resources flattened, with units normalised to canonical'
AS
WITH parsed AS (
  SELECT
    from_json(raw, 'id STRING, status STRING, meta STRUCT<lastUpdated: STRING>,
      code STRUCT<coding: ARRAY<STRUCT<system: STRING, code: STRING>>>,
      subject STRUCT<reference: STRING>, encounter STRUCT<reference: STRING>,
      effectiveDateTime STRING, valueQuantity STRUCT<value: DOUBLE, unit: STRING>') AS r,
    raw,
    source_file,
    ingested_at
  FROM STREAM(bronze_fhir_raw)
  WHERE get_json_object(raw, '$.resourceType') = 'Observation'
),
flat AS (
  SELECT
    r.id AS observation_id,
    r.status,
    try_element_at(filter(r.code.coding, c -> c.system = 'http://loinc.org'), 1).code AS loinc_code,
    nullif(regexp_extract(r.subject.reference, '^Patient/(.+)$', 1), '') AS patient_id,
    nullif(regexp_extract(r.encounter.reference, '^Encounter/(.+)$', 1), '') AS encounter_id,
    -- ISO-8601 with Z or a local offset; stored as UTC
    try_to_timestamp(r.effectiveDateTime) AS effective_at,
    r.valueQuantity.value AS value_raw,
    r.valueQuantity.unit AS unit_raw,
    try_to_timestamp(r.meta.lastUpdated) AS last_updated,
    raw,
    source_file,
    ingested_at
  FROM parsed
)
SELECT
  f.observation_id,
  f.status,
  f.loinc_code,
  ref.measure,
  f.patient_id,
  f.encounter_id,
  f.effective_at,
  CASE
    WHEN f.loinc_code = '8310-5' AND f.unit_raw IN ('degF', '[degF]') THEN round((f.value_raw - 32) * 5 / 9, 2)
    WHEN f.loinc_code = '2524-7' AND f.unit_raw = 'mg/dL' THEN round(f.value_raw / 9.01, 2)
    ELSE f.value_raw
  END AS value,
  CASE
    WHEN f.loinc_code = '8310-5' AND f.unit_raw IN ('degF', '[degF]') THEN 'Cel'
    WHEN f.loinc_code = '2524-7' AND f.unit_raw = 'mg/dL' THEN 'mmol/L'
    ELSE f.unit_raw
  END AS unit,
  f.value_raw,
  f.unit_raw,
  ref.canonical_unit,
  ref.min_plausible,
  ref.max_plausible,
  f.last_updated,
  f.raw,
  f.source_file,
  f.ingested_at
FROM flat AS f
LEFT JOIN ref_loinc_measures AS ref ON ref.loinc_code = f.loinc_code;

CREATE OR REFRESH STREAMING TABLE silver_observation_changes (
  CONSTRAINT has_id EXPECT (observation_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT has_patient EXPECT (patient_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT has_effective_time EXPECT (effective_at IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT has_last_updated EXPECT (last_updated IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT known_measure EXPECT (measure IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT known_status EXPECT (status IN ('final', 'amended', 'corrected', 'preliminary', 'entered-in-error'))
    ON VIOLATION DROP ROW,
  CONSTRAINT plausible_value EXPECT (value IS NULL OR value BETWEEN min_plausible AND max_plausible)
    ON VIOLATION DROP ROW,
  -- warn only: tracked in the event log, row kept
  CONSTRAINT canonical_unit EXPECT (unit IS NULL OR unit = canonical_unit)
)
COMMENT 'Every valid version of every observation'
AS SELECT
  observation_id, status, loinc_code, measure, patient_id, encounter_id, effective_at,
  value, unit, canonical_unit, min_plausible, max_plausible, last_updated, source_file, ingested_at
FROM STREAM(silver_observation_parsed);

CREATE OR REFRESH STREAMING TABLE silver_observation_quarantine
COMMENT 'Observation versions that failed a quality rule, with the reason. Mirrors the DROP ROW expectations.'
AS SELECT
  CASE
    WHEN observation_id IS NULL THEN 'missing_id'
    WHEN patient_id IS NULL THEN 'missing_patient'
    WHEN effective_at IS NULL THEN 'invalid_effective_time'
    WHEN last_updated IS NULL THEN 'invalid_last_updated'
    WHEN measure IS NULL THEN 'unknown_code'
    WHEN status NOT IN ('final', 'amended', 'corrected', 'preliminary', 'entered-in-error') THEN 'unknown_status'
    ELSE 'implausible_value'
  END AS reason,
  observation_id, loinc_code, value_raw, unit_raw, raw, source_file, ingested_at
FROM STREAM(silver_observation_parsed)
WHERE NOT coalesce(
  observation_id IS NOT NULL
  AND patient_id IS NOT NULL
  AND effective_at IS NOT NULL
  AND last_updated IS NOT NULL
  AND measure IS NOT NULL
  AND status IN ('final', 'amended', 'corrected', 'preliminary', 'entered-in-error')
  AND (value IS NULL OR value BETWEEN min_plausible AND max_plausible),
  false
);

CREATE OR REFRESH STREAMING TABLE silver_observations
COMMENT 'Current version of each observation (SCD1). Results marked entered-in-error are removed.';

-- APPLY CHANGES was renamed AUTO CDC in 2025; both keywords work.
-- A redelivered duplicate carries the same lastUpdated as the original, so the
-- source file breaks the tie deterministically.
APPLY CHANGES INTO silver_observations
FROM STREAM(silver_observation_changes)
KEYS (observation_id)
APPLY AS DELETE WHEN status = 'entered-in-error'
SEQUENCE BY struct(last_updated, source_file)
COLUMNS * EXCEPT (canonical_unit, min_plausible, max_plausible, source_file, ingested_at)
STORED AS SCD TYPE 1;
