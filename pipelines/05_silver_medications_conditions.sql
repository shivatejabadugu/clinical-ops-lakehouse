CREATE OR REFRESH STREAMING TABLE silver_medication_admin_changes (
  CONSTRAINT has_id EXPECT (med_admin_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT has_encounter EXPECT (encounter_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT has_time EXPECT (administered_at IS NOT NULL) ON VIOLATION DROP ROW
)
AS
WITH parsed AS (
  SELECT
    from_json(raw, 'id STRING, status STRING, meta STRUCT<lastUpdated: STRING>,
      medicationCodeableConcept STRUCT<text: STRING>,
      category STRUCT<coding: ARRAY<STRUCT<code: STRING>>>,
      subject STRUCT<reference: STRING>, context STRUCT<reference: STRING>,
      effectiveDateTime STRING') AS r,
    source_file
  FROM STREAM(bronze_fhir_raw)
  WHERE get_json_object(raw, '$.resourceType') = 'MedicationAdministration'
)
SELECT
  r.id AS med_admin_id,
  r.status,
  lower(r.medicationCodeableConcept.text) AS medication,
  try_element_at(r.category.coding, 1).code AS category,
  nullif(regexp_extract(r.subject.reference, '^Patient/(.+)$', 1), '') AS patient_id,
  nullif(regexp_extract(r.context.reference, '^Encounter/(.+)$', 1), '') AS encounter_id,
  try_to_timestamp(r.effectiveDateTime) AS administered_at,
  try_to_timestamp(r.meta.lastUpdated) AS last_updated,
  source_file
FROM parsed;

CREATE OR REFRESH STREAMING TABLE silver_medication_administrations;

APPLY CHANGES INTO silver_medication_administrations
FROM STREAM(silver_medication_admin_changes)
KEYS (med_admin_id)
SEQUENCE BY struct(last_updated, source_file)
COLUMNS * EXCEPT (source_file)
STORED AS SCD TYPE 1;

CREATE OR REFRESH STREAMING TABLE silver_condition_changes (
  CONSTRAINT has_id EXPECT (condition_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT has_code EXPECT (icd10_code IS NOT NULL) ON VIOLATION DROP ROW
)
AS
WITH parsed AS (
  SELECT
    from_json(raw, 'id STRING, meta STRUCT<lastUpdated: STRING>,
      code STRUCT<coding: ARRAY<STRUCT<system: STRING, code: STRING>>>,
      subject STRUCT<reference: STRING>, encounter STRUCT<reference: STRING>,
      recordedDate STRING') AS r,
    source_file
  FROM STREAM(bronze_fhir_raw)
  WHERE get_json_object(raw, '$.resourceType') = 'Condition'
)
SELECT
  r.id AS condition_id,
  try_element_at(filter(r.code.coding, c -> c.system = 'http://hl7.org/fhir/sid/icd-10-cm'), 1).code AS icd10_code,
  nullif(regexp_extract(r.subject.reference, '^Patient/(.+)$', 1), '') AS patient_id,
  nullif(regexp_extract(r.encounter.reference, '^Encounter/(.+)$', 1), '') AS encounter_id,
  try_to_timestamp(r.recordedDate) AS recorded_at,
  try_to_timestamp(r.meta.lastUpdated) AS last_updated,
  source_file
FROM parsed;

CREATE OR REFRESH STREAMING TABLE silver_conditions;

APPLY CHANGES INTO silver_conditions
FROM STREAM(silver_condition_changes)
KEYS (condition_id)
SEQUENCE BY struct(last_updated, source_file)
COLUMNS * EXCEPT (source_file)
STORED AS SCD TYPE 1;
