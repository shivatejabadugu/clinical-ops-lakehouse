-- Patients: SCD Type 2. A new Patient version (e.g. a new address) closes the
-- previous row and opens a new one; __START_AT / __END_AT are maintained by
-- APPLY CHANGES. lastUpdated alone changing doesn't create a version.

CREATE OR REFRESH STREAMING TABLE silver_patient_changes (
  CONSTRAINT has_id EXPECT (patient_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT has_mrn EXPECT (mrn IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT has_last_updated EXPECT (last_updated IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_birth_date EXPECT (birth_date IS NOT NULL AND birth_date <= current_date())
    ON VIOLATION DROP ROW
)
COMMENT 'Every valid version of every patient'
AS
WITH parsed AS (
  SELECT
    from_json(raw, 'id STRING, meta STRUCT<lastUpdated: STRING>,
      identifier ARRAY<STRUCT<system: STRING, value: STRING>>,
      name ARRAY<STRUCT<family: STRING, given: ARRAY<STRING>>>,
      gender STRING, birthDate STRING,
      address ARRAY<STRUCT<postalCode: STRING, state: STRING>>') AS r,
    source_file,
    ingested_at
  FROM STREAM(bronze_fhir_raw)
  WHERE get_json_object(raw, '$.resourceType') = 'Patient'
)
SELECT
  r.id AS patient_id,
  try_element_at(filter(r.identifier, i -> i.system = 'urn:example:mrn'), 1).value AS mrn,
  try_element_at(r.name, 1).family AS family_name,
  try_element_at(try_element_at(r.name, 1).given, 1) AS given_name,
  r.gender,
  try_cast(r.birthDate AS DATE) AS birth_date,
  try_element_at(r.address, 1).postalCode AS postal_code,
  try_element_at(r.address, 1).state AS state,
  try_to_timestamp(r.meta.lastUpdated) AS last_updated,
  source_file,
  ingested_at
FROM parsed;

CREATE OR REFRESH STREAMING TABLE silver_patients_history
COMMENT 'Patient demographics with full history (SCD2). Contains PHI: exposed only through clinical_secure views.';

APPLY CHANGES INTO silver_patients_history
FROM STREAM(silver_patient_changes)
KEYS (patient_id)
SEQUENCE BY last_updated
COLUMNS * EXCEPT (source_file, ingested_at)
STORED AS SCD TYPE 2
TRACK HISTORY ON * EXCEPT (last_updated);
