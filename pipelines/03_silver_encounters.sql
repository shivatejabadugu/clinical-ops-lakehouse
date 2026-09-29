-- Encounters arrive once when the patient shows up (in-progress) and again
-- when the visit ends (finished). The nested FHIR arrays are flattened here:
-- triage time from statusHistory, provider-seen time from participant, and the
-- unit-by-unit location history kept as an array of structs for gold to explode.

CREATE OR REFRESH STREAMING TABLE silver_encounter_changes (
  CONSTRAINT has_id EXPECT (encounter_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT has_patient EXPECT (patient_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT has_start EXPECT (start_at IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT known_class EXPECT (encounter_class IN ('EMER', 'IMP')) ON VIOLATION DROP ROW,
  CONSTRAINT ends_after_start EXPECT (end_at IS NULL OR end_at >= start_at) ON VIOLATION DROP ROW
)
COMMENT 'Every valid version of every encounter'
AS
WITH parsed AS (
  SELECT
    from_json(raw, 'id STRING, status STRING, meta STRUCT<lastUpdated: STRING>,
      class STRUCT<code: STRING>,
      subject STRUCT<reference: STRING>,
      serviceProvider STRUCT<reference: STRING>,
      partOf STRUCT<reference: STRING>,
      period STRUCT<start: STRING, `end`: STRING>,
      statusHistory ARRAY<STRUCT<status: STRING, period: STRUCT<start: STRING, `end`: STRING>>>,
      participant ARRAY<STRUCT<type: ARRAY<STRUCT<coding: ARRAY<STRUCT<code: STRING>>>>, period: STRUCT<start: STRING>>>,
      location ARRAY<STRUCT<location: STRUCT<reference: STRING>, period: STRUCT<start: STRING, `end`: STRING>>>,
      hospitalization STRUCT<dischargeDisposition: STRUCT<coding: ARRAY<STRUCT<code: STRING>>>>') AS r,
    source_file,
    ingested_at
  FROM STREAM(bronze_fhir_raw)
  WHERE get_json_object(raw, '$.resourceType') = 'Encounter'
)
SELECT
  r.id AS encounter_id,
  r.status,
  r.class.code AS encounter_class,
  nullif(regexp_extract(r.subject.reference, '^Patient/(.+)$', 1), '') AS patient_id,
  nullif(regexp_extract(r.serviceProvider.reference, '^Organization/(.+)$', 1), '') AS facility_id,
  nullif(regexp_extract(r.partOf.reference, '^Encounter/(.+)$', 1), '') AS part_of_encounter_id,
  try_to_timestamp(r.period.start) AS start_at,
  try_to_timestamp(r.period.`end`) AS end_at,
  try_to_timestamp(try_element_at(filter(r.statusHistory, h -> h.status = 'triaged'), 1).period.start) AS triage_at,
  -- earliest start among participants whose type includes ATND (attending)
  array_min(transform(
    filter(r.participant, p -> exists(p.type, t -> exists(t.coding, c -> c.code = 'ATND'))),
    p -> try_to_timestamp(p.period.start)
  )) AS provider_seen_at,
  try_element_at(r.hospitalization.dischargeDisposition.coding, 1).code AS disposition,
  transform(r.location, l -> named_struct(
    'location_id', nullif(regexp_extract(l.location.reference, '^Location/(.+)$', 1), ''),
    'start_at', try_to_timestamp(l.period.start),
    'end_at', try_to_timestamp(l.period.`end`)
  )) AS locations,
  try_to_timestamp(r.meta.lastUpdated) AS last_updated,
  source_file,
  ingested_at
FROM parsed;

CREATE OR REFRESH STREAMING TABLE silver_encounters
COMMENT 'Current version of each encounter (SCD1)';

APPLY CHANGES INTO silver_encounters
FROM STREAM(silver_encounter_changes)
KEYS (encounter_id)
SEQUENCE BY struct(last_updated, source_file)
COLUMNS * EXCEPT (source_file, ingested_at)
STORED AS SCD TYPE 1;
