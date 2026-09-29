-- Bronze: every line of every FHIR bulk-export file, untouched. Auto Loader
-- (read_files in a streaming table) tracks which files it has already ingested,
-- so each pipeline update only reads new export batches.

CREATE OR REFRESH STREAMING TABLE bronze_fhir_raw
COMMENT 'Raw FHIR NDJSON lines as exported, one row per line'
TBLPROPERTIES ('quality' = 'bronze')
AS SELECT
  value AS raw,
  _metadata.file_path AS source_file,
  regexp_extract(_metadata.file_path, 'export_(\\d{4}-\\d{2}-\\d{2})', 1) AS export_date,
  current_timestamp() AS ingested_at
FROM STREAM read_files('${landing_path}', format => 'text');

CREATE OR REFRESH STREAMING TABLE quarantine_unparseable
COMMENT 'Lines that are not valid FHIR JSON (truncated writes, garbage). Kept for replay.'
AS SELECT raw, source_file, export_date, ingested_at
FROM STREAM(bronze_fhir_raw)
WHERE get_json_object(raw, '$.resourceType') IS NULL;
