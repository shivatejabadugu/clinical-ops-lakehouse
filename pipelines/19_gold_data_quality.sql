-- Record counts at each hop, so a drop in any layer is visible in one table.
-- Expectation pass/fail rates also land in the pipeline event log (see
-- monitoring/expectations.sql); this view adds the reasons.

CREATE OR REFRESH MATERIALIZED VIEW gold_dq_summary
COMMENT 'Row counts per layer and quarantine reasons'
AS
SELECT 'bronze' AS layer, 'all lines' AS metric, count(*) AS records FROM bronze_fhir_raw
UNION ALL
SELECT 'bronze', 'unparseable lines', count(*) FROM quarantine_unparseable
UNION ALL
SELECT 'silver', concat('observation versions quarantined: ', reason), count(*)
FROM silver_observation_quarantine GROUP BY reason
UNION ALL
SELECT 'silver', 'observation versions accepted', count(*) FROM silver_observation_changes
UNION ALL
SELECT 'silver', 'observations current', count(*) FROM silver_observations
UNION ALL
SELECT 'silver', 'encounters current', count(*) FROM silver_encounters
UNION ALL
SELECT 'silver', 'patient versions', count(*) FROM silver_patients_history;
