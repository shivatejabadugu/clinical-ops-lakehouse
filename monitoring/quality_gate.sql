-- Fails the job (raise_error) if a quality threshold is breached after a
-- refresh, so a bad load pages someone instead of reaching dashboards.

USE CATALOG IDENTIFIER(:catalog);
USE SCHEMA clinical;

-- more than 2% of observation versions quarantined
SELECT
  CASE WHEN q.n > 0.02 * (q.n + a.n)
    THEN raise_error(concat('Observation quarantine rate ', round(100 * q.n / (q.n + a.n), 2), '% exceeds 2%'))
  END
FROM (SELECT count(*) AS n FROM silver_observation_quarantine) AS q
CROSS JOIN (SELECT count(*) AS n FROM silver_observation_changes) AS a;

-- no new data in 6 hours
SELECT
  CASE WHEN max(ingested_at) < current_timestamp() - INTERVAL 6 HOURS
    THEN raise_error('No FHIR data ingested in the last 6 hours')
  END
FROM bronze_fhir_raw;

-- every patient must have exactly one current version
SELECT
  CASE WHEN count(*) > 0 THEN raise_error(concat(count(*), ' patients without exactly one current version')) END
FROM (
  SELECT patient_id FROM silver_patients_history
  GROUP BY patient_id HAVING count_if(__END_AT IS NULL) <> 1
);
