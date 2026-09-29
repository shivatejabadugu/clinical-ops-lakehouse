-- Episodes that met sepsis criteria but were never coded: lost revenue and
-- under-reported quality data. A list for the clinical documentation team.
SELECT visit_id, mrn, facility_id, time_zero, used_icu, bundle_compliant
FROM clinical_dev.clinical_secure.sepsis_episodes
WHERE NOT sepsis_coded
ORDER BY time_zero DESC;
