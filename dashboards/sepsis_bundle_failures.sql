-- Where does the sepsis bundle break? First failed element by facility.
SELECT
  facility_id,
  coalesce(first_failed_element, 'compliant') AS outcome,
  count(*) AS episodes,
  round(100 * count(*) / sum(count(*)) OVER (PARTITION BY facility_id), 1) AS pct_of_facility
FROM clinical_dev.clinical_secure.sepsis_episodes
GROUP BY ALL
ORDER BY facility_id, episodes DESC;
