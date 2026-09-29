-- Databricks SQL alert: trigger when any facility has 5+ patients boarded over
-- 4 hours in the most recent complete day.
SELECT facility_id, arrival_date_local, boarded_over_4h
FROM clinical_dev.clinical_secure.ed_daily
WHERE arrival_date_local = current_date() - INTERVAL 1 DAY
  AND boarded_over_4h >= 5;
