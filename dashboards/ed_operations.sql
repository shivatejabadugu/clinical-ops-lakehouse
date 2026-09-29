-- ED operations board: last 14 days by facility.
SELECT
  facility_id,
  arrival_date_local,
  arrivals,
  median_door_to_provider_min,
  p90_door_to_provider_min,
  lwbs_pct,
  median_boarding_hours,
  boarded_over_4h
FROM clinical_dev.clinical_secure.ed_daily
WHERE arrival_date_local >= current_date() - INTERVAL 14 DAYS
ORDER BY facility_id, arrival_date_local;
