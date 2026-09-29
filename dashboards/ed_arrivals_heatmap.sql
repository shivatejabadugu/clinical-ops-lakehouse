-- Arrivals by weekday and hour: staffing heatmap.
SELECT
  date_format(arrival_date_local, 'E') AS weekday,
  dayofweek(arrival_date_local) AS weekday_num,
  arrival_hour_local,
  round(count(*) / count(DISTINCT arrival_date_local), 1) AS avg_arrivals,
  percentile_approx(door_to_provider_min, 0.5) AS median_door_to_provider_min
FROM clinical_dev.clinical_secure.ed_visits
GROUP BY ALL
ORDER BY weekday_num, arrival_hour_local;
