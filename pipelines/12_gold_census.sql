-- Hourly census: how many patients were physically in each unit at the top of
-- each hour. Each location period is exploded into the hour marks it covers,
-- then joined onto a complete unit x hour spine so empty hours show as zero.

CREATE OR REFRESH MATERIALIZED VIEW gold_fct_unit_census_hourly
COMMENT 'Patients present per unit at the top of each hour, with occupancy against staffed beds'
AS
WITH data_as_of AS (
  SELECT max(last_updated) AS as_of FROM silver_encounters
),
periods AS (
  SELECT
    e.encounter_id,
    loc.location_id,
    loc.start_at,
    coalesce(loc.end_at, a.as_of) AS end_at
  FROM silver_encounters AS e
  CROSS JOIN data_as_of AS a
  LATERAL VIEW explode(e.locations) x AS loc
  WHERE loc.start_at IS NOT NULL
),
present AS (
  SELECT encounter_id, location_id, hour_mark
  FROM (
    SELECT
      p.*,
      explode(sequence(date_trunc('HOUR', p.start_at), date_trunc('HOUR', p.end_at), INTERVAL 1 HOUR)) AS hour_mark
    FROM periods AS p
    WHERE p.end_at >= p.start_at
  )
  WHERE hour_mark >= start_at AND hour_mark < end_at
),
spine AS (
  SELECT u.facility_id, u.unit_id, u.unit_type, u.staffed_beds, h.hour_mark
  FROM ref_units AS u
  CROSS JOIN (
    SELECT explode(sequence(date_trunc('HOUR', min(start_at)), date_trunc('HOUR', max(end_at)), INTERVAL 1 HOUR))
      AS hour_mark
    FROM periods
  ) AS h
),
counts AS (
  SELECT location_id, hour_mark, count(DISTINCT encounter_id) AS census
  FROM present
  GROUP BY location_id, hour_mark
)
SELECT
  s.facility_id,
  s.unit_id,
  s.unit_type,
  s.hour_mark,
  from_utc_timestamp(s.hour_mark, 'America/New_York') AS hour_mark_local,
  coalesce(c.census, 0) AS census,
  s.staffed_beds,
  round(100 * try_divide(coalesce(c.census, 0), s.staffed_beds), 1) AS occupancy_pct,
  coalesce(c.census, 0) > s.staffed_beds AS over_capacity
FROM spine AS s
LEFT JOIN counts AS c ON c.location_id = s.unit_id AND c.hour_mark = s.hour_mark;

CREATE OR REFRESH MATERIALIZED VIEW gold_agg_unit_occupancy_daily
COMMENT 'Daily peak and average occupancy per unit (local dates)'
AS SELECT
  facility_id,
  unit_id,
  unit_type,
  to_date(hour_mark_local) AS census_date_local,
  max(census) AS peak_census,
  round(avg(census), 1) AS avg_census,
  max(staffed_beds) AS staffed_beds,
  max(occupancy_pct) AS peak_occupancy_pct,
  count_if(over_capacity) AS hours_over_capacity
FROM gold_fct_unit_census_hourly
GROUP BY facility_id, unit_id, unit_type, to_date(hour_mark_local);
