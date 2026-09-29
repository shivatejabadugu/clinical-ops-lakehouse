"""Run the whole pipeline on local Spark and print the headline gold tables.

python -m generator.generate_fhir && python -m local.run_local
"""

import sys

from local.dlt_local import LocalPipeline, local_spark


def main() -> None:
    landing = sys.argv[1] if len(sys.argv) > 1 else "data/landing/fhir"
    spark = local_spark()
    result = LocalPipeline(spark, landing).run()
    print(f"\nbuilt {len(result.tables)} tables")
    for (table, rule), failed in sorted(result.expectation_failures.items()):
        if failed:
            print(f"  expectation {table}.{rule}: {failed} rows failed")

    for title, sql in [
        ("Data quality", "SELECT * FROM gold_dq_summary"),
        ("ED performance (averages across days)",
         "SELECT facility_id, round(avg(arrivals)) arrivals_per_day, round(avg(median_door_to_provider_min)) "
         "door_to_provider_min, round(avg(lwbs_pct), 1) lwbs_pct, round(avg(median_boarding_hours), 1) "
         "boarding_hours, sum(boarded_over_4h) boarded_over_4h FROM gold_agg_ed_daily GROUP BY facility_id"),
        ("Sepsis bundle compliance", "SELECT * FROM gold_agg_sepsis_monthly ORDER BY facility_id, month_local"),
        ("Unit occupancy (peak / average)",
         "SELECT facility_id, unit_type, max(peak_census) peak, round(avg(avg_census), 1) avg, max(staffed_beds) beds, "
         "sum(hours_over_capacity) hours_over FROM gold_agg_unit_occupancy_daily GROUP BY ALL ORDER BY 1, 2"),
        ("AKI by stage", "SELECT kdigo_stage, count(*) visits FROM gold_fct_aki_events GROUP BY 1 ORDER BY 1"),
    ]:  # fmt: skip
        print(f"\n== {title}")
        spark.sql(sql).show(50, truncate=False)


if __name__ == "__main__":
    main()
