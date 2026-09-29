"""Invariants that must hold on a realistic generated dataset."""


def count(spark, sql):
    return spark.sql(sql).first()[0]


def test_every_pipeline_table_built(generated):
    expected = [
        "bronze_fhir_raw", "silver_observations", "silver_encounters", "silver_patients_history",
        "gold_fct_ed_visits", "gold_agg_ed_daily", "gold_fct_unit_census_hourly", "gold_fct_vitals_hourly",
        "gold_fct_sepsis_episodes", "gold_agg_sepsis_monthly", "gold_fct_aki_events", "gold_dq_summary",
    ]  # fmt: skip
    for t in expected:
        assert t in generated.tables


def test_current_tables_have_one_row_per_key(spark, generated):
    for table, key in [("silver_observations", "observation_id"), ("silver_encounters", "encounter_id"),
                       ("gold_fct_ed_visits", "ed_encounter_id"), ("gold_dim_visits", "visit_id")]:  # fmt: skip
        assert count(spark, f"SELECT count(*) - count(DISTINCT {key}) FROM {table}") == 0, table


def test_no_retracted_results_survive(spark, generated):
    assert count(spark, "SELECT count(*) FROM silver_observations WHERE status = 'entered-in-error'") == 0


def test_all_values_in_canonical_units(spark, generated):
    assert count(spark, "SELECT count(*) FROM silver_observations WHERE unit IN ('degF', 'mg/dL') "
                        "AND measure IN ('temperature', 'lactate')") == 0  # fmt: skip
    t = spark.sql("SELECT min(value) lo, max(value) hi FROM silver_observations WHERE measure = 'temperature'").first()
    assert 33 < t.lo and t.hi < 42.5


def test_one_current_version_per_patient(spark, generated):
    assert count(spark, "SELECT count(*) FROM (SELECT patient_id FROM silver_patients_history "
                        "WHERE __END_AT IS NULL GROUP BY patient_id HAVING count(*) <> 1)") == 0  # fmt: skip


def test_ed_intervals_are_non_negative(spark, generated):
    assert count(spark, "SELECT count(*) FROM gold_fct_ed_visits WHERE door_to_provider_min < 0 "
                        "OR ed_length_of_stay_min < 0 OR boarding_min < 0") == 0  # fmt: skip


def test_census_spine_is_complete(spark, generated):
    hours = count(spark, "SELECT count(DISTINCT hour_mark) FROM gold_fct_unit_census_hourly")
    units = count(spark, "SELECT count(*) FROM ref_units")
    assert count(spark, "SELECT count(*) FROM gold_fct_unit_census_hourly") == hours * units


def test_sepsis_episodes_found_and_scored(spark, generated):
    assert count(spark, "SELECT count(*) FROM gold_fct_sepsis_episodes") > 5
    assert count(spark, "SELECT count(*) FROM gold_fct_sepsis_episodes "
                        "WHERE bundle_compliant <> (first_failed_element IS NULL)") == 0  # fmt: skip


def test_dirty_data_was_caught(generated):
    fails = generated.expectation_failures
    assert fails[("silver_observation_changes", "has_patient")] > 0
    assert fails[("silver_observation_changes", "plausible_value")] > 0
