"""Hand-built patients with known answers, run through the full pipeline."""


def row(spark, sql):
    rows = spark.sql(sql).collect()
    assert len(rows) == 1, f"expected 1 row, got {len(rows)}: {sql}"
    return rows[0]


def test_compliant_sepsis_episode(spark, scenarios):
    r = row(spark, "SELECT * FROM gold_fct_sepsis_episodes WHERE visit_id = 'e1'")
    assert str(r.time_zero) == "2026-06-01 10:05:00"  # floored to arrival, not the 10:00 hour
    assert r.minutes_to_antibiotic == 70
    assert r.lactate_measured and r.cultures_before_antibiotic and r.antibiotic_within_3h
    assert r.repeat_lactate_if_needed and r.bundle_compliant
    assert r.first_failed_element is None


def test_bundle_failures_are_attributed(spark, scenarios):
    r = row(spark, "SELECT * FROM gold_fct_sepsis_episodes WHERE visit_id = 'e2'")
    assert not r.cultures_before_antibiotic
    assert not r.repeat_lactate_if_needed  # lactate 4.0, never repeated
    assert not r.bundle_compliant
    assert r.first_failed_element == "antibiotic_before_culture"


def test_rule_out_without_organ_dysfunction_is_not_an_episode(spark, scenarios):
    assert spark.sql("SELECT * FROM gold_fct_sepsis_episodes WHERE visit_id = 'e3'").count() == 0
    # but the SIRS screen itself did fire
    assert spark.sql("SELECT max(sirs_count) m FROM gold_fct_vitals_hourly WHERE visit_id = 'e3'").first().m >= 2


def test_ed_throughput_and_boarding(spark, scenarios):
    r = row(spark, "SELECT * FROM gold_fct_ed_visits WHERE ed_encounter_id = 'e4'")
    assert r.ed_outcome == "admitted"
    assert r.door_to_provider_min == 40
    assert r.boarding_min == 180  # decision 09:00, left the ED 12:00
    assert r.ed_length_of_stay_min == 360


def test_left_without_being_seen(spark, scenarios):
    r = row(spark, "SELECT * FROM gold_fct_ed_visits WHERE ed_encounter_id = 'e5'")
    assert r.ed_outcome == "left_without_being_seen"
    assert r.door_to_triage_min == 10 and r.door_to_provider_min is None


def test_visit_spans_ed_and_inpatient(spark, scenarios):
    v = row(spark, "SELECT * FROM gold_dim_visits WHERE visit_id = 'e4'")
    assert v.was_admitted and v.used_icu and v.inpatient_encounter_id == "i4"
    assert str(v.visit_end) == "2026-06-03 12:00:00"


def test_aki_detected_on_7_day_baseline(spark, scenarios):
    r = row(spark, "SELECT * FROM gold_fct_aki_events WHERE visit_id = 'e4'")
    # 1.00 -> 1.20 (+0.2 in 24h: not yet AKI) -> 2.10 (2.1x baseline)
    assert str(r.aki_detected_at) == "2026-06-03 07:00:00"
    assert r.baseline_creatinine == 1.0 and r.kdigo_stage == 2


def test_icu_census_counts_the_patient_on_the_hour(spark, scenarios):
    census = spark.sql(
        """SELECT hour_mark, census FROM gold_fct_unit_census_hourly
           WHERE unit_id = 'ATL-MAIN-ICU' AND census > 0 ORDER BY hour_mark"""
    ).collect()
    # in ICU from 12:00 on day 1 until 12:00 on day 3 -> 48 hour marks
    assert len(census) == 48 and all(c.census == 1 for c in census)
    assert str(census[0].hour_mark) == "2026-06-01 12:00:00"


def test_out_of_order_amendment_wins(spark, scenarios):
    # the original (value 199) arrived in a later batch but has an older lastUpdated
    assert row(spark, "SELECT value FROM silver_observations WHERE observation_id = 'o6a'").value == 99


def test_entered_in_error_is_removed(spark, scenarios):
    assert spark.sql("SELECT * FROM silver_observations WHERE observation_id = 'o6e'").count() == 0


def test_legacy_fahrenheit_and_offset_are_normalised(spark, scenarios):
    r = row(spark, "SELECT value, unit, effective_at FROM silver_observations WHERE observation_id = 'o6t'")
    assert r.value == 38.0 and r.unit == "Cel"
    assert str(r.effective_at) == "2026-06-02 08:12:00"  # 04:12 EDT = 08:12 UTC


def test_redelivered_duplicate_collapses(spark, scenarios):
    assert spark.sql("SELECT * FROM silver_observation_changes WHERE observation_id = 'o6t'").count() == 2
    assert spark.sql("SELECT * FROM silver_observations WHERE observation_id = 'o6t'").count() == 1


def test_bad_records_are_quarantined_not_lost(spark, scenarios):
    assert scenarios.expectation_failures[("silver_observation_changes", "plausible_value")] == 1
    q = row(spark, "SELECT reason FROM silver_observation_quarantine WHERE observation_id = 'o6x'")
    assert q.reason == "implausible_value"
    assert spark.sql("SELECT * FROM quarantine_unparseable").count() == 1


def test_patient_scd2(spark, scenarios):
    versions = spark.sql(
        "SELECT postal_code, __START_AT, __END_AT FROM silver_patients_history "
        "WHERE patient_id = 'p1' ORDER BY __START_AT"
    ).collect()
    assert [v.postal_code for v in versions] == ["30303", "30318"]
    assert versions[0]["__END_AT"] == versions[1]["__START_AT"] and versions[1]["__END_AT"] is None
    assert row(spark, "SELECT postal_code FROM gold_dim_patient WHERE patient_id = 'p1'").postal_code == "30318"
