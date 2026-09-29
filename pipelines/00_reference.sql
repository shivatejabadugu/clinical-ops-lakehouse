-- Reference data, defined inline so the pipeline has no external dependencies.

CREATE OR REFRESH MATERIALIZED VIEW ref_loinc_measures
COMMENT 'LOINC codes used downstream, with canonical unit and plausible value range'
AS SELECT * FROM VALUES
  ('8867-4',  'heart_rate',    '/min',    20.0, 250.0),
  ('9279-1',  'resp_rate',     '/min',     4.0,  60.0),
  ('8480-6',  'systolic_bp',   'mm[Hg]',  40.0, 260.0),
  ('8310-5',  'temperature',   'Cel',     30.0,  43.0),
  ('59408-5', 'spo2',          '%',       50.0, 100.0),
  ('9269-2',  'gcs',           '{score}',  3.0,  15.0),
  ('2524-7',  'lactate',       'mmol/L',   0.2,  30.0),
  ('2160-0',  'creatinine',    'mg/dL',    0.1,  20.0),
  ('6690-2',  'wbc',           '10*3/uL',  0.1, 200.0),
  ('600-7',   'blood_culture', NULL,      NULL,  NULL)
AS t(loinc_code, measure, canonical_unit, min_plausible, max_plausible);

CREATE OR REFRESH MATERIALIZED VIEW ref_units
COMMENT 'Hospital units and staffed bed counts, for occupancy'
AS SELECT * FROM VALUES
  ('ATL-MAIN', 'ATL-MAIN-ED',      'ED',      40),
  ('ATL-MAIN', 'ATL-MAIN-ICU',     'ICU',     16),
  ('ATL-MAIN', 'ATL-MAIN-TELE',    'TELE',    24),
  ('ATL-MAIN', 'ATL-MAIN-MEDSURG', 'MEDSURG', 48),
  ('ATL-EAST', 'ATL-EAST-ED',      'ED',      25),
  ('ATL-EAST', 'ATL-EAST-ICU',     'ICU',     10),
  ('ATL-EAST', 'ATL-EAST-TELE',    'TELE',    16),
  ('ATL-EAST', 'ATL-EAST-MEDSURG', 'MEDSURG', 32)
AS t(facility_id, unit_id, unit_type, staffed_beds);
