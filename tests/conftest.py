import json
import os
import sys
import time
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
os.environ["TZ"] = "UTC"
time.tzset()

from generator.generate_fhir import FhirGenerator  # noqa: E402
from local.dlt_local import LocalPipeline, local_spark  # noqa: E402


@pytest.fixture(scope="session")
def spark():
    s = local_spark()
    yield s
    s.stop()


@pytest.fixture(scope="session")
def generated(spark, tmp_path_factory):
    """Five days of generated data run through the whole pipeline."""
    landing = tmp_path_factory.mktemp("generated")
    g = FhirGenerator(days=5, seed=3)
    g.run()
    g.write(landing)
    result = LocalPipeline(spark, str(landing)).run()
    return result


class Landing:
    """Builds a tiny FHIR export by hand, one batch folder per day."""

    def __init__(self, root: Path):
        self.root = root

    def add(self, batch: str, resource: dict, last_updated: str) -> None:
        resource = {**resource, "meta": {"lastUpdated": last_updated}}
        folder = self.root / f"export_{batch}"
        folder.mkdir(parents=True, exist_ok=True)
        path = folder / f"{resource['resourceType']}.ndjson"
        with path.open("a") as f:
            f.write(json.dumps(resource) + "\n")

    def add_raw(self, batch: str, rtype: str, line: str) -> None:
        folder = self.root / f"export_{batch}"
        folder.mkdir(parents=True, exist_ok=True)
        with (folder / f"{rtype}.ndjson").open("a") as f:
            f.write(line + "\n")


def patient(pid, zip_code="30303"):
    return {
        "resourceType": "Patient",
        "id": pid,
        "identifier": [{"system": "urn:example:mrn", "value": f"MRN-{pid}"}],
        "name": [{"family": "Test", "given": ["Pat"]}],
        "gender": "female",
        "birthDate": "1960-05-05",
        "address": [{"postalCode": zip_code, "state": "GA"}],
    }


def ed_encounter(eid, pid, arrive, depart=None, triage=None, provider=None, facility="ATL-MAIN", status="finished"):
    enc = {
        "resourceType": "Encounter",
        "id": eid,
        "status": status,
        "class": {"code": "EMER"},
        "subject": {"reference": f"Patient/{pid}"},
        "serviceProvider": {"reference": f"Organization/{facility}"},
        "period": {"start": arrive, **({"end": depart} if depart else {})},
        "location": [
            {"location": {"reference": f"Location/{facility}-ED"},
             "period": {"start": arrive, **({"end": depart} if depart else {})}}
        ],
    }  # fmt: skip
    if triage:
        enc["statusHistory"] = [{"status": "triaged", "period": {"start": triage}}]
    if provider:
        enc["participant"] = [{"type": [{"coding": [{"code": "ATND"}]}], "period": {"start": provider}}]
    return enc


def ip_encounter(eid, pid, ed_id, decision, unit_start, discharge, unit="ICU", facility="ATL-MAIN"):
    return {
        "resourceType": "Encounter",
        "id": eid,
        "status": "finished",
        "class": {"code": "IMP"},
        "subject": {"reference": f"Patient/{pid}"},
        "serviceProvider": {"reference": f"Organization/{facility}"},
        "partOf": {"reference": f"Encounter/{ed_id}"},
        "period": {"start": decision, "end": discharge},
        "location": [{"location": {"reference": f"Location/{facility}-{unit}"},
                      "period": {"start": unit_start, "end": discharge}}],
        "hospitalization": {"dischargeDisposition": {"coding": [{"code": "home"}]}},
    }  # fmt: skip


LOINC = {"hr": "8867-4", "rr": "9279-1", "sbp": "8480-6", "temp": "8310-5", "gcs": "9269-2",
         "lactate": "2524-7", "creatinine": "2160-0", "wbc": "6690-2", "culture": "600-7"}  # fmt: skip
UNITS = {"hr": "/min", "rr": "/min", "sbp": "mm[Hg]", "temp": "Cel", "gcs": "{score}",
         "lactate": "mmol/L", "creatinine": "mg/dL", "wbc": "10*3/uL"}  # fmt: skip


def observation(oid, pid, eid, kind, when, value=None, unit=None, status="final"):
    obs = {
        "resourceType": "Observation",
        "id": oid,
        "status": status,
        "code": {"coding": [{"system": "http://loinc.org", "code": LOINC[kind]}]},
        "subject": {"reference": f"Patient/{pid}"},
        "encounter": {"reference": f"Encounter/{eid}"},
        "effectiveDateTime": when,
    }
    if value is not None:
        obs["valueQuantity"] = {"value": value, "unit": unit or UNITS[kind]}
    return obs


def antibiotic(mid, pid, eid, when):
    return {
        "resourceType": "MedicationAdministration",
        "id": mid,
        "status": "completed",
        "medicationCodeableConcept": {"text": "Ceftriaxone"},
        "category": {"coding": [{"code": "antibiotic"}]},
        "subject": {"reference": f"Patient/{pid}"},
        "context": {"reference": f"Encounter/{eid}"},
        "effectiveDateTime": when,
    }


@pytest.fixture(scope="session")
def scenarios(spark, tmp_path_factory):
    """Hand-built patients with known correct answers, run through the pipeline once."""
    L = Landing(tmp_path_factory.mktemp("scenarios"))
    D1, D2, D3 = "2026-06-01", "2026-06-02", "2026-06-03"

    # --- S1: septic, bundle met. Arrives 10:05, antibiotics 11:15 (70 min).
    L.add(D1, patient("p1"), "2026-06-01T10:00:00Z")
    L.add(
        D1,
        ed_encounter(
            "e1",
            "p1",
            "2026-06-01T10:05:00Z",
            "2026-06-01T12:00:00Z",
            triage="2026-06-01T10:10:00Z",
            provider="2026-06-01T10:20:00Z",
        ),
        "2026-06-01T12:00:00Z",
    )
    for i, (k, v) in enumerate([("hr", 124), ("rr", 25), ("temp", 38.9), ("sbp", 86), ("gcs", 15)]):
        L.add(D1, observation(f"o1v{i}", "p1", "e1", k, "2026-06-01T10:12:00Z", v), "2026-06-01T10:15:00Z")
    L.add(D1, observation("o1c", "p1", "e1", "culture", "2026-06-01T10:30:00Z"), "2026-06-01T10:31:00Z")
    L.add(D1, observation("o1l1", "p1", "e1", "lactate", "2026-06-01T10:40:00Z", 3.1), "2026-06-01T10:50:00Z")
    L.add(D1, antibiotic("m1", "p1", "e1", "2026-06-01T11:15:00Z"), "2026-06-01T11:16:00Z")
    L.add(D1, observation("o1l2", "p1", "e1", "lactate", "2026-06-01T13:30:00Z", 1.8), "2026-06-01T13:40:00Z")

    # --- S2: septic, antibiotic given BEFORE cultures, and no repeat lactate.
    L.add(D1, patient("p2"), "2026-06-01T09:00:00Z")
    L.add(
        D1,
        ed_encounter(
            "e2",
            "p2",
            "2026-06-01T09:00:00Z",
            "2026-06-01T11:00:00Z",
            triage="2026-06-01T09:05:00Z",
            provider="2026-06-01T09:15:00Z",
        ),
        "2026-06-01T11:00:00Z",
    )
    for i, (k, v) in enumerate([("hr", 118), ("rr", 23), ("temp", 39.1), ("sbp", 95)]):
        L.add(D1, observation(f"o2v{i}", "p2", "e2", k, "2026-06-01T09:10:00Z", v), "2026-06-01T09:12:00Z")
    L.add(D1, antibiotic("m2", "p2", "e2", "2026-06-01T09:40:00Z"), "2026-06-01T09:41:00Z")
    L.add(D1, observation("o2c", "p2", "e2", "culture", "2026-06-01T09:50:00Z"), "2026-06-01T09:51:00Z")
    L.add(D1, observation("o2l", "p2", "e2", "lactate", "2026-06-01T10:00:00Z", 4.0), "2026-06-01T10:05:00Z")

    # --- S3: rule-out. SIRS + culture but no organ dysfunction: not an episode.
    L.add(D1, patient("p3"), "2026-06-01T08:00:00Z")
    L.add(
        D1,
        ed_encounter("e3", "p3", "2026-06-01T08:00:00Z", "2026-06-01T10:00:00Z", provider="2026-06-01T08:30:00Z"),
        "2026-06-01T10:00:00Z",
    )
    for i, (k, v) in enumerate([("hr", 105), ("rr", 22), ("temp", 38.4), ("sbp", 125)]):
        L.add(D1, observation(f"o3v{i}", "p3", "e3", k, "2026-06-01T08:10:00Z", v), "2026-06-01T08:12:00Z")
    L.add(D1, observation("o3c", "p3", "e3", "culture", "2026-06-01T08:40:00Z"), "2026-06-01T08:41:00Z")

    # --- S4: admitted, boarded 3h in the ED, then 2 days in ICU with rising creatinine (AKI).
    L.add(D1, patient("p4"), "2026-06-01T06:00:00Z")
    L.add(
        D1,
        ed_encounter("e4", "p4", "2026-06-01T06:00:00Z", "2026-06-01T12:00:00Z", provider="2026-06-01T06:40:00Z"),
        "2026-06-01T12:00:00Z",
    )
    L.add(
        D3,
        ip_encounter("i4", "p4", "e4", "2026-06-01T09:00:00Z", "2026-06-01T12:00:00Z", "2026-06-03T12:00:00Z"),
        "2026-06-03T12:00:00Z",
    )
    L.add(D1, observation("o4k1", "p4", "i4", "creatinine", "2026-06-01T13:00:00Z", 1.00), "2026-06-01T14:00:00Z")
    L.add(D2, observation("o4k2", "p4", "i4", "creatinine", "2026-06-02T13:00:00Z", 1.20), "2026-06-02T14:00:00Z")
    L.add(D3, observation("o4k3", "p4", "i4", "creatinine", "2026-06-03T07:00:00Z", 2.10), "2026-06-03T08:00:00Z")

    # --- S5: left without being seen.
    L.add(D2, patient("p5"), "2026-06-02T14:00:00Z")
    L.add(
        D2,
        ed_encounter("e5", "p5", "2026-06-02T14:00:00Z", "2026-06-02T16:30:00Z", triage="2026-06-02T14:10:00Z"),
        "2026-06-02T16:30:00Z",
    )

    # --- S6: versioning. The amended value arrives in an EARLIER batch than the
    # original (out of order); a retracted result; a redelivered duplicate; a
    # degF temperature from the legacy interface with a local offset.
    L.add(D2, patient("p6"), "2026-06-02T08:00:00Z")
    L.add(D2, ed_encounter("e6", "p6", "2026-06-02T08:00:00Z", "2026-06-02T10:00:00Z", provider="2026-06-02T08:20:00Z",
                           facility="ATL-EAST"), "2026-06-02T10:00:00Z")  # fmt: skip
    L.add(
        D2, observation("o6a", "p6", "e6", "hr", "2026-06-02T08:10:00Z", 99, status="amended"), "2026-06-02T09:00:00Z"
    )
    L.add(D3, observation("o6a", "p6", "e6", "hr", "2026-06-02T08:10:00Z", 199), "2026-06-02T08:15:00Z")
    L.add(D2, observation("o6e", "p6", "e6", "rr", "2026-06-02T08:10:00Z", 18), "2026-06-02T08:15:00Z")
    L.add(
        D3,
        observation("o6e", "p6", "e6", "rr", "2026-06-02T08:10:00Z", 18, status="entered-in-error"),
        "2026-06-03T09:00:00Z",
    )
    L.add(
        D2,
        observation("o6t", "p6", "e6", "temp", "2026-06-02T04:12:00-04:00", 100.4, unit="degF"),
        "2026-06-02T08:15:00Z",
    )
    L.add(
        D3,
        observation("o6t", "p6", "e6", "temp", "2026-06-02T04:12:00-04:00", 100.4, unit="degF"),
        "2026-06-02T08:15:00Z",
    )  # redelivery: same lastUpdated, later batch
    L.add(D2, observation("o6x", "p6", "e6", "hr", "2026-06-02T08:40:00Z", 900), "2026-06-02T08:45:00Z")
    L.add_raw(D2, "Observation", '{"resourceType":"Observation","id":"o6trunc","sta')

    # --- S7: patient moves: two SCD2 versions.
    L.add(D3, patient("p1", zip_code="30318"), "2026-06-03T10:00:00Z")

    result = LocalPipeline(spark, str(L.root)).run()
    return result
