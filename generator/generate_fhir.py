"""Synthetic FHIR R4 data for two hospitals, written as daily bulk-export batches
of NDJSON (one resource per line), the format FHIR $export produces:

  data/landing/fhir/export_2026-06-01/Patient.ndjson
                                     /Encounter.ndjson
                                     /Observation.ndjson
                                     /MedicationAdministration.ndjson
                                     /Condition.ndjson

Everything is fictional. The feed behaves like a real EHR interface:
  * encounters are re-sent as they progress (in-progress, then finished), so
    the latest version per id has to win
  * observations get amended or marked entered-in-error in a later batch
  * one hospital's legacy interface sends temperatures in degF, lactate in
    mg/dL, and timestamps with a local UTC offset instead of Z
  * duplicates, impossible values, missing patient references, and the
    occasional truncated line

Seeded and deterministic.   python -m generator.generate_fhir --days 45
"""

from __future__ import annotations

import argparse
import json
import random
from collections import defaultdict
from datetime import UTC, datetime, timedelta, timezone
from pathlib import Path

START = datetime(2026, 6, 1, tzinfo=UTC)
FACILITIES = {"ATL-MAIN": 45, "ATL-EAST": 30}  # ED arrivals per day
LEGACY_FACILITY = "ATL-EAST"  # sends degF, mg/dL lactate, -04:00 offsets
EDT = timezone(timedelta(hours=-4))

LOINC = {
    "hr": ("8867-4", "Heart rate", "/min"),
    "rr": ("9279-1", "Respiratory rate", "/min"),
    "sbp": ("8480-6", "Systolic blood pressure", "mm[Hg]"),
    "temp": ("8310-5", "Body temperature", "Cel"),
    "spo2": ("59408-5", "Oxygen saturation", "%"),
    "gcs": ("9269-2", "Glasgow coma score total", "{score}"),
    "lactate": ("2524-7", "Lactate", "mmol/L"),
    "creatinine": ("2160-0", "Creatinine", "mg/dL"),
    "wbc": ("6690-2", "Leukocytes", "10*3/uL"),
    "blood_culture": ("600-7", "Bacteria identified in Blood by Culture", None),
}
ANTIBIOTICS = ["ceftriaxone", "piperacillin-tazobactam", "vancomycin", "cefepime"]
ZIPS = ["30017", "30039", "30052", "30078", "30303", "30309", "30318", "30324"]


def iso(ts: datetime, legacy: bool = False) -> str:
    ts = ts.replace(microsecond=0)
    if legacy:
        return ts.astimezone(EDT).isoformat()
    return ts.astimezone(UTC).isoformat().replace("+00:00", "Z")


class FhirGenerator:
    def __init__(self, days: int, seed: int = 11, n_patients: int = 4000):
        self.rng = random.Random(seed)
        self.days = days
        self.end = START + timedelta(days=days)
        self.batches: dict[str, dict[str, list[str]]] = defaultdict(lambda: defaultdict(list))
        self.seq = defaultdict(int)
        self.patients = [self._patient(i) for i in range(1, n_patients + 1)]
        self.practitioners = [f"prac-{i:03d}" for i in range(1, 41)]

    # ------------------------------------------------------------------ helpers
    def _id(self, kind: str) -> str:
        self.seq[kind] += 1
        return f"{kind}-{self.seq[kind]:07d}"

    def emit(self, resource: dict, when: datetime | None, batch_at: datetime | None = None) -> None:
        """Place a resource in an export batch. `when` sets meta.lastUpdated; a
        redelivery passes when=None to keep the original lastUpdated and lands in
        the batch for `batch_at`."""
        if when is not None:
            when = min(when, self.end - timedelta(seconds=1))
            resource.setdefault("meta", {})["lastUpdated"] = iso(when)
        at = min(batch_at or when, self.end - timedelta(seconds=1))
        day = at.astimezone(UTC).date().isoformat()
        self.batches[day][resource["resourceType"]].append(json.dumps(resource, separators=(",", ":")))

    def _patient(self, i: int) -> dict:
        age = max(18, min(95, int(self.rng.gauss(55, 19))))
        return {
            "id": f"pat-{i:06d}",
            "mrn": f"MRN{7000000 + i}",
            "gender": self.rng.choice(["male", "female"]),
            "birthDate": f"{2026 - age}-{self.rng.randint(1, 12):02d}-{self.rng.randint(1, 28):02d}",
            "zip": self.rng.choice(ZIPS),
            "family": self.rng.choice(["Smith", "Patel", "Nguyen", "Garcia", "Okafor", "Kim", "Brown", "Reddy"]),
            "given": self.rng.choice(["Alex", "Sam", "Jordan", "Priya", "Maria", "Kwame", "Mei", "David"]),
            "emitted": False,
        }

    def patient_resource(self, p: dict) -> dict:
        return {
            "resourceType": "Patient",
            "id": p["id"],
            "identifier": [{"system": "urn:example:mrn", "value": p["mrn"]}],
            "name": [{"family": p["family"], "given": [p["given"]]}],
            "gender": p["gender"],
            "birthDate": p["birthDate"],
            "address": [{"postalCode": p["zip"], "state": "GA"}],
        }

    # ------------------------------------------------------------------ observations
    def obs(self, pat, enc_id, facility, kind, value, when, status="final"):
        code, display, unit = LOINC[kind]
        legacy = facility == LEGACY_FACILITY
        if kind == "temp" and legacy:
            value, unit = round(value * 9 / 5 + 32, 1), "degF"
        if kind == "lactate" and legacy:
            value, unit = round(value * 9.01, 1), "mg/dL"
        res = {
            "resourceType": "Observation",
            "id": self._id("obs"),
            "status": status,
            "category": [{"coding": [{"code": "laboratory" if kind in ("lactate", "creatinine", "wbc",
                                                                         "blood_culture") else "vital-signs"}]}],
            "code": {"coding": [{"system": "http://loinc.org", "code": code, "display": display}]},
            "subject": {"reference": f"Patient/{pat['id']}"},
            "encounter": {"reference": f"Encounter/{enc_id}"},
            "effectiveDateTime": iso(when, legacy),
        }  # fmt: skip
        if value is not None:
            res["valueQuantity"] = {"value": round(value, 2), "unit": unit}
        self._emit_obs_with_dirt(res, when)

    def _emit_obs_with_dirt(self, res: dict, when: datetime) -> None:
        r = self.rng.random()
        received = when + timedelta(minutes=self.rng.randint(1, 30))
        if r < 0.003 and "valueQuantity" in res:
            res["valueQuantity"]["value"] = 900.0  # impossible value
        elif r < 0.006:
            res.pop("subject")
        self.emit(res, received)
        r = self.rng.random()
        if r < 0.01:  # duplicate delivery in the next day's batch
            # a true redelivery keeps its original lastUpdated
            self.emit(json.loads(json.dumps(res)), None, batch_at=received + timedelta(days=1))
        elif r < 0.02 and "valueQuantity" in res:  # amended later with a corrected value
            fixed = json.loads(json.dumps(res))
            fixed["status"] = "amended"
            fixed["valueQuantity"]["value"] = round(fixed["valueQuantity"]["value"] * self.rng.uniform(0.97, 1.03), 1)
            self.emit(fixed, received + timedelta(hours=self.rng.randint(2, 30)))
        elif r < 0.025:  # retracted
            bad = json.loads(json.dumps(res))
            bad["status"] = "entered-in-error"
            self.emit(bad, received + timedelta(hours=self.rng.randint(1, 12)))

    def vitals(self, pat, enc_id, facility, when, septic=False, severity=0.0):
        g = self.rng.gauss
        hr = g(118, 10) if septic else g(82, 12)
        rr = g(24, 3) if septic else g(16, 2.5)
        sbp = g(96 - 12 * severity, 10) if septic else g(128, 15)
        temp = g(38.7, 0.4) if septic else g(36.9, 0.35)
        gcs = 14 if septic and self.rng.random() < 0.4 else 15
        for kind, v in (("hr", hr), ("rr", rr), ("sbp", sbp), ("temp", temp), ("spo2", min(100, g(96, 2))),
                        ("gcs", gcs)):  # fmt: skip
            self.obs(pat, enc_id, facility, kind, v, when + timedelta(seconds=self.rng.randint(0, 120)))

    # ------------------------------------------------------------------ encounters
    def run(self) -> None:
        for day in range(self.days):
            day_start = START + timedelta(days=day)
            for facility, per_day in FACILITIES.items():
                for _ in range(self._poisson(per_day)):
                    # arrivals peak late morning to evening
                    hour = min(23.9, max(0, self.rng.gauss(14, 5)))
                    self.ed_visit(facility, day_start + timedelta(hours=hour))

    def _poisson(self, lam: float) -> int:
        return max(0, int(round(self.rng.gauss(lam, lam**0.5))))

    def _touch_patient(self, pat: dict, when: datetime) -> None:
        if not pat["emitted"]:
            pat["emitted"] = True
            self.emit(self.patient_resource(pat), when - timedelta(minutes=5))
        elif self.rng.random() < 0.08:  # moved since last visit: new Patient version
            pat["zip"] = self.rng.choice([z for z in ZIPS if z != pat["zip"]])
            self.emit(self.patient_resource(pat), when - timedelta(minutes=5))

    def ed_visit(self, facility: str, arrive: datetime) -> None:
        pat = self.rng.choice(self.patients)
        self._touch_patient(pat, arrive)
        enc_id = self._id("enc")
        triage = arrive + timedelta(minutes=self.rng.randint(2, 20))
        septic = self.rng.random() < 0.045
        lwbs = not septic and self.rng.random() < 0.035
        admitted = not lwbs and (septic or self.rng.random() < 0.22)
        provider = None if lwbs else triage + timedelta(minutes=int(self.rng.expovariate(1 / 35)) + 5)

        if lwbs:
            depart = triage + timedelta(minutes=self.rng.randint(60, 200))
        elif admitted:
            decision = provider + timedelta(minutes=self.rng.randint(60, 240))
            boarding_scale = 5 if facility == "ATL-MAIN" else 2.5  # MAIN is chronically short of beds
            depart = decision + timedelta(hours=self.rng.expovariate(1 / boarding_scale))
        else:
            depart = provider + timedelta(minutes=self.rng.randint(45, 240))

        ed = {
            "resourceType": "Encounter",
            "id": enc_id,
            "status": "in-progress",
            "class": {"system": "http://terminology.hl7.org/CodeSystem/v3-ActCode", "code": "EMER"},
            "subject": {"reference": f"Patient/{pat['id']}"},
            "serviceProvider": {"reference": f"Organization/{facility}"},
            "period": {"start": iso(arrive)},
            "statusHistory": [{"status": "arrived", "period": {"start": iso(arrive), "end": iso(triage)}}],
            "location": [{"location": {"reference": f"Location/{facility}-ED"}, "period": {"start": iso(arrive)}}],
        }
        self.emit(json.loads(json.dumps(ed)), triage)  # first version: patient is here
        ed["status"] = "finished"
        ed["period"]["end"] = iso(depart)
        ed["statusHistory"].append({"status": "triaged", "period": {"start": iso(triage), "end": iso(depart)}})
        ed["location"][0]["period"]["end"] = iso(depart)
        if provider:
            ed["participant"] = [
                {
                    "type": [{"coding": [{"code": "ATND"}]}],
                    "period": {"start": iso(provider)},
                    "individual": {"reference": f"Practitioner/{self.rng.choice(self.practitioners)}"},
                }
            ]
        disposition = "lwbs" if lwbs else ("admit" if admitted else "home")
        ed["hospitalization"] = {"dischargeDisposition": {"coding": [{"code": disposition}]}}
        self.emit(ed, depart)

        # ED vitals and, for septic patients, the sepsis workup
        t = arrive + timedelta(minutes=self.rng.randint(3, 15))
        while t < depart:
            self.vitals(pat, enc_id, facility, t, septic=septic, severity=0.5)
            t += timedelta(minutes=self.rng.randint(30, 90))
        if septic:
            # screened at triage; workup starts shortly after
            self.sepsis_workup(pat, enc_id, facility, triage + timedelta(minutes=self.rng.randint(5, 40)))
        elif self.rng.random() < 0.06:  # ruled-out infection workup
            self.obs(pat, enc_id, facility, "blood_culture", None, triage + timedelta(minutes=40))
            self.obs(pat, enc_id, facility, "wbc", self.rng.gauss(8, 2), triage + timedelta(minutes=45))
        if admitted:
            self.inpatient_stay(pat, facility, enc_id, decision, depart, septic)

    def sepsis_workup(self, pat, enc_id, facility, recognized: datetime) -> None:
        """Bundle timing varies so compliance is realistic: most elements on time,
        some late or missing."""
        self.obs(pat, enc_id, facility, "wbc", self.rng.gauss(16, 3), recognized + timedelta(minutes=20))
        culture_at = recognized + timedelta(minutes=self.rng.randint(10, 70))
        if self.rng.random() < 0.93:
            self.obs(pat, enc_id, facility, "blood_culture", None, culture_at)
        first_lactate = self.rng.uniform(2.3, 5.5)
        lactate_at = recognized + timedelta(minutes=self.rng.randint(10, 90) if self.rng.random() < 0.9 else 260)
        self.obs(pat, enc_id, facility, "lactate", first_lactate, lactate_at)
        if self.rng.random() < 0.7:
            self.obs(
                pat,
                enc_id,
                facility,
                "lactate",
                first_lactate * 0.6,
                lactate_at + timedelta(hours=self.rng.uniform(2, 5)),
            )
        abx_delay = self.rng.choice([20, 35, 50, 60, 75, 90, 110, 130, 170, 230])  # minutes after recognition
        abx_at = recognized + timedelta(minutes=abx_delay + self.rng.randint(0, 15))
        if self.rng.random() < 0.08:
            abx_at = culture_at - timedelta(minutes=20)  # antibiotics before cultures: a bundle miss
        self.emit(
            {
                "resourceType": "MedicationAdministration",
                "id": self._id("medadm"),
                "status": "completed",
                "medicationCodeableConcept": {"text": self.rng.choice(ANTIBIOTICS)},
                "category": {"coding": [{"code": "antibiotic"}]},
                "subject": {"reference": f"Patient/{pat['id']}"},
                "context": {"reference": f"Encounter/{enc_id}"},
                "effectiveDateTime": iso(abx_at),
            },
            abx_at + timedelta(minutes=5),
        )

    def inpatient_stay(self, pat, facility, ed_id, decision, ed_depart, septic) -> None:
        enc_id = self._id("enc")
        icu = septic and self.rng.random() < 0.55 or self.rng.random() < 0.08
        first_unit = "ICU" if icu else self.rng.choice(["MEDSURG", "MEDSURG", "TELE"])
        los_hours = self.rng.uniform(30, 160) + (48 if septic else 0)
        discharge = ed_depart + timedelta(hours=los_hours)
        units = [(first_unit, ed_depart, discharge)]
        if icu and los_hours > 60:
            step_down = ed_depart + timedelta(hours=self.rng.uniform(24, los_hours - 24))
            units = [("ICU", ed_depart, step_down), ("MEDSURG", step_down, discharge)]
        in_progress = discharge >= self.end
        enc = {
            "resourceType": "Encounter",
            "id": enc_id,
            "status": "in-progress" if in_progress else "finished",
            "class": {"system": "http://terminology.hl7.org/CodeSystem/v3-ActCode", "code": "IMP"},
            "subject": {"reference": f"Patient/{pat['id']}"},
            "serviceProvider": {"reference": f"Organization/{facility}"},
            "partOf": {"reference": f"Encounter/{ed_id}"},
            "period": {"start": iso(decision)},
            "location": [],
        }
        for unit, s, e in units:
            period = {"start": iso(s)}
            if e < self.end:
                period["end"] = iso(e)
            enc["location"].append({"location": {"reference": f"Location/{facility}-{unit}"}, "period": period})
        if not in_progress:
            enc["period"]["end"] = iso(discharge)
            dispo = self.rng.choices(["home", "snf", "exp"], [0.8, 0.17, 0.03 + 0.05 * septic])[0]
            enc["hospitalization"] = {"dischargeDisposition": {"coding": [{"code": dispo}]}}
        self.emit(enc, min(discharge, self.end - timedelta(seconds=1)))

        if septic and self.rng.random() < 0.8:  # 20% never coded: a documentation gap to find
            self.emit(
                {
                    "resourceType": "Condition",
                    "id": self._id("cond"),
                    "code": {"coding": [{"system": "http://hl7.org/fhir/sid/icd-10-cm", "code": "A41.9"}]},
                    "subject": {"reference": f"Patient/{pat['id']}"},
                    "encounter": {"reference": f"Encounter/{enc_id}"},
                    "recordedDate": iso(discharge),
                },
                min(discharge, self.end - timedelta(seconds=1)),
            )

        # q4h vitals, daily creatinine; 12% develop acute kidney injury
        aki = self.rng.random() < (0.3 if septic else 0.1)
        base_cr = self.rng.uniform(0.7, 1.2)
        t = ed_depart + timedelta(hours=1)
        stop = min(discharge, self.end)
        day_n = 0
        while t < stop:
            self.vitals(pat, enc_id, facility, t, septic=septic and t < ed_depart + timedelta(hours=24))
            if (t - ed_depart).total_seconds() // 3600 % 24 < 4:
                cr = base_cr + (0.25 * day_n if aki and day_n >= 1 else self.rng.gauss(0, 0.05))
                self.obs(pat, enc_id, facility, "creatinine", cr, t + timedelta(minutes=30))
                day_n += 1
            t += timedelta(hours=4)

    # ------------------------------------------------------------------ write
    def write(self, out: Path) -> dict:
        counts = defaultdict(int)
        for day, by_type in sorted(self.batches.items()):
            folder = out / f"export_{day}"
            folder.mkdir(parents=True, exist_ok=True)
            for rtype, lines in by_type.items():
                if rtype == "Observation" and self.rng.random() < 0.3:
                    lines = lines + [lines[-1][: len(lines[-1]) // 2]]  # truncated line from a failed write
                (folder / f"{rtype}.ndjson").write_text("\n".join(lines) + "\n")
                counts[rtype] += len(lines)
        return dict(counts)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", type=int, default=45)
    ap.add_argument("--seed", type=int, default=11)
    ap.add_argument("--out", default="data/landing/fhir")
    args = ap.parse_args()
    g = FhirGenerator(args.days, args.seed)
    g.run()
    counts = g.write(Path(args.out))
    print(f"wrote {sum(counts.values())} resources {counts} -> {args.out}")


if __name__ == "__main__":
    main()
