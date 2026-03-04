import sqlite3
from pathlib import Path

from src.ingest import ingest_directory
from src.models import Medication, Patient, Visit, init_db
from src.validate import print_report, run_validation


def _results_by_name(db_url: str) -> dict[str, int]:
    return {result.name: result.count for result in run_validation(db_url)}


def test_validate_passes_for_clean_ingested_data(tmp_path):
    db_url = f"sqlite:///{tmp_path / 'validate_pass.sqlite'}"
    ingest_directory(Path("data/sample_reports"), db_url)

    results = _results_by_name(db_url)

    assert results["EDSS in [0, 10]"] == 0
    assert results["diagnosis_year <= current year"] == 0
    assert results["Visit FK integrity (patient_id must exist)"] == 0
    assert results["Medication names normalized (trim + consistent casing)"] == 0
    assert print_report(db_url) == 0


def test_validate_fails_for_non_normalized_medication_name(tmp_path):
    db_url = f"sqlite:///{tmp_path / 'validate_med_fail.sqlite'}"
    engine = init_db(db_url)

    from sqlalchemy.orm import sessionmaker

    SessionLocal = sessionmaker(bind=engine)
    with SessionLocal() as session:
        patient = Patient(mrn="VAL-MED-1", full_name="Synthetic Case")
        session.add(patient)
        session.flush()

        visit = Visit(patient_id=patient.id, note_id="val-med-visit-1", raw_text="synthetic")
        session.add(visit)
        session.flush()

        session.add(Medication(visit_id=visit.id, name="  oCRELIZUMAB  ", dose="600 mg", frequency="q6mo"))
        session.commit()

    results = _results_by_name(db_url)
    assert results["Medication names normalized (trim + consistent casing)"] == 1
    assert print_report(db_url) == 1


def test_validate_fails_for_orphan_visit_when_fk_is_bypassed(tmp_path):
    db_path = tmp_path / "validate_fk_fail.sqlite"
    db_url = f"sqlite:///{db_path}"
    init_db(db_url)

    conn = sqlite3.connect(db_path)
    try:
        conn.execute("PRAGMA foreign_keys=OFF")
        conn.execute(
            "INSERT INTO visits (patient_id, note_id, raw_text) VALUES (?, ?, ?)",
            (99999, "orphan-visit-1", "synthetic orphan visit"),
        )
        conn.commit()
    finally:
        conn.close()

    results = _results_by_name(db_url)
    assert results["Visit FK integrity (patient_id must exist)"] == 1
    assert print_report(db_url) == 1
