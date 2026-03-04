from datetime import datetime
import json
from pathlib import Path

import pytest
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import sessionmaker

from src.ingest import ingest_directory
from src.models import AuditLog, Patient, init_db


def test_foreign_key_constraint_on_visit_patient(tmp_path):
    db_url = f"sqlite:///{tmp_path / 'fk_test.sqlite'}"
    engine = init_db(db_url)

    with pytest.raises(IntegrityError):
        with engine.begin() as conn:
            conn.execute(
                text(
                    """
                    INSERT INTO visits (patient_id, note_id, raw_text)
                    VALUES (:patient_id, :note_id, :raw_text)
                    """
                ),
                {"patient_id": 9999, "note_id": "orphan-visit", "raw_text": "synthetic note"},
            )


def test_check_constraints_for_edss_and_diagnosis_year(tmp_path):
    db_url = f"sqlite:///{tmp_path / 'check_test.sqlite'}"
    engine = init_db(db_url)

    with engine.begin() as conn:
        conn.execute(
            text(
                """
                INSERT INTO patients (mrn, full_name)
                VALUES (:mrn, :full_name)
                """
            ),
            {"mrn": "SYN-CHECK-1", "full_name": "Synthetic Person"},
        )
        patient_id = conn.execute(
            text("SELECT id FROM patients WHERE mrn = :mrn"),
            {"mrn": "SYN-CHECK-1"},
        ).scalar_one()

    with pytest.raises(IntegrityError):
        with engine.begin() as conn:
            conn.execute(
                text(
                    """
                    INSERT INTO visits (patient_id, note_id, raw_text, edss)
                    VALUES (:patient_id, :note_id, :raw_text, :edss)
                    """
                ),
                {"patient_id": patient_id, "note_id": "bad-edss", "raw_text": "synthetic", "edss": 10.5},
            )

    with pytest.raises(IntegrityError):
        with engine.begin() as conn:
            conn.execute(
                text(
                    """
                    INSERT INTO patients (mrn, full_name, diagnosis_year)
                    VALUES (:mrn, :full_name, :diagnosis_year)
                    """
                ),
                {
                    "mrn": "SYN-CHECK-2",
                    "full_name": "Future Year Case",
                    "diagnosis_year": datetime.now().year + 1,
                },
            )


def test_audit_log_records_inserts_during_ingest(tmp_path):
    db_url = f"sqlite:///{tmp_path / 'audit_test.sqlite'}"
    engine = init_db(db_url)
    ingest_directory(Path("data/sample_reports"), db_url)

    SessionLocal = sessionmaker(bind=engine)
    with SessionLocal() as session:
        logs = session.query(AuditLog).order_by(AuditLog.id.asc()).all()

    assert len(logs) > 0
    assert all(log.action == "INSERT" for log in logs)
    tracked_tables = {"patients", "visits", "medications", "mri_results"}
    assert tracked_tables.issubset({log.table_name for log in logs})
    assert all(log.created_at is not None for log in logs)
    assert all(
        log.source_file is not None and log.source_file.strip().endswith(".txt")
        for log in logs
        if log.table_name in tracked_tables
    )
    assert all("note_id" in json.loads(log.details_json or "{}") for log in logs)
