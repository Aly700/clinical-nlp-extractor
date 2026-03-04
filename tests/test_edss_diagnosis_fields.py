import pytest
from sqlalchemy.orm import sessionmaker

from src.models import Patient, Visit, init_db


def test_edss_2_5_pass_and_edss_12_rejected(tmp_path):
    db_url = f"sqlite:///{tmp_path / 'edss_field_test.sqlite'}"
    engine = init_db(db_url)
    SessionLocal = sessionmaker(bind=engine)

    with SessionLocal() as session:
        patient = Patient(mrn="EDSS-001", full_name="Synthetic EDSS")
        session.add(patient)
        session.flush()

        good_visit = Visit(patient_id=patient.id, note_id="edss-good", raw_text="synthetic", edss=2.5)
        session.add(good_visit)
        session.commit()

    with SessionLocal() as session:
        patient = session.query(Patient).filter(Patient.mrn == "EDSS-001").one()
        with pytest.raises(ValueError):
            session.add(Visit(patient_id=patient.id, note_id="edss-bad", raw_text="synthetic", edss=12))


def test_diagnosis_year_2018_pass_and_2035_rejected(tmp_path):
    db_url = f"sqlite:///{tmp_path / 'diag_year_field_test.sqlite'}"
    engine = init_db(db_url)
    SessionLocal = sessionmaker(bind=engine)

    with SessionLocal() as session:
        good_patient = Patient(mrn="YEAR-2018", full_name="Synthetic Year", diagnosis_year=2018)
        session.add(good_patient)
        session.commit()

    with SessionLocal() as session:
        with pytest.raises(ValueError):
            session.add(Patient(mrn="YEAR-2035", full_name="Synthetic Future", diagnosis_year=2035))

