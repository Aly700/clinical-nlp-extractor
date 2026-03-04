from pathlib import Path

from sqlalchemy import create_engine, text

from src.ingest import ingest_directory


def test_bad_notes_are_quarantined(tmp_path):
    db_url = f"sqlite:///{tmp_path / 'quarantine_test.sqlite'}"
    expected_bad_count = len(list(Path("data/sample_reports_bad").glob("*.txt")))
    ingest_directory(Path("data/sample_reports_bad"), db_url)

    engine = create_engine(db_url)
    with engine.connect() as conn:
        quarantine_count = conn.execute(text("select count(*) from quarantine_records")).scalar_one()
        patient_count = conn.execute(text("select count(*) from patients")).scalar_one()
        visit_count = conn.execute(text("select count(*) from visits")).scalar_one()
        med_count = conn.execute(text("select count(*) from medications")).scalar_one()
        mri_count = conn.execute(text("select count(*) from mri_results")).scalar_one()
        errors = conn.execute(text("select errors_json from quarantine_records order by id")).fetchall()

    assert quarantine_count == expected_bad_count
    assert patient_count == 0
    assert visit_count == 0
    assert med_count == 0
    assert mri_count == 0
    assert "edss" in errors[0][0].lower() or "edss" in errors[1][0].lower()
    assert "diagnosis_year" in errors[0][0].lower() or "diagnosis_year" in errors[1][0].lower()


def test_quarantine_count_increments_after_bad_ingest(tmp_path):
    db_url = f"sqlite:///{tmp_path / 'quarantine_increment.sqlite'}"
    engine = create_engine(db_url)
    expected_bad_count = len(list(Path("data/sample_reports_bad").glob("*.txt")))

    ingest_directory(Path("data/sample_reports"), db_url)
    with engine.connect() as conn:
        before_quarantine = conn.execute(text("select count(*) from quarantine_records")).scalar_one()
        before_visits = conn.execute(text("select count(*) from visits")).scalar_one()

    ingest_directory(Path("data/sample_reports_bad"), db_url)
    with engine.connect() as conn:
        after_quarantine = conn.execute(text("select count(*) from quarantine_records")).scalar_one()
        after_visits = conn.execute(text("select count(*) from visits")).scalar_one()

    assert before_quarantine == 0
    assert after_quarantine == before_quarantine + expected_bad_count
    assert before_visits == 5
    assert after_visits == before_visits


def test_reingest_bad_folder_does_not_duplicate_quarantine(tmp_path, capsys):
    db_url = f"sqlite:///{tmp_path / 'quarantine_no_duplicates.sqlite'}"
    expected_bad_count = len(list(Path("data/sample_reports_bad").glob("*.txt")))

    ingest_directory(Path("data/sample_reports_bad"), db_url)
    capsys.readouterr()
    ingest_directory(Path("data/sample_reports_bad"), db_url)
    second_ingest_output = capsys.readouterr().out

    engine = create_engine(db_url)
    with engine.connect() as conn:
        quarantine_count = conn.execute(text("select count(*) from quarantine_records")).scalar_one()

    assert quarantine_count == expected_bad_count
    assert f"already quarantined {expected_bad_count} note(s)" in second_ingest_output
