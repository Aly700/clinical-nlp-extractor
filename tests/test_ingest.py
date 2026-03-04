from pathlib import Path

from sqlalchemy import create_engine, text

from src.ingest import ingest_directory


def test_ingest_directory_creates_rows(tmp_path):
    db_path = tmp_path / "test.sqlite"
    db_url = f"sqlite:///{db_path}"
    sample_dir = Path("data/sample_reports")

    ingest_directory(sample_dir, db_url)

    engine = create_engine(db_url)
    with engine.connect() as conn:
        patient_count = conn.execute(text("select count(*) from patients")).scalar_one()
        visit_count = conn.execute(text("select count(*) from visits")).scalar_one()
        med_count = conn.execute(text("select count(*) from medications")).scalar_one()
        mri_count = conn.execute(text("select count(*) from mri_results")).scalar_one()

    assert patient_count == 5
    assert visit_count == 5
    assert med_count >= 5
    assert mri_count == 5


def test_ingest_reports_already_processed_and_verbose(tmp_path, capsys):
    db_path = tmp_path / "test_verbose.sqlite"
    db_url = f"sqlite:///{db_path}"
    sample_dir = Path("data/sample_reports")

    ingest_directory(sample_dir, db_url)
    _ = capsys.readouterr()

    ingest_directory(sample_dir, db_url, verbose=True)
    output = capsys.readouterr().out

    assert "already processed 5 note(s)" in output
    assert "Skipped note_ids (5):" in output
    assert "note_001" in output
    assert "note_005" in output
