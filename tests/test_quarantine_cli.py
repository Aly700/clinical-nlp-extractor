import subprocess
import sys
from pathlib import Path

from src.ingest import ingest_directory
from src.quarantine import list_quarantined_records


def test_quarantine_list_returns_rows(tmp_path):
    db_url = f"sqlite:///{tmp_path / 'quarantine_cli.sqlite'}"
    expected_bad_count = len(list(Path("data/sample_reports_bad").glob("*.txt")))
    ingest_directory(Path("data/sample_reports_bad"), db_url)

    rows = list_quarantined_records(db_url, limit=10)

    assert len(rows) == expected_bad_count
    assert all(row["source_file"] for row in rows)
    assert any("edss" in row["reasons"].lower() for row in rows)
    assert any("diagnosis_year" in row["reasons"].lower() for row in rows)


def test_quarantine_cli_prints_rows(tmp_path):
    db_url = f"sqlite:///{tmp_path / 'quarantine_cli_print.sqlite'}"
    expected_bad_count = len(list(Path("data/sample_reports_bad").glob("*.txt")))
    ingest_directory(Path("data/sample_reports_bad"), db_url)

    result = subprocess.run(
        [sys.executable, "-m", "src.quarantine", "--db", db_url, "--limit", "10"],
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0
    assert f"Newest {expected_bad_count} quarantined record(s)" in result.stdout
    assert "ID" in result.stdout
    assert "Source File" in result.stdout
    assert "Reasons" in result.stdout
