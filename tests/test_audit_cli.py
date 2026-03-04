import subprocess
import sys
from pathlib import Path

from src.audit import list_audit_logs
from src.ingest import ingest_directory


def test_ingest_one_note_creates_audit_logs(tmp_path):
    db_url = f"sqlite:///{tmp_path / 'audit_one_note.sqlite'}"
    input_dir = tmp_path / "one_note"
    input_dir.mkdir(parents=True, exist_ok=True)
    note_text = Path("data/sample_reports/note_001.txt").read_text(encoding="utf-8")
    (input_dir / "note_001.txt").write_text(note_text, encoding="utf-8")

    ingest_directory(input_dir, db_url)

    rows = list_audit_logs(db_url, limit=20)

    assert len(rows) > 0
    assert {"patients", "visits", "medications", "mri_results"}.issubset({row["table_name"] for row in rows})


def test_audit_cli_returns_rows(tmp_path):
    db_url = f"sqlite:///{tmp_path / 'audit_cli.sqlite'}"
    ingest_directory(Path("data/sample_reports"), db_url)

    result = subprocess.run(
        [sys.executable, "-m", "src.audit", "--db", db_url, "--limit", "20"],
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0
    assert "Newest" in result.stdout
    assert "ID" in result.stdout
    assert "Table" in result.stdout
    assert any(name in result.stdout for name in ["patients", "visits", "medications", "mri_results"])
