import sqlite3
from pathlib import Path

import pytest

from scripts.backup_db import backup_database
from scripts.restore_db import restore_database


def _create_sqlite_db(path: Path, value: int) -> None:
    with sqlite3.connect(path) as conn:
        conn.execute("CREATE TABLE IF NOT EXISTS marker (value INTEGER)")
        conn.execute("DELETE FROM marker")
        conn.execute("INSERT INTO marker (value) VALUES (?)", (value,))
        conn.commit()


def _read_marker(path: Path) -> int:
    with sqlite3.connect(path) as conn:
        return int(conn.execute("SELECT value FROM marker LIMIT 1").fetchone()[0])


def test_restore_refuses_overwrite_without_force(tmp_path):
    backup_file = tmp_path / "backup.sqlite"
    target_file = tmp_path / "target.sqlite"
    _create_sqlite_db(backup_file, 200)
    _create_sqlite_db(target_file, 100)

    with pytest.raises(FileExistsError):
        restore_database(
            db_url=f"sqlite:///{target_file}",
            backup=str(backup_file),
            backup_dir=tmp_path,
            force=False,
            dry_run=False,
            log_file=tmp_path / "logs/backup_restore.log",
        )

    assert _read_marker(target_file) == 100


def test_restore_dry_run_does_not_modify_target_and_logs(tmp_path):
    backup_file = tmp_path / "backup.sqlite"
    target_file = tmp_path / "target.sqlite"
    log_file = tmp_path / "logs/backup_restore.log"
    _create_sqlite_db(backup_file, 900)
    _create_sqlite_db(target_file, 111)

    restore_database(
        db_url=f"sqlite:///{target_file}",
        backup=str(backup_file),
        backup_dir=tmp_path,
        force=True,
        dry_run=True,
        log_file=log_file,
    )

    assert _read_marker(target_file) == 111
    assert log_file.exists()
    assert "RESTORE_DRY_RUN" in log_file.read_text(encoding="utf-8")


def test_restore_rejects_invalid_sqlite_header(tmp_path):
    bad_backup = tmp_path / "not_sqlite.db"
    bad_backup.write_text("this is not sqlite", encoding="utf-8")
    target_file = tmp_path / "target.sqlite"

    with pytest.raises(ValueError, match="header check failed"):
        restore_database(
            db_url=f"sqlite:///{target_file}",
            backup=str(bad_backup),
            backup_dir=tmp_path,
            force=True,
            dry_run=False,
            log_file=tmp_path / "logs/backup_restore.log",
        )


def test_backup_dry_run_logs_action(tmp_path):
    db_file = tmp_path / "musicallite.db"
    log_file = tmp_path / "logs/backup_restore.log"
    _create_sqlite_db(db_file, 1)

    backup_path = backup_database(
        db_url=f"sqlite:///{db_file}",
        backup_dir=tmp_path / "backups",
        keep=14,
        dry_run=True,
        log_file=log_file,
    )

    assert not backup_path.exists()
    assert log_file.exists()
    assert "BACKUP_DRY_RUN" in log_file.read_text(encoding="utf-8")

