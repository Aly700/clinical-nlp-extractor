#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import shutil
import sys
from datetime import datetime
from pathlib import Path

DEFAULT_LOG_FILE = Path("logs/backup_restore.log")
SQLITE_HEADER = b"SQLite format 3\x00"


def parse_sqlite_db_path(db_url: str) -> Path:
    prefix = "sqlite:///"
    if not db_url.startswith(prefix):
        raise ValueError("Only sqlite:/// URLs are supported by this restore script.")

    raw_path = db_url[len(prefix) :]
    if raw_path == ":memory:":
        raise ValueError("sqlite:///:memory: cannot be restored to a file.")
    if not raw_path:
        raise ValueError("Missing sqlite database path in URL.")

    db_path = Path(raw_path).expanduser()
    if not db_path.is_absolute():
        db_path = Path.cwd() / db_path
    return db_path.resolve()


def _log_action(log_file: Path, action: str, message: str) -> None:
    log_file = log_file.resolve()
    log_file.parent.mkdir(parents=True, exist_ok=True)
    with log_file.open("a", encoding="utf-8") as f:
        f.write(f"{datetime.now().isoformat()} | {action} | {message}\n")


def has_valid_sqlite_header(file_path: Path) -> bool:
    try:
        with file_path.open("rb") as f:
            header = f.read(len(SQLITE_HEADER))
    except OSError:
        return False
    return header == SQLITE_HEADER


def resolve_backup_path(backup: str, backup_dir: Path) -> Path:
    candidate = Path(backup).expanduser()
    if candidate.exists():
        return candidate.resolve()
    candidate_in_backup_dir = (backup_dir / backup).expanduser()
    if candidate_in_backup_dir.exists():
        return candidate_in_backup_dir.resolve()
    raise FileNotFoundError(f"Backup file not found: {backup}")


def restore_database(
    db_url: str,
    backup: str,
    backup_dir: Path,
    force: bool = False,
    dry_run: bool = False,
    log_file: Path = DEFAULT_LOG_FILE,
) -> Path:
    db_path = parse_sqlite_db_path(db_url)
    backup_dir = backup_dir.resolve()
    backup_path = resolve_backup_path(backup, backup_dir)

    if not backup_path.is_file():
        raise FileNotFoundError(f"Backup path is not a file: {backup_path}")
    if backup_path.resolve() == db_path.resolve():
        raise ValueError("Backup file and target database path are identical.")
    if not has_valid_sqlite_header(backup_path):
        raise ValueError(f"Backup is not a valid SQLite file (header check failed): {backup_path}")

    db_path.parent.mkdir(parents=True, exist_ok=True)

    if db_path.exists():
        if not force:
            raise FileExistsError(
                f"Target DB exists: {db_path}. Re-run with --force to restore and overwrite safely."
            )
        if dry_run:
            _log_action(
                log_file,
                "RESTORE_PRE_SNAPSHOT_DRY_RUN",
                f"would_snapshot_existing_db={db_path} into={backup_dir}",
            )
        else:
            backup_dir.mkdir(parents=True, exist_ok=True)
            pre_restore_name = f"{db_path.stem}_pre_restore_{datetime.now().strftime('%Y%m%d_%H%M%S')}{db_path.suffix or '.db'}"
            pre_restore_path = backup_dir / pre_restore_name
            shutil.copy2(db_path, pre_restore_path)
            _log_action(log_file, "RESTORE_PRE_SNAPSHOT", f"snapshot={pre_restore_path} target={db_path}")
            print(f"Pre-restore snapshot created: {pre_restore_path}")

    if dry_run:
        _log_action(
            log_file,
            "RESTORE_DRY_RUN",
            f"db={db_path} backup={backup_path} force={force}",
        )
        return db_path

    temp_restore_path = db_path.with_name(f"{db_path.name}.restore_tmp")
    shutil.copy2(backup_path, temp_restore_path)
    os.replace(temp_restore_path, db_path)
    _log_action(log_file, "RESTORE_APPLY", f"db={db_path} backup={backup_path} force={force}")

    return db_path


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Restore a SQLite DB from a backup file.")
    parser.add_argument("--db", default="sqlite:///musicallite.db", help="SQLite DB URL.")
    parser.add_argument(
        "--backup",
        required=True,
        help="Backup filename or full path. If only a filename is provided, it is resolved under --backup-dir.",
    )
    parser.add_argument("--backup-dir", default="backups", help="Directory used to resolve backup filenames.")
    parser.add_argument(
        "--force",
        action="store_true",
        help="Allow restore when target DB already exists. Existing DB is snapshotted before overwrite.",
    )
    parser.add_argument("--dry-run", action="store_true", help="Show actions without modifying files.")
    parser.add_argument("--log-file", default=str(DEFAULT_LOG_FILE), help="Log file path.")
    return parser


def main() -> None:
    args = build_parser().parse_args()
    try:
        restored_path = restore_database(
            db_url=args.db,
            backup=args.backup,
            backup_dir=Path(args.backup_dir),
            force=args.force,
            dry_run=args.dry_run,
            log_file=Path(args.log_file),
        )
    except (ValueError, FileNotFoundError, FileExistsError) as exc:
        _log_action(Path(args.log_file), "RESTORE_ERROR", str(exc))
        print(f"Error: {exc}", file=sys.stderr)
        raise SystemExit(1)
    if args.dry_run:
        print(f"Dry run: database would be restored to {restored_path}")
    else:
        print(f"Database restored to: {restored_path}")


if __name__ == "__main__":
    main()
