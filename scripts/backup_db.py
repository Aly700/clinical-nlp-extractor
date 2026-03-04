#!/usr/bin/env python3
from __future__ import annotations

import argparse
import shutil
import sys
from datetime import datetime
from pathlib import Path

DEFAULT_LOG_FILE = Path("logs/backup_restore.log")


def parse_sqlite_db_path(db_url: str) -> Path:
    prefix = "sqlite:///"
    if not db_url.startswith(prefix):
        raise ValueError("Only sqlite:/// URLs are supported by this backup script.")

    raw_path = db_url[len(prefix) :]
    if raw_path == ":memory:":
        raise ValueError("sqlite:///:memory: cannot be backed up to a file.")
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


def backup_database(
    db_url: str,
    backup_dir: Path,
    keep: int = 14,
    dry_run: bool = False,
    log_file: Path = DEFAULT_LOG_FILE,
) -> Path:
    if keep < 1:
        raise ValueError("--keep must be at least 1.")

    db_path = parse_sqlite_db_path(db_url)
    if not db_path.exists() or not db_path.is_file():
        raise FileNotFoundError(f"Database file not found: {db_path}")

    backup_dir = backup_dir.resolve()
    backup_dir.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    suffix = db_path.suffix or ".db"
    backup_name = f"{db_path.stem}_{timestamp}{suffix}"
    backup_path = backup_dir / backup_name

    if dry_run:
        _log_action(log_file, "BACKUP_DRY_RUN", f"db={db_path} backup={backup_path} keep={keep}")
    else:
        shutil.copy2(db_path, backup_path)
        _log_action(log_file, "BACKUP_CREATE", f"db={db_path} backup={backup_path} keep={keep}")

    pattern = f"{db_path.stem}_*{suffix}"
    backups = sorted(backup_dir.glob(pattern), key=lambda p: p.name, reverse=True)
    if dry_run:
        for old_backup in backups[keep:]:
            _log_action(log_file, "BACKUP_PRUNE_DRY_RUN", f"would_remove={old_backup}")
    else:
        for old_backup in backups[keep:]:
            old_backup.unlink(missing_ok=True)
            _log_action(log_file, "BACKUP_PRUNE", f"removed={old_backup}")

    return backup_path


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Create a timestamped SQLite backup and keep only the latest N copies."
    )
    parser.add_argument("--db", default="sqlite:///musicallite.db", help="SQLite DB URL.")
    parser.add_argument("--backup-dir", default="backups", help="Backup output directory.")
    parser.add_argument("--keep", type=int, default=14, help="Number of most recent backups to retain.")
    parser.add_argument("--dry-run", action="store_true", help="Show actions without copying or deleting files.")
    parser.add_argument("--log-file", default=str(DEFAULT_LOG_FILE), help="Log file path.")
    return parser


def main() -> None:
    args = build_parser().parse_args()
    try:
        backup_path = backup_database(
            db_url=args.db,
            backup_dir=Path(args.backup_dir),
            keep=args.keep,
            dry_run=args.dry_run,
            log_file=Path(args.log_file),
        )
    except (ValueError, FileNotFoundError) as exc:
        _log_action(Path(args.log_file), "BACKUP_ERROR", str(exc))
        print(f"Error: {exc}", file=sys.stderr)
        raise SystemExit(1)
    if args.dry_run:
        print(f"Dry run: backup would be created at {backup_path}")
    else:
        print(f"Backup created: {backup_path}")


if __name__ == "__main__":
    main()
