#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path


def parse_sqlite_db_path(db_url: str) -> Path:
    prefix = "sqlite:///"
    if not db_url.startswith(prefix):
        raise ValueError("Only sqlite:/// URLs are supported by this migration script.")

    raw_path = db_url[len(prefix) :]
    if raw_path == ":memory:":
        raise ValueError("sqlite:///:memory: is not supported for file migration.")
    if not raw_path:
        raise ValueError("Missing sqlite database path in URL.")

    db_path = Path(raw_path).expanduser()
    if not db_path.is_absolute():
        db_path = Path.cwd() / db_path
    return db_path.resolve()


def table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    row = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
        (table_name,),
    ).fetchone()
    return row is not None


def column_exists(conn: sqlite3.Connection, table_name: str, column_name: str) -> bool:
    rows = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
    return any(row[1] == column_name for row in rows)


def index_exists(conn: sqlite3.Connection, index_name: str) -> bool:
    row = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='index' AND name=?",
        (index_name,),
    ).fetchone()
    return row is not None


def migrate_sqlite(db_url: str) -> int:
    db_path = parse_sqlite_db_path(db_url)
    if not db_path.exists():
        raise FileNotFoundError(f"Database file not found: {db_path}")

    targets = {
        "visits": {
            "edss": "FLOAT",
        },
        "patients": {
            "diagnosis_year": "INTEGER",
            "ms_subtype": "VARCHAR(16)",
        },
        "audit_logs": {
            "created_at": "DATETIME",
            "source_file": "VARCHAR(512)",
            "details_json": "TEXT",
        },
        "quarantine_records": {
            "created_at": "DATETIME",
            "source_file": "VARCHAR(512)",
            "raw_text": "TEXT",
            "extracted_json": "TEXT",
            "errors_json": "TEXT",
        },
    }

    added_columns: list[str] = []
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS audit_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                created_at DATETIME NOT NULL,
                action VARCHAR(16) NOT NULL,
                table_name VARCHAR(64) NOT NULL,
                record_id INTEGER NOT NULL,
                source_file VARCHAR(512),
                details_json TEXT
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS quarantine_records (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                created_at DATETIME NOT NULL,
                source_file VARCHAR(512) NOT NULL UNIQUE,
                raw_text TEXT NOT NULL,
                extracted_json TEXT,
                errors_json TEXT NOT NULL
            )
            """
        )
        for table_name, columns in targets.items():
            if not table_exists(conn, table_name):
                print(f"Skipping table '{table_name}' (not found)")
                continue
            for column_name, column_type in columns.items():
                if column_exists(conn, table_name, column_name):
                    print(f"{table_name}.{column_name}: already exists")
                    continue
                conn.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_type}")
                added_columns.append(f"{table_name}.{column_name}")
                print(f"{table_name}.{column_name}: added")

        # Backfill created_at from legacy timestamp field if present.
        if table_exists(conn, "audit_logs") and column_exists(conn, "audit_logs", "timestamp"):
            if column_exists(conn, "audit_logs", "created_at"):
                conn.execute(
                    """
                    UPDATE audit_logs
                    SET created_at = COALESCE(created_at, timestamp)
                    WHERE created_at IS NULL
                    """
                )

        # Ensure created_at has a value for any rows after migration.
        if table_exists(conn, "audit_logs") and column_exists(conn, "audit_logs", "created_at"):
            conn.execute(
                """
                UPDATE audit_logs
                SET created_at = COALESCE(created_at, CURRENT_TIMESTAMP)
                WHERE created_at IS NULL
                """
            )

        # Ensure quarantine source_file is unique. If legacy duplicates exist,
        # keep the earliest row per source_file so the unique index can be created.
        quarantine_index = "uq_quarantine_records_source_file"
        if table_exists(conn, "quarantine_records") and column_exists(conn, "quarantine_records", "source_file"):
            if not index_exists(conn, quarantine_index):
                before_count = conn.execute("SELECT COUNT(*) FROM quarantine_records").fetchone()[0]
                conn.execute(
                    """
                    DELETE FROM quarantine_records
                    WHERE id NOT IN (
                        SELECT MIN(id)
                        FROM quarantine_records
                        GROUP BY source_file
                    )
                    """
                )
                after_count = conn.execute("SELECT COUNT(*) FROM quarantine_records").fetchone()[0]
                removed = before_count - after_count
                if removed > 0:
                    print(f"quarantine_records: removed {removed} duplicate row(s) by source_file")
                conn.execute(
                    f"CREATE UNIQUE INDEX IF NOT EXISTS {quarantine_index} ON quarantine_records(source_file)"
                )
                print("quarantine_records.source_file: unique index ensured")
            else:
                print("quarantine_records.source_file: unique index already exists")
        conn.commit()

    print(f"Migration complete. Added {len(added_columns)} column(s).")
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Lightweight SQLite migration to add missing clinical and audit/quarantine columns."
    )
    parser.add_argument("--db", default="sqlite:///musicallite.db", help="SQLite DB URL.")
    return parser


def main() -> None:
    args = build_parser().parse_args()
    try:
        raise SystemExit(migrate_sqlite(args.db))
    except (ValueError, FileNotFoundError, sqlite3.DatabaseError) as exc:
        print(f"Error: {exc}", file=sys.stderr)
        raise SystemExit(1)


if __name__ == "__main__":
    main()
