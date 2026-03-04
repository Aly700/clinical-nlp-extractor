from __future__ import annotations

import argparse
import importlib
import sqlite3
import sys
from pathlib import Path


def _format_version() -> str:
    return ".".join(str(part) for part in sys.version_info[:3])


def _import_version(module_name: str) -> tuple[bool, str]:
    try:
        module = importlib.import_module(module_name)
    except ImportError:
        return False, f"not installed (install with: pip install {module_name})"
    version = getattr(module, "__version__", "unknown")
    return True, str(version)


def _sqlite_path_from_url(db_url: str) -> Path | None:
    prefix = "sqlite:///"
    if not db_url.startswith(prefix):
        return None
    raw_path = db_url[len(prefix) :]
    if raw_path == ":memory:":
        return None
    if not raw_path:
        return None
    db_path = Path(raw_path).expanduser()
    if not db_path.is_absolute():
        db_path = Path.cwd() / db_path
    return db_path.resolve()


def _check_db_connectivity(db_url: str) -> tuple[bool, str]:
    prefix = "sqlite:///"
    if not db_url.startswith(prefix):
        return False, "unsupported DB URL (expected sqlite:///...)"

    sqlite_path = _sqlite_path_from_url(db_url)
    try:
        if sqlite_path is None:
            conn = sqlite3.connect(":memory:")
        else:
            sqlite_path.parent.mkdir(parents=True, exist_ok=True)
            conn = sqlite3.connect(str(sqlite_path))
        with conn:
            conn.execute("SELECT 1")
        conn.close()
    except sqlite3.Error as exc:
        return False, str(exc)
    return True, "OK"


def run_doctor(db_url: str) -> int:
    py_version = _format_version()
    py_path = sys.executable

    sqlalchemy_ok, sqlalchemy_info = _import_version("sqlalchemy")
    spacy_ok, spacy_info = _import_version("spacy")
    db_ok, db_info = _check_db_connectivity(db_url)

    print("Environment Check")
    print("-----------------")
    print(f"Python Version: {py_version}")
    print(f"Python Path: {py_path}")
    print("")
    print(f"SQLAlchemy: {'OK (' + sqlalchemy_info + ')' if sqlalchemy_ok else f'FAIL ({sqlalchemy_info})'}")
    print(f"spaCy: {'OK (' + spacy_info + ')' if spacy_ok else f'FAIL ({spacy_info})'}")
    print("")
    print(f"Database Connection: {'OK' if db_ok else f'FAIL ({db_info})'}")

    return 0 if sqlalchemy_ok and spacy_ok and db_ok else 1


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run environment checks for ClinicalNLP Extractor.")
    parser.add_argument("--db", default="sqlite:///musicallite.db", help="Database URL to test connectivity.")
    return parser


def main() -> None:
    args = build_parser().parse_args()
    raise SystemExit(run_doctor(args.db))


if __name__ == "__main__":
    main()
