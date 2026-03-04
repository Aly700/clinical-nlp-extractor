from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import datetime

from sqlalchemy import text

from src.models import get_engine


@dataclass
class CheckResult:
    name: str
    count: int

    @property
    def passed(self) -> bool:
        return self.count == 0


def _single_count(conn, sql: str, params: dict | None = None) -> int:
    return int(conn.execute(text(sql), params or {}).scalar() or 0)


def _table_exists(conn, table_name: str) -> bool:
    result = conn.execute(
        text("SELECT name FROM sqlite_master WHERE type='table' AND name=:name"),
        {"name": table_name},
    ).fetchone()
    return result is not None


def _column_exists(conn, table_name: str, column_name: str) -> bool:
    if not _table_exists(conn, table_name):
        return False
    rows = conn.execute(text(f"PRAGMA table_info({table_name})")).fetchall()
    return any(row[1] == column_name for row in rows)


def normalize_medication_name(name: str) -> str:
    collapsed = " ".join(name.strip().split())
    return collapsed.title()


def _count_non_normalized_medications(conn) -> int:
    rows = conn.execute(text("SELECT name FROM medications WHERE name IS NOT NULL")).fetchall()
    count = 0
    for (name,) in rows:
        if name != normalize_medication_name(name):
            count += 1
    return count


def run_validation(db_url: str) -> list[CheckResult]:
    engine = get_engine(db_url)
    current_year = datetime.now().year

    checks: list[CheckResult] = []
    with engine.connect() as conn:
        has_visits = _table_exists(conn, "visits")
        has_patients = _table_exists(conn, "patients")
        has_medications = _table_exists(conn, "medications")
        has_patient_id = _column_exists(conn, "visits", "patient_id")
        has_edss = _column_exists(conn, "visits", "edss")
        has_diagnosis_year = _column_exists(conn, "patients", "diagnosis_year")
        has_med_name = _column_exists(conn, "medications", "name")

        checks.append(
            CheckResult(
                "Visit FK integrity (patient_id must exist)",
                (
                    _single_count(
                        conn,
                        """
                        SELECT COUNT(*)
                        FROM visits v
                        LEFT JOIN patients p ON p.id = v.patient_id
                        WHERE p.id IS NULL
                        """,
                    )
                    if has_visits and has_patients and has_patient_id
                    else 1
                ),
            )
        )
        checks.append(
            CheckResult(
                "EDSS in [0, 10]",
                (
                    _single_count(
                        conn,
                        "SELECT COUNT(*) FROM visits WHERE edss IS NOT NULL AND (edss < 0 OR edss > 10)",
                    )
                    if has_edss
                    else 1
                ),
            )
        )
        checks.append(
            CheckResult(
                "diagnosis_year <= current year",
                (
                    _single_count(
                        conn,
                        "SELECT COUNT(*) FROM patients WHERE diagnosis_year IS NOT NULL AND diagnosis_year > :year",
                        {"year": current_year},
                    )
                    if has_diagnosis_year
                    else 1
                ),
            )
        )
        checks.append(
            CheckResult(
                "Medication names normalized (trim + consistent casing)",
                _count_non_normalized_medications(conn) if has_medications and has_med_name else 1,
            )
        )

    return checks


def print_report(db_url: str) -> int:
    checks = run_validation(db_url)
    failures = [c for c in checks if not c.passed]
    passes = len(checks) - len(failures)

    print(f"Validation report for: {db_url}")
    for check in checks:
        status = "PASS" if check.passed else "FAIL"
        print(f"[{status}] {check.name}: {check.count}")

    print(f"Summary: pass={passes} fail={len(failures)}")
    if failures:
        print("Result: FAIL")
        return 1
    print("Result: PASS")
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Validate clinical NLP database consistency checks.")
    parser.add_argument("--db", required=True, help="Database URL, e.g., sqlite:///musicallite.db")
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    raise SystemExit(print_report(args.db))


if __name__ == "__main__":
    main()
