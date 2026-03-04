from __future__ import annotations

import argparse
import json

from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

from src.models import get_engine


def _summarize_reasons(errors_json: str | None) -> str:
    if not errors_json:
        return ""
    try:
        payload = json.loads(errors_json)
    except json.JSONDecodeError:
        return errors_json

    if not isinstance(payload, list):
        return str(payload)

    parts: list[str] = []
    for item in payload:
        if not isinstance(item, dict):
            parts.append(str(item))
            continue
        field = item.get("field", "unknown")
        reason = item.get("reason", "unknown")
        observed = item.get("observed")
        if observed is not None:
            parts.append(f"{field}:{reason}({observed})")
        else:
            parts.append(f"{field}:{reason}")
    return "; ".join(parts)


def list_quarantined_records(db_url: str, limit: int = 10) -> list[dict]:
    engine = get_engine(db_url)
    sql = """
        SELECT id, source_file, errors_json
        FROM quarantine_records
        ORDER BY created_at DESC, id DESC
        LIMIT :limit
    """
    try:
        with engine.connect() as conn:
            rows = conn.execute(text(sql), {"limit": int(limit)}).fetchall()
    except SQLAlchemyError as exc:
        raise ValueError(
            f"Could not query quarantine_records: {exc}. "
            "Ensure schema is initialized/migrated before using this command."
        ) from exc

    return [
        {
            "id": row[0],
            "source_file": row[1],
            "reasons": _summarize_reasons(row[2]),
        }
        for row in rows
    ]


def print_quarantined_records(db_url: str, limit: int = 10) -> int:
    rows = list_quarantined_records(db_url, limit=limit)
    headers = ["ID", "Source File", "Reasons"]

    table_rows = [
        [str(row["id"]), str(row["source_file"] or ""), str(row["reasons"] or "")]
        for row in rows
    ]

    widths = [len(h) for h in headers]
    for row in table_rows:
        for i, value in enumerate(row):
            widths[i] = max(widths[i], len(value))

    def sep() -> str:
        return "+" + "+".join("-" * (w + 2) for w in widths) + "+"

    def fmt(values: list[str]) -> str:
        return "|" + "|".join(f" {value.ljust(widths[i])} " for i, value in enumerate(values)) + "|"

    print(f"Newest {len(rows)} quarantined record(s) from {db_url}")
    print(sep())
    print(fmt(headers))
    print(sep())
    for row in table_rows:
        print(fmt(row))
    print(sep())
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="List recent quarantined records.")
    parser.add_argument("--db", required=True, help="Database URL, e.g., sqlite:///musicallite.db")
    parser.add_argument("--limit", type=int, default=10, help="Maximum rows to print.")
    return parser


def main() -> None:
    args = build_parser().parse_args()
    raise SystemExit(print_quarantined_records(db_url=args.db, limit=args.limit))


if __name__ == "__main__":
    main()

