from __future__ import annotations

import argparse

from sqlalchemy import text

from src.models import get_engine


def list_audit_logs(db_url: str, limit: int = 20) -> list[dict]:
    engine = get_engine(db_url)
    sql = """
        SELECT id, created_at, action, table_name, record_id, source_file, details_json
        FROM audit_logs
        ORDER BY created_at DESC, id DESC
        LIMIT :limit
    """
    with engine.connect() as conn:
        rows = conn.execute(text(sql), {"limit": int(limit)}).fetchall()
    return [
        {
            "id": row[0],
            "created_at": row[1],
            "action": row[2],
            "table_name": row[3],
            "record_id": row[4],
            "source_file": row[5],
            "details_json": row[6],
        }
        for row in rows
    ]


def print_audit_logs(db_url: str, limit: int = 20) -> int:
    rows = list_audit_logs(db_url, limit=limit)
    headers = ["ID", "Created At (UTC)", "Action", "Table", "Record ID", "Source File"]
    table_rows = [
        [
            str(row["id"]),
            str(row["created_at"]),
            str(row["action"]),
            str(row["table_name"]),
            str(row["record_id"]),
            str(row["source_file"] or ""),
        ]
        for row in rows
    ]

    widths = [len(h) for h in headers]
    for row in table_rows:
        for idx, value in enumerate(row):
            widths[idx] = max(widths[idx], len(value))

    def sep() -> str:
        return "+" + "+".join("-" * (w + 2) for w in widths) + "+"

    def fmt(values: list[str]) -> str:
        cells = [f" {value.ljust(widths[idx])} " for idx, value in enumerate(values)]
        return "|" + "|".join(cells) + "|"

    print(f"Newest {len(rows)} audit log(s) from {db_url}")
    print(sep())
    print(fmt(headers))
    print(sep())
    for row in table_rows:
        print(fmt(row))
    print(sep())
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="List recent audit logs.")
    parser.add_argument("--db", required=True, help="Database URL, e.g., sqlite:///musicallite.db")
    parser.add_argument("--limit", type=int, default=20, help="Maximum rows to print.")
    return parser


def main() -> None:
    args = build_parser().parse_args()
    raise SystemExit(print_audit_logs(db_url=args.db, limit=args.limit))


if __name__ == "__main__":
    main()
