#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path

from src.ingest import ingest_directory


def main() -> None:
    parser = argparse.ArgumentParser(description="Wrapper script for note ingestion.")
    parser.add_argument("--input", default="data/sample_reports", help="Directory with sample report text files")
    parser.add_argument("--db", default="sqlite:///musicallite.db", help="Database URL")
    parser.add_argument("--verbose", action="store_true", help="Print skipped note_ids for already processed notes.")
    args = parser.parse_args()
    ingest_directory(Path(args.input), args.db, verbose=args.verbose)


if __name__ == "__main__":
    main()
