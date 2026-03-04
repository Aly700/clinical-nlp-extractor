#!/usr/bin/env bash
set -e

DB="sqlite:///musicallite.db"

echo "=============================="
echo "ClinicalNLP Extractor Demo"
echo "=============================="

echo ""
echo "1) Ingesting good sample reports"
python -m src.ingest --input data/sample_reports --db $DB

echo ""
echo "2) Running validation"
python -m src.validate --db $DB

echo ""
echo "3) Showing recent audit logs"
python -m src.audit --db $DB --limit 10

echo ""
echo "4) Ingesting bad reports (quarantine demo)"
python -m src.ingest --input data/sample_reports_bad --db $DB

echo ""
echo "5) Showing quarantined records"
python -m src.quarantine --db $DB --limit 10

echo ""
echo "6) Creating database backup"
python scripts/backup_db.py --db $DB

echo ""
echo "Demo completed successfully."
