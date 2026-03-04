from __future__ import annotations

import argparse
import json
import re
from dataclasses import asdict
from datetime import datetime
from pathlib import Path

from src.extractor import ClinicalExtractor
from src.models import AuditLog, MRIResult, Medication, Patient, QuarantineRecord, Visit, init_db

EDSS_PATTERNS = [
    r"\bEDSS\b(?:\s*score)?\s*(?:of|is|=|:)?\s*([0-9]{1,2}(?:\.[0-9]+)?)\b",
    r"\bExpanded Disability Status Scale(?:\s*\(EDSS\))?(?:\s*score)?\s*(?:of|is|=|:)?\s*([0-9]{1,2}(?:\.[0-9]+)?)\b",
]
DIAGNOSIS_YEAR_PATTERNS = [
    r"\bdiagnosis\s*year\s*[:=]?\s*((?:19|20)\d{2})\b",
    r"\bdiagnos(?:ed|is|e)\b[^.\n;]{0,40}\b((?:19|20)\d{2})\b",
    r"\b(?:dx|diagnosis)\b[^.\n;]{0,30}\b((?:19|20)\d{2})\b",
    r"\bonset\b[^.\n;]{0,25}\b((?:19|20)\d{2})\b",
    r"\bsince\s+((?:19|20)\d{2})\b",
]


def _find_edss_values(text: str) -> list[float]:
    values: list[float] = []
    seen: set[float] = set()
    for pattern in EDSS_PATTERNS:
        for match in re.finditer(pattern, text, flags=re.IGNORECASE):
            value = float(match.group(1))
            if value in seen:
                continue
            seen.add(value)
            values.append(value)
    return values


def _find_diagnosis_year_values(text: str) -> list[int]:
    values: list[int] = []
    seen: set[int] = set()
    for pattern in DIAGNOSIS_YEAR_PATTERNS:
        for match in re.finditer(pattern, text, flags=re.IGNORECASE):
            value = int(match.group(1))
            if value in seen:
                continue
            seen.add(value)
            values.append(value)
    return values


def _collect_quarantine_errors(text: str) -> list[dict]:
    current_year = datetime.now().year
    errors: list[dict] = []

    out_of_range_edss = sorted(v for v in _find_edss_values(text) if v < 0.0 or v > 10.0)
    if out_of_range_edss:
        errors.append(
            {
                "field": "edss",
                "reason": "out_of_range",
                "expected": "[0, 10]",
                "observed": out_of_range_edss,
            }
        )

    future_years = sorted(v for v in _find_diagnosis_year_values(text) if v > current_year)
    if future_years:
        errors.append(
            {
                "field": "diagnosis_year",
                "reason": "future_year",
                "expected_max": current_year,
                "observed": future_years,
            }
        )

    return errors


def ingest_directory(input_dir: Path, db_url: str, verbose: bool = False) -> None:
    if not input_dir.exists() or not input_dir.is_dir():
        raise FileNotFoundError(f"Input directory not found: {input_dir}")

    engine = init_db(db_url)
    extractor = ClinicalExtractor()

    note_files = sorted([p for p in input_dir.glob("*.txt") if p.is_file()])
    if not note_files:
        print(f"No .txt files found in {input_dir}")
        return

    from sqlalchemy.orm import sessionmaker

    SessionLocal = sessionmaker(bind=engine)
    session = SessionLocal()
    ingested = 0
    quarantined = 0
    already_quarantined = 0
    already_processed = 0
    skipped_note_ids: list[str] = []

    def log_insert(table_name: str, record_id: int, source_file: Path, note_id: str, details: dict) -> None:
        payload = {"note_id": note_id}
        payload.update(details)
        session.add(
            AuditLog(
                action="INSERT",
                table_name=table_name,
                record_id=record_id,
                source_file=str(source_file),
                details_json=json.dumps(payload, sort_keys=True),
            )
        )

    try:
        for file_path in note_files:
            source_file = str(file_path)
            raw_text = file_path.read_text(encoding="utf-8")
            extracted = extractor.extract_from_text(file_path.stem, raw_text)
            quarantine_errors = _collect_quarantine_errors(raw_text)
            if quarantine_errors:
                existing_quarantine = (
                    session.query(QuarantineRecord.id).filter(QuarantineRecord.source_file == source_file).one_or_none()
                )
                if existing_quarantine is not None:
                    already_quarantined += 1
                    continue
                session.add(
                    QuarantineRecord(
                        source_file=source_file,
                        raw_text=raw_text,
                        extracted_json=json.dumps(asdict(extracted), default=str, sort_keys=True),
                        errors_json=json.dumps(quarantine_errors, sort_keys=True),
                    )
                )
                quarantined += 1
                continue

            patient = session.query(Patient).filter(Patient.mrn == extracted.mrn).one_or_none()
            if patient is None:
                patient = Patient(
                    mrn=extracted.mrn,
                    full_name=extracted.patient_name,
                    diagnosis_year=extracted.diagnosis_year,
                    ms_subtype=extracted.ms_subtype,
                )
                session.add(patient)
                session.flush()
                log_insert(
                    table_name="patients",
                    record_id=patient.id,
                    source_file=file_path,
                    note_id=extracted.note_id,
                    details={
                        "mrn": patient.mrn,
                        "full_name": patient.full_name,
                        "diagnosis_year": patient.diagnosis_year,
                        "ms_subtype": patient.ms_subtype,
                    },
                )
            else:
                if extracted.diagnosis_year is not None and patient.diagnosis_year is None:
                    patient.diagnosis_year = extracted.diagnosis_year
                if extracted.ms_subtype and not patient.ms_subtype:
                    patient.ms_subtype = extracted.ms_subtype

            visit = session.query(Visit).filter(Visit.note_id == extracted.note_id).one_or_none()
            if visit is not None:
                already_processed += 1
                skipped_note_ids.append(extracted.note_id)
                continue

            visit = Visit(
                patient_id=patient.id,
                visit_date=extracted.visit_date,
                note_id=extracted.note_id,
                raw_text=raw_text,
                edss=extracted.edss_score,
                current_dmt=extracted.current_dmt,
                mri_new_lesions_count=extracted.mri_new_lesions_count,
            )
            session.add(visit)
            session.flush()
            log_insert(
                table_name="visits",
                record_id=visit.id,
                source_file=file_path,
                note_id=extracted.note_id,
                details={
                    "patient_id": visit.patient_id,
                    "edss": visit.edss,
                    "current_dmt": visit.current_dmt,
                    "mri_new_lesions_count": visit.mri_new_lesions_count,
                },
            )

            for med in extracted.medications:
                med_row = Medication(
                    visit_id=visit.id,
                    name=med.name,
                    dose=med.dose,
                    frequency=med.frequency,
                )
                session.add(med_row)
                session.flush()
                log_insert(
                    table_name="medications",
                    record_id=med_row.id,
                    source_file=file_path,
                    note_id=extracted.note_id,
                    details={
                        "visit_id": med_row.visit_id,
                        "name": med_row.name,
                        "dose": med_row.dose,
                        "frequency": med_row.frequency,
                    },
                )

            for mri in extracted.mri_results:
                mri_row = MRIResult(
                    visit_id=visit.id,
                    body_site=mri.body_site,
                    finding=mri.finding,
                    severity_score=mri.severity_score,
                )
                session.add(mri_row)
                session.flush()
                log_insert(
                    table_name="mri_results",
                    record_id=mri_row.id,
                    source_file=file_path,
                    note_id=extracted.note_id,
                    details={
                        "visit_id": mri_row.visit_id,
                        "body_site": mri_row.body_site,
                        "finding": mri_row.finding,
                        "severity_score": mri_row.severity_score,
                    },
                )

            ingested += 1

        session.commit()
    finally:
        session.close()

    print(
        f"Ingested {ingested} note(s), already processed {already_processed} note(s), "
        f"quarantined {quarantined} note(s), already quarantined {already_quarantined} note(s) "
        f"from {input_dir} into {db_url}"
    )
    if verbose and skipped_note_ids:
        print(f"Skipped note_ids ({len(skipped_note_ids)}): {', '.join(skipped_note_ids)}")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Ingest synthetic clinical notes into a SQLAlchemy database.")
    parser.add_argument("--input", required=True, help="Path to directory containing .txt note files")
    parser.add_argument("--db", required=True, help="Database URL, e.g., sqlite:///musicallite.db")
    parser.add_argument("--verbose", action="store_true", help="Print skipped note_ids for already processed notes.")
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    ingest_directory(input_dir=Path(args.input), db_url=args.db, verbose=args.verbose)


if __name__ == "__main__":
    main()
