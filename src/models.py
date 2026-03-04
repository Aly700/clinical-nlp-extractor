from datetime import datetime

from sqlalchemy import (
    CheckConstraint,
    Column,
    Date,
    DateTime,
    Float,
    ForeignKey,
    Integer,
    String,
    Text,
    create_engine,
    event,
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, sessionmaker, validates

Base = declarative_base()
CURRENT_YEAR = datetime.now().year


class Patient(Base):
    __tablename__ = "patients"
    __table_args__ = (
        CheckConstraint(
            f"diagnosis_year IS NULL OR (diagnosis_year >= 1900 AND diagnosis_year <= {CURRENT_YEAR})",
            name="ck_patients_diagnosis_year",
        ),
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    mrn = Column(String(64), unique=True, nullable=False, index=True)
    full_name = Column(String(255), nullable=False)
    date_of_birth = Column(Date, nullable=True)
    diagnosis_year = Column(Integer, nullable=True)
    ms_subtype = Column(String(16), nullable=True)

    visits = relationship("Visit", back_populates="patient", cascade="all, delete-orphan")

    @validates("diagnosis_year")
    def validate_diagnosis_year(self, _, value):
        if value is None:
            return value
        current_year = datetime.now().year
        if not (1900 <= int(value) <= current_year):
            raise ValueError(f"Diagnosis year must be between 1900 and {current_year}.")
        return int(value)


class Visit(Base):
    __tablename__ = "visits"
    __table_args__ = (
        CheckConstraint(
            "edss IS NULL OR (edss >= 0 AND edss <= 10)",
            name="ck_visits_edss_range",
        ),
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    patient_id = Column(Integer, ForeignKey("patients.id", ondelete="CASCADE"), nullable=False, index=True)
    visit_date = Column(Date, nullable=True)
    note_id = Column(String(128), unique=True, nullable=False, index=True)
    raw_text = Column(String, nullable=False)
    edss = Column(Float, nullable=True)
    current_dmt = Column(String(128), nullable=True)
    mri_new_lesions_count = Column(Integer, nullable=True)

    patient = relationship("Patient", back_populates="visits")
    medications = relationship("Medication", back_populates="visit", cascade="all, delete-orphan")
    mri_results = relationship("MRIResult", back_populates="visit", cascade="all, delete-orphan")

    @validates("edss")
    def validate_edss(self, _, value):
        if value is None:
            return value
        if not (0.0 <= float(value) <= 10.0):
            raise ValueError("EDSS score must be between 0 and 10.")
        return float(value)


class Medication(Base):
    __tablename__ = "medications"

    id = Column(Integer, primary_key=True, autoincrement=True)
    visit_id = Column(Integer, ForeignKey("visits.id"), nullable=False, index=True)
    name = Column(String(128), nullable=False)
    dose = Column(String(64), nullable=True)
    frequency = Column(String(64), nullable=True)

    visit = relationship("Visit", back_populates="medications")


class MRIResult(Base):
    __tablename__ = "mri_results"

    id = Column(Integer, primary_key=True, autoincrement=True)
    visit_id = Column(Integer, ForeignKey("visits.id"), nullable=False, index=True)
    body_site = Column(String(128), nullable=True)
    finding = Column(String(255), nullable=False)
    severity_score = Column(Float, nullable=True)

    visit = relationship("Visit", back_populates="mri_results")


class AuditLog(Base):
    __tablename__ = "audit_logs"
    __table_args__ = (
        CheckConstraint("action IN ('INSERT', 'UPDATE', 'DELETE')", name="ck_audit_logs_action"),
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    table_name = Column(String(64), nullable=False, index=True)
    record_id = Column(Integer, nullable=False, index=True)
    action = Column(String(16), nullable=False)
    source_file = Column(String(512), nullable=True)
    details_json = Column(Text, nullable=True)


class QuarantineRecord(Base):
    __tablename__ = "quarantine_records"

    id = Column(Integer, primary_key=True, autoincrement=True)
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    source_file = Column(String(512), nullable=False, unique=True)
    raw_text = Column(Text, nullable=False)
    extracted_json = Column(Text, nullable=True)
    errors_json = Column(Text, nullable=False)


def get_engine(db_url: str):
    engine = create_engine(db_url)
    if db_url.startswith("sqlite"):

        @event.listens_for(engine, "connect")
        def set_sqlite_pragma(dbapi_connection, _):
            cursor = dbapi_connection.cursor()
            cursor.execute("PRAGMA foreign_keys=ON")
            cursor.close()

    return engine


def init_db(db_url: str):
    engine = get_engine(db_url)
    Base.metadata.create_all(engine)
    return engine


def get_session_factory(db_url: str):
    return sessionmaker(bind=get_engine(db_url))
