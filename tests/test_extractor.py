from src.extractor import ClinicalExtractor


def test_extractor_parses_structured_fields():
    note = """Patient: Test Person
MRN: MRN-9999
Visit Date: 2026-01-01
Medication: Aspirin | Dose: 81 mg | Frequency: daily
MRI: Brain | Finding: No acute abnormality | Severity: 1.0
"""

    extracted = ClinicalExtractor().extract_from_text("note_test", note)

    assert extracted.mrn == "MRN-9999"
    assert extracted.patient_name == "Test Person"
    assert len(extracted.medications) == 1
    assert extracted.medications[0].name == "Aspirin"
    assert extracted.mri_results[0].body_site == "Brain"

