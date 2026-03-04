import pytest

from src.extractor import ClinicalExtractor


CASES = [
    {
        "id": "case_01",
        "note": """Patient: Avery Lane
MRN: SYN-0001
EDSS score: 2.5
MS subtype: RRMS
Diagnosed in 2018 after sensory relapse.
Current DMT: Ocrelizumab.
Brain MRI shows 3 new lesions.
""",
        "expected": {"edss": 2.5, "subtype": "RRMS", "year": 2018, "dmt": "Ocrelizumab", "lesions": 3},
    },
    {
        "id": "case_02",
        "note": """Patient: Jordan Vale
MRN: SYN-0002
Expanded Disability Status Scale (EDSS) remains at 6.0.
Secondary progressive multiple sclerosis (SPMS) noted.
Diagnosis year = 2007.
Currently receiving Siponimod.
MRI: no new lesions.
""",
        "expected": {"edss": 6.0, "subtype": "SPMS", "year": 2007, "dmt": "Siponimod", "lesions": 0},
    },
    {
        "id": "case_03",
        "note": """Patient: Morgan Cruz
MRN: SYN-0003
EDSS is 0.0 with normal gait today.
Assessment: relapsing-remitting MS.
DX 2012.
Taking Tysabri monthly.
MRI without new lesions.
""",
        "expected": {"edss": 0.0, "subtype": "RRMS", "year": 2012, "dmt": "Natalizumab", "lesions": 0},
    },
    {
        "id": "case_04",
        "note": """Patient: Riley Stone
MRN: SYN-0004
PPMS phenotype.
EDSS=4
Diagnosed 2016.
Currently on Ocrevus infusions.
MRI: 1 new lesion.
""",
        "expected": {"edss": 4.0, "subtype": "PPMS", "year": 2016, "dmt": "Ocrelizumab", "lesions": 1},
    },
    {
        "id": "case_05",
        "note": """Patient: Taylor Quinn
MRN: SYN-0005
EDSS 3.5 at follow-up.
Diagnosed in 2014.
Previously on Natalizumab; currently on Fingolimod.
MRI new lesions: none.
""",
        "expected": {"edss": 3.5, "subtype": None, "year": 2014, "dmt": "Fingolimod", "lesions": 0},
    },
    {
        "id": "case_06",
        "note": """Patient: Casey North
MRN: SYN-0006
Transitioned from RRMS to SPMS over several years.
Switched from Fingolimod to Mayzent in 2024.
EDSS 5.5.
Diagnosis year: 2010.
MRI revealed two new lesions.
""",
        "expected": {"edss": 5.5, "subtype": "SPMS", "year": 2010, "dmt": "Siponimod", "lesions": 2},
    },
    {
        "id": "case_07",
        "note": """Patient: Devon Hart
MRN: SYN-0007
Primary progressive MS since 2019.
EDSS 4.0.
Not on any DMT currently.
MRI shows no new T2 lesions.
""",
        "expected": {"edss": 4.0, "subtype": "PPMS", "year": 2019, "dmt": None, "lesions": 0},
    },
    {
        "id": "case_08",
        "note": """Patient: Sky Mercer
MRN: SYN-0008
Visit Date: 2026-02-01
Diagnosed in 2011 after optic neuritis.
Current medication is Teriflunomide.
EDSS around 2.0.
MRI: 0 new lesions.
""",
        "expected": {"edss": 2.0, "subtype": None, "year": 2011, "dmt": "Teriflunomide", "lesions": 0},
    },
    {
        "id": "case_09",
        "note": """Patient: Drew Vale
MRN: SYN-0009
EDSS 12 mentioned in error; revised EDSS score is 7.0.
RRMS.
Diagnosis year: 2003.
Treated with Glatiramer acetate.
MRI showed three new lesions.
""",
        "expected": {"edss": 7.0, "subtype": "RRMS", "year": 2003, "dmt": "Glatiramer acetate", "lesions": 3},
    },
    {
        "id": "case_10",
        "note": """Patient: Blair Sloane
MRN: SYN-0010
EDSS: 1.5
Relapsing-remitting multiple sclerosis diagnosed 2020.
Currently on Kesimpta.
MRI new lesions = 2.
""",
        "expected": {"edss": 1.5, "subtype": "RRMS", "year": 2020, "dmt": "Ofatumumab", "lesions": 2},
    },
    {
        "id": "case_11",
        "note": """Patient: Parker Wynn
MRN: SYN-0011
Diagnosis year: 2015.
Current therapy: Rituximab.
Expanded Disability Status Scale score of 3.
MRI: zero new lesions.
""",
        "expected": {"edss": 3.0, "subtype": None, "year": 2015, "dmt": "Rituximab", "lesions": 0},
    },
    {
        "id": "case_12",
        "note": """Patient: Hayden Cole
MRN: SYN-0012
SPMS since 2008.
EDSS score is 6.5.
Currently taking Cladribine.
MRI brain: 2 new lesions; cervical spine: 1 new lesion.
""",
        "expected": {"edss": 6.5, "subtype": "SPMS", "year": 2008, "dmt": "Cladribine", "lesions": 3},
    },
]


@pytest.mark.parametrize("case", CASES, ids=[case["id"] for case in CASES])
def test_extract_ms_specific_fields(case):
    extracted = ClinicalExtractor().extract_from_text(case["id"], case["note"])
    expected = case["expected"]

    if expected["edss"] is None:
        assert extracted.edss_score is None
    else:
        assert extracted.edss_score == pytest.approx(expected["edss"])
    assert extracted.ms_subtype == expected["subtype"]
    assert extracted.diagnosis_year == expected["year"]
    assert extracted.current_dmt == expected["dmt"]
    assert extracted.mri_new_lesions_count == expected["lesions"]

