from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Optional

try:
    import spacy
except ModuleNotFoundError:  # pragma: no cover - exercised only in minimal envs
    spacy = None


@dataclass
class MedicationEntity:
    name: str
    dose: Optional[str] = None
    frequency: Optional[str] = None


@dataclass
class MRIEntity:
    body_site: Optional[str]
    finding: str
    severity_score: Optional[float] = None


@dataclass
class ExtractedNote:
    note_id: str
    mrn: str
    patient_name: str
    visit_date: Optional[datetime.date]
    edss_score: Optional[float] = None
    ms_subtype: Optional[str] = None
    diagnosis_year: Optional[int] = None
    current_dmt: Optional[str] = None
    mri_new_lesions_count: Optional[int] = None
    medications: list[MedicationEntity] = field(default_factory=list)
    mri_results: list[MRIEntity] = field(default_factory=list)


class ClinicalExtractor:
    """
    spaCy-enabled scaffold with rule extraction placeholders.

    This extractor is intentionally rule/regex-based and uses a lightweight
    blank English pipeline (`spacy.blank("en")`) with sentencizer only.
    """

    _MS_SUBTYPE_PATTERNS: list[tuple[str, str]] = [
        (r"\bRRMS\b", "RRMS"),
        (r"\bSPMS\b", "SPMS"),
        (r"\bPPMS\b", "PPMS"),
        (r"\brelapsing[\s-]?remitting(?:\s+multiple\s+sclerosis|\s+ms)?\b", "RRMS"),
        (r"\bsecondary[\s-]?progressive(?:\s+multiple\s+sclerosis|\s+ms)?\b", "SPMS"),
        (r"\bprimary[\s-]?progressive(?:\s+multiple\s+sclerosis|\s+ms)?\b", "PPMS"),
    ]
    _WORD_TO_INT = {
        "zero": 0,
        "one": 1,
        "two": 2,
        "three": 3,
        "four": 4,
        "five": 5,
        "six": 6,
        "seven": 7,
        "eight": 8,
        "nine": 9,
        "ten": 10,
    }
    _DMT_ALIASES = {
        "ocrelizumab": "Ocrelizumab",
        "ocrevus": "Ocrelizumab",
        "natalizumab": "Natalizumab",
        "tysabri": "Natalizumab",
        "fingolimod": "Fingolimod",
        "gilenya": "Fingolimod",
        "dimethyl fumarate": "Dimethyl fumarate",
        "tecfidera": "Dimethyl fumarate",
        "teriflunomide": "Teriflunomide",
        "aubagio": "Teriflunomide",
        "glatiramer acetate": "Glatiramer acetate",
        "copaxone": "Glatiramer acetate",
        "interferon beta-1a": "Interferon beta-1a",
        "avonex": "Interferon beta-1a",
        "rebif": "Interferon beta-1a",
        "interferon beta-1b": "Interferon beta-1b",
        "betaseron": "Interferon beta-1b",
        "extavia": "Interferon beta-1b",
        "siponimod": "Siponimod",
        "mayzent": "Siponimod",
        "ozanimod": "Ozanimod",
        "zeposia": "Ozanimod",
        "ponesimod": "Ponesimod",
        "ponvory": "Ponesimod",
        "ofatumumab": "Ofatumumab",
        "kesimpta": "Ofatumumab",
        "alemtuzumab": "Alemtuzumab",
        "lemtrada": "Alemtuzumab",
        "cladribine": "Cladribine",
        "mavenclad": "Cladribine",
        "rituximab": "Rituximab",
    }

    def __init__(self) -> None:
        if spacy is None:
            self.nlp = None
            return

        self.nlp = spacy.blank("en")

        if "sentencizer" not in self.nlp.pipe_names:
            self.nlp.add_pipe("sentencizer")

    def extract_from_text(self, note_id: str, text: str) -> ExtractedNote:
        if self.nlp is not None:
            _ = self.nlp(text)

        mrn = self._extract_single(r"MRN:\s*([A-Za-z0-9\-]+)", text, default=f"UNK-{note_id}")
        patient_name = self._extract_single(r"Patient:\s*(.+)", text, default="Unknown Patient")
        visit_date_str = self._extract_single(r"Visit Date:\s*([0-9]{4}-[0-9]{2}-[0-9]{2})", text)
        visit_date = datetime.strptime(visit_date_str, "%Y-%m-%d").date() if visit_date_str else None
        edss_score = self._extract_edss_score(text)
        ms_subtype = self._extract_ms_subtype(text)
        diagnosis_year = self._extract_diagnosis_year(text)
        current_dmt = self._extract_current_dmt(text)
        mri_new_lesions_count = self._extract_mri_new_lesions_count(text)

        meds = self._extract_medications(text)
        mri = self._extract_mri(text)

        return ExtractedNote(
            note_id=note_id,
            mrn=mrn,
            patient_name=patient_name,
            visit_date=visit_date,
            edss_score=edss_score,
            ms_subtype=ms_subtype,
            diagnosis_year=diagnosis_year,
            current_dmt=current_dmt,
            mri_new_lesions_count=mri_new_lesions_count,
            medications=meds,
            mri_results=mri,
        )

    def extract_from_file(self, file_path: Path) -> ExtractedNote:
        text = file_path.read_text(encoding="utf-8")
        return self.extract_from_text(note_id=file_path.stem, text=text)

    @staticmethod
    def _extract_single(pattern: str, text: str, default: Optional[str] = None) -> Optional[str]:
        match = re.search(pattern, text, flags=re.IGNORECASE)
        if not match:
            return default
        return match.group(1).strip()

    @staticmethod
    def _extract_medications(text: str) -> list[MedicationEntity]:
        meds: list[MedicationEntity] = []
        for line in text.splitlines():
            if not line.lower().startswith("medication:"):
                continue

            # Expected scaffold format:
            # Medication: NAME | Dose: VALUE | Frequency: VALUE
            med_name = ClinicalExtractor._extract_single(r"Medication:\s*([^|]+)", line)
            dose = ClinicalExtractor._extract_single(r"Dose:\s*([^|]+)", line)
            frequency = ClinicalExtractor._extract_single(r"Frequency:\s*([^|]+)", line)
            if med_name:
                meds.append(MedicationEntity(name=med_name, dose=dose, frequency=frequency))
        return meds

    @staticmethod
    def _extract_mri(text: str) -> list[MRIEntity]:
        mri_results: list[MRIEntity] = []
        for line in text.splitlines():
            if not line.lower().startswith("mri:"):
                continue

            # Expected scaffold format:
            # MRI: BODY_SITE | Finding: VALUE | Severity: FLOAT
            body_site = ClinicalExtractor._extract_single(r"MRI:\s*([^|]+)", line)
            finding = ClinicalExtractor._extract_single(r"Finding:\s*([^|]+)", line, default="Unspecified finding")
            severity_raw = ClinicalExtractor._extract_single(r"Severity:\s*([0-9]+(?:\.[0-9]+)?)", line)
            severity = float(severity_raw) if severity_raw else None
            mri_results.append(MRIEntity(body_site=body_site, finding=finding, severity_score=severity))
        return mri_results

    @staticmethod
    def _extract_edss_score(text: str) -> Optional[float]:
        patterns = [
            r"\bEDSS\b(?:\s*score)?\s*(?:of|is|=|:)?\s*([0-9]{1,2}(?:\.[0-9]+)?)\b",
            r"\bExpanded Disability Status Scale(?:\s*\(EDSS\))?(?:\s*score)?\s*(?:of|is|=|:)?\s*([0-9]{1,2}(?:\.[0-9]+)?)\b",
            r"\bEDSS\b[^0-9]{0,18}([0-9]{1,2}(?:\.[0-9]+)?)\b",
        ]
        for pattern in patterns:
            for match in re.finditer(pattern, text, flags=re.IGNORECASE):
                value = float(match.group(1))
                if 0.0 <= value <= 10.0:
                    return value
        return None

    @classmethod
    def _extract_ms_subtype(cls, text: str) -> Optional[str]:
        matches: list[tuple[int, str]] = []
        for pattern, subtype in cls._MS_SUBTYPE_PATTERNS:
            for match in re.finditer(pattern, text, flags=re.IGNORECASE):
                matches.append((match.start(), subtype))
        if not matches:
            return None
        matches.sort(key=lambda x: x[0])
        return matches[-1][1]

    @staticmethod
    def _extract_diagnosis_year(text: str) -> Optional[int]:
        patterns = [
            r"\bdiagnosis\s*year\s*[:=]?\s*((?:19|20)\d{2})\b",
            r"\bdiagnos(?:ed|is|e)\b[^.\n;]{0,40}\b((?:19|20)\d{2})\b",
            r"\b(?:dx|diagnosis)\b[^.\n;]{0,30}\b((?:19|20)\d{2})\b",
            r"\bonset\b[^.\n;]{0,25}\b((?:19|20)\d{2})\b",
            r"\bsince\s+((?:19|20)\d{2})\b",
        ]
        candidates: list[tuple[int, int]] = []
        for pattern in patterns:
            for match in re.finditer(pattern, text, flags=re.IGNORECASE):
                year = int(match.group(1))
                if 1900 <= year <= datetime.now().year:
                    candidates.append((match.start(), year))
        if not candidates:
            return None
        candidates.sort(key=lambda x: x[0])
        return candidates[0][1]

    @classmethod
    def _extract_current_dmt(cls, text: str) -> Optional[str]:
        lower_text = text.lower()
        if re.search(r"\bnot on\b[^.\n]{0,20}\b(dmt|disease[-\s]?modifying)", lower_text):
            return None
        if re.search(r"\bno\b[^.\n]{0,10}\bcurrent\b[^.\n]{0,10}\bdmt\b", lower_text):
            return None

        aliases = sorted(cls._DMT_ALIASES.keys(), key=len, reverse=True)
        alias_pattern = r"(" + "|".join(re.escape(alias) for alias in aliases) + r")"

        switch_match = re.search(
            r"\bswitched\s+from\b[^.\n]{0,60}\bto\s+" + alias_pattern + r"\b",
            lower_text,
            flags=re.IGNORECASE,
        )
        if switch_match:
            alias = switch_match.group(1).lower()
            return cls._DMT_ALIASES.get(alias)

        current_patterns = [
            r"\bcurrent\s+(?:dmt|medication|therapy)\s*(?:is|:)?\s*" + alias_pattern + r"\b",
            r"\b(?:currently|presently)\s+(?:on|taking|receiving)\s+" + alias_pattern + r"\b",
            r"\b(?:continues?|maintained)\s+on\s+" + alias_pattern + r"\b",
            r"\bon\s+" + alias_pattern + r"\b",
            r"\btaking\s+" + alias_pattern + r"\b",
            r"\btreated\s+with\s+" + alias_pattern + r"\b",
        ]
        for pattern in current_patterns:
            match = re.search(pattern, lower_text, flags=re.IGNORECASE)
            if not match:
                continue
            alias = match.group(1).lower()
            return cls._DMT_ALIASES.get(alias)

        candidates: list[tuple[int, str]] = []
        for match in re.finditer(alias_pattern, lower_text, flags=re.IGNORECASE):
            context = lower_text[max(0, match.start() - 32) : min(len(lower_text), match.end() + 32)]
            if re.search(r"\b(previous|prior|formerly|stopped|discontinued|history of|used to)\b", context):
                continue
            alias = match.group(1).lower()
            canonical = cls._DMT_ALIASES.get(alias)
            if canonical:
                candidates.append((match.start(), canonical))
        if not candidates:
            return None
        candidates.sort(key=lambda x: x[0])
        return candidates[-1][1]

    @classmethod
    def _extract_mri_new_lesions_count(cls, text: str) -> Optional[int]:
        number_matches = []

        for match in re.finditer(r"\b(\d+)\s+new\s+(?:t2\s+)?lesions?\b", text, flags=re.IGNORECASE):
            number_matches.append((match.start(), int(match.group(1))))

        for match in re.finditer(r"\bnew\s+(?:t2\s+)?lesions?\s*[:=]\s*(\d+)\b", text, flags=re.IGNORECASE):
            number_matches.append((match.start(), int(match.group(1))))

        word_keys = "|".join(cls._WORD_TO_INT.keys())
        for match in re.finditer(
            r"\b(" + word_keys + r")\s+new\s+(?:t2\s+)?lesions?\b",
            text,
            flags=re.IGNORECASE,
        ):
            number_matches.append((match.start(), cls._WORD_TO_INT[match.group(1).lower()]))

        if number_matches:
            return sum(value for _, value in number_matches)

        no_new_patterns = [
            r"\b(no|none|zero)\s+new\s+(?:t2\s+)?lesions?\b",
            r"\bwithout\s+new\s+(?:t2\s+)?lesions?\b",
            r"\bnew\s+(?:t2\s+)?lesions?\s*[:=]\s*none\b",
            r"\bno\b[^.\n]{0,20}\bnew\s+lesions?\b",
        ]
        for pattern in no_new_patterns:
            if re.search(pattern, text, flags=re.IGNORECASE):
                return 0

        return None
