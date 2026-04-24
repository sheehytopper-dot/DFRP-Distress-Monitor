"""Parse Texas probate filing text / metadata.

Muniment of title and determination of heirship are the two case types
we care about (per user spec). Other probate types (estate administration,
guardianship, will contest) are out of scope.

Real-estate detection is best-effort: if the filing / inventory text
mentions real property, acreage, addresses, or standard legal-description
language, set involves_real_estate=TRUE. If unambiguously not real
estate, FALSE. Otherwise NULL (unknown).
"""
import re
from typing import Optional

# Case type classification ----------------------------------------------------

_MUNIMENT_RE = re.compile(r"\b(muniment\s+of\s+title)\b", re.I)
_HEIRSHIP_RE = re.compile(
    r"\b(determination\s+of\s+heirship|application\s+for\s+(?:proof\s+of\s+)?heirship)\b",
    re.I,
)


def classify_case_type(text: str) -> Optional[str]:
    """Return 'muniment_of_title' or 'heirship', else None (not in scope)."""
    if not text:
        return None
    if _MUNIMENT_RE.search(text):
        return "muniment_of_title"
    if _HEIRSHIP_RE.search(text):
        return "heirship"
    return None


# Real estate detection -------------------------------------------------------

_REAL_ESTATE_POSITIVE = re.compile(
    r"\b(real\s+property|real\s+estate|acres?|acreage|\b[Ll]ot\s+\d+\b|block\s+\d+|"
    r"abstract\s+\d+|tract\b|ranch|residence|homestead|undivided\s+interest|"
    r"\b\d{1,6}\s+[A-Z][a-zA-Z]+\s+(?:St|Street|Ave|Avenue|Rd|Road|Blvd|Dr|Drive|Ln|Hwy))\b",
    re.I,
)
_REAL_ESTATE_NEGATIVE = re.compile(
    r"\bno\s+real\s+(?:property|estate)\b|"
    r"\bestate\s+(?:contained|consisted)\s+(?:only|solely)\s+of\s+personal\s+property\b",
    re.I,
)


def involves_real_estate(text: str) -> Optional[bool]:
    """Best-effort classification. None means unknown (include in leaderboard)."""
    if not text:
        return None
    if _REAL_ESTATE_NEGATIVE.search(text):
        return False
    if _REAL_ESTATE_POSITIVE.search(text):
        return True
    return None


# Name / entity extraction ----------------------------------------------------

_DECEDENT_RE = re.compile(
    r"(?:Decedent|Deceased|Estate\s+of|In\s+(?:the\s+)?Estate\s+of)\s*[:.]?\s*"
    r"(?P<name>[A-Z][A-Za-z.'-]+(?:\s+[A-Z][A-Za-z.'-]+){1,4})",
)
_APPLICANT_RE = re.compile(
    r"(?:Applicant|Petitioner)\s*[:.]?\s*"
    r"(?P<name>[A-Z][A-Za-z.'-]+(?:\s+[A-Z][A-Za-z.'-]+){1,4})",
)
_ATTORNEY_RE = re.compile(
    r"(?:Attorney\s+(?:of\s+Record|for\s+Applicant)|Applicant's\s+Attorney)\s*[:.]?\s*"
    r"(?P<name>[A-Z][A-Za-z.'-]+(?:\s+[A-Z][A-Za-z.'-]+){1,4})",
    re.I,
)
_BAR_NUMBER_RE = re.compile(r"(?:SBN|State\s+Bar(?:\s+No)?\.?)[:\s]*(\d{6,10})", re.I)


def extract_decedent(text: str) -> Optional[str]:
    if not text:
        return None
    m = _DECEDENT_RE.search(text)
    return m.group("name").strip(" .,;") if m else None


def extract_applicant(text: str) -> Optional[str]:
    if not text:
        return None
    m = _APPLICANT_RE.search(text)
    return m.group("name").strip(" .,;") if m else None


def extract_attorney(text: str) -> Optional[str]:
    if not text:
        return None
    m = _ATTORNEY_RE.search(text)
    return m.group("name").strip(" .,;") if m else None


def extract_bar_number(text: str) -> Optional[str]:
    if not text:
        return None
    m = _BAR_NUMBER_RE.search(text)
    return m.group(1) if m else None


# Normalization ---------------------------------------------------------------

def normalize_name(name: str) -> str:
    """Lowercase, collapse whitespace, strip punctuation. Used as a dedup key
    for probate_attorneys.name_normalized."""
    if not name:
        return ""
    s = re.sub(r"[^\w\s]", " ", name.lower())
    s = re.sub(r"\s+", " ", s).strip()
    return s
