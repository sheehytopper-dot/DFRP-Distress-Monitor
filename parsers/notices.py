"""Parse Texas substitute-trustee notice text.

The notice format is loosely standardized by Tex. Prop. Code § 51.002: most
notices have labeled fields like 'Sale Date', 'Original Principal Amount',
'Property Address', but exact formatting varies by trustee firm.
"""
import re
from datetime import datetime
from typing import Optional

# --- Sale date -------------------------------------------------------------

_MONTHS = "January|February|March|April|May|June|July|August|September|October|November|December"
_SALE_DATE_LABELS = re.compile(
    rf"(?:Sale\s+Date|Date\s+of\s+Sale|Scheduled\s+Sale\s+Date)[:\s]+"
    rf"(?:[A-Za-z]+,?\s*)?"
    rf"(?P<month>{_MONTHS})\s+(?P<day>\d{{1,2}}),?\s*(?P<year>\d{{4}})",
    re.I,
)


def extract_sale_date(text: str) -> Optional[str]:
    """Return YYYY-MM-DD or None."""
    if not text:
        return None
    m = _SALE_DATE_LABELS.search(text)
    if not m:
        return None
    try:
        dt = datetime.strptime(f"{m['month']} {m['day']} {m['year']}", "%B %d %Y")
        return dt.date().isoformat()
    except ValueError:
        return None


# --- Original principal ----------------------------------------------------

_ORIGINAL_PRINCIPAL = re.compile(
    r"(?:Original\s+)?Principal(?:\s+Amount)?(?:\s+of\s+Note)?[:\s]+"
    r"\$\s*([\d,]+(?:\.\d{2})?)",
    re.I,
)


def extract_original_principal(text: str) -> Optional[int]:
    """Return original principal as whole USD, or None if not labeled."""
    if not text:
        return None
    m = _ORIGINAL_PRINCIPAL.search(text)
    if not m:
        return None
    try:
        return int(float(m.group(1).replace(",", "")))
    except ValueError:
        return None


# --- Property address ------------------------------------------------------

_ADDR_LABELED = re.compile(
    r"(?:Property\s+Address|Common\s+Address|Property\s+Commonly\s+Known\s+As)[:\s]+"
    r"(?P<addr>.+?)(?:\n|$)",
    re.I,
)

# Heuristic fallback: street-looking line.
_ADDR_STREET = re.compile(
    r"\b\d{1,6}\s+[A-Za-z][A-Za-z.' -]{0,60}?\s+"
    r"(?:St|Street|Ave|Avenue|Rd|Road|Blvd|Dr|Drive|Ln|Lane|Way|Hwy|Pkwy|Ct|Court|Pl|Place|Trl|Trail)\b"
    r"[^\n]{0,80}",
)


def extract_address(text: str) -> Optional[str]:
    if not text:
        return None
    m = _ADDR_LABELED.search(text)
    if m:
        return m.group("addr").strip(" .,;")
    m = _ADDR_STREET.search(text)
    return m.group(0).strip() if m else None


# --- Borrower / grantor ----------------------------------------------------

_GRANTOR = re.compile(
    r"(?:Grantor(?:s?\s*\(Borrower\))?|Borrower|Mortgagor)[:\s]+(?P<g>.+?)(?:\n|$)",
    re.I,
)


def extract_borrower(text: str) -> Optional[str]:
    if not text:
        return None
    m = _GRANTOR.search(text)
    return m.group("g").strip(" .,;") if m else None
