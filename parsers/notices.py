"""Parse Texas substitute-trustee notice text.

Notice format per Tex. Prop. Code § 51.002: usually labeled fields
('Sale Date', 'Original Principal Amount', 'Property Address') but
formatting varies.
"""
import re
from datetime import date, datetime
from typing import Optional

_MONTHS = "January|February|March|April|May|June|July|August|September|October|November|December"

# Labeled: "Sale Date: Tuesday, May 5, 2026" / "Date of Sale: May 5, 2026"
_SALE_DATE_LABELED = re.compile(
    rf"(?:Sale\s+Date|Date\s+of\s+Sale|Scheduled\s+Sale\s+Date)[:\s]+"
    rf"(?:[A-Za-z]+,?\s*)?"
    rf"(?P<month>{_MONTHS})\s+(?P<day>\d{{1,2}}),?\s*(?P<year>\d{{4}})",
    re.I,
)

# Tax-sale header: "JOHNSON COUNTY SALES FOR MAY 5, 2026"
_SALE_DATE_HEADER = re.compile(
    rf"\bSALES?\s+FOR\s+(?P<month>{_MONTHS})\s+(?P<day>\d{{1,2}}),?\s*(?P<year>\d{{4}})",
    re.I,
)

# Ordinal long form, common in Dallas County notices:
# "Date: Tuesday, the 3rd day of March, 2026"
_SALE_DATE_ORDINAL = re.compile(
    rf"\bDate:\s*(?:[A-Za-z]+day,?\s*)?(?:the\s+)?"
    rf"(?P<day>\d{{1,2}})(?:st|nd|rd|th)?\s+day\s+of\s+"
    rf"(?P<month>{_MONTHS}),?\s*(?P<year>\d{{4}})",
    re.I,
)


def extract_sale_date(text: str) -> Optional[str]:
    """Return YYYY-MM-DD of the latest plausible sale date mentioned, or None.

    Prefers the latest date because republished / continued notices reference
    earlier failed sale dates along with the new one.
    """
    if not text:
        return None
    dates: list[str] = []
    for pat in (_SALE_DATE_LABELED, _SALE_DATE_HEADER, _SALE_DATE_ORDINAL):
        for m in pat.finditer(text):
            try:
                dt = datetime.strptime(f"{m['month']} {m['day']} {m['year']}", "%B %d %Y")
                dates.append(dt.date().isoformat())
            except ValueError:
                continue
    return max(dates) if dates else None


def is_past_sale(sale_date: Optional[str], *, today: Optional[str] = None) -> bool:
    """True if sale_date is strictly before today. Unknown dates return False."""
    if not sale_date:
        return False
    ref = today or date.today().isoformat()
    return sale_date < ref


_ORIGINAL_PRINCIPAL = re.compile(
    r"(?:Original\s+)?Principal(?:\s+Amount)?(?:\s+of\s+Note)?[:\s]+"
    r"\$\s*([\d,]+(?:\.\d{2})?)",
    re.I,
)


def extract_original_principal(text: str) -> Optional[int]:
    if not text:
        return None
    m = _ORIGINAL_PRINCIPAL.search(text)
    if not m:
        return None
    try:
        return int(float(m.group(1).replace(",", "")))
    except ValueError:
        return None


_ADDR_LABELED = re.compile(
    r"(?:Property\s+Address|Common\s+Address|Property\s+Commonly\s+Known\s+As)[:\s]+"
    r"(?P<addr>.+?)(?:\n|$)",
    re.I,
)

_ADDR_STREET = re.compile(
    r"\b\d{1,6}\s+[A-Za-z][A-Za-z.' -]{0,60}?\s+"
    r"(?:St|Street|Ave|Avenue|Rd|Road|Blvd|Dr|Drive|Ln|Lane|Way|Hwy|Pkwy|Ct|Court|Pl|Place|Trl|Trail)\b"
    r"[^\n]{0,80}",
)

# Extract up through Texas ZIP (5 or 9 digits) to trim scanner/OCR trailing garbage.
_TX_ADDR_CLEANUP = re.compile(
    r"^(.*?\bTX\s*,?\s*\d{5}(?:-\d{4})?)",
    re.I | re.S,
)


def extract_address(text: str) -> Optional[str]:
    if not text:
        return None
    m = _ADDR_LABELED.search(text)
    if m:
        addr = m.group("addr").strip(" .,;")
    else:
        m2 = _ADDR_STREET.search(text)
        addr = m2.group(0).strip() if m2 else None
    if not addr:
        return None
    # Trim trailing OCR garbage after the ZIP code.
    cleanup = _TX_ADDR_CLEANUP.match(addr)
    return cleanup.group(1).strip() if cleanup else addr.strip()


_GRANTOR = re.compile(
    r"(?:Grantor(?:s?\s*\(Borrower\))?|Borrower|Mortgagor)[:\s]+(?P<g>.+?)(?:\n|$)",
    re.I,
)


def extract_borrower(text: str) -> Optional[str]:
    if not text:
        return None
    m = _GRANTOR.search(text)
    return m.group("g").strip(" .,;") if m else None
