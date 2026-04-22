"""Classify a property description string into a coarse type.

Texas tax sale notices include a legal description that may contain terms
like "LOT 1 BLK 2 ACME COMMERCIAL PARK" or "15.3 ACRES A123 ABST..." but
rarely use a clean structured type field. Keyword matching is best-effort;
we fall back to 'unknown' and the $2M threshold handles the rest.
"""
import re

_PATTERNS: list[tuple[str, re.Pattern]] = [
    ("industrial",  re.compile(r"\b(warehouse|industrial|manufacturing|distribution)\b", re.I)),
    ("multifamily", re.compile(r"\b(apartment|multifamily|multi-family|duplex|triplex|fourplex|\d+[\s-]?unit)\b", re.I)),
    ("commercial",  re.compile(r"\b(commercial|retail|office|shopping center|strip center|hotel|motel|restaurant)\b", re.I)),
    ("ranch",       re.compile(r"\b(ranch)\b", re.I)),
    ("agricultural",re.compile(r"\b(agricultural|farm(?!land\s*ave)|ag[\s-]?use|pasture)\b", re.I)),
    ("land",        re.compile(r"\b(\d+(\.\d+)?\s*acres?|acreage|vacant\s*(lot|land)|undeveloped|raw\s*land|tract\b)", re.I)),
    ("residential", re.compile(r"\b(single[\s-]family|sfr|residence|residential|lot\s+\d+.*block)\b", re.I)),
]


def classify(description: str | None) -> str:
    """Return one of: commercial|industrial|land|multifamily|ranch|agricultural|residential|unknown."""
    if not description:
        return "unknown"
    for label, pat in _PATTERNS:
        if pat.search(description):
            return label
    return "unknown"
