"""Extract USD dollar amounts from free text.

Used by Phase 2 (trustee notices) to pull the largest $ figure as an
original-loan proxy, and by Phase 1 as a fallback when PDF tables are
unparseable.
"""
import re
from typing import Iterable

# Matches $1,234, $1,234.56, $ 1 234, 1,234,567.00 (without $ only if followed by 'USD').
# Requires at least 4 digits to skip page numbers / cause numbers / years.
_WITH_DOLLAR = re.compile(r"\$\s*([\d]{1,3}(?:[,\s][\d]{3})+(?:\.\d{2})?|\d{4,}(?:\.\d{2})?)")


def find_all_amounts(text: str) -> list[int]:
    """Return all USD amounts in text as whole-dollar integers. Cents dropped."""
    out: list[int] = []
    for m in _WITH_DOLLAR.finditer(text or ""):
        raw = m.group(1).replace(",", "").replace(" ", "")
        try:
            out.append(int(float(raw)))
        except ValueError:
            continue
    return out


def largest_amount(text: str) -> int | None:
    amounts = find_all_amounts(text)
    return max(amounts) if amounts else None


def total_amount(text: str, *, exclude_smaller_than: int = 0) -> int | None:
    """Sum of all amounts above a threshold. Used for trustee notices where
    multiple liens are listed and we want the aggregate."""
    amounts = [a for a in find_all_amounts(text) if a >= exclude_smaller_than]
    return sum(amounts) if amounts else None
