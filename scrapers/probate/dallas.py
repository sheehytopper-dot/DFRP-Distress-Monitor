"""Dallas County probate scraper.

Source: courtsportal.dallascounty.org/DALLASPROD — Tyler Odyssey
'Smart Search' portal for Dallas County. Public access, no login.

Strategy:
1. Open the portal landing page in headless Chromium with capture_json=True.
2. Walk every captured JSON response for objects shaped like a case row
   (CaseId/CaseNumber/CaseType/FileDate/CaseStyle).
3. Filter to muniment-of-title and heirship cases filed in the last
   N_DAYS_BACK days.
4. Yield ProbateFiling rows.

This first cut is diagnostic-leaning: if zero JSON responses are
captured (e.g., the page only fires XHRs after a search-form submit),
the next run's sample_text will show what the rendered HTML looks like
so we can refine to either auto-submit the search form or hit a
discovered XHR endpoint directly.
"""
import json
import logging
import re
from datetime import date, timedelta
from typing import Any, Iterator, Optional

from scrapers.playwright_util import render_html
from scrapers.probate.base import ProbateFiling, ProbateScraperBase

log = logging.getLogger(__name__)

PORTAL = "https://courtsportal.dallascounty.org/DALLASPROD"
DASHBOARD_URL = f"{PORTAL}/Home/Dashboard/29"

N_DAYS_BACK = 14  # how far back to look for filings on each run

_PROBATE_TYPE_RE = re.compile(r"muniment\s+of\s+title|heirship", re.I)

# Property keys we look for to identify case-shaped JSON objects.
_CASE_INDICATOR_KEYS = {
    "CaseId", "CaseID", "case_id",
    "CaseNumber", "caseNumber", "case_number",
    "CaseType", "caseType", "case_type",
    "FileDate", "fileDate", "file_date",
    "CaseStyle", "caseStyle", "case_style", "Style", "style",
}


class DallasProbate(ProbateScraperBase):
    county = "dallas"
    throttle_s = 0.5

    def fetch(self) -> Iterator[ProbateFiling]:
        log.info("dallas_probate: loading %s", DASHBOARD_URL)
        html, _pdf_urls, json_responses = render_html(
            DASHBOARD_URL,
            wait_ms=8000,
            capture_pdfs=False,
            capture_json=True,
        )

        if not html:
            raise RuntimeError("dallas_probate: Playwright returned no HTML — "
                               "site may be blocking headless or unreachable")

        log.info("dallas_probate: rendered %d chars HTML, captured %d JSON responses",
                 len(html), len(json_responses))

        # Diagnostic: stash a summary of what XHRs fired into sample_text.
        # If the next run's report shows e.g. /Search/SmartSearchResults in
        # this list, we have a target endpoint to hit directly going forward.
        endpoints = sorted({_url_path(r["url"]) for r in json_responses})
        if endpoints:
            self.sample_text = "Captured JSON endpoints:\n" + "\n".join(endpoints)
        else:
            # No JSON fired — fall back to first 2000 chars of rendered HTML
            # so we can see what the page looks like and refine our approach.
            self.sample_text = html[:2000]

        cutoff = (date.today() - timedelta(days=N_DAYS_BACK)).isoformat()
        seen_case_numbers: set[str] = set()

        for resp in json_responses:
            for case in _walk_case_objects(resp["body"]):
                yield from _maybe_yield(case, resp["url"], cutoff,
                                        seen_case_numbers)


def _url_path(url: str) -> str:
    """Strip query string for endpoint summary."""
    return url.split("?", 1)[0]


def _walk_case_objects(body: Any) -> Iterator[dict]:
    """Yield dicts likely to be a case row."""
    stack = [body]
    seen = 0
    while stack and seen < 50000:
        node = stack.pop()
        seen += 1
        if isinstance(node, dict):
            if _is_case(node):
                yield node
            else:
                stack.extend(node.values())
        elif isinstance(node, list):
            stack.extend(node)


def _is_case(d: dict) -> bool:
    return len(set(d.keys()) & _CASE_INDICATOR_KEYS) >= 2


def _pick(d: dict, *keys: str) -> Optional[Any]:
    for k in keys:
        if k in d and d[k] not in (None, ""):
            return d[k]
    return None


def _maybe_yield(case: dict, source_url: str, cutoff_date: str,
                 seen: set) -> Iterator[ProbateFiling]:
    case_type = _pick(case, "CaseType", "caseType", "case_type") or ""
    if not _PROBATE_TYPE_RE.search(str(case_type)):
        return
    case_number = str(_pick(case, "CaseNumber", "caseNumber", "case_number") or "")
    if not case_number or case_number in seen:
        return
    seen.add(case_number)

    file_date = _pick(case, "FileDate", "fileDate", "file_date")
    if file_date and isinstance(file_date, str) and len(file_date) >= 10:
        if file_date[:10] < cutoff_date:
            return

    style = _pick(case, "CaseStyle", "caseStyle", "case_style", "Style", "style")
    raw_text = json.dumps(case)

    yield ProbateFiling(
        county="dallas",
        case_number=case_number,
        case_type=("muniment_of_title"
                   if "muniment" in str(case_type).lower()
                   else "heirship"),
        filed_date=str(file_date)[:10] if file_date else None,
        decedent_name=str(style) if style else None,
        url=f"{PORTAL}/Search/CaseInformation?caseNumber={case_number}",
        raw_text=raw_text,
    )
