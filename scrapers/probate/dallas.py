"""Dallas County probate scraper.

Source: courtsportal.dallascounty.org/DALLASPROD — Tyler Odyssey
Smart Search portal. Public access, no login.

Strategy:
1. Open the Smart Search dashboard in headless Chromium.
2. Submit a Smart Search query for 'muniment' to surface
   muniment-of-title cases. Capture the resulting XHR JSON.
3. Submit a second query for 'heirship'.
4. Walk both payloads for case rows, filter by file date (last
   N_DAYS_BACK days), yield ProbateFiling records.

Tyler Odyssey 2021+ Smart Search renders results via XHR returning
JSON; we register a response listener before submitting so we catch
the response regardless of what selectors the page uses internally.

The form-interaction selectors are best-effort. If they fail, the
next run's sample_text shows what HTML is on the page so we can
narrow them.
"""
import json
import logging
import re
from datetime import date, timedelta
from typing import Any, Iterator, Optional

from scrapers.probate.base import ProbateFiling, ProbateScraperBase

log = logging.getLogger(__name__)

PORTAL = "https://courtsportal.dallascounty.org/DALLASPROD"
DASHBOARD_URL = f"{PORTAL}/Home/Dashboard/29"

N_DAYS_BACK = 14

_PROBATE_TYPE_RE = re.compile(r"muniment\s+of\s+title|heirship", re.I)

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
        try:
            from playwright.sync_api import sync_playwright
        except ImportError:
            raise RuntimeError("dallas_probate: playwright not installed")

        json_responses: list[dict] = []
        endpoints_seen: list[str] = []

        with sync_playwright() as p:
            browser = p.chromium.launch(
                headless=True,
                args=["--disable-blink-features=AutomationControlled"],
            )
            try:
                context = browser.new_context(
                    user_agent=("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                                "AppleWebKit/605.1.15 (KHTML, like Gecko) "
                                "Version/17.0 Safari/605.1.15"),
                    viewport={"width": 1440, "height": 900},
                    locale="en-US",
                )
                context.add_init_script(
                    "Object.defineProperty(navigator, 'webdriver', {get: () => undefined});"
                )
                page = context.new_page()

                def on_response(resp):
                    ct = (resp.headers.get("content-type") or "").lower()
                    if "json" not in ct:
                        return
                    endpoints_seen.append(_url_path(resp.url))
                    try:
                        body = resp.json()
                    except Exception:
                        return
                    json_responses.append({"url": resp.url, "body": body})

                page.on("response", on_response)

                log.info("dallas_probate: loading %s", DASHBOARD_URL)
                page.goto(DASHBOARD_URL, wait_until="domcontentloaded", timeout=30000)
                page.wait_for_timeout(3000)

                # Submit a search for each probate keyword. Tyler Odyssey
                # Smart Search treats this as a free-text query that matches
                # case type, party names, attorneys.
                for query in ("muniment", "heirship"):
                    try:
                        _submit_smart_search(page, query)
                        page.wait_for_timeout(5000)  # let XHR settle
                    except Exception as e:
                        log.warning("dallas_probate: '%s' search failed: %s", query, e)
            finally:
                browser.close()

        log.info("dallas_probate: captured %d JSON responses across %d endpoints",
                 len(json_responses), len(set(endpoints_seen)))

        # Diagnostic dump for next-run iteration.
        unique_endpoints = sorted(set(endpoints_seen))
        if unique_endpoints:
            self.sample_text = "JSON endpoints captured:\n" + "\n".join(unique_endpoints)
        elif json_responses:
            self.sample_text = json.dumps(json_responses[0])[:2000]
        else:
            self.sample_text = "No JSON responses captured. Search form interaction may have failed."

        cutoff = (date.today() - timedelta(days=N_DAYS_BACK)).isoformat()
        seen: set[str] = set()
        for resp in json_responses:
            for case in _walk_case_objects(resp["body"]):
                yield from _maybe_yield(case, resp["url"], cutoff, seen)


def _submit_smart_search(page, query: str) -> None:
    """Type a query into the Smart Search input and click search.

    Selectors are best-effort across Tyler Odyssey 2021+ variations:
    - the search input is usually <input type=text> inside the SmartSearch
      panel; placeholder is something like 'Search by Name, Case Number...'
    - submit can be a button labelled 'Search' or with a search icon.
    """
    # Make sure we're back on the dashboard for each query.
    page.goto(DASHBOARD_URL, wait_until="domcontentloaded", timeout=30000)
    page.wait_for_timeout(2000)

    # Try several selectors in order of specificity.
    for sel in [
        "input.k-textbox",
        "input[name='SearchCriteria']",
        "input[placeholder*='Name' i]",
        "input[type='text']",
    ]:
        try:
            page.locator(sel).first.fill(query, timeout=3000)
            break
        except Exception:
            continue
    else:
        raise RuntimeError("could not find search input")

    for sel in [
        "button:has-text('Search')",
        "input[type='submit'][value='Search']",
        "a:has-text('Search')",
    ]:
        try:
            page.locator(sel).first.click(timeout=3000)
            return
        except Exception:
            continue
    raise RuntimeError("could not find search submit")


def _url_path(url: str) -> str:
    return url.split("?", 1)[0]


def _walk_case_objects(body: Any) -> Iterator[dict]:
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

    yield ProbateFiling(
        county="dallas",
        case_number=case_number,
        case_type=("muniment_of_title"
                   if "muniment" in str(case_type).lower()
                   else "heirship"),
        filed_date=str(file_date)[:10] if file_date else None,
        decedent_name=str(style) if style else None,
        url=f"{PORTAL}/Search/CaseInformation?caseNumber={case_number}",
        raw_text=json.dumps(case),
    )
