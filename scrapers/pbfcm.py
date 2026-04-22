"""Perdue Brandon (pbfcm.com) tax sale + resale scraper.

Strategy:
1. GET https://pbfcm.com/taxsale.html (upcoming sheriff sales, first Tuesday
   each month) and https://pbfcm.com/taxresale.html (struck-off resales).
2. Parse the HTML index for PDF links in /docs/taxdocs/.
3. Keep only links for DFW counties.
4. Download each PDF, extract text + tables with pdfplumber.
5. Split into per-property blocks and emit DistressRecord rows where the
   largest $ amount >= threshold OR classified type is commercial/land/etc.

Site blocks aggressive scraping; we use a realistic User-Agent, no parallelism,
and 2 second delay between PDF downloads.
"""
import hashlib
import logging
import re
import time
from typing import Iterator
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

from config.settings import COMMERCIAL_TYPES, DISTRESS_MIN_USD
from parsers.amounts import find_all_amounts, largest_amount
from parsers.pdf import extract_text, split_into_property_blocks
from parsers.property_type import classify
from scrapers.base import BaseScraper, DistressRecord

log = logging.getLogger(__name__)

INDEX_URLS = [
    ("sale",   "https://pbfcm.com/taxsale.html"),
    ("resale", "https://pbfcm.com/taxresale.html"),
]

DFW_COUNTIES = {
    "dallas", "tarrant", "collin", "denton",
    "rockwall", "kaufman", "ellis", "johnson",
}

USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 "
    "(KHTML, like Gecko) Version/17.0 Safari/605.1.15"
)

_MONTH_CNTY_RE = re.compile(
    r"/(?:sales|resales)/(?:\d{2}-\d{4})?(?P<slug>[a-z][a-z0-9]+?)(?:cad|isd|countytaxresale|taxsale|taxresale|tx)",
    re.I,
)


class PbfcmScraper(BaseScraper):
    source = "pbfcm"

    def __init__(self, session: requests.Session | None = None, throttle_s: float = 2.0):
        self.session = session or requests.Session()
        self.session.headers["User-Agent"] = USER_AGENT
        self.throttle_s = throttle_s

    def fetch(self) -> Iterator[DistressRecord]:
        index_failures = 0
        for kind, url in INDEX_URLS:
            try:
                pdf_links = self._discover_pdfs(url)
            except Exception as e:
                log.warning("pbfcm index fetch failed for %s: %s", url, e)
                index_failures += 1
                continue
            log.info("pbfcm %s: %d candidate PDFs", kind, len(pdf_links))
            for county, pdf_url in pdf_links:
                time.sleep(self.throttle_s)
                try:
                    yield from self._parse_pdf(county, kind, pdf_url)
                except Exception as e:
                    log.warning("pbfcm pdf parse failed %s: %s", pdf_url, e)

        if index_failures == len(INDEX_URLS):
            raise RuntimeError(
                f"all {len(INDEX_URLS)} pbfcm index URLs failed; check site status or block"
            )

    def _discover_pdfs(self, index_url: str) -> list[tuple[str, str]]:
        r = self.session.get(index_url, timeout=30)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "lxml")
        out: list[tuple[str, str]] = []
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if not href.lower().endswith(".pdf"):
                continue
            full = urljoin(index_url, href)
            county = _county_from_link(a.get_text(" ", strip=True), full)
            if county in DFW_COUNTIES:
                out.append((county, full))
        return out

    def _parse_pdf(self, county: str, kind: str, pdf_url: str) -> Iterator[DistressRecord]:
        r = self.session.get(pdf_url, timeout=60)
        r.raise_for_status()
        text = extract_text(r.content)
        if not text:
            log.info("pbfcm no text extracted: %s", pdf_url)
            return

        for block in split_into_property_blocks(text):
            largest = largest_amount(block)
            ptype = classify(block)
            if not _passes_filter(largest, ptype):
                continue

            source_id = _stable_id(pdf_url, block)
            yield DistressRecord(
                source=self.source,
                source_id=source_id,
                county=county,
                url=pdf_url,
                property_address=_guess_address(block),
                property_type=ptype,
                amount_usd=largest,
                amount_kind="adjudged" if kind == "sale" else "resale_minimum",
                sale_date=None,
                raw_text=block[:4000],
                extra={"pdf_kind": kind, "all_amounts": find_all_amounts(block)[:10]},
            )


def _passes_filter(amount: int | None, ptype: str) -> bool:
    if amount is not None and amount >= DISTRESS_MIN_USD:
        return True
    if ptype in COMMERCIAL_TYPES:
        return True
    return False


def _county_from_link(link_text: str, href: str) -> str:
    hay = f"{link_text} {href}".lower()
    for county in DFW_COUNTIES:
        if county in hay:
            return county
    return ""


_ADDR_RE = re.compile(
    r"\b\d{1,6}\s+[A-Za-z][A-Za-z.' -]{0,60}?\s+"
    r"(?:St|Street|Ave|Avenue|Rd|Road|Blvd|Dr|Drive|Ln|Lane|Way|Hwy|Pkwy|Ct|Court|Pl|Place|Trl|Trail)\b"
)


def _guess_address(block: str) -> str | None:
    m = _ADDR_RE.search(block)
    return m.group(0).strip() if m else None


def _stable_id(pdf_url: str, block: str) -> str:
    """Dedup on (PDF URL, first ~200 chars of block). PDFs replace monthly,
    so inside a given PDF the block text is the stable signature.
    """
    h = hashlib.sha1(f"{pdf_url}|{block[:200]}".encode()).hexdigest()[:16]
    return h
