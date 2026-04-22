"""Kaufman County — kaufmancounty.net.

Kaufman publishes foreclosure listings on CivicPlus CMS pages keyed by
year, e.g. /383/Foreclosures (current) and /628/Foreclosures-2025.
Each page lists PDF notices as download links. We scrape the current
year's page for PDF anchors.
"""
import logging
import time
from datetime import date
from typing import Iterator
from urllib.parse import urljoin

from bs4 import BeautifulSoup

from parsers.pdf import extract_text
from scrapers.base import DistressRecord
from scrapers.trustee.common import TrusteeScraperBase, build_record

log = logging.getLogger(__name__)

BASE = "https://www.kaufmancounty.net"

# Known page IDs. If Kaufman adds a new year page we'll fall back to /383/Foreclosures.
# First-Tuesday year matters because notices stay posted ~month; we try both.
_CANDIDATES = [
    "/383/Foreclosures",
    f"/{{year_id}}/Foreclosures-{{year}}",  # e.g. /628/Foreclosures-2025
]


class KaufmanTrustee(TrusteeScraperBase):
    county = "kaufman"

    def fetch(self) -> Iterator[DistressRecord]:
        current_year = date.today().year
        pages = [
            f"{BASE}/383/Foreclosures",
            # Year-specific page ID is unknown until first run surfaces it.
            # /383 is the evergreen landing; it usually links to the year page.
        ]

        pdf_links: list[str] = []
        failures = 0
        for page in pages:
            try:
                r = self.session.get(page, timeout=30)
                r.raise_for_status()
            except Exception as e:
                log.warning("kaufman page %s failed: %s", page, e)
                failures += 1
                continue
            soup = BeautifulSoup(r.text, "lxml")
            for a in soup.find_all("a", href=True):
                href = a["href"]
                if href.lower().endswith(".pdf"):
                    pdf_links.append(urljoin(page, href))
                # Follow links to year-specific foreclosure pages.
                elif "Foreclosures" in href and str(current_year) in href:
                    pages.append(urljoin(page, href))

        pdf_links = list(dict.fromkeys(pdf_links))  # de-dup, preserve order
        if failures == len(pages) and not pdf_links:
            raise RuntimeError("kaufman: every landing page fetch failed")
        if not pdf_links:
            raise RuntimeError("kaufman: no PDF notices discovered on any page")

        log.info("kaufman: %d PDF notices", len(pdf_links))
        pdf_failures = 0
        for pdf_url in pdf_links:
            time.sleep(self.throttle_s)
            try:
                resp = self.session.get(pdf_url, timeout=60)
                resp.raise_for_status()
                text = extract_text(resp.content)
            except Exception as e:
                log.warning("kaufman pdf %s failed: %s", pdf_url, e)
                pdf_failures += 1
                continue
            rec = build_record(
                source=self.source, county=self.county,
                notice_url=pdf_url, notice_text=text,
            )
            if rec:
                yield rec

        if pdf_failures == len(pdf_links):
            raise RuntimeError("kaufman: every PDF fetch failed")
