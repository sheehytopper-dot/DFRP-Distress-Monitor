"""Kaufman County — kaufmancounty.net.

Kaufman puts foreclosure notices on a year-specific CivicEngage page
(/658/Foreclosure-2026 for 2026). Each month's notice PDF is linked
via DocumentCenter, e.g. /DocumentCenter/View/10354/6-June-2026.

We visit both the generic /383/Foreclosures landing (which sometimes
links to the current year page) and the year page directly, then grab
every /DocumentCenter/View/* link and PDF anchor.
"""
import logging
import re
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

_DOC_RE = re.compile(r"/DocumentCenter/View/\d+", re.I)


class KaufmanTrustee(TrusteeScraperBase):
    county = "kaufman"

    def fetch(self) -> Iterator[DistressRecord]:
        year = date.today().year
        start_pages = [
            f"{BASE}/{year - 2008}/Foreclosure-{year}",  # CivicEngage IDs trend linearly; fuzzy first guess
            f"{BASE}/658/Foreclosure-{year}",            # known for 2026
            f"{BASE}/628/Foreclosure-{year - 1}",        # known for 2025
            f"{BASE}/383/Foreclosures",                  # evergreen landing
        ]
        doc_links: list[str] = []
        seen: set[str] = set()
        landing_failures = 0
        attempted = 0

        for page_url in start_pages:
            attempted += 1
            try:
                r = self.session.get(page_url, timeout=30)
                r.raise_for_status()
            except Exception as e:
                log.warning("kaufman page %s failed: %s", page_url, e)
                landing_failures += 1
                continue
            soup = BeautifulSoup(r.text, "lxml")
            for a in soup.find_all("a", href=True):
                href = a["href"]
                if _DOC_RE.search(href) or href.lower().endswith(".pdf"):
                    full = urljoin(page_url, href)
                    if full not in seen:
                        seen.add(full)
                        doc_links.append(full)

        if landing_failures == attempted:
            raise RuntimeError("kaufman: every landing page fetch failed")
        if not doc_links:
            raise RuntimeError("kaufman: no notice PDFs discovered on any page")

        log.info("kaufman: %d candidate notice documents", len(doc_links))
        failures = 0
        for url in doc_links:
            time.sleep(self.throttle_s)
            try:
                resp = self.session.get(url, timeout=60)
                resp.raise_for_status()
                if resp.content[:4] == b"%PDF":
                    text = extract_text(resp.content)
                else:
                    # DocumentCenter sometimes redirects / wraps; try to pull the inner PDF
                    text = BeautifulSoup(resp.text, "lxml").get_text("\n", strip=True)
            except Exception as e:
                log.warning("kaufman doc %s failed: %s", url, e)
                failures += 1
                continue
            rec = build_record(
                source=self.source, county=self.county,
                notice_url=url, notice_text=text,
            )
            if rec:
                yield rec

        if failures == len(doc_links):
            raise RuntimeError("kaufman: every document fetch failed")
