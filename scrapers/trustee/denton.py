"""Denton County — dentoncounty.gov/294/Foreclosure-Sale-Property-Search.

County documentation states notices are 'neither indexed nor searchable'
and 'each month's file is destroyed the day after the sale'. That makes
scraping via the public site low-yield.

Denton also uses GovEase for online sales. We scrape the search page
for any PDF anchors (best-effort) and log clearly if nothing's there so
we can reroute to a different source in a follow-up commit.
"""
import logging
import time
from typing import Iterator
from urllib.parse import urljoin

from bs4 import BeautifulSoup

from parsers.pdf import extract_text
from scrapers.base import DistressRecord
from scrapers.trustee.common import TrusteeScraperBase, build_record

log = logging.getLogger(__name__)

CANDIDATE_URLS = [
    "https://www.dentoncounty.gov/294/Foreclosure-Sale-Property-Search",
    "https://www.dentoncounty.gov/293/Foreclosure-Information",
]


class DentonTrustee(TrusteeScraperBase):
    county = "denton"

    def fetch(self) -> Iterator[DistressRecord]:
        failures = 0
        pdf_urls: set[str] = set()
        for url in CANDIDATE_URLS:
            try:
                r = self.session.get(url, timeout=30)
                r.raise_for_status()
            except Exception as e:
                log.warning("denton %s failed: %s", url, e)
                failures += 1
                continue
            soup = BeautifulSoup(r.text, "lxml")
            for a in soup.find_all("a", href=True):
                href = a["href"]
                if href.lower().endswith(".pdf"):
                    pdf_urls.add(urljoin(url, href))

        if failures == len(CANDIDATE_URLS):
            raise RuntimeError("denton: all landing pages failed")
        if not pdf_urls:
            log.warning(
                "denton: no notice PDFs on landing pages — county doesn't "
                "publish an online list. Consider GovEase or CivicEngage API."
            )
            return

        log.info("denton: %d candidate PDFs", len(pdf_urls))
        for pdf_url in sorted(pdf_urls):
            time.sleep(self.throttle_s)
            try:
                resp = self.session.get(pdf_url, timeout=60)
                resp.raise_for_status()
                text = extract_text(resp.content)
            except Exception as e:
                log.warning("denton pdf %s failed: %s", pdf_url, e)
                continue
            rec = self._consider(notice_url=pdf_url, notice_text=text)
            if rec:
                yield rec
