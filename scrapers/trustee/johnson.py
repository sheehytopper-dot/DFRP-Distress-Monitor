"""Johnson County — johnsoncountytx.org.

Public info says notices are online at the foreclosure-sales page. We
scrape that page for PDF download links.
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

LANDING = "https://www.johnsoncountytx.org/government/county-clerk/land-records-vitals/foreclosure-sales"


class JohnsonTrustee(TrusteeScraperBase):
    county = "johnson"

    def fetch(self) -> Iterator[DistressRecord]:
        try:
            r = self.session.get(LANDING, timeout=30)
            r.raise_for_status()
        except Exception as e:
            raise RuntimeError(f"johnson landing fetch failed: {e}") from e

        soup = BeautifulSoup(r.text, "lxml")
        pdf_urls: list[str] = []
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if href.lower().endswith(".pdf"):
                pdf_urls.append(urljoin(LANDING, href))
        pdf_urls = list(dict.fromkeys(pdf_urls))

        if not pdf_urls:
            log.warning("johnson: no notice PDFs found on landing page")
            return

        log.info("johnson: %d notice PDFs", len(pdf_urls))
        failures = 0
        for pdf_url in pdf_urls:
            time.sleep(self.throttle_s)
            try:
                resp = self.session.get(pdf_url, timeout=60)
                resp.raise_for_status()
                text = extract_text(resp.content)
            except Exception as e:
                log.warning("johnson pdf %s failed: %s", pdf_url, e)
                failures += 1
                continue
            rec = build_record(
                source=self.source, county=self.county,
                notice_url=pdf_url, notice_text=text,
            )
            if rec:
                yield rec

        if failures == len(pdf_urls):
            raise RuntimeError("johnson: every PDF fetch failed")
