"""Johnson County — johnsoncountytx.org.

First run got 403 on the simple UA-only request. Johnson's server seems
to require additional browser-like headers (Accept, Accept-Language,
Accept-Encoding). Adding them and retrying.
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

_BROWSER_HEADERS = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "DNT": "1",
    "Upgrade-Insecure-Requests": "1",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Sec-Fetch-User": "?1",
}


class JohnsonTrustee(TrusteeScraperBase):
    county = "johnson"

    def __init__(self, session=None):
        super().__init__(session)
        self.session.headers.update(_BROWSER_HEADERS)

    def fetch(self) -> Iterator[DistressRecord]:
        try:
            r = self.session.get(LANDING, timeout=30)
            r.raise_for_status()
        except Exception as e:
            raise RuntimeError(f"johnson landing fetch failed: {e}") from e

        soup = BeautifulSoup(r.text, "lxml")
        pdf_urls: list[str] = []
        seen: set[str] = set()
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if href.lower().endswith(".pdf"):
                full = urljoin(LANDING, href)
                if full not in seen:
                    seen.add(full)
                    pdf_urls.append(full)

        if not pdf_urls:
            log.warning("johnson: landing loaded but no PDFs linked — site may have moved")
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
