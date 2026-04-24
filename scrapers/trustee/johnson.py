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
from scrapers.playwright_util import render_html
from scrapers.trustee.common import TrusteeScraperBase, build_record

log = logging.getLogger(__name__)

LANDING = "https://www.johnsoncountytx.org/government/county-clerk/land-records-vitals/foreclosure-sales"


class JohnsonTrustee(TrusteeScraperBase):
    county = "johnson"

    def fetch(self) -> Iterator[DistressRecord]:
        html = ""
        captured_pdfs: list[str] = []
        try:
            r = self.session.get(LANDING, timeout=30)
            r.raise_for_status()
            html = r.text
        except Exception as e:
            log.warning("johnson landing static fetch failed (%s); trying Playwright", e)
            html, captured_pdfs = render_html(LANDING, wait_ms=5000, capture_pdfs=True)
            if not html and not captured_pdfs:
                raise RuntimeError(f"johnson landing fetch failed (static + Playwright): {e}") from e

        soup = BeautifulSoup(html, "lxml") if html else None
        pdf_urls: list[str] = list(captured_pdfs)  # start with any PDFs we saw while rendering
        seen: set[str] = set(pdf_urls)
        if soup:
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
            rec = self._consider(notice_url=pdf_url, notice_text=text)
            if rec:
                yield rec

        if failures == len(pdf_urls):
            raise RuntimeError("johnson: every PDF fetch failed")
