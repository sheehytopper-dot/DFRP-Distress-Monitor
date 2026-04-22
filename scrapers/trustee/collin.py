"""Collin County — apps.collincountytx.gov/ForeclosureNotices.

This is an ASP.NET MVC app at /ForeclosureNotices/Property/Index with
paginated results and filter params (showAllCities, showAllPropTypes,
searchString, cityFilter, propTypeFilter, saleDateFilter, pageNumber).

Strategy: request pages 1..N until an empty page comes back, scrape the
result table, and for each row follow the detail link to get the full
notice text (that's where the original-principal amount lives).
"""
import logging
import time
from typing import Iterator
from urllib.parse import urljoin

from bs4 import BeautifulSoup

from scrapers.base import DistressRecord
from scrapers.trustee.common import TrusteeScraperBase, build_record

log = logging.getLogger(__name__)

BASE = "https://apps.collincountytx.gov/ForeclosureNotices"
INDEX = f"{BASE}/Property/Index"
MAX_PAGES = 25  # safety cap; Collin rarely has >10 pages


class CollinTrustee(TrusteeScraperBase):
    county = "collin"

    def fetch(self) -> Iterator[DistressRecord]:
        index_failures = 0
        detail_links = list(self._iter_detail_links())

        if not detail_links:
            # No rows could mean empty month or total block — treat as failure.
            raise RuntimeError("collin: no detail links found across all pages")

        log.info("collin: %d detail links", len(detail_links))
        for link in detail_links:
            time.sleep(self.throttle_s)
            try:
                text = self._fetch_notice_text(link)
            except Exception as e:
                log.warning("collin notice fetch failed %s: %s", link, e)
                index_failures += 1
                continue
            rec = build_record(
                source=self.source,
                county=self.county,
                notice_url=link,
                notice_text=text,
            )
            if rec:
                yield rec

        if index_failures and index_failures == len(detail_links):
            raise RuntimeError("collin: all detail fetches failed")

    def _iter_detail_links(self) -> Iterator[str]:
        for page in range(1, MAX_PAGES + 1):
            params = {
                "showAllCities": "true",
                "showAllPropTypes": "true",
                "pageNumber": str(page),
                "searchString": "",
            }
            r = self.session.get(INDEX, params=params, timeout=30)
            r.raise_for_status()
            soup = BeautifulSoup(r.text, "lxml")
            rows = soup.select("table a[href*='Property/Details']") or \
                   soup.select("a[href*='/ForeclosureNotices/Property/']")
            if not rows:
                log.info("collin: page %d empty, stopping", page)
                break
            for a in rows:
                href = a.get("href", "").strip()
                if href:
                    yield urljoin(BASE + "/", href)

    def _fetch_notice_text(self, url: str) -> str:
        r = self.session.get(url, timeout=30)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "lxml")
        # Detail pages typically embed the notice PDF or display it inline.
        # We'll scrape all visible text; if a PDF link is present, download + extract.
        pdf_a = soup.find("a", href=lambda h: h and h.lower().endswith(".pdf"))
        if pdf_a:
            pdf_url = urljoin(url, pdf_a["href"])
            pdf_resp = self.session.get(pdf_url, timeout=60)
            pdf_resp.raise_for_status()
            from parsers.pdf import extract_text
            return extract_text(pdf_resp.content)
        # Fallback: text content of the main area
        main = soup.find("main") or soup.find("div", class_=["container", "content", "main-content"]) or soup.body
        return main.get_text("\n", strip=True) if main else soup.get_text("\n", strip=True)
