"""Collin County — apps.collincountytx.gov/ForeclosureNotices.

The root app URL is /ForeclosureNotices (the /Property/Index path we
tried first 404s). The app also has a mirror at apps2.collincountytx.gov.
Detail pages are at /ForeclosureNotices/DetailPage/{id}.

Strategy: fetch the landing page, scrape every DetailPage link, visit
each and extract notice text. Try both subdomains so a move between
them doesn't break us.
"""
import logging
import re
import time
from typing import Iterator
from urllib.parse import urljoin

from bs4 import BeautifulSoup

from parsers.pdf import extract_text
from scrapers.base import DistressRecord
from scrapers.trustee.common import TrusteeScraperBase, build_record

log = logging.getLogger(__name__)

CANDIDATE_BASES = [
    "https://apps.collincountytx.gov/ForeclosureNotices",
    "https://apps2.collincountytx.gov/ForeclosureNotices",
]

_DETAIL_RE = re.compile(r"/ForeclosureNotices/DetailPage/\d+", re.I)


class CollinTrustee(TrusteeScraperBase):
    county = "collin"

    def fetch(self) -> Iterator[DistressRecord]:
        detail_links: list[str] = []
        seen: set[str] = set()
        landing_failures = 0

        for base in CANDIDATE_BASES:
            try:
                r = self.session.get(base, timeout=30)
                r.raise_for_status()
            except Exception as e:
                log.warning("collin landing %s failed: %s", base, e)
                landing_failures += 1
                continue
            soup = BeautifulSoup(r.text, "lxml")
            for a in soup.find_all("a", href=True):
                if _DETAIL_RE.search(a["href"]):
                    full = urljoin(base, a["href"])
                    if full not in seen:
                        seen.add(full)
                        detail_links.append(full)

        if landing_failures == len(CANDIDATE_BASES):
            raise RuntimeError("collin: both landing pages failed")
        if not detail_links:
            raise RuntimeError(f"collin: landing loaded but no DetailPage links found (tried {len(CANDIDATE_BASES)} bases)")

        log.info("collin: %d detail links", len(detail_links))
        failures = 0
        for url in detail_links:
            time.sleep(self.throttle_s)
            try:
                text = self._fetch_notice_text(url)
            except Exception as e:
                log.warning("collin detail %s failed: %s", url, e)
                failures += 1
                continue
            rec = build_record(
                source=self.source,
                county=self.county,
                notice_url=url,
                notice_text=text,
            )
            if rec:
                yield rec

        if failures == len(detail_links):
            raise RuntimeError("collin: every detail fetch failed")

    def _fetch_notice_text(self, url: str) -> str:
        r = self.session.get(url, timeout=30)
        r.raise_for_status()
        # Some notices display as embedded PDFs; others render inline text.
        if r.content[:4] == b"%PDF":
            return extract_text(r.content)
        soup = BeautifulSoup(r.text, "lxml")
        for a in soup.find_all("a", href=True):
            href = a["href"].lower()
            if href.endswith(".pdf"):
                pdf_url = urljoin(url, a["href"])
                pdf_resp = self.session.get(pdf_url, timeout=60)
                pdf_resp.raise_for_status()
                if pdf_resp.content[:4] == b"%PDF":
                    return extract_text(pdf_resp.content)
        # Fallback: all visible text on the detail page.
        main = soup.find("main") or soup.find("div", class_=["container", "content", "main-content"]) or soup.body
        return main.get_text("\n", strip=True) if main else soup.get_text("\n", strip=True)
