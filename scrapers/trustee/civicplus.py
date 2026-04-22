"""Shared scraper for CivicPlus Archive Centers.

URL pattern is identical across CivicPlus-hosted county sites:
  /Archive.aspx?AMID={amid}             — list of archived items
  /ArchiveCenter/ViewFile/Item/{id}     — individual item (usually PDF)

Rockwall and Ellis both publish foreclosure notices as CivicPlus
archives, so both subclass this with their AMID and base URL.
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

_ITEM_LINK_RE = re.compile(r"/ArchiveCenter/ViewFile/Item/\d+", re.I)


class CivicPlusArchiveTrustee(TrusteeScraperBase):
    """Subclasses must set: county, base_url, amid."""
    base_url: str = ""
    amid: int = 0

    def fetch(self) -> Iterator[DistressRecord]:
        if not self.base_url or not self.amid:
            raise RuntimeError(f"{type(self).__name__} missing base_url or amid")

        archive_url = f"{self.base_url.rstrip('/')}/Archive.aspx?AMID={self.amid}"
        try:
            r = self.session.get(archive_url, timeout=30)
            r.raise_for_status()
        except Exception as e:
            raise RuntimeError(f"{self.county}: archive page fetch failed: {e}") from e

        soup = BeautifulSoup(r.text, "lxml")
        item_links: list[str] = []
        seen: set[str] = set()
        for a in soup.find_all("a", href=True):
            if _ITEM_LINK_RE.search(a["href"]):
                full = urljoin(archive_url, a["href"])
                if full not in seen:
                    seen.add(full)
                    item_links.append(full)

        if not item_links:
            raise RuntimeError(f"{self.county}: no archive items found at {archive_url}")

        log.info("%s: %d archive items", self.county, len(item_links))
        failures = 0
        for item_url in item_links:
            time.sleep(self.throttle_s)
            try:
                resp = self.session.get(item_url, timeout=60)
                resp.raise_for_status()
                text = extract_text(resp.content) if resp.content.startswith(b"%PDF") \
                    else BeautifulSoup(resp.text, "lxml").get_text("\n", strip=True)
            except Exception as e:
                log.warning("%s item %s failed: %s", self.county, item_url, e)
                failures += 1
                continue
            rec = build_record(
                source=self.source,
                county=self.county,
                notice_url=item_url,
                notice_text=text,
            )
            if rec:
                yield rec

        if failures == len(item_links):
            raise RuntimeError(f"{self.county}: every archive item fetch failed")
