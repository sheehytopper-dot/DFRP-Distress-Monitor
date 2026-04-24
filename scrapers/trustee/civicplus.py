"""Shared scraper for CivicPlus / CivicEngage archive centers.

CivicEngage has two conventions for monthly archive items:
  (a) /ArchiveCenter/ViewFile/Item/{id}   — direct PDF view
  (b) Archive.aspx?ADID={id}              — detail page wrapping a PDF

Rockwall and Ellis both use form (b); Kaufman uses its own DocumentCenter
scheme (handled in scrapers/trustee/kaufman.py).

Archive listings themselves live at /Archive.aspx?AMID={amid}. Some
counties (Rockwall) use a different AMID per month (AMID=74=January,
AMID=78=May, ...). Subclasses can either set a fixed `amid` or override
`discover_amids()` to pick the current month dynamically.
"""
import logging
import re
import time
from typing import Iterator, Optional
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup

from parsers.pdf import extract_text
from scrapers.base import DistressRecord
from scrapers.trustee.common import TrusteeScraperBase, build_record

log = logging.getLogger(__name__)

_ITEM_LINK_RE = re.compile(
    r"(?:/ArchiveCenter/ViewFile/Item/\d+|Archive\.aspx\?ADID=\d+)",
    re.I,
)


class CivicPlusArchiveTrustee(TrusteeScraperBase):
    """Subclasses must set: county, base_url, and either amid or discover_amids()."""
    base_url: str = ""
    amid: int = 0

    def discover_amids(self) -> list[int]:
        """Override to dynamically pick month AMIDs. Default = [self.amid]."""
        return [self.amid] if self.amid else []

    def fetch(self) -> Iterator[DistressRecord]:
        if not self.base_url:
            raise RuntimeError(f"{type(self).__name__} missing base_url")

        amids = self.discover_amids()
        if not amids:
            raise RuntimeError(f"{self.county}: no AMIDs to scan")

        all_items: list[str] = []
        seen: set[str] = set()
        for amid in amids:
            archive_url = f"{self.base_url.rstrip('/')}/Archive.aspx?AMID={amid}"
            log.info("%s: scanning archive AMID=%d", self.county, amid)
            try:
                r = self.session.get(archive_url, timeout=30)
                r.raise_for_status()
            except Exception as e:
                log.warning("%s archive AMID=%d fetch failed: %s", self.county, amid, e)
                continue
            soup = BeautifulSoup(r.text, "lxml")
            for a in soup.find_all("a", href=True):
                if _ITEM_LINK_RE.search(a["href"]):
                    full = urljoin(archive_url, a["href"])
                    if full not in seen:
                        seen.add(full)
                        all_items.append(full)

        if not all_items:
            raise RuntimeError(f"{self.county}: no archive items found across AMIDs {amids}")

        log.info("%s: %d archive items across %d AMID(s)", self.county, len(all_items), len(amids))
        failures = 0
        for item_url in all_items:
            time.sleep(self.throttle_s)
            try:
                text = self._fetch_item_text(item_url)
            except Exception as e:
                log.warning("%s item %s failed: %s", self.county, item_url, e)
                failures += 1
                continue
            rec = self._consider(notice_url=item_url, notice_text=text)
            if rec:
                yield rec

        if failures == len(all_items):
            raise RuntimeError(f"{self.county}: every archive item fetch failed")

    def _fetch_item_text(self, url: str) -> str:
        """Fetch an item. Handles:
         - Direct PDF at the URL
         - HTML wrapper with a PDF <a> tag
         - Archive.aspx?ADID=N wrapper where the content is behind an
           iframe / JS viewer — fall back to /ArchiveCenter/ViewFile/Item/N
           with the same id (CivicEngage reuses ADID as item id).
        """
        resp = self.session.get(url, timeout=60)
        resp.raise_for_status()

        # Direct PDF
        if resp.content[:4] == b"%PDF":
            return extract_text(resp.content)

        soup = BeautifulSoup(resp.text, "lxml")
        pdf_link = _find_pdf_link(soup, base_url=url)
        if pdf_link:
            pdf_resp = self.session.get(pdf_link, timeout=60)
            pdf_resp.raise_for_status()
            if pdf_resp.content[:4] == b"%PDF":
                return extract_text(pdf_resp.content)

        # ADID fallback: try /ArchiveCenter/ViewFile/Item/{id}. CivicEngage
        # sites publish the same item under both URLs; the ADID view is an
        # HTML viewer, the ViewFile/Item is the raw PDF.
        m = re.search(r"[?&]ADID=(\d+)", url, re.I)
        if m:
            root = f"{urlparse(url).scheme}://{urlparse(url).netloc}"
            viewfile_url = f"{root}/ArchiveCenter/ViewFile/Item/{m.group(1)}"
            try:
                pdf_resp = self.session.get(viewfile_url, timeout=60)
                pdf_resp.raise_for_status()
                if pdf_resp.content[:4] == b"%PDF":
                    return extract_text(pdf_resp.content)
            except Exception as e:
                log.warning("%s ViewFile/Item fallback failed %s: %s",
                            self.county, viewfile_url, e)

        # Fallback — scrape any visible text (will be mostly empty for
        # CivicEngage viewers; signals to the caller that this item was
        # unextractable).
        return soup.get_text("\n", strip=True)


def _find_pdf_link(soup: BeautifulSoup, base_url: str) -> Optional[str]:
    """Find a likely PDF link on a wrapper page."""
    for a in soup.find_all("a", href=True):
        href = a["href"].lower()
        if href.endswith(".pdf") or "/archivecenter/viewfile/" in href or "/documentcenter/view/" in href:
            return urljoin(base_url, a["href"])
    return None


# -- Helpers for subclasses that discover AMIDs from an index page ----------

_AMID_LINK_RE = re.compile(r"Archive\.aspx\?AMID=(\d+)", re.I)


def discover_amids_from_page(session, index_url: str) -> list[int]:
    """Scrape a CivicEngage page for all Archive.aspx?AMID=N links."""
    r = session.get(index_url, timeout=30)
    r.raise_for_status()
    ids: list[int] = []
    seen: set[int] = set()
    for m in _AMID_LINK_RE.finditer(r.text):
        i = int(m.group(1))
        if i not in seen:
            seen.add(i)
            ids.append(i)
    return ids
