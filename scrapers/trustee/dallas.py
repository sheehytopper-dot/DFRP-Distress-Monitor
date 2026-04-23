"""Dallas County — dallascounty.org/government/county-clerk/recording/foreclosures.php.

Dallas publishes individual notice PDFs organized into monthly folders
with city-indexed filenames, e.g.:
  dallascounty.org/department/countyclerk/media/foreclosure/November/Dallas_4.pdf
  dallascounty.org/department/countyclerk/media/foreclosure/March/Irving_5.pdf

The foreclosures.php landing page exposes a searchable interface that
ultimately links to these PDFs. Rather than drive the search UI, we
scrape every <a href="*.pdf"> under /department/countyclerk/media/foreclosure/
that we can find on the landing page. Missing months (expired notices)
naturally drop out once the county rotates them.
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

LANDING = "https://www.dallascounty.org/government/county-clerk/recording/foreclosures.php"
NOTICE_PATH = "/department/countyclerk/media/foreclosure/"


class DallasTrustee(TrusteeScraperBase):
    county = "dallas"

    def fetch(self) -> Iterator[DistressRecord]:
        try:
            pdf_links = self._discover_pdfs()
        except Exception as e:
            raise RuntimeError(f"dallas landing fetch failed: {e}") from e

        if not pdf_links:
            raise RuntimeError("dallas: no PDFs discovered on landing page")

        log.info("dallas: %d candidate notice PDFs", len(pdf_links))
        pdf_failures = 0
        yielded = 0

        for pdf_url in pdf_links:
            time.sleep(self.throttle_s)
            try:
                r = self.session.get(pdf_url, timeout=60)
                r.raise_for_status()
                text = extract_text(r.content)
            except Exception as e:
                log.warning("dallas pdf %s failed: %s", pdf_url, e)
                pdf_failures += 1
                continue
            rec = self._consider(notice_url=pdf_url, notice_text=text)
            if rec:
                yielded += 1
                yield rec

        if pdf_failures == len(pdf_links):
            raise RuntimeError("dallas: every notice PDF fetch failed")
        log.info("dallas: %d notices passed filter", yielded)

    def _discover_pdfs(self) -> list[str]:
        r = self.session.get(LANDING, timeout=30)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "lxml")
        out: list[str] = []
        seen: set[str] = set()
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if NOTICE_PATH in href and href.lower().endswith(".pdf"):
                full = urljoin(LANDING, href)
                if full not in seen:
                    seen.add(full)
                    out.append(full)
        return out
