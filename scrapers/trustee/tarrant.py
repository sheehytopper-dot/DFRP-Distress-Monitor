"""Tarrant County — tarrantcountytx.gov + tarrantcounty.granicus.com.

The clerk's office explicitly does NOT publish a list; each notice is a
standalone PDF on Granicus with an opaque filename hash. Best we can do
without a list endpoint is scrape the foreclosures landing page for any
links to Granicus docs and follow them.

Very likely to return few/zero rows initially. First live run's logs
will tell us whether the landing page exposes anything and point us to
a better ingress (e.g. the Commercial Record, which publishes a monthly
list but requires subscription).
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

LANDING_URLS = [
    "https://www.tarrantcountytx.gov/en/county-clerk/real-estate-records/foreclosures.html",
    "https://www.tarrantcounty.com/en/county-clerk/administration/foreclosures.html",
]

_GRANICUS_RE = re.compile(r"tarrantcounty\.granicus\.com/DocumentViewer\.php\?file=[\w]+\.pdf", re.I)


class TarrantTrustee(TrusteeScraperBase):
    county = "tarrant"

    def fetch(self) -> Iterator[DistressRecord]:
        landing_failures = 0
        pdf_urls: set[str] = set()

        for url in LANDING_URLS:
            try:
                r = self.session.get(url, timeout=30)
                r.raise_for_status()
            except Exception as e:
                log.warning("tarrant landing %s failed: %s", url, e)
                landing_failures += 1
                continue
            soup = BeautifulSoup(r.text, "lxml")
            for a in soup.find_all("a", href=True):
                href = a["href"]
                if _GRANICUS_RE.search(href) or href.lower().endswith(".pdf"):
                    pdf_urls.add(urljoin(url, href))
            # Also pull raw href-like strings from script/text if clerk embeds
            # links in JavaScript (Granicus sometimes does).
            for m in _GRANICUS_RE.finditer(r.text):
                pdf_urls.add("https://" + m.group(0))

        if landing_failures == len(LANDING_URLS):
            raise RuntimeError("tarrant: all landing pages failed")
        if not pdf_urls:
            log.warning(
                "tarrant: no notice PDFs found on landing pages. "
                "County may publish via Granicus search/API instead; needs investigation."
            )
            return

        log.info("tarrant: %d candidate notice PDFs", len(pdf_urls))
        for pdf_url in sorted(pdf_urls):
            time.sleep(self.throttle_s)
            try:
                resp = self.session.get(pdf_url, timeout=60)
                resp.raise_for_status()
                text = extract_text(resp.content)
            except Exception as e:
                log.warning("tarrant pdf %s failed: %s", pdf_url, e)
                continue
            rec = build_record(
                source=self.source, county=self.county,
                notice_url=pdf_url, notice_text=text,
            )
            if rec:
                yield rec
