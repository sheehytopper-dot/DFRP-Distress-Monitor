"""Placeholder for counties whose probate source hasn't been wired yet.

Subclasses set county and description. fetch() raises with a clear message
that lands in scrape_runs.error_message so the digest sees it. The
infrastructure (DB, parsers, report) is ready to ingest as soon as we
build a real scraper for that county.
"""
from typing import Iterator

from scrapers.probate.base import ProbateFiling, ProbateScraperBase


class StubProbateScraper(ProbateScraperBase):
    """Subclasses set county + source_note."""
    source_note: str = "not yet implemented"

    def fetch(self) -> Iterator[ProbateFiling]:
        raise RuntimeError(
            f"probate_{self.county}: {self.source_note}. "
            "Portal requires Tyler Odyssey / CAPTCHA automation — deferred "
            "until we have a workable ingress."
        )
