"""Shared helpers for county trustee-sale scrapers.

Each county has its own source module (scrapers/trustee/{county}.py) that
subclasses TrusteeScraperBase. The base:
  - defines source as 'trustee_{county}'
  - gives every subclass a shared requests.Session with a realistic UA
  - offers one helper for building a DistressRecord from notice text so
    every county emits the same shape.
"""
import hashlib
import logging
from typing import Optional

import requests

from config.settings import DISTRESS_MIN_USD
from parsers.amounts import largest_amount
from parsers.notices import (
    extract_address,
    extract_borrower,
    extract_original_principal,
    extract_sale_date,
    is_past_sale,
)
from parsers.property_type import classify
from scrapers.base import BaseScraper, DistressRecord

log = logging.getLogger(__name__)

USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 "
    "(KHTML, like Gecko) Version/17.0 Safari/605.1.15"
)


class TrusteeScraperBase(BaseScraper):
    county: str = ""  # subclasses must set
    throttle_s: float = 1.5

    def __init__(self, session: Optional[requests.Session] = None):
        self.session = session or requests.Session()
        self.session.headers["User-Agent"] = USER_AGENT
        if not self.county:
            raise ValueError(f"{type(self).__name__} must set county")

    @property
    def source(self) -> str:
        return f"trustee_{self.county}"


def build_record(
    *,
    source: str,
    county: str,
    notice_url: str,
    notice_text: str,
) -> Optional[DistressRecord]:
    """Turn a notice text into a DistressRecord if it passes the filter.

    Filter: largest $ in text >= $DISTRESS_MIN_USD (user's "original loan
    proxy"). We also store the labeled 'Original Principal Amount' in extra
    when present so downstream diagnostics can see both numbers.
    """
    amount = largest_amount(notice_text)
    labeled_principal = extract_original_principal(notice_text)
    if amount is None or amount < DISTRESS_MIN_USD:
        return None

    sale_date = extract_sale_date(notice_text)
    if is_past_sale(sale_date):
        log.debug("dropping past sale %s for %s", sale_date, notice_url)
        return None

    address = extract_address(notice_text)
    ptype = classify(notice_text)
    source_id = hashlib.sha1(notice_url.encode()).hexdigest()[:16]

    return DistressRecord(
        source=source,
        source_id=source_id,
        county=county,
        url=notice_url,
        property_address=address,
        property_type=ptype,
        amount_usd=amount,
        amount_kind="original_loan_proxy",
        sale_date=sale_date,
        raw_text=notice_text[:8000],
        extra={
            "labeled_principal": labeled_principal,
            "borrower": extract_borrower(notice_text),
        },
    )
