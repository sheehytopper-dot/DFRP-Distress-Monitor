"""Rockwall County — rockwallcountytexas.com.

Rockwall uses one AMID per month (e.g. AMID=74 for January, AMID=78 for
May). Hard-coding one number breaks every other month. We scrape
/792/Foreclosure-Notices (the public landing page) for any
Archive.aspx?AMID=N links and feed them into the shared CivicPlus
scraper.
"""
import logging

from scrapers.trustee.civicplus import (
    CivicPlusArchiveTrustee,
    discover_amids_from_page,
)

log = logging.getLogger(__name__)


class RockwallTrustee(CivicPlusArchiveTrustee):
    county = "rockwall"
    base_url = "https://www.rockwallcountytexas.com"

    def discover_amids(self) -> list[int]:
        landing = f"{self.base_url}/792/Foreclosure-Notices"
        try:
            ids = discover_amids_from_page(self.session, landing)
        except Exception as e:
            log.warning("rockwall AMID discovery failed: %s", e)
            return []
        log.info("rockwall discovered AMIDs: %s", ids)
        return ids
