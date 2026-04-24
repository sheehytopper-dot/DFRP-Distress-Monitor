"""Weekly orchestrator entrypoint. Runs every enabled scraper, then sends both digests.

Usage:
    python run.py                  # normal weekly run
    python run.py --baseline       # first run: mark everything as baseline, suppress digest
    python run.py --no-email       # run scrapers only, skip sending
"""
import argparse
import logging
import sys

from db.connection import get_conn, init_db
from scrapers.auction_com import AuctionComScraper
from scrapers.base import BaseScraper
from scrapers.lgbs import LgbsScraper
from scrapers.pbfcm import PbfcmScraper
from scrapers.trustee.collin import CollinTrustee
from scrapers.trustee.dallas import DallasTrustee
from scrapers.trustee.denton import DentonTrustee
from scrapers.trustee.ellis import EllisTrustee
from scrapers.trustee.johnson import JohnsonTrustee
from scrapers.trustee.kaufman import KaufmanTrustee
from scrapers.trustee.rockwall import RockwallTrustee
from scrapers.trustee.tarrant import TarrantTrustee

log = logging.getLogger("run")

DISTRESS_SCRAPERS: list[type[BaseScraper]] = [
    # Phase 1 — statewide
    PbfcmScraper,
    LgbsScraper,
    # Phase 2 — per-county trustee notices
    DallasTrustee,
    TarrantTrustee,
    CollinTrustee,
    DentonTrustee,
    RockwallTrustee,
    KaufmanTrustee,
    EllisTrustee,
    JohnsonTrustee,
    # Phase 2b — auction.com + ten-x
    AuctionComScraper,
]


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--baseline", action="store_true",
                        help="Mark all new rows as baseline; suppresses digest.")
    parser.add_argument("--no-email", action="store_true",
                        help="Skip sending emails.")
    parser.add_argument("--preview", action="store_true",
                        help="Render the digest with baseline rows included so "
                             "you can see what it looks like without a fresh run. "
                             "Implies --no-email.")
    parser.add_argument("--only", default=None,
                        help="Comma-separated scraper source names to run (e.g. 'pbfcm,lgbs').")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    init_db()

    only = set(s.strip() for s in args.only.split(",")) if args.only else None
    results: dict[str, dict] = {}

    with get_conn() as conn:
        for cls in DISTRESS_SCRAPERS:
            inst = cls()
            if only and inst.source not in only:
                continue
            log.info("running scraper: %s", inst.source)
            try:
                results[inst.source] = inst.run(conn, baseline=args.baseline)
            except Exception:
                log.exception("orchestrator caught scraper crash (should have been caught in .run)")
                results[inst.source] = {"status": "failed", "error": "orchestrator-level crash"}

    log.info("scraper results: %s", results)

    # TODO: Phase 3 — per-county probate scrapers

    # Preview mode: always show baseline rows so the user sees real content
    # without having to do a live-then-baseline dance first.
    if args.preview or args.no_email or args.baseline:
        from alerts.digest import build_digest
        include_baseline = args.preview or args.baseline
        with get_conn() as conn:
            subject, html = build_digest(conn, include_baseline=include_baseline)
        log.info("(dry run) digest subject=%r html=%d chars", subject, len(html))
        # Print HTML body to a file so the user can see the actual output in
        # the workflow log via artifact.
        with open("db/digest_preview.html", "w") as f:
            f.write(html)
        log.info("digest preview written to db/digest_preview.html")
        return 0

    from alerts.digest import send_digest
    with get_conn() as conn:
        try:
            send_digest(conn)
        except Exception:
            log.exception("digest send failed")
            return 1

    # TODO: send probate report
    return 0


if __name__ == "__main__":
    sys.exit(main())
