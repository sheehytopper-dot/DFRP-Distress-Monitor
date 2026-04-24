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
from scrapers.probate.collin import CollinProbate
from scrapers.probate.dallas import DallasProbate
from scrapers.probate.denton import DentonProbate
from scrapers.probate.ellis import EllisProbate
from scrapers.probate.hunt import HuntProbate
from scrapers.probate.johnson import JohnsonProbate
from scrapers.probate.kaufman import KaufmanProbate
from scrapers.probate.rockwall import RockwallProbate
from scrapers.probate.tarrant import TarrantProbate
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

PROBATE_SCRAPERS = [
    DallasProbate,
    TarrantProbate,
    CollinProbate,
    DentonProbate,
    RockwallProbate,
    KaufmanProbate,
    EllisProbate,
    JohnsonProbate,
    HuntProbate,
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

    # Phase 3 — probate scrapers. Each gets its own scrape_runs row via the
    # separate ProbateScraperBase.run() so failures don't block the distress
    # digest from sending.
    probate_results: dict[str, dict] = {}
    with get_conn() as conn:
        for cls in PROBATE_SCRAPERS:
            inst = cls()
            if only and inst.source not in only:
                continue
            log.info("running probate scraper: %s", inst.source)
            try:
                probate_results[inst.source] = inst.run(conn, baseline=args.baseline)
            except Exception:
                log.exception("orchestrator caught probate crash")
                probate_results[inst.source] = {"status": "failed", "error": "crash"}
    log.info("probate results: %s", probate_results)

    from alerts.digest import build_digest, send_digest
    from alerts.probate_report import build_report, send_report

    if args.preview or args.no_email or args.baseline:
        include_baseline = args.preview or args.baseline
        with get_conn() as conn:
            d_subject, d_html = build_digest(conn, include_baseline=include_baseline)
            p_subject, p_html = build_report(conn, include_baseline=include_baseline)
        with open("db/digest_preview.html", "w") as f:
            f.write(d_html)
        with open("db/probate_preview.html", "w") as f:
            f.write(p_html)
        log.info("(dry run) digest=%r probate=%r", d_subject, p_subject)
        log.info("previews written to db/digest_preview.html + db/probate_preview.html")
        return 0

    # Send both emails. Capture exceptions so one failing doesn't block the other.
    with get_conn() as conn:
        for send_fn, label in [(send_digest, "distress digest"),
                               (send_report, "probate report")]:
            try:
                send_fn(conn)
            except Exception:
                log.exception("%s send failed", label)
    return 0


if __name__ == "__main__":
    sys.exit(main())
