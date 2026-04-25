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
    # Probate is suppressed when there are zero filings AND zero successful
    # probate scrapers — sending an all-failures email every week is just noise
    # while the per-county portals remain unimplemented.
    email_status: dict[str, dict] = {}
    with get_conn() as conn:
        probate_filings_count = conn.execute(
            "SELECT COUNT(*) FROM probate_filings"
        ).fetchone()[0]
        successful_probate_runs = conn.execute(
            "SELECT COUNT(*) FROM scrape_runs "
            "WHERE source LIKE 'probate_%' AND status='ok' AND rows_found > 0"
        ).fetchone()[0]
        suppress_probate = probate_filings_count == 0 and successful_probate_runs == 0
        if suppress_probate:
            log.info("probate report suppressed: no filings, no successful probate scrapers")
            email_status["probate_report"] = {"ok": False, "error": "suppressed (no data yet)"}

        try:
            email_id = send_digest(conn)
            email_status["distress_digest"] = {"ok": True, "id": email_id}
        except Exception as e:
            log.exception("distress digest send failed")
            email_status["distress_digest"] = {"ok": False, "error": f"{type(e).__name__}: {e}"}

        if not suppress_probate:
            try:
                email_id = send_report(conn)
                email_status["probate_report"] = {"ok": True, "id": email_id}
            except Exception as e:
                log.exception("probate report send failed")
                email_status["probate_report"] = {"ok": False, "error": f"{type(e).__name__}: {e}"}

    # Loud status block — appears verbatim in the workflow log so a missing
    # secret or rejected send is impossible to miss.
    print("\n" + "=" * 60)
    print("EMAIL STATUS")
    print("=" * 60)
    for label, status in email_status.items():
        if status.get("ok"):
            print(f"  {label:<18} SENT  resend_id={status.get('id')}")
        else:
            print(f"  {label:<18} FAILED  {status.get('error')}")
    print("=" * 60)
    return 0


if __name__ == "__main__":
    sys.exit(main())
