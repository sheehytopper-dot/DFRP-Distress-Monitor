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

log = logging.getLogger("run")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--baseline", action="store_true",
                        help="Mark all new rows as baseline; suppresses digest.")
    parser.add_argument("--no-email", action="store_true",
                        help="Skip sending emails.")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    init_db()

    with get_conn() as conn:
        # TODO: Phase 1 — lgbs, pbfcm
        # TODO: Phase 2 — per-county trustee scrapers
        # TODO: Phase 2b — auction.com
        # TODO: Phase 3 — per-county probate scrapers
        log.info("no scrapers registered yet; scaffold only")

    if args.no_email or args.baseline:
        log.info("skipping email (baseline=%s, no_email=%s)", args.baseline, args.no_email)
        return 0

    # TODO: send distress digest
    # TODO: send probate report
    return 0


if __name__ == "__main__":
    sys.exit(main())
