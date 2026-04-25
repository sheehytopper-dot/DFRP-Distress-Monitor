"""Shared base class for probate scrapers.

Each county has its own source module. All subclasses share:
 - source name 'probate_{county}'
 - a requests.Session with browser-like headers
 - a _consider() wrapper that filters case type + upserts probate_filings
   and probate_attorneys rows, tracking drop reasons for diagnostics

ProbateScraper subclasses yield ProbateFiling dataclasses from fetch().
The base.run() method will handle upsert + counter tracking.

Phase 3 is intentionally light on county scrapers — most TX probate
records live behind Tyler Odyssey portals that require click-through
ToS acceptance and CAPTCHA. County-specific fetchers will be filled in
iteratively. The infrastructure (schema, parsers, report) is ready.
"""
import hashlib
import logging
import sqlite3
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Iterator, Optional

import requests

from parsers.probate import (
    classify_case_type,
    extract_applicant,
    extract_attorney,
    extract_bar_number,
    extract_decedent,
    involves_real_estate,
    normalize_name,
)

log = logging.getLogger(__name__)

USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 "
    "(KHTML, like Gecko) Version/17.0 Safari/605.1.15"
)
_DEFAULT_HEADERS = {
    "User-Agent": USER_AGENT,
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Upgrade-Insecure-Requests": "1",
}


@dataclass
class ProbateFiling:
    """Raw fields collected from a probate source. build_filing() converts
    this into the right DB rows."""
    county: str
    case_number: str
    case_type: Optional[str] = None  # will be classified if None
    filed_date: Optional[str] = None
    decedent_name: Optional[str] = None
    applicant_name: Optional[str] = None
    applicant_address: Optional[str] = None
    applicant_phone: Optional[str] = None
    attorney_name: Optional[str] = None
    attorney_firm: Optional[str] = None
    attorney_bar_number: Optional[str] = None
    attorney_phone: Optional[str] = None
    attorney_email: Optional[str] = None
    attorney_address: Optional[str] = None
    url: Optional[str] = None
    raw_text: Optional[str] = None


class ProbateScraperBase:
    """Subclasses set `county` and implement fetch()."""
    county: str = ""
    throttle_s: float = 1.5

    def __init__(self, session: Optional[requests.Session] = None):
        if not self.county:
            raise ValueError(f"{type(self).__name__} must set county")
        self.session = session or requests.Session()
        self.session.headers.update(_DEFAULT_HEADERS)
        self.filings_considered = 0
        self.drop_reasons: dict[str, int] = {}
        self.sample_text: Optional[str] = None

    @property
    def source(self) -> str:
        return f"probate_{self.county}"

    def fetch(self) -> Iterator[ProbateFiling]:
        raise NotImplementedError

    def run(self, conn: sqlite3.Connection, baseline: bool = False) -> dict:
        started = _utcnow()
        run_id = _insert_run(conn, self.source, started)
        rows_new = 0
        rows_found = 0
        error: Optional[str] = None
        try:
            for filing in self.fetch():
                self.filings_considered += 1
                accepted, is_new = upsert_filing(conn, filing, started, baseline,
                                                 self.drop_reasons)
                if accepted:
                    rows_found += 1
                    if is_new:
                        rows_new += 1
            status = "ok"
        except Exception as e:
            log.exception("probate scraper %s failed", self.source)
            error = f"{type(e).__name__}: {e}"
            status = "failed"

        import json
        drop_json = json.dumps(self.drop_reasons) if self.drop_reasons else None
        sample = self.sample_text[:2000] if self.sample_text else None
        _finish_run(conn, run_id, _utcnow(), status,
                    self.filings_considered, rows_found, rows_new,
                    drop_json, sample, error)
        log.info("%s summary: considered=%d kept=%d new=%d drops=%s status=%s",
                 self.source, self.filings_considered, rows_found, rows_new,
                 self.drop_reasons or {}, status)
        return {"status": status, "considered": self.filings_considered,
                "found": rows_found, "new": rows_new, "error": error}


def upsert_filing(
    conn: sqlite3.Connection,
    filing: ProbateFiling,
    now: str,
    baseline: bool,
    reasons: dict,
) -> tuple[bool, bool]:
    """Write filing to probate_filings. Returns (accepted, is_new).

    Accepted = case_type was muniment or heirship (per user spec).
    """
    text = filing.raw_text or ""
    case_type = filing.case_type or classify_case_type(text)
    if case_type not in ("muniment_of_title", "heirship"):
        reasons["wrong_case_type"] = reasons.get("wrong_case_type", 0) + 1
        return False, False

    # Merge text-extracted fields over whatever the scraper provided.
    decedent = filing.decedent_name or extract_decedent(text)
    applicant = filing.applicant_name or extract_applicant(text)
    attorney_name = filing.attorney_name or extract_attorney(text)
    bar_number = filing.attorney_bar_number or extract_bar_number(text)
    re_flag = involves_real_estate(text)

    attorney_id = None
    if attorney_name:
        attorney_id = _upsert_attorney(conn, attorney_name,
                                        firm=filing.attorney_firm,
                                        bar_number=bar_number,
                                        phone=filing.attorney_phone,
                                        email=filing.attorney_email,
                                        address=filing.attorney_address,
                                        now=now)

    existing = conn.execute(
        "SELECT id FROM probate_filings WHERE county=? AND case_number=?",
        (filing.county, filing.case_number),
    ).fetchone()

    if existing is None:
        conn.execute(
            """INSERT INTO probate_filings
               (county, case_number, case_type, filed_date,
                decedent_name, applicant_name, applicant_address, applicant_phone,
                attorney_id, involves_real_estate, url, raw_text,
                baseline, scraped_at, first_seen_at)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (filing.county, filing.case_number, case_type, filing.filed_date,
             decedent, applicant, filing.applicant_address, filing.applicant_phone,
             attorney_id,
             1 if re_flag is True else (0 if re_flag is False else None),
             filing.url, (text[:8000] if text else None),
             1 if baseline else 0, now, now),
        )
        reasons["kept"] = reasons.get("kept", 0) + 1
        return True, True
    else:
        conn.execute(
            """UPDATE probate_filings
               SET scraped_at=?,
                   decedent_name=COALESCE(?, decedent_name),
                   applicant_name=COALESCE(?, applicant_name),
                   attorney_id=COALESCE(?, attorney_id)
               WHERE id=?""",
            (now, decedent, applicant, attorney_id, existing["id"]),
        )
        reasons["duplicate"] = reasons.get("duplicate", 0) + 1
        return True, False


def _upsert_attorney(conn, name, *, firm, bar_number, phone, email, address, now) -> int:
    name_norm = normalize_name(name)
    existing = conn.execute(
        "SELECT id FROM probate_attorneys WHERE name_normalized=? AND COALESCE(firm,'')=COALESCE(?,'')",
        (name_norm, firm),
    ).fetchone()
    if existing:
        conn.execute(
            """UPDATE probate_attorneys
               SET last_seen_at=?,
                   bar_number=COALESCE(?, bar_number),
                   phone=COALESCE(?, phone),
                   email=COALESCE(?, email),
                   address=COALESCE(?, address)
               WHERE id=?""",
            (now, bar_number, phone, email, address, existing["id"]),
        )
        return existing["id"]
    cur = conn.execute(
        """INSERT INTO probate_attorneys
           (name_normalized, display_name, firm, bar_number, phone, email, address,
            first_seen_at, last_seen_at)
           VALUES (?,?,?,?,?,?,?,?,?)""",
        (name_norm, name, firm, bar_number, phone, email, address, now, now),
    )
    return cur.lastrowid


def _utcnow() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _insert_run(conn, source, started) -> int:
    cur = conn.execute(
        "INSERT INTO scrape_runs(source, started_at, status) VALUES (?, ?, 'running')",
        (source, started),
    )
    return cur.lastrowid


def _finish_run(conn, run_id, finished, status, considered, found, new,
                drop_json, sample_text, error) -> None:
    conn.execute(
        """UPDATE scrape_runs
           SET finished_at=?, status=?, rows_considered=?, rows_found=?, rows_new=?,
               drop_reasons_json=?, sample_text=?, error_message=?
           WHERE id=?""",
        (finished, status, considered, found, new, drop_json, sample_text, error, run_id),
    )
