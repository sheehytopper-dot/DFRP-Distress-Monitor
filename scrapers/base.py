import json
import logging
import sqlite3
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Iterator, Optional

log = logging.getLogger(__name__)


@dataclass
class DistressRecord:
    source: str
    source_id: str
    county: str
    url: Optional[str] = None
    property_address: Optional[str] = None
    property_type: Optional[str] = None
    amount_usd: Optional[int] = None
    amount_kind: Optional[str] = None
    sale_date: Optional[str] = None
    raw_text: Optional[str] = None
    extra: dict = field(default_factory=dict)


class BaseScraper:
    """Subclasses implement `fetch()` yielding DistressRecord instances.

    run() handles upsert, activity transitions, and scrape_runs bookkeeping.
    A scraper that raises propagates a failed scrape_runs row but does not
    abort the orchestrator.
    """

    source: str = ""

    def fetch(self) -> Iterator[DistressRecord]:
        raise NotImplementedError

    def run(self, conn: sqlite3.Connection, baseline: bool = False) -> dict:
        started = _utcnow()
        run_id = _insert_run(conn, self.source, started)
        seen_ids: list[str] = []
        rows_new = 0
        rows_found = 0
        error: Optional[str] = None

        try:
            for rec in self.fetch():
                rows_found += 1
                is_new = _upsert(conn, rec, started, baseline)
                if is_new:
                    rows_new += 1
                seen_ids.append(rec.source_id)
            _transition_missing(conn, self.source, seen_ids, started)
            status = "ok"
        except Exception as e:
            log.exception("scraper %s failed", self.source)
            error = f"{type(e).__name__}: {e}"
            status = "failed"

        _finish_run(conn, run_id, _utcnow(), status, rows_found, rows_new, error)
        return {"status": status, "found": rows_found, "new": rows_new, "error": error}


def _utcnow() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _insert_run(conn: sqlite3.Connection, source: str, started: str) -> int:
    cur = conn.execute(
        "INSERT INTO scrape_runs(source, started_at, status) VALUES (?, ?, 'running')",
        (source, started),
    )
    return cur.lastrowid


def _finish_run(conn, run_id, finished, status, found, new, error) -> None:
    conn.execute(
        "UPDATE scrape_runs SET finished_at=?, status=?, rows_found=?, rows_new=?, error_message=? WHERE id=?",
        (finished, status, found, new, error, run_id),
    )


def _upsert(conn: sqlite3.Connection, r: DistressRecord, now: str, baseline: bool) -> bool:
    existing = conn.execute(
        "SELECT id FROM distress_notices WHERE source=? AND source_id=?",
        (r.source, r.source_id),
    ).fetchone()
    extra = json.dumps(r.extra) if r.extra else None

    if existing is None:
        conn.execute(
            """INSERT INTO distress_notices
               (source, source_id, county, url, property_address, property_type,
                amount_usd, amount_kind, sale_date, raw_text, extra_json,
                status, baseline, scraped_at, first_seen_at, last_seen_at, last_confirmed_active)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,'active',?,?,?,?,?)""",
            (r.source, r.source_id, r.county, r.url, r.property_address, r.property_type,
             r.amount_usd, r.amount_kind, r.sale_date, r.raw_text, extra,
             1 if baseline else 0, now, now, now, now),
        )
        return True

    conn.execute(
        """UPDATE distress_notices
           SET last_seen_at=?, last_confirmed_active=?, status='active', removed_at=NULL,
               url=COALESCE(?, url),
               property_address=COALESCE(?, property_address),
               property_type=COALESCE(?, property_type),
               amount_usd=COALESCE(?, amount_usd),
               amount_kind=COALESCE(?, amount_kind),
               sale_date=COALESCE(?, sale_date),
               raw_text=COALESCE(?, raw_text),
               extra_json=COALESCE(?, extra_json)
           WHERE id=?""",
        (now, now, r.url, r.property_address, r.property_type, r.amount_usd,
         r.amount_kind, r.sale_date, r.raw_text, extra, existing["id"]),
    )
    return False


def _transition_missing(conn: sqlite3.Connection, source: str, seen_ids: list[str], now: str) -> None:
    """Flip previously-active rows that didn't appear this run to expired/removed."""
    if seen_ids:
        placeholders = ",".join("?" * len(seen_ids))
        missing = conn.execute(
            f"""SELECT id, sale_date FROM distress_notices
                WHERE source=? AND status='active' AND source_id NOT IN ({placeholders})""",
            (source, *seen_ids),
        ).fetchall()
    else:
        missing = conn.execute(
            "SELECT id, sale_date FROM distress_notices WHERE source=? AND status='active'",
            (source,),
        ).fetchall()

    today = now[:10]
    for row in missing:
        new_status = "expired" if (row["sale_date"] and row["sale_date"] < today) else "removed"
        conn.execute(
            "UPDATE distress_notices SET status=?, removed_at=? WHERE id=?",
            (new_status, now, row["id"]),
        )
