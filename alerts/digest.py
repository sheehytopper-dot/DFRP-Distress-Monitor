"""Build and send the weekly distress digest email.

Pulls from the already-populated distress_notices table. Two views:
  - New this week: first_seen_at in last 7 days, upcoming or unknown sale
  - Still active: count of anything previously seen that's still live
Plus per-scraper status from the latest scrape_runs entries so failures
surface alongside results.

Emails go via alerts/mail.py (Resend). Keep rendering cheap: run on the
same conn the scrapers just finished writing to.
"""
import json
import logging
import sqlite3
from datetime import date, timedelta
from pathlib import Path
from typing import Optional

from jinja2 import Environment, FileSystemLoader, select_autoescape

from alerts.mail import send
from config.settings import DISTRESS_MIN_USD

log = logging.getLogger(__name__)

_TEMPLATE_DIR = Path(__file__).parent / "templates"
_env = Environment(
    loader=FileSystemLoader(_TEMPLATE_DIR),
    autoescape=select_autoescape(["html"]),
)


def build_digest(
    conn: sqlite3.Connection,
    *,
    today: Optional[str] = None,
    include_baseline: bool = False,
) -> tuple[str, str]:
    """Return (subject, html_body).

    include_baseline=True disables the 'baseline' filter — for previewing
    the digest against seed data without running a real weekly cycle first.
    """
    today = today or date.today().isoformat()
    week_ago = (date.fromisoformat(today) - timedelta(days=7)).isoformat()
    conn.row_factory = sqlite3.Row
    base_cond = "" if include_baseline else "AND baseline=0"

    new_this_week = list(conn.execute(
        f"""SELECT * FROM distress_notices
            WHERE status='active' {base_cond}
              AND first_seen_at >= ?
              AND (sale_date IS NULL OR sale_date >= ?)
            ORDER BY amount_usd DESC""",
        (week_ago, today),
    ))
    still_active_count = conn.execute(
        f"""SELECT COUNT(*) FROM distress_notices
            WHERE status='active' {base_cond}
              AND first_seen_at < ?
              AND (sale_date IS NULL OR sale_date >= ?)""",
        (week_ago, today),
    ).fetchone()[0]
    dropped_this_week = conn.execute(
        """SELECT COUNT(*) FROM distress_notices
           WHERE status IN ('removed', 'expired')
             AND removed_at >= ?""",
        (week_ago,),
    ).fetchone()[0]
    scraper_rows = list(conn.execute(
        """SELECT source, status, rows_considered, rows_found,
                  drop_reasons_json, error_message
           FROM scrape_runs
           WHERE id IN (SELECT MAX(id) FROM scrape_runs GROUP BY source)
           ORDER BY source"""
    ))
    scrapers = []
    for r in scraper_rows:
        scrapers.append({
            "source": r["source"],
            "status": r["status"],
            "considered": r["rows_considered"] or 0,
            "found": r["rows_found"] or 0,
            "error": r["error_message"],
            "drops": json.loads(r["drop_reasons_json"]) if r["drop_reasons_json"] else {},
        })

    failed_scrapers = [s for s in scrapers if s["status"] == "failed"]

    html = _env.get_template("digest.html").render(
        today=today,
        new_this_week=new_this_week,
        still_active_count=still_active_count,
        dropped_this_week=dropped_this_week,
        scrapers=scrapers,
        failed_scrapers=failed_scrapers,
        threshold_usd=DISTRESS_MIN_USD,
    )
    subject = f"DFRP Distress Digest — {today} — {len(new_this_week)} new"
    return subject, html


def send_digest(conn: sqlite3.Connection) -> str:
    subject, html = build_digest(conn)
    email_id = send(subject, html)
    log.info("digest sent: id=%s subject=%r", email_id, subject)
    return email_id
