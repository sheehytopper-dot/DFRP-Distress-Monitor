"""Weekly probate report: new filings + 12-month attorney leaderboard.

Two sections per the user spec:
 1. Lead list — new muniment-of-title / heirship filings from the last
    7 days across the 9 DFW counties.
 2. Attorney leaderboard — ranked by case volume over the last 12 months,
    filtered to cases classified as involving real estate (involves_real_estate
    IS NOT FALSE — counts TRUE + UNKNOWN).
"""
import json
import logging
import sqlite3
from datetime import date, timedelta
from pathlib import Path
from typing import Optional

from jinja2 import Environment, FileSystemLoader, select_autoescape

from alerts.mail import send

log = logging.getLogger(__name__)

_TEMPLATE_DIR = Path(__file__).parent / "templates"
_env = Environment(
    loader=FileSystemLoader(_TEMPLATE_DIR),
    autoescape=select_autoescape(["html"]),
)


def build_report(
    conn: sqlite3.Connection,
    *,
    today: Optional[str] = None,
    include_baseline: bool = False,
) -> tuple[str, str]:
    today = today or date.today().isoformat()
    week_ago = (date.fromisoformat(today) - timedelta(days=7)).isoformat()
    year_ago = (date.fromisoformat(today) - timedelta(days=365)).isoformat()
    conn.row_factory = sqlite3.Row

    base_cond = "" if include_baseline else "AND f.baseline=0"

    new_filings = list(conn.execute(
        f"""SELECT f.*, a.display_name AS attorney_display, a.firm AS attorney_firm
            FROM probate_filings f
            LEFT JOIN probate_attorneys a ON f.attorney_id = a.id
            WHERE f.first_seen_at >= ? {base_cond}
            ORDER BY f.filed_date DESC, f.county""",
        (week_ago,),
    ))

    # Attorney leaderboard: rolling 12 months, count filings with real-estate
    # likely (TRUE) or unknown (NULL). Exclude only the explicit FALSE.
    leaderboard_rows = list(conn.execute(
        f"""SELECT a.display_name, a.firm, a.phone, a.email,
                   COUNT(DISTINCT f.id) AS case_count,
                   GROUP_CONCAT(DISTINCT f.county) AS counties
            FROM probate_attorneys a
            JOIN probate_filings f ON f.attorney_id = a.id
            WHERE (f.filed_date >= ? OR f.first_seen_at >= ?)
              AND (f.involves_real_estate IS NULL OR f.involves_real_estate = 1)
              {base_cond}
            GROUP BY a.id
            ORDER BY case_count DESC, a.display_name ASC
            LIMIT 25""",
        (year_ago, year_ago),
    ))

    # Per-scraper status for diagnostic visibility (same pattern as distress)
    scrapers = list(conn.execute(
        """SELECT source, status, rows_considered, rows_found,
                  drop_reasons_json, error_message
           FROM scrape_runs
           WHERE source LIKE 'probate_%'
             AND id IN (SELECT MAX(id) FROM scrape_runs WHERE source LIKE 'probate_%' GROUP BY source)
           ORDER BY source"""
    ))
    scrapers_clean = []
    for r in scrapers:
        scrapers_clean.append({
            "source": r["source"],
            "status": r["status"],
            "considered": r["rows_considered"] or 0,
            "found": r["rows_found"] or 0,
            "error": r["error_message"],
        })

    html = _env.get_template("probate_report.html").render(
        today=today,
        new_filings=new_filings,
        leaderboard=leaderboard_rows,
        scrapers=scrapers_clean,
    )
    subject = f"DFRP Probate Report — {today} — {len(new_filings)} new filings"
    return subject, html


def send_report(conn: sqlite3.Connection) -> str:
    subject, html = build_report(conn)
    email_id = send(subject, html)
    log.info("probate report sent: id=%s subject=%r", email_id, subject)
    return email_id
