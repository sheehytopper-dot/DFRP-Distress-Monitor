"""Linebarger Goggan Blair & Sampson (taxsales.lgbs.com) scraper.

The site at taxsales.lgbs.com/map is a JS single-page app backed by a JSON
API. We do *not* scrape the rendered DOM — we let Playwright load the page,
intercept the XHR calls the app fires, grab the JSON, and parse it directly.

Why: the DOM is a map widget, not a table. The JSON has clean fields
(county, assessed value, sale date, address). It's also far less fragile
across redesigns.

The API URL gets logged on first run so we can cut over to plain `requests`
in a follow-up commit — Playwright is a heavyweight tool we only need
until we know the endpoint.
"""
import json
import logging
import re
from typing import Any, Iterator

from config.settings import COMMERCIAL_TYPES, DISTRESS_MIN_USD
from parsers.property_type import classify
from scrapers.base import BaseScraper, DistressRecord

log = logging.getLogger(__name__)

MAP_URL = "https://taxsales.lgbs.com/map"

DFW_COUNTIES = {
    "dallas", "tarrant", "collin", "denton",
    "rockwall", "kaufman", "ellis", "johnson",
}

# Heuristic match for JSON responses we care about. Covers common patterns
# (/api/..., /search, /properties, ...). Narrow once we see a real response.
_API_MATCHERS = [
    re.compile(r"/api/.*propert", re.I),
    re.compile(r"/api/.*sale", re.I),
    re.compile(r"/api/.*search", re.I),
    re.compile(r"/properties\?", re.I),
]


class LgbsScraper(BaseScraper):
    source = "lgbs"

    def __init__(self, headless: bool = True, timeout_ms: int = 45000):
        super().__init__()
        self.headless = headless
        self.timeout_ms = timeout_ms

    def fetch(self) -> Iterator[DistressRecord]:
        from playwright.sync_api import sync_playwright

        captured: list[dict[str, Any]] = []

        with sync_playwright() as p:
            browser = p.chromium.launch(headless=self.headless)
            context = browser.new_context(
                user_agent=(
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                    "AppleWebKit/605.1.15 (KHTML, like Gecko) "
                    "Version/17.0 Safari/605.1.15"
                )
            )
            page = context.new_page()

            def on_response(resp):
                url = resp.url
                if not any(pat.search(url) for pat in _API_MATCHERS):
                    return
                ct = (resp.headers.get("content-type") or "").lower()
                if "json" not in ct:
                    return
                try:
                    body = resp.json()
                except Exception:
                    return
                log.info("lgbs captured JSON from %s (%s keys / %s items)",
                         url,
                         list(body.keys()) if isinstance(body, dict) else "list",
                         len(body) if isinstance(body, list) else None)
                captured.append({"url": url, "body": body})

            page.on("response", on_response)

            try:
                page.goto(MAP_URL, wait_until="networkidle", timeout=self.timeout_ms)
            except Exception as e:
                log.warning("lgbs page load timed out / failed: %s", e)

            browser.close()

        if not captured:
            log.warning("lgbs: no JSON API responses captured — site structure likely changed, or blocked")
            return

        # Count property-shaped objects across all payloads so
        # records_considered can distinguish "no data" from "strict filter".
        self.records_considered = sum(
            1 for cap in captured for _ in _walk_property_objects(cap["body"])
        )
        log.info("lgbs: %d property objects found across %d payloads",
                 self.records_considered, len(captured))

        for cap in captured:
            yield from _extract_records(cap["url"], cap["body"])


def _extract_records(api_url: str, body: Any) -> Iterator[DistressRecord]:
    """Walk a JSON response and emit DistressRecord for each DFW property
    meeting the filter. The response shape is unknown until first run, so
    we scan for dict objects with plausible property keys.
    """
    for obj in _walk_property_objects(body):
        county = _pick(obj, "county", "CountyName", "county_name", "county_display")
        if not county:
            continue
        county_norm = re.sub(r"\s*county\s*$", "", str(county).strip().lower())
        if county_norm not in DFW_COUNTIES:
            continue

        amount = _first_number(
            obj,
            "adjudged_value", "adjudgedValue", "assessed_value", "assessedValue",
            "appraised_value", "market_value", "minimum_bid", "minimumBid",
        )
        description = _pick(
            obj,
            "legal_description", "legalDescription", "description",
            "property_description", "address",
        )
        ptype = classify(description)
        if not _passes_filter(amount, ptype):
            continue

        source_id = str(_pick(obj, "id", "property_id", "propertyId", "uid", "cause_number") or "")
        if not source_id:
            # Synthesize — stable-ish from key fields.
            import hashlib
            sig = f"{county_norm}|{description}|{amount}"
            source_id = hashlib.sha1(sig.encode()).hexdigest()[:16]

        yield DistressRecord(
            source="lgbs",
            source_id=source_id,
            county=county_norm,
            url=MAP_URL,
            property_address=_pick(obj, "address", "situs_address", "property_address"),
            property_type=ptype,
            amount_usd=amount,
            amount_kind="adjudged",
            sale_date=_pick(obj, "sale_date", "saleDate", "auction_date"),
            raw_text=json.dumps(obj)[:4000],
            extra={"api_url": api_url},
        )


def _walk_property_objects(body: Any):
    """Yield dict objects likely to represent a property."""
    stack = [body]
    seen = 0
    while stack and seen < 10000:
        node = stack.pop()
        seen += 1
        if isinstance(node, dict):
            if _looks_like_property(node):
                yield node
            else:
                stack.extend(node.values())
        elif isinstance(node, list):
            stack.extend(node)


_PROPERTY_KEYS = {
    "county", "CountyName", "county_name",
    "legal_description", "legalDescription",
    "adjudged_value", "assessed_value", "minimum_bid",
    "sale_date", "saleDate",
    "address", "situs_address",
}


def _looks_like_property(d: dict) -> bool:
    keys = set(d.keys())
    return len(keys & _PROPERTY_KEYS) >= 2


def _passes_filter(amount: int | None, ptype: str) -> bool:
    if amount is not None and amount >= DISTRESS_MIN_USD:
        return True
    return ptype in COMMERCIAL_TYPES


def _pick(d: dict, *keys: str):
    for k in keys:
        if k in d and d[k] not in (None, ""):
            return d[k]
    return None


def _first_number(d: dict, *keys: str) -> int | None:
    for k in keys:
        v = d.get(k)
        if v is None or v == "":
            continue
        if isinstance(v, (int, float)):
            return int(v)
        if isinstance(v, str):
            stripped = re.sub(r"[^\d.]", "", v)
            if stripped:
                try:
                    return int(float(stripped))
                except ValueError:
                    continue
    return None
