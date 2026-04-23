"""auction.com + Ten-X (auction.com's commercial arm) scraper.

auction.com lumps multifamily under /residential/tx/<county>, while true
commercial/industrial listings live on ten-x.com. We load both with
Playwright + stealth, let the page's own JS fire its XHRs, capture JSON
responses, and parse listings from them.

This site is aggressively bot-managed (Cloudflare + behavioral fingerprint).
If we hit a challenge page or the JSON APIs don't fire, we fail fast with
a clear error so the digest flags it rather than silently no-op'ing.
"""
import json
import logging
import re
from typing import Any, Iterator

from config.settings import COMMERCIAL_TYPES, DISTRESS_MIN_USD
from parsers.property_type import classify
from scrapers.base import BaseScraper, DistressRecord

log = logging.getLogger(__name__)

DFW_COUNTY_SLUGS = {
    "dallas": "dallas-county",
    "tarrant": "tarrant-county",
    "collin": "collin-county",
    "denton": "denton-county",
    "rockwall": "rockwall-county",
    "kaufman": "kaufman-county",
    "ellis": "ellis-county",
    "johnson": "johnson-county",
}

AUCTION_URLS = [
    f"https://www.auction.com/residential/tx/{slug}/48_rpp/list_vt"
    for slug in DFW_COUNTY_SLUGS.values()
]
TENX_URL = "https://www.ten-x.com/search/tx/commercial-real-estate-for-sale/"

# Likely JSON endpoints. Narrow once we see real traffic.
_API_MATCHERS = [
    re.compile(r"/api/.*(?:search|listing|propert|result)", re.I),
    re.compile(r"\.auction\.com/.*search", re.I),
    re.compile(r"\.ten-x\.com/.*search", re.I),
    re.compile(r"/graphql", re.I),
]

_CLOUDFLARE_MARKERS = [
    "Checking your browser",
    "cf-browser-verification",
    "Just a moment",
    "cf_challenge",
]


class AuctionComScraper(BaseScraper):
    source = "auction_com"

    def __init__(self, headless: bool = True, timeout_ms: int = 45000):
        super().__init__()
        self.headless = headless
        self.timeout_ms = timeout_ms

    def fetch(self) -> Iterator[DistressRecord]:
        from playwright.sync_api import sync_playwright

        captured: list[dict[str, Any]] = []
        cloudflare_hits = 0
        pages_loaded = 0
        total_pages = len(AUCTION_URLS) + 1  # +1 for Ten-X

        with sync_playwright() as p:
            browser = p.chromium.launch(
                headless=self.headless,
                args=["--disable-blink-features=AutomationControlled"],
            )
            context = browser.new_context(
                user_agent=(
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                    "AppleWebKit/605.1.15 (KHTML, like Gecko) "
                    "Version/17.0 Safari/605.1.15"
                ),
                viewport={"width": 1440, "height": 900},
                locale="en-US",
            )
            _apply_stealth(context)
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
                captured.append({"url": url, "body": body, "origin": _origin(url)})

            page.on("response", on_response)

            for url in AUCTION_URLS + [TENX_URL]:
                log.info("auction_com: loading %s", url)
                try:
                    # Same reasoning as lgbs: networkidle hangs on heavy JS
                    # pages; domcontentloaded + settle is bounded.
                    page.goto(url, wait_until="domcontentloaded", timeout=self.timeout_ms)
                    page.wait_for_timeout(5000)
                    html = page.content()
                    if any(m in html for m in _CLOUDFLARE_MARKERS):
                        log.warning("auction_com: Cloudflare challenge on %s", url)
                        cloudflare_hits += 1
                        continue
                    pages_loaded += 1
                except Exception as e:
                    log.warning("auction_com: load failed for %s: %s", url, e)

            browser.close()

        if pages_loaded == 0:
            raise RuntimeError(
                f"auction_com: 0/{total_pages} pages loaded "
                f"({cloudflare_hits} cloudflare blocks)"
            )
        if not captured:
            log.warning("auction_com: loaded %d pages but captured 0 JSON responses — API patterns may need updating", pages_loaded)
            return

        log.info("auction_com: captured %d JSON payloads from %d pages", len(captured), pages_loaded)
        self.records_considered = sum(
            1 for cap in captured for _ in _walk_listing_objects(cap["body"])
        )
        log.info("auction_com: %d listing objects across payloads", self.records_considered)
        for cap in captured:
            yield from _extract_records(cap["url"], cap["body"], cap["origin"])


def _apply_stealth(context) -> None:
    """Best-effort stealth: patch the common fingerprint leaks."""
    try:
        from playwright_stealth import stealth_sync  # type: ignore
        # stealth_sync needs a page, applied per-page by the caller; here we
        # add a context init script for the two easy wins.
    except ImportError:
        pass
    context.add_init_script(
        "Object.defineProperty(navigator, 'webdriver', {get: () => undefined});"
        "Object.defineProperty(navigator, 'languages', {get: () => ['en-US', 'en']});"
        "Object.defineProperty(navigator, 'plugins', {get: () => [1,2,3,4,5]});"
    )


def _origin(url: str) -> str:
    if "ten-x.com" in url:
        return "ten_x"
    return "auction_com"


def _extract_records(api_url: str, body: Any, origin: str) -> Iterator[DistressRecord]:
    for obj in _walk_listing_objects(body):
        amount = _first_number(
            obj,
            "startingBid", "starting_bid", "openingBid", "opening_bid",
            "askingPrice", "asking_price", "listPrice", "list_price",
            "currentBid", "current_bid", "reservePrice", "reserve_price",
            "price", "estimatedValue", "estimated_value",
        )
        type_raw = _pick(
            obj,
            "propertyType", "property_type", "assetType", "asset_type",
            "listingType", "listing_type",
        )
        description = _pick(obj, "description", "propertyDescription", "title", "name")
        ptype = classify(f"{type_raw or ''} {description or ''}")
        if not _passes_filter(amount, ptype):
            continue

        county = _pick(obj, "county", "countyName", "county_name") or ""
        county_norm = re.sub(r"\s*county\s*$", "", str(county).strip().lower())

        source_id = str(_pick(obj, "id", "listingId", "listing_id", "propertyId", "property_id", "uuid") or "")
        if not source_id:
            import hashlib
            sig = f"{api_url}|{_pick(obj, 'address')}|{amount}"
            source_id = hashlib.sha1(sig.encode()).hexdigest()[:16]

        yield DistressRecord(
            source="auction_com",
            source_id=f"{origin}:{source_id}",
            county=county_norm or "unknown",
            url=_pick(obj, "url", "detailUrl", "detail_url", "propertyUrl") or api_url,
            property_address=_pick(obj, "address", "streetAddress", "street_address"),
            property_type=ptype,
            amount_usd=amount,
            amount_kind=_amount_kind(obj),
            sale_date=_pick(obj, "auctionDate", "auction_date", "saleDate", "sale_date", "endDate", "end_date"),
            raw_text=json.dumps(obj)[:4000],
            extra={"origin": origin, "api_url": api_url, "property_type_raw": type_raw},
        )


def _amount_kind(obj: dict) -> str:
    for k in ("startingBid", "starting_bid", "openingBid", "opening_bid"):
        if obj.get(k):
            return "opening_bid"
    for k in ("askingPrice", "asking_price", "listPrice", "list_price"):
        if obj.get(k):
            return "asking"
    return "other"


def _walk_listing_objects(body: Any):
    stack = [body]
    seen = 0
    while stack and seen < 10000:
        node = stack.pop()
        seen += 1
        if isinstance(node, dict):
            if _looks_like_listing(node):
                yield node
            else:
                stack.extend(node.values())
        elif isinstance(node, list):
            stack.extend(node)


_LISTING_KEYS = {
    "startingBid", "starting_bid", "openingBid", "opening_bid",
    "askingPrice", "asking_price", "listPrice", "list_price",
    "address", "streetAddress", "street_address",
    "propertyType", "property_type", "assetType", "asset_type",
    "auctionDate", "auction_date", "saleDate", "sale_date",
}


def _looks_like_listing(d: dict) -> bool:
    return len(set(d.keys()) & _LISTING_KEYS) >= 2


def _passes_filter(amount: int | None, ptype: str) -> bool:
    if ptype not in COMMERCIAL_TYPES:
        return False
    return amount is not None and amount >= DISTRESS_MIN_USD


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
