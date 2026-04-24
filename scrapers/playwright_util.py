"""Shared Playwright helper for scrapers that need a JS-rendered page.

Some county sites (Collin, Johnson, CivicEngage viewers) either return
essentially-empty HTML to a raw GET or 403 a plain-requests client.
Loading them in headless Chromium with light stealth gets past both.

We keep this separate from the lgbs/auction_com scrapers (which drive
their own Playwright flow) because those are map/XHR-capture flows; the
trustee scrapers just need rendered HTML plus any PDF URLs the page
fetched while loading.
"""
import logging
from typing import Optional

log = logging.getLogger(__name__)

_USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 "
    "(KHTML, like Gecko) Version/17.0 Safari/605.1.15"
)


def render_html(
    url: str,
    *,
    wait_ms: int = 5000,
    timeout_ms: int = 30000,
    capture_pdfs: bool = False,
) -> tuple[str, list[str]]:
    """Load URL in headless Chromium. Returns (rendered_html, captured_pdf_urls).

    capture_pdfs=True registers a response handler that records every response
    whose Content-Type starts with application/pdf. That's how we discover
    PDF URLs hidden behind JS viewers (CivicEngage, etc.).
    """
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        log.warning("playwright not installed; cannot render %s", url)
        return "", []

    pdf_urls: list[str] = []
    html = ""
    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=True,
            args=["--disable-blink-features=AutomationControlled"],
        )
        try:
            context = browser.new_context(
                user_agent=_USER_AGENT,
                viewport={"width": 1440, "height": 900},
                locale="en-US",
            )
            context.add_init_script(
                "Object.defineProperty(navigator, 'webdriver', {get: () => undefined});"
                "Object.defineProperty(navigator, 'languages', {get: () => ['en-US', 'en']});"
                "Object.defineProperty(navigator, 'plugins', {get: () => [1,2,3,4,5]});"
            )
            page = context.new_page()

            if capture_pdfs:
                def on_response(resp):
                    ct = (resp.headers.get("content-type") or "").lower()
                    if ct.startswith("application/pdf") or ".pdf" in resp.url.lower():
                        pdf_urls.append(resp.url)
                page.on("response", on_response)

            try:
                page.goto(url, wait_until="domcontentloaded", timeout=timeout_ms)
                page.wait_for_timeout(wait_ms)
                html = page.content()
            except Exception as e:
                log.warning("playwright render failed for %s: %s", url, e)
        finally:
            browser.close()
    return html, pdf_urls
