from scrapers.pbfcm import (
    _county_from_link,
    _guess_address,
    _passes_filter,
    _stable_id,
)


def test_county_from_link_text():
    assert _county_from_link("Tarrant County Sales March 2026", "") == "tarrant"
    assert _county_from_link("", "https://pbfcm.com/docs/taxdocs/sales/03-2026dallastaxsale.pdf") == "dallas"
    assert _county_from_link("Hidalgo County", "https://pbfcm.com/x.pdf") == ""


def test_passes_filter_threshold():
    assert _passes_filter(2_000_000) is True
    assert _passes_filter(1_999_999) is False
    assert _passes_filter(None) is False


def test_passes_filter_does_not_accept_low_amount_regardless_of_type():
    # The old behavior accepted any commercial-typed block, which picked
    # up disclaimer boilerplate. New behavior requires a real amount signal.
    assert _passes_filter(100_000) is False


def test_guess_address():
    block = "Cause 2024-42 LOT 5 BLOCK 2 123 Main Street Fort Worth TX. Min bid $500,000."
    assert _guess_address(block) == "123 Main Street"


def test_stable_id_deterministic():
    a = _stable_id("https://x/y.pdf", "Cause No. 123 ABC")
    b = _stable_id("https://x/y.pdf", "Cause No. 123 ABC")
    c = _stable_id("https://x/y.pdf", "Cause No. 999 XYZ")
    assert a == b and a != c
