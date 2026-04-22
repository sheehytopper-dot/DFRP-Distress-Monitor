from scrapers.auction_com import (
    _amount_kind,
    _extract_records,
    _first_number,
    _looks_like_listing,
    _passes_filter,
    _pick,
)


def test_passes_filter_rejects_residential():
    assert _passes_filter(5_000_000, "residential") is False


def test_passes_filter_requires_threshold():
    assert _passes_filter(1_900_000, "commercial") is False
    assert _passes_filter(2_000_000, "commercial") is True
    assert _passes_filter(None, "land") is False


def test_passes_filter_accepts_ranch():
    # User said "land and ranches too"
    assert _passes_filter(2_000_000, "ranch") is True


def test_first_number_string_formats():
    assert _first_number({"askingPrice": "$3,500,000"}, "askingPrice") == 3_500_000
    assert _first_number({"askingPrice": 3500000.0}, "askingPrice") == 3_500_000


def test_looks_like_listing():
    assert _looks_like_listing({"askingPrice": 1, "address": "x"}) is True
    assert _looks_like_listing({"foo": 1}) is False


def test_amount_kind():
    assert _amount_kind({"startingBid": 100}) == "opening_bid"
    assert _amount_kind({"asking_price": 100}) == "asking"
    assert _amount_kind({}) == "other"


def test_extract_records_end_to_end():
    body = {
        "page": 1,
        "results": [
            # commercial, $3M → keep
            {"id": "a1", "propertyType": "Commercial", "askingPrice": 3_000_000,
             "address": "100 Commerce St", "county": "Dallas",
             "auctionDate": "2026-05-01"},
            # multifamily but below threshold → drop
            {"id": "a2", "propertyType": "Multifamily", "startingBid": 1_500_000,
             "address": "200 Elm", "county": "Tarrant"},
            # residential, high price → drop (wrong type)
            {"id": "a3", "propertyType": "SingleFamily", "askingPrice": 5_000_000,
             "address": "300 Oak", "county": "Collin"},
            # land, $2M → keep
            {"id": "a4", "propertyType": "Land", "listPrice": 2_100_000,
             "address": "10 acres FM 123", "county": "Denton"},
        ],
    }
    records = list(_extract_records("https://www.ten-x.com/api/search", body, "ten_x"))
    ids = {r.source_id for r in records}
    assert ids == {"ten_x:a1", "ten_x:a4"}
    a1 = next(r for r in records if r.source_id == "ten_x:a1")
    assert a1.county == "dallas"
    assert a1.amount_usd == 3_000_000
    assert a1.amount_kind == "asking"
    assert a1.property_type == "commercial"
    assert a1.extra["origin"] == "ten_x"


def test_pick_fallback():
    assert _pick({"a": None, "b": "x"}, "a", "b") == "x"
    assert _pick({}, "a") is None
