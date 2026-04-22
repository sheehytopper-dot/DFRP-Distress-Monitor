from scrapers.lgbs import (
    _extract_records,
    _first_number,
    _looks_like_property,
    _passes_filter,
    _pick,
)


def test_pick_prefers_first_nonempty():
    d = {"a": "", "b": None, "c": "ok"}
    assert _pick(d, "a", "b", "c") == "ok"
    assert _pick(d, "missing") is None


def test_first_number_handles_strings_and_ints():
    assert _first_number({"adjudged_value": "$2,500,000.00"}, "adjudged_value") == 2_500_000
    assert _first_number({"adjudged_value": 1500000}, "adjudged_value") == 1_500_000
    assert _first_number({"adjudged_value": ""}, "adjudged_value") is None


def test_looks_like_property():
    assert _looks_like_property({"county": "Dallas", "sale_date": "2026-05-01"}) is True
    assert _looks_like_property({"county": "Dallas"}) is False
    assert _looks_like_property({"foo": 1, "bar": 2}) is False


def test_passes_filter():
    assert _passes_filter(2_000_000, "unknown") is True
    assert _passes_filter(500_000, "commercial") is True
    assert _passes_filter(500_000, "residential") is False


def test_extract_records_filters_non_dfw_and_below_threshold():
    # Realistic-looking mock response: a list wrapped in a dict with other noise.
    body = {
        "status": "ok",
        "properties": [
            # DFW + $2.5M → keep
            {"id": "p1", "county": "Dallas", "adjudged_value": 2_500_000,
             "sale_date": "2026-06-02", "address": "123 Main St",
             "legal_description": "RETAIL CENTER"},
            # DFW but below $2M and residential → drop
            {"id": "p2", "county": "Tarrant", "adjudged_value": 150_000,
             "sale_date": "2026-06-02", "address": "456 Oak Dr",
             "legal_description": "LOT 5 BLK 2"},
            # Non-DFW county → drop
            {"id": "p3", "county": "Harris", "adjudged_value": 5_000_000,
             "sale_date": "2026-06-02", "address": "789 Elm",
             "legal_description": "Warehouse"},
            # DFW + land (commercial type) below threshold → keep via type
            {"id": "p4", "county": "Collin", "adjudged_value": 800_000,
             "sale_date": "2026-06-02", "address": "1200 Ranch Rd",
             "legal_description": "25 acres raw land"},
        ],
    }
    records = list(_extract_records("https://taxsales.lgbs.com/api/properties", body))
    ids = {r.source_id for r in records}
    assert ids == {"p1", "p4"}
    p1 = next(r for r in records if r.source_id == "p1")
    assert p1.county == "dallas"
    assert p1.amount_usd == 2_500_000
    assert p1.property_type == "commercial"


def test_extract_records_handles_empty_body():
    assert list(_extract_records("u", {})) == []
    assert list(_extract_records("u", [])) == []
    assert list(_extract_records("u", None)) == []
