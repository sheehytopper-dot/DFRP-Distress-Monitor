from parsers.property_type import classify


def test_commercial_keywords():
    assert classify("RETAIL STRIP CENTER") == "commercial"
    assert classify("Office building on Main St") == "commercial"


def test_industrial_keywords():
    assert classify("Warehouse, 40,000 sqft") == "industrial"


def test_multifamily_keywords():
    assert classify("12-unit apartment complex") == "multifamily"
    assert classify("Duplex on Elm") == "multifamily"


def test_land_keywords():
    assert classify("15.3 acres in ABST 123") == "land"
    assert classify("Vacant lot, undeveloped") == "land"


def test_ranch():
    assert classify("Ranch property, 200 acres") == "ranch"


def test_residential():
    assert classify("LOT 5 BLOCK 2 SUNSET SUBDIVISION") == "residential"
    assert classify("Single-family residence") == "residential"


def test_unknown():
    assert classify("") == "unknown"
    assert classify(None) == "unknown"
    assert classify("Cause 2024-123") == "unknown"
