from parsers.notices import (
    extract_address,
    extract_borrower,
    extract_original_principal,
    extract_sale_date,
)


SAMPLE = """NOTICE OF SUBSTITUTE TRUSTEE'S SALE

Deed of Trust Information:
Date: August 15, 2019
Grantor(s)/Borrower: ACME Commercial Holdings LLC
Original Principal Amount: $3,500,000.00
Recording Information: Document 2019-10042

Property Address: 1234 Main Street, Dallas, TX 75201

Date of Sale: Tuesday, February 3, 2026
Earliest Time Sale Will Begin: 10:00 AM
Place of Sale: George Allen Courts Building, 600 Commerce Street, Dallas, TX
"""


def test_sale_date_labeled():
    assert extract_sale_date(SAMPLE) == "2026-02-03"


def test_sale_date_missing():
    assert extract_sale_date("no date here") is None


def test_original_principal():
    assert extract_original_principal(SAMPLE) == 3_500_000


def test_original_principal_plain_keyword():
    assert extract_original_principal("Principal Amount: $1,250,000.00") == 1_250_000


def test_original_principal_missing():
    assert extract_original_principal("Some text without that field") is None


def test_address_labeled():
    assert extract_address(SAMPLE).startswith("1234 Main Street")


def test_address_fallback_to_street():
    text = "Lot 5 Block 2, 789 Oak Drive, Frisco, TX."
    got = extract_address(text)
    assert got is not None and "789 Oak Drive" in got


def test_borrower():
    assert extract_borrower(SAMPLE).startswith("ACME Commercial")
