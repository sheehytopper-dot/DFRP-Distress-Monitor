from scrapers.trustee.common import build_record


NOTICE_BIG = """NOTICE OF SUBSTITUTE TRUSTEE'S SALE

Deed of Trust: August 15, 2019
Grantor: ACME Holdings LLC
Original Principal Amount: $3,500,000.00

Property Address: 1234 Main Street, Dallas, TX 75201
Property Description: Retail center

Date of Sale: Tuesday, December 1, 2099
Earliest Time Sale Will Begin: 10:00 AM
"""

NOTICE_SMALL = """NOTICE OF SUBSTITUTE TRUSTEE'S SALE
Original Principal Amount: $250,000.00
Property Address: 789 Oak Drive, Plano, TX
Sale Date: December 1, 2099
"""

NOTICE_PAST = """NOTICE OF SUBSTITUTE TRUSTEE'S SALE
Original Principal Amount: $5,000,000.00
Property Address: 1 Old Street, Dallas, TX 75201
Sale Date: January 1, 2020
"""


def test_build_record_keeps_large_notice():
    rec = build_record(
        source="trustee_dallas",
        county="dallas",
        notice_url="https://example.com/notice1.pdf",
        notice_text=NOTICE_BIG,
    )
    assert rec is not None
    assert rec.county == "dallas"
    assert rec.amount_usd == 3_500_000
    assert rec.sale_date == "2099-12-01"
    assert rec.property_address.startswith("1234 Main Street")
    assert rec.property_type == "commercial"
    assert rec.extra["labeled_principal"] == 3_500_000
    assert rec.extra["borrower"].startswith("ACME")


def test_build_record_drops_past_sale():
    rec = build_record(
        source="trustee_dallas",
        county="dallas",
        notice_url="https://example.com/past.pdf",
        notice_text=NOTICE_PAST,
    )
    assert rec is None


def test_build_record_drops_small_notice():
    rec = build_record(
        source="trustee_dallas",
        county="dallas",
        notice_url="https://example.com/notice2.pdf",
        notice_text=NOTICE_SMALL,
    )
    assert rec is None


def test_build_record_stable_source_id():
    a = build_record(source="trustee_x", county="x",
                     notice_url="https://u/1.pdf", notice_text=NOTICE_BIG)
    b = build_record(source="trustee_x", county="x",
                     notice_url="https://u/1.pdf", notice_text=NOTICE_BIG + "\nextra")
    assert a.source_id == b.source_id
