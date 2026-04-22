from scrapers.trustee.civicplus import CivicPlusArchiveTrustee


class EllisTrustee(CivicPlusArchiveTrustee):
    county = "ellis"
    base_url = "https://co.ellis.tx.us"
    amid = 60
