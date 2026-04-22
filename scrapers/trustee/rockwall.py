from scrapers.trustee.civicplus import CivicPlusArchiveTrustee


class RockwallTrustee(CivicPlusArchiveTrustee):
    county = "rockwall"
    base_url = "https://www.rockwallcountytexas.com"
    amid = 83
