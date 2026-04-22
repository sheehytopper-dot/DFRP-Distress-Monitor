# DFRP Distress Monitor

Weekly scraper pipeline that surfaces DFW commercial distressed real estate
opportunities ($2M+ or commercial/industrial/land/multifamily/ranch) and probate
leads from muniment-of-title and determination-of-heirship filings.

Outputs two emails every Thursday morning via Resend:
1. **Distress digest** — new tax-foreclosure, trustee-sale, and auction.com
   listings meeting the threshold.
2. **Probate report** — new muniment/heirship filings plus a rolling 12-month
   attorney leaderboard.

## Stack
- Python 3.11
- `requests` + `beautifulsoup4` for static HTML
- Playwright (headless Chromium) for JS-heavy sites
- `pdfplumber` for PDF notices
- SQLite for dedup state (stored on the `data` branch, not `main`)
- GitHub Actions for weekly cron
- Resend for transactional email

## Layout
```
scrapers/   # one module per source; subclasses of BaseScraper
parsers/    # PDF, address, amount, probate helpers
alerts/     # digest builders + Resend sender + Jinja templates
db/         # schema.sql + connection helpers
config/     # .env loader + counties.yml
tests/      # pytest against saved fixtures, never live
```

## Schedule
Weekly: **Thursday 12:00 UTC** (6 AM CST / 7 AM CDT).
Set in `.github/workflows/weekly.yml`.

## DB persistence
The SQLite file is not committed to `main`. The weekly workflow pulls
`db/distress.db` from the `data` branch at the start of each run and commits
the updated file back to `data` at the end. This keeps `main` free of
binary diffs and avoids merge conflicts between concurrent runs.

## Local development

```bash
python3.11 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python -m playwright install chromium

cp .env.example .env
# edit .env — fill in RESEND_API_KEY and DIGEST_TO

python run.py --no-email            # test scrapers without sending
python run.py --baseline            # first real run; seeds DB, suppresses digest
python run.py                       # normal run
```

## Secrets (production)
Set as GitHub repository secrets:
- `RESEND_API_KEY` — from https://resend.com/api-keys
- `DIGEST_FROM` — a verified Resend sender, or `onboarding@resend.dev` while testing
- `DIGEST_TO` — recipient email

## Build status
- [x] Scaffold (schema, config, orchestrator, workflow)
- [ ] Phase 1: lgbs.com + pbfcm.com
- [ ] Phase 2: county trustee sale notices
- [ ] Phase 2b: auction.com commercial TX
- [ ] Phase 3: probate filings + attorney leaderboard
- [ ] Email digests
