"""Send a minimal test email to verify Resend credentials are wired.

Usage (from project root, after setting RESEND_API_KEY in .env):
    python scripts/send_test_email.py
"""
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from alerts.mail import EmailConfigError, send  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)


def main() -> int:
    now = datetime.now(timezone.utc).isoformat(timespec="seconds")
    subject = "DFRP Monitor — Resend configuration test"
    html = (
        "<p>This confirms your Resend API key, sender address, and recipient "
        "are wired correctly for the DFRP Distress Monitor pipeline.</p>"
        f"<p>Sent at {now} UTC.</p>"
    )
    try:
        email_id = send(subject, html)
    except EmailConfigError as e:
        print(f"config error: {e}", file=sys.stderr)
        return 2
    print(f"sent. resend id: {email_id}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
