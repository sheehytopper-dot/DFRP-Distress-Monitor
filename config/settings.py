import os
from pathlib import Path

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent.parent
load_dotenv(ROOT / ".env")

RESEND_API_KEY = os.environ.get("RESEND_API_KEY", "")
DIGEST_FROM = os.environ.get("DIGEST_FROM", "onboarding@resend.dev")
DIGEST_TO = os.environ.get("DIGEST_TO", "")

DISTRESS_MIN_USD = int(os.environ.get("DISTRESS_MIN_USD", "2000000"))

COMMERCIAL_TYPES = {"commercial", "industrial", "land", "multifamily", "ranch", "agricultural"}

COUNTIES_PATH = ROOT / "config" / "counties.yml"
