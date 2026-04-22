"""Email sender using Resend. All recipient/sender/key config reads from
config.settings (which reads from env / .env). Nothing is hardcoded.
"""
import logging
from typing import Optional

import resend

from config import settings

log = logging.getLogger(__name__)


class EmailConfigError(RuntimeError):
    """Raised when RESEND_API_KEY or DIGEST_TO are missing."""


def send(subject: str, html: str, text: Optional[str] = None) -> str:
    if not settings.RESEND_API_KEY:
        raise EmailConfigError("RESEND_API_KEY is not set")
    if not settings.DIGEST_TO:
        raise EmailConfigError("DIGEST_TO is not set")

    resend.api_key = settings.RESEND_API_KEY

    params: dict = {
        "from": settings.DIGEST_FROM,
        "to": [settings.DIGEST_TO],
        "subject": subject,
        "html": html,
    }
    if text:
        params["text"] = text

    result = resend.Emails.send(params)
    email_id = (
        result.get("id") if isinstance(result, dict)
        else getattr(result, "id", None)
    )
    log.info("resend sent: subject=%r id=%s to=%s",
             subject, email_id, settings.DIGEST_TO)
    return email_id or ""
