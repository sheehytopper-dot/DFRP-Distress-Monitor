"""pdfplumber wrappers. Kept tiny so scrapers can swap in richer parsing later."""
import io
import logging
from typing import Iterable

log = logging.getLogger(__name__)


def extract_text(pdf_bytes: bytes) -> str:
    """Return concatenated text from every page. Returns '' on failure."""
    import pdfplumber
    try:
        with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
            return "\n".join((p.extract_text() or "") for p in pdf.pages)
    except Exception as e:
        log.warning("pdf text extraction failed: %s", e)
        return ""


def extract_tables(pdf_bytes: bytes) -> list[list[list[str]]]:
    """Return a list of tables; each table is a list of rows; each row a list of cells."""
    import pdfplumber
    tables: list[list[list[str]]] = []
    try:
        with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
            for page in pdf.pages:
                for t in page.extract_tables() or []:
                    tables.append([[c or "" for c in row] for row in t])
    except Exception as e:
        log.warning("pdf table extraction failed: %s", e)
    return tables


def split_into_property_blocks(text: str) -> Iterable[str]:
    """Split a notice PDF's text into per-property chunks.

    Texas tax sale notices typically separate properties with 'Cause No.' or
    'Tract X:' headers, or by account number lines. We try a few heuristics
    and yield any block containing a dollar amount.
    """
    import re
    # Split on common section delimiters. Keep the delimiter with its block.
    chunks = re.split(r"(?=\b(?:Cause\s+No\.?|Tract\s+\d+:|Property\s+ID:|Account\s+No\.?|ACCT\s*#))", text, flags=re.I)
    for chunk in chunks:
        if "$" in chunk and len(chunk.strip()) > 20:
            yield chunk.strip()
