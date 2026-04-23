import sqlite3
from contextlib import contextmanager
from pathlib import Path

DB_PATH = Path(__file__).parent / "distress.db"
SCHEMA_PATH = Path(__file__).parent / "schema.sql"


def _connect(path: Path = DB_PATH) -> sqlite3.Connection:
    conn = sqlite3.connect(str(path))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA journal_mode = WAL")
    return conn


def init_db(path: Path = DB_PATH) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with _connect(path) as conn:
        conn.executescript(SCHEMA_PATH.read_text())
        _apply_migrations(conn)


def _apply_migrations(conn: sqlite3.Connection) -> None:
    """Add-only schema migrations. SQLite's ALTER TABLE supports adding
    columns; we never rename or drop in-place so old DBs pulled from the
    data branch keep working."""
    existing_cols = {r[1] for r in conn.execute("PRAGMA table_info(scrape_runs)").fetchall()}
    if "rows_considered" not in existing_cols:
        conn.execute("ALTER TABLE scrape_runs ADD COLUMN rows_considered INTEGER")
    if "drop_reasons_json" not in existing_cols:
        conn.execute("ALTER TABLE scrape_runs ADD COLUMN drop_reasons_json TEXT")
    if "sample_text" not in existing_cols:
        conn.execute("ALTER TABLE scrape_runs ADD COLUMN sample_text TEXT")


@contextmanager
def get_conn(path: Path = DB_PATH):
    conn = _connect(path)
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
