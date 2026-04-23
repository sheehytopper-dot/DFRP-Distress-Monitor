PRAGMA foreign_keys = ON;
PRAGMA journal_mode = WAL;

CREATE TABLE IF NOT EXISTS distress_notices (
    id                       INTEGER PRIMARY KEY,
    source                   TEXT NOT NULL,
    source_id                TEXT NOT NULL,
    county                   TEXT NOT NULL,
    url                      TEXT,
    property_address         TEXT,
    property_type            TEXT,
    amount_usd               INTEGER,
    amount_kind              TEXT,
    sale_date                DATE,
    raw_text                 TEXT,
    extra_json               TEXT,
    status                   TEXT NOT NULL DEFAULT 'active',
    baseline                 INTEGER NOT NULL DEFAULT 0,
    scraped_at               TIMESTAMP NOT NULL,
    first_seen_at            TIMESTAMP NOT NULL,
    last_seen_at             TIMESTAMP NOT NULL,
    last_confirmed_active    TIMESTAMP NOT NULL,
    removed_at               TIMESTAMP,
    UNIQUE(source, source_id)
);

CREATE INDEX IF NOT EXISTS idx_distress_sale_date   ON distress_notices(sale_date);
CREATE INDEX IF NOT EXISTS idx_distress_first_seen  ON distress_notices(first_seen_at);
CREATE INDEX IF NOT EXISTS idx_distress_status      ON distress_notices(status);
CREATE INDEX IF NOT EXISTS idx_distress_county      ON distress_notices(county);

CREATE TABLE IF NOT EXISTS probate_attorneys (
    id               INTEGER PRIMARY KEY,
    name_normalized  TEXT NOT NULL,
    display_name     TEXT NOT NULL,
    firm             TEXT,
    bar_number       TEXT,
    phone            TEXT,
    email            TEXT,
    address          TEXT,
    first_seen_at    TIMESTAMP NOT NULL,
    last_seen_at     TIMESTAMP NOT NULL,
    UNIQUE(name_normalized, firm)
);

CREATE INDEX IF NOT EXISTS idx_attorneys_name ON probate_attorneys(name_normalized);

CREATE TABLE IF NOT EXISTS probate_filings (
    id                    INTEGER PRIMARY KEY,
    county                TEXT NOT NULL,
    case_number           TEXT NOT NULL,
    case_type             TEXT NOT NULL,
    filed_date            DATE,
    decedent_name         TEXT,
    applicant_name        TEXT,
    applicant_address     TEXT,
    applicant_phone       TEXT,
    attorney_id           INTEGER REFERENCES probate_attorneys(id),
    involves_real_estate  INTEGER,
    url                   TEXT,
    raw_text              TEXT,
    baseline              INTEGER NOT NULL DEFAULT 0,
    scraped_at            TIMESTAMP NOT NULL,
    first_seen_at         TIMESTAMP NOT NULL,
    UNIQUE(county, case_number)
);

CREATE INDEX IF NOT EXISTS idx_probate_filed_date     ON probate_filings(filed_date);
CREATE INDEX IF NOT EXISTS idx_probate_county         ON probate_filings(county);
CREATE INDEX IF NOT EXISTS idx_probate_attorney       ON probate_filings(attorney_id);
CREATE INDEX IF NOT EXISTS idx_probate_real_estate    ON probate_filings(involves_real_estate);

CREATE TABLE IF NOT EXISTS scrape_runs (
    id              INTEGER PRIMARY KEY,
    source          TEXT NOT NULL,
    started_at      TIMESTAMP NOT NULL,
    finished_at     TIMESTAMP,
    status          TEXT NOT NULL,
    rows_considered INTEGER,
    rows_found      INTEGER,
    rows_new        INTEGER,
    error_message   TEXT
);

CREATE INDEX IF NOT EXISTS idx_scrape_runs_started ON scrape_runs(started_at);
