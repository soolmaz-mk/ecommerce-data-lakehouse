-- Table:    silver.dim_date
-- SCD:      Type 0 (static calendar, seeded once by seed_dim_date)
-- Grain:    one row per calendar day, 2020-01-01 .. 2030-12-31
-- Note:     date_key is the yyyymmdd integer every fact's *_date_key points at.

CREATE TABLE IF NOT EXISTS silver.dim_date (
    date_key    INT     NOT NULL COMMENT 'yyyymmdd surrogate key',
    full_date   DATE    NOT NULL COMMENT 'The calendar date itself',
    year        INT              COMMENT 'Calendar year',
    quarter     INT              COMMENT 'Calendar quarter 1-4',
    month       INT              COMMENT 'Month number 1-12',
    month_name  STRING           COMMENT 'Full month name',
    iso_week    INT              COMMENT 'ISO week number',
    day_of_week INT              COMMENT 'ISO weekday, 1=Monday .. 7=Sunday',
    day_name    STRING           COMMENT 'Full day name',
    is_weekend  BOOLEAN          COMMENT 'True for Saturday/Sunday'
)
USING iceberg
COMMENT 'Static date dimension shared by every fact.';
