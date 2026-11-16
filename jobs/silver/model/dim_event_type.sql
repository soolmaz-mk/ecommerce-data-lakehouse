-- Table:    silver.dim_event_type
-- SCD:      Type 1 decode table, rebuilt from the allowed domain each run
-- Grain:    one row per behavioural event type
-- Note:     all 10 funnel stages are present in the source.

CREATE TABLE IF NOT EXISTS silver.dim_event_type (
    event_type_key INT    NOT NULL COMMENT 'Surrogate key, -1 = Unknown',
    event_type     STRING NOT NULL COMMENT 'e.g. page_view, add_to_cart, checkout_start'
)
USING iceberg
COMMENT 'Behavioural event type decode table.';
