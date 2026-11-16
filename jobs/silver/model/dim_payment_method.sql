-- Table:    silver.dim_payment_method
-- SCD:      Type 1 decode table, rebuilt from the allowed domain each run
-- Grain:    one row per payment method
-- Sentinels: -1 Unknown. An unrecognised source value maps here rather than
--            failing the run - a new upstream payment method must not stop the
--            pipeline.

CREATE TABLE IF NOT EXISTS silver.dim_payment_method (
    payment_method_key INT    NOT NULL COMMENT 'Surrogate key, -1 = Unknown',
    payment_method     STRING NOT NULL COMMENT 'e.g. bank_transfer, shaparak, credit_card'
)
USING iceberg
COMMENT 'Payment method decode table.';
