-- Table:    silver.dim_return_reason
-- SCD:      Type 1 decode table, rebuilt from the allowed domain each run
-- Grain:    one row per return reason
-- Source:   transactional.returns_refunds.return_reason - 7 distinct values
--           observed across the full topic (2,659 messages).

CREATE TABLE IF NOT EXISTS silver.dim_return_reason (
    return_reason_key INT    NOT NULL COMMENT 'Surrogate key, -1 = Unknown',
    return_reason     STRING NOT NULL COMMENT 'defective, damaged, wrong_item, not_as_described, size_issue, late_delivery, customer_changed_mind'
)
USING iceberg
COMMENT 'Return reason decode table.';
