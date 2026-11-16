-- Table:    silver.dim_order_status
-- SCD:      Type 1 decode table, rebuilt from the allowed domain each run
-- Grain:    one row per order status
-- Note:     the source only ever emits 'created' today, so any status funnel
--           will read 100% created. The other members are modelled so the
--           dimension is ready when upstream starts emitting them.

CREATE TABLE IF NOT EXISTS silver.dim_order_status (
    order_status_key INT    NOT NULL COMMENT 'Surrogate key, -1 = Unknown',
    order_status     STRING NOT NULL COMMENT 'created, shipped, delivered, cancelled, refunded'
)
USING iceberg
COMMENT 'Order status decode table.';
