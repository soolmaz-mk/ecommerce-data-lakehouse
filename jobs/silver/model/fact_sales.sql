-- Table:    silver.fact_sales
-- Type:     atomic transaction fact - the PRIMARY fact of the star
-- Grain:    ONE ROW PER ORDER LINE ITEM (order_id + order_item_id)
-- Load:     incremental over [interval_start - lookback, interval_end],
--           written with dynamic_partition_overwrite on order_date_key.
-- Note:     order_items carries no timestamp of its own, so the date and the
--           user are sourced by joining orders. product_key is attached with a
--           point-in-time (join_asof) lookup at order_timestamp, NOT by
--           is_current - joining on is_current would re-price historical orders
--           at today's catalog price, the exact bug SCD2 exists to prevent.
-- Money:    gross = quantity * unit_price (pre-discount)
--           net   = item_total_amount     (post-discount)
--           discount = gross - net        (DERIVED - the source has no
--                                          discount field at any level)

CREATE TABLE IF NOT EXISTS silver.fact_sales (
    order_date_key     INT           NOT NULL COMMENT 'FK -> dim_date',
    user_key           BIGINT        NOT NULL COMMENT 'FK -> dim_user, point-in-time at order time',
    product_key        BIGINT        NOT NULL COMMENT 'FK -> dim_product, point-in-time at order time',
    payment_method_key INT           NOT NULL COMMENT 'FK -> dim_payment_method',
    order_status       STRING                 COMMENT 'Order status as an attribute (always created today)',
    order_id           STRING        NOT NULL COMMENT 'Degenerate dimension',
    order_item_id      STRING        NOT NULL COMMENT 'Degenerate dimension, completes the grain',
    order_timestamp    TIMESTAMP     NOT NULL COMMENT 'Order placement time, drives point-in-time joins',
    quantity           INT                    COMMENT 'Units on this line, additive',
    unit_price         DECIMAL(10,2)          COMMENT 'Transaction price per unit, NON-additive (average only)',
    gross_sales_amount DECIMAL(18,2)          COMMENT 'quantity * unit_price, additive',
    discount_amount    DECIMAL(18,2)          COMMENT 'gross - net, derived, additive',
    net_sales_amount   DECIMAL(18,2)          COMMENT 'item_total_amount, additive'
)
USING iceberg
PARTITIONED BY (order_date_key)
COMMENT 'Atomic sales fact, one row per order line item.';
