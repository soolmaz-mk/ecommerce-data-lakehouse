-- Table:    silver.fact_orders
-- Type:     consolidated / header fact
-- Grain:    ONE ROW PER ORDER
-- Load:     incremental over [interval_start - lookback, interval_end],
--           written with dynamic_partition_overwrite on order_date_key so a
--           re-run of a window converges instead of duplicating.
-- WARNING:  never join fact_orders to fact_sales and sum - they overlap.
--           fact_orders IS a rollup of fact_sales. Use one or the other.
-- Note:     order_gross_amount is the source-asserted orders.total; the source
--           makes it exactly equal to sum(qty * unit_price), which is why
--           source_total_matches_lines is materialised as a free DQ signal.

CREATE TABLE IF NOT EXISTS silver.fact_orders (
    order_date_key             INT           NOT NULL COMMENT 'FK -> dim_date',
    user_key                   BIGINT        NOT NULL COMMENT 'FK -> dim_user, point-in-time at order time',
    payment_method_key         INT           NOT NULL COMMENT 'FK -> dim_payment_method',
    order_status_key           INT           NOT NULL COMMENT 'FK -> dim_order_status',
    order_id                   STRING        NOT NULL COMMENT 'Degenerate dimension, the grain',
    order_timestamp            TIMESTAMP     NOT NULL COMMENT 'Order placement time (UTC)',
    order_gross_amount         DECIMAL(18,2)          COMMENT 'orders.total - pre-discount, additive',
    order_net_amount           DECIMAL(18,2)          COMMENT 'Rolled up sum(item_total_amount), additive',
    order_discount_amount      DECIMAL(18,2)          COMMENT 'gross - net, derived, additive',
    order_line_count           INT                    COMMENT 'Number of order lines, additive',
    order_total_quantity       INT                    COMMENT 'Sum of line quantities, additive',
    source_total_matches_lines BOOLEAN                COMMENT 'Reconciliation flag: total == sum(line gross)'
)
USING iceberg
PARTITIONED BY (order_date_key)
COMMENT 'Order header fact, one row per order.';
