-- Table:    silver.fact_returns
-- Type:     atomic transaction fact
-- Grain:    ONE ROW PER RETURN (return_refund_id)
-- Source:   transactional.returns_refunds - registered 2026-07-28 with 2,659
--           messages, after being empty and unregistered for the whole project.
--
-- Grain note: the source schema makes order_item_id nullable, but it is
--   non-null in 2,659/2,659 rows, so returns are line-grain in practice and
--   conform to fact_sales on (order_id, order_item_id). The NULL case is still
--   handled - it resolves product_key to the -1 member rather than dropping.
--
-- NO return_quantity: the source has no quantity field, so return RATE must be
--   computed as sum(refund_amount) / sum(net_sales_amount) against fact_sales.
--   A unit-based ratio is not buildable from this source.
--
-- LOAD IS A FULL REBUILD, not incremental, deliberately:
--   96.4% of source rows carry a return_timestamp in the FUTURE (observed up to
--   two weeks ahead of wall clock). A window of
--   [interval_start - lookback, interval_end] therefore matches almost nothing,
--   and a future-dated row would be silently skipped by every run until its
--   date arrived. At 2,659 rows a full rebuild costs nothing and is immune.
--   Revisit if the topic ever grows past a few million rows.
--
-- Separate fact, not folded into fact_sales: different process, different date,
--   different actor. Folding would force NULLs onto every non-returned line and
--   destroy the return-date timeline.

CREATE TABLE IF NOT EXISTS silver.fact_returns (
    return_date_key   INT           NOT NULL COMMENT 'FK -> dim_date, from return_timestamp',
    order_date_key    INT           NOT NULL COMMENT 'FK -> dim_date, from the original order, -1 when unresolvable',
    user_key          BIGINT        NOT NULL COMMENT 'FK -> dim_user, point-in-time at return time',
    product_key       BIGINT        NOT NULL COMMENT 'FK -> dim_product, point-in-time at ORDER time (the version sold)',
    return_reason_key INT           NOT NULL COMMENT 'FK -> dim_return_reason',
    return_refund_id  STRING        NOT NULL COMMENT 'Degenerate dimension, the grain',
    order_id          STRING        NOT NULL COMMENT 'Degenerate, conforms to fact_orders',
    order_item_id     STRING                 COMMENT 'Degenerate, conforms to fact_sales; nullable per source schema',
    return_timestamp  TIMESTAMP     NOT NULL COMMENT 'When the return happened (UTC)',
    order_timestamp   TIMESTAMP              COMMENT 'When the original order happened, NULL when unresolvable',
    refund_amount     DECIMAL(18,2)          COMMENT 'Amount refunded, additive',
    return_lag_days   INT                    COMMENT 'Days between order and return, NON-additive (average only)',
    return_count      INT                    COMMENT 'Always 1, additive'
)
USING iceberg
PARTITIONED BY (return_date_key)
COMMENT 'Atomic returns fact, one row per return.';
