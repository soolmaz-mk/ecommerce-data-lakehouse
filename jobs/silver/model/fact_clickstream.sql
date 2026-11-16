-- Table:    silver.fact_clickstream
-- Type:     atomic transaction fact (essentially factless)
-- Grain:    ONE ROW PER BEHAVIOURAL EVENT
-- Load:     incremental over [interval_start - lookback, interval_end],
--           written with dynamic_partition_overwrite on event_date_key.
-- Note:     the source has no event_id, so event_key is a synthetic hash of
--           (user_id, session_id, event_timestamp, event_type, product_id).
--           Timestamps are 1-second resolution, so two genuine identical events
--           in the same second collapse into one. Small, quantified, accepted.
-- Note:     product_id is present on exactly the product-scoped event types and
--           absent on the rest (~81% of rows), so product_key resolves to the
--           -1 Not-Applicable member rather than NULL. order_id is non-null
--           only on order_complete, which is the bridge to fact_orders.
-- Partition: identity on event_date_key, NOT days(event_timestamp) -
--            dynamic_partition_overwrite rejects non-identity transforms.

CREATE TABLE IF NOT EXISTS silver.fact_clickstream (
    event_date_key  INT           NOT NULL COMMENT 'FK -> dim_date',
    event_hour      INT                    COMMENT 'Hour of day 0-23 for intraday funnels',
    user_key        BIGINT        NOT NULL COMMENT 'FK -> dim_user, point-in-time at event time',
    product_key     BIGINT        NOT NULL COMMENT 'FK -> dim_product, -1 when not product-scoped',
    event_type_key  INT           NOT NULL COMMENT 'FK -> dim_event_type',
    device_key      INT           NOT NULL COMMENT 'FK -> dim_device',
    session_id      STRING                 COMMENT 'Degenerate dimension',
    order_id        STRING                 COMMENT 'Degenerate, non-null only on order_complete',
    event_key       STRING        NOT NULL COMMENT 'Synthetic dedup identity, the grain',
    event_timestamp TIMESTAMP     NOT NULL COMMENT 'Event time (UTC)',
    event_count     INT                    COMMENT 'Always 1, additive',
    quantity        INT                    COMMENT 'Sparse, event-type specific',
    cart_value      DECIMAL(18,2)          COMMENT 'Sparse, event-type specific',
    duration_sec    INT                    COMMENT 'Sparse, event-type specific'
)
USING iceberg
PARTITIONED BY (event_date_key)
COMMENT 'Atomic clickstream fact, one row per behavioural event.';
