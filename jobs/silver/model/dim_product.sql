-- Table:    silver.dim_product
-- SCD:      Type 2, driven by the REBUILT price timeline
-- Grain:    one row per product per price version
-- Note:     the source product_price_history has valid_to/is_current that are
--           unusable (all TRUE / all NULL), so the timeline is rebuilt from
--           valid_from ordering with price_history_id as the deterministic
--           tiebreak for identical valid_from collisions.
-- Note:     category_* are 'Unknown' until upstream populates products.category,
--           which is NULL in every message. Kept as columns so the model lights
--           up with no schema change once the generator is fixed.

CREATE TABLE IF NOT EXISTS silver.dim_product (
    product_key   BIGINT        NOT NULL COMMENT 'Surrogate key: positive_hash(product_id, valid_from)',
    product_id    STRING        NOT NULL COMMENT 'Natural key from transactional.products',
    product_name  STRING                 COMMENT 'Type 1 - never changes in source',
    list_price    DECIMAL(10,2)          COMMENT 'Type 2 - catalog price for this version',
    category_top  STRING                 COMMENT 'Top of the category tree (currently Unknown)',
    category_sub  STRING                 COMMENT 'Middle of the category tree (currently Unknown)',
    category_leaf STRING                 COMMENT 'Leaf of the category tree (currently Unknown)',
    valid_from    TIMESTAMP     NOT NULL COMMENT 'Version valid from (inclusive)',
    valid_to      TIMESTAMP              COMMENT 'Version valid to (exclusive), NULL when current',
    is_current    BOOLEAN       NOT NULL COMMENT 'True for the latest version',
    row_hash      STRING        NOT NULL COMMENT 'Hash of Type 2 attributes - change detection'
)
USING iceberg
COMMENT 'Product dimension, SCD Type 2 over a rebuilt list-price timeline.';
