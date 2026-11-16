-- Table:    silver.dim_category
-- SCD:      Type 1
-- Grain:    one row per category node (117 nodes, 10 roots)
-- Note:     the source tree is exactly <=3 levels deep, so it is flattened to
--           top/sub/leaf rather than snowflaked or bridged.
-- Note:     category_id is a STRING in the source ('C1', 'C2', ...), NOT an
--           integer. An earlier INT declaration here made stage_categories die
--           with "conversion from `str` to `i32` failed ... 117 out of 117".
--           category_key is therefore a hash surrogate, like dim_user/product,
--           rather than a copy of the natural key.
-- Note:     currently unreachable from any fact - products.category is NULL in
--           every source message, so there is no join path. Built and correct,
--           waiting on an upstream fix.

CREATE TABLE IF NOT EXISTS silver.dim_category (
    category_key       BIGINT NOT NULL COMMENT 'Surrogate key: positive_hash(category_id), -1 = Unknown',
    category_id        STRING NOT NULL COMMENT 'Natural key from transactional.categories, e.g. C42',
    category_name      STRING          COMMENT 'Node name',
    parent_category_id STRING          COMMENT 'Parent node, NULL at the root',
    category_top       STRING          COMMENT 'Level 1 ancestor name',
    category_sub       STRING          COMMENT 'Level 2 ancestor name',
    category_leaf      STRING          COMMENT 'Level 3 / leaf name'
)
USING iceberg
COMMENT 'Category dimension with the source tree flattened to three levels.';
