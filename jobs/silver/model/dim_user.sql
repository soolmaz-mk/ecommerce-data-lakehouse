-- Table:    silver.dim_user
-- SCD:      Type 2 on loyalty_tier, city, province (Type 1 on username, email)
-- Grain:    one row per user per tracked-attribute version
-- Sentinels: -1 Unknown/N-A, -2 Late arriving. FKs are never NULL.
-- Note:     the source never re-emits users, so this currently yields exactly
--           one version per user. The machinery is correct; the data is quiet.

CREATE TABLE IF NOT EXISTS silver.dim_user (
    user_key     BIGINT    NOT NULL COMMENT 'Surrogate key: positive_hash(user_id, valid_from)',
    user_id      STRING    NOT NULL COMMENT 'Natural key from transactional.users',
    username     STRING             COMMENT 'Type 1 - corrections do not fork history',
    email        STRING             COMMENT 'Type 1 - corrections do not fork history',
    signup_date  DATE               COMMENT 'Account creation date',
    city         STRING             COMMENT 'Type 2 - split from source location',
    province     STRING             COMMENT 'Type 2 - split from source location',
    loyalty_tier STRING             COMMENT 'Type 2 - Bronze/Silver/Gold/Platinum',
    valid_from   TIMESTAMP NOT NULL COMMENT 'Version valid from (inclusive)',
    valid_to     TIMESTAMP          COMMENT 'Version valid to (exclusive), NULL when current',
    is_current   BOOLEAN   NOT NULL COMMENT 'True for the latest version',
    row_hash     STRING    NOT NULL COMMENT 'Hash of Type 2 attributes - change detection'
)
USING iceberg
COMMENT 'User dimension, SCD Type 2 over loyalty tier and location.';
