-- Table:    silver.dim_device
-- SCD:      Type 1 decode table, rebuilt from the allowed domain each run
-- Grain:    one row per device type
-- Note:     the real device signal lives on behavioral.events.device;
--           users.device is NULL in every source row and is dropped.

CREATE TABLE IF NOT EXISTS silver.dim_device (
    device_key INT    NOT NULL COMMENT 'Surrogate key, -1 = Unknown',
    device     STRING NOT NULL COMMENT 'mobile, desktop or tablet'
)
USING iceberg
COMMENT 'Device type decode table.';
