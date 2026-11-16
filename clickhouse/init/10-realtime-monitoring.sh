#!/usr/bin/env bash
# Real-time pipeline monitoring: Kafka engine -> materialized view -> MergeTree.
# Idempotent, so it runs both from initdb.d and by hand on a live server.
set -euo pipefail

if [ "${MONITORING_ENABLED:-0}" != "1" ]; then
    echo "[monitoring] skipped (MONITORING_ENABLED != 1) -- layer is parked, see metabase/README.md 4b"
    exit 0
fi

CH_USER="${CLICKHOUSE_USER:-default}"
CH_PASSWORD="${CLICKHOUSE_PASSWORD:-}"
BROKERS="${KAFKA_BOOTSTRAP_SERVERS:?KAFKA_BOOTSTRAP_SERVERS is required}"
REGISTRY="${SCHEMA_REGISTRY_URL:?SCHEMA_REGISTRY_URL is required}"
RETENTION_HOURS="${MONITORING_RETENTION_HOURS:-48}"
CONSUMER_GROUP="${MONITORING_CONSUMER_GROUP:-clickhouse_monitoring}"

ch() { clickhouse-client --user "$CH_USER" --password "$CH_PASSWORD" --multiquery "$@"; }

echo "[monitoring] brokers=$BROKERS registry=$REGISTRY retention=${RETENTION_HOURS}h"

ch <<SQL
CREATE DATABASE IF NOT EXISTS monitoring;
SQL

ch <<SQL
CREATE TABLE IF NOT EXISTS monitoring.kafka_orders_raw
(
    order_id       Nullable(String),
    user_id        Nullable(String),
    timestamp      Nullable(DateTime64(6)),
    total          Nullable(Decimal(18, 2)),
    status         Nullable(String),
    payment_method Nullable(String)
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = '${BROKERS}',
    kafka_topic_list = 'transactional.orders',
    kafka_group_name = '${CONSUMER_GROUP}_orders',
    kafka_format = 'AvroConfluent',
    kafka_num_consumers = 1,
    kafka_handle_error_mode = 'stream',
    kafka_max_block_size = 65536,
    kafka_poll_max_batch_size = 8192,
    kafka_flush_interval_ms = 5000,
    format_avro_schema_registry_url = '${REGISTRY}',
    input_format_avro_allow_missing_fields = 1;

CREATE TABLE IF NOT EXISTS monitoring.kafka_events_raw
(
    timestamp  Nullable(String),
    user_id    Nullable(String),
    session_id Nullable(String),
    event_type Nullable(String),
    device     Nullable(String),
    product_id Nullable(String)
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = '${BROKERS}',
    kafka_topic_list = 'behavioral.events',
    kafka_group_name = '${CONSUMER_GROUP}_events',
    kafka_format = 'AvroConfluent',
    kafka_num_consumers = 1,
    kafka_handle_error_mode = 'stream',
    kafka_max_block_size = 65536,
    kafka_poll_max_batch_size = 8192,
    kafka_flush_interval_ms = 5000,
    format_avro_schema_registry_url = '${REGISTRY}',
    input_format_avro_allow_missing_fields = 1;
SQL

ch <<SQL
CREATE TABLE IF NOT EXISTS monitoring.orders_stream
(
    ingested_at    DateTime DEFAULT now(),
    order_ts       Nullable(DateTime64(6)),
    order_id       String,
    user_id        String,
    total          Nullable(Decimal(18, 2)),
    status         LowCardinality(String),
    payment_method LowCardinality(String)
)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(ingested_at)
ORDER BY (ingested_at, order_id)
TTL ingested_at + INTERVAL ${RETENTION_HOURS} HOUR;

CREATE TABLE IF NOT EXISTS monitoring.events_stream
(
    ingested_at     DateTime DEFAULT now(),
    event_ts        Nullable(DateTime64(6)),
    user_id         String,
    session_id      String,
    event_type      LowCardinality(String),
    device          LowCardinality(String),
    product_id      String,
    missing_user    UInt8,
    missing_session UInt8
)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(ingested_at)
ORDER BY (ingested_at, event_type)
TTL ingested_at + INTERVAL ${RETENTION_HOURS} HOUR;

CREATE TABLE IF NOT EXISTS monitoring.ingest_errors
(
    ingested_at DateTime DEFAULT now(),
    topic       LowCardinality(String),
    partition   UInt64,
    offset      UInt64,
    error       String,
    raw         String
)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(ingested_at)
ORDER BY (ingested_at, topic)
TTL ingested_at + INTERVAL ${RETENTION_HOURS} HOUR;
SQL

ch <<SQL
CREATE MATERIALIZED VIEW IF NOT EXISTS monitoring.mv_orders
TO monitoring.orders_stream AS
SELECT
    now()                             AS ingested_at,
    timestamp                         AS order_ts,
    ifNull(order_id, '')              AS order_id,
    ifNull(user_id, '')               AS user_id,
    total                             AS total,
    ifNull(status, 'unknown')         AS status,
    ifNull(payment_method, 'unknown') AS payment_method
FROM monitoring.kafka_orders_raw
WHERE length(_error) = 0;

CREATE MATERIALIZED VIEW IF NOT EXISTS monitoring.mv_events
TO monitoring.events_stream AS
SELECT
    now()                                                  AS ingested_at,
    parseDateTime64BestEffortOrNull(ifNull(timestamp, '')) AS event_ts,
    ifNull(user_id, '')                                    AS user_id,
    ifNull(session_id, '')                                 AS session_id,
    ifNull(event_type, 'unknown')                          AS event_type,
    ifNull(device, 'unknown')                              AS device,
    ifNull(product_id, '')                                 AS product_id,
    toUInt8(user_id IS NULL OR user_id = '')               AS missing_user,
    toUInt8(session_id IS NULL OR session_id = '')         AS missing_session
FROM monitoring.kafka_events_raw
WHERE length(_error) = 0;

CREATE MATERIALIZED VIEW IF NOT EXISTS monitoring.mv_order_errors
TO monitoring.ingest_errors AS
SELECT now() AS ingested_at, _topic AS topic, _partition AS partition,
       _offset AS offset, _error AS error, _raw_message AS raw
FROM monitoring.kafka_orders_raw
WHERE length(_error) > 0;

CREATE MATERIALIZED VIEW IF NOT EXISTS monitoring.mv_event_errors
TO monitoring.ingest_errors AS
SELECT now() AS ingested_at, _topic AS topic, _partition AS partition,
       _offset AS offset, _error AS error, _raw_message AS raw
FROM monitoring.kafka_events_raw
WHERE length(_error) > 0;
SQL

ch <<SQL
CREATE OR REPLACE VIEW monitoring.v_order_rate AS
SELECT
    toStartOfMinute(ingested_at)          AS minute,
    count()                               AS orders,
    uniqExact(user_id)                    AS distinct_users,
    round(avgOrNull(toFloat64(total)), 2) AS avg_order_value
FROM monitoring.orders_stream
WHERE ingested_at >= now() - INTERVAL 6 HOUR
GROUP BY minute
ORDER BY minute;

CREATE OR REPLACE VIEW monitoring.v_funnel_health AS
SELECT
    toStartOfFiveMinutes(ingested_at)      AS bucket,
    countIf(event_type = 'page_view')      AS page_views,
    countIf(event_type = 'add_to_cart')    AS add_to_carts,
    countIf(event_type = 'checkout_start') AS checkout_starts,
    countIf(event_type = 'order_complete') AS orders_completed,
    round(countIf(event_type = 'add_to_cart') /
          nullIf(countIf(event_type = 'page_view'), 0), 4) AS cart_per_view
FROM monitoring.events_stream
WHERE ingested_at >= now() - INTERVAL 6 HOUR
GROUP BY bucket
ORDER BY bucket;

CREATE OR REPLACE VIEW monitoring.v_order_anomalies AS
SELECT
    toStartOfFiveMinutes(ingested_at)          AS bucket,
    count()                                    AS orders,
    round(avgOrNull(toFloat64(total)), 2)      AS avg_total,
    round(quantile(0.99)(toFloat64(total)), 2) AS p99_total,
    round(maxOrNull(toFloat64(total)), 2)      AS max_total,
    countIf(toFloat64(total) < 0)              AS negative_totals,
    countIf(total IS NULL)                     AS null_totals,
    countIf(toFloat64(total) = 0)              AS zero_totals
FROM monitoring.orders_stream
WHERE ingested_at >= now() - INTERVAL 6 HOUR
GROUP BY bucket
ORDER BY bucket;

CREATE OR REPLACE VIEW monitoring.v_order_status_mix AS
SELECT
    toStartOfFiveMinutes(ingested_at) AS bucket,
    status,
    count() AS orders,
    round(count() / sum(count()) OVER (PARTITION BY toStartOfFiveMinutes(ingested_at)), 4) AS share
FROM monitoring.orders_stream
WHERE ingested_at >= now() - INTERVAL 6 HOUR
GROUP BY bucket, status
ORDER BY bucket, status;

CREATE OR REPLACE VIEW monitoring.v_data_quality AS
SELECT
    toStartOfFiveMinutes(ingested_at) AS bucket,
    count()                           AS events,
    sum(missing_user)                 AS missing_user_id,
    sum(missing_session)              AS missing_session_id,
    countIf(event_ts IS NULL)         AS unparseable_timestamp,
    countIf(event_type NOT IN (
        'page_view','product_search','cart_view','add_to_cart','wishlist_add',
        'remove_from_cart','checkout_start','payment_attempt','order_complete',
        'review_submit'))             AS unknown_event_type
FROM monitoring.events_stream
WHERE ingested_at >= now() - INTERVAL 6 HOUR
GROUP BY bucket
ORDER BY bucket;

CREATE OR REPLACE VIEW monitoring.v_ingest_errors AS
SELECT
    toStartOfFiveMinutes(ingested_at) AS bucket,
    topic,
    count()    AS errors,
    any(error) AS sample_error
FROM monitoring.ingest_errors
WHERE ingested_at >= now() - INTERVAL 6 HOUR
GROUP BY bucket, topic
ORDER BY bucket, topic;

CREATE OR REPLACE VIEW monitoring.v_health_summary AS
SELECT
    (SELECT count() FROM monitoring.orders_stream
      WHERE ingested_at >= now() - INTERVAL 5 MINUTE)  AS orders_last_5m,
    (SELECT count() FROM monitoring.events_stream
      WHERE ingested_at >= now() - INTERVAL 5 MINUTE)  AS events_last_5m,
    (SELECT ifNull(dateDiff('second', max(ingested_at), now()), -1)
       FROM monitoring.orders_stream)                  AS seconds_since_last_order,
    (SELECT ifNull(dateDiff('second', max(ingested_at), now()), -1)
       FROM monitoring.events_stream)                  AS seconds_since_last_event,
    (SELECT count() FROM monitoring.ingest_errors
      WHERE ingested_at >= now() - INTERVAL 15 MINUTE) AS decode_errors_last_15m;
SQL

echo "[monitoring] ready: $(ch --query 'SELECT count() FROM system.tables WHERE database = '"'"'monitoring'"'"'') objects in monitoring"
