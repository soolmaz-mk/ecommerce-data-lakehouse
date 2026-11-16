-- name: Pipeline health right now
-- dashboard: Real-Time Pipeline Monitoring
-- display: table
-- description: Freshness, not liveness. seconds_since_last_* is the number that actually detects a stalled pipeline.
SELECT
    orders_last_5m,
    events_last_5m,
    seconds_since_last_order,
    seconds_since_last_event,
    decode_errors_last_15m
FROM monitoring.v_health_summary
