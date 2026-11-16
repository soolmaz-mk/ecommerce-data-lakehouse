-- name: Dirty data arriving from Kafka
-- dashboard: Real-Time Pipeline Monitoring
-- display: line
-- description: Mandatory fields arriving empty, unparseable timestamps and event types outside the allowed domain.
SELECT
    bucket,
    events,
    missing_user_id,
    missing_session_id,
    unparseable_timestamp,
    unknown_event_type
FROM monitoring.v_data_quality
ORDER BY bucket
