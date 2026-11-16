-- name: Avro decode failures by topic
-- dashboard: Real-Time Pipeline Monitoring
-- display: table
-- description: Records the Kafka consumer could not decode, with a sample error. Non-zero here usually means a schema was registered that the consumer has not picked up.
SELECT
    bucket,
    topic,
    errors,
    sample_error
FROM monitoring.v_ingest_errors
ORDER BY bucket DESC, errors DESC
LIMIT 100
