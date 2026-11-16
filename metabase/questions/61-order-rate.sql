-- name: Order rate per minute (business pulse)
-- dashboard: Real-Time Pipeline Monitoring
-- display: line
-- description: Orders arriving per minute over the last 6 hours. A flat line at zero means the producer or the Kafka path has stopped.
SELECT
    minute,
    orders,
    distinct_users,
    avg_order_value
FROM monitoring.v_order_rate
ORDER BY minute
