-- name: Order value anomalies
-- dashboard: Real-Time Pipeline Monitoring
-- display: line
-- description: Average, p99 and max order value per 5 minutes, plus counts of negative, zero and NULL totals.
SELECT
    bucket,
    orders,
    avg_total,
    p99_total,
    max_total,
    negative_totals,
    zero_totals,
    null_totals
FROM monitoring.v_order_anomalies
ORDER BY bucket
