-- name: Order status mix (cancelled / refunded watch)
-- dashboard: Real-Time Pipeline Monitoring
-- display: bar
-- description: Share of each order status per 5 minutes. A jump in cancelled or refunded points at payment or fulfilment.
SELECT
    bucket,
    status,
    orders,
    share
FROM monitoring.v_order_status_mix
ORDER BY bucket, status
