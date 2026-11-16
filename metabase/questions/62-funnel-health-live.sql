-- name: Funnel health (add-to-cart per page-view)
-- dashboard: Real-Time Pipeline Monitoring
-- display: line
-- description: Live cart/view ratio. Page views healthy but carts collapsing means broken UI or broken event tagging, not a dead pipeline.
SELECT
    bucket,
    page_views,
    add_to_carts,
    checkout_starts,
    orders_completed,
    cart_per_view
FROM monitoring.v_funnel_health
ORDER BY bucket
