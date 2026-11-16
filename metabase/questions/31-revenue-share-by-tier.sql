-- name: Revenue share by loyalty tier
-- dashboard: Retention & Returns
-- display: pie
-- width: half
-- description: How concentrated revenue is across tiers.
-- viz: {"pie.dimension": "loyalty_tier", "pie.metric": "net_revenue"}
SELECT
    loyalty_tier,
    round(sum(total_order_value), 2) AS net_revenue
FROM {{GOLD_DB}}.obt_orders
GROUP BY loyalty_tier
ORDER BY net_revenue DESC
