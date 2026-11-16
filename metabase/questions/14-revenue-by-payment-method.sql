-- name: Orders and revenue by payment method
-- dashboard: Revenue & Discounts
-- display: bar
-- width: half
-- description: Order-grain view, so this one reads obt_orders rather than obt_sales.
SELECT
    payment_method,
    uniqExact(order_id)                  AS orders,
    round(sum(total_order_value), 2)     AS net_revenue,
    round(sum(discount_amount), 2)       AS discount,
    round(sum(total_order_value) / nullIf(uniqExact(order_id), 0), 2) AS avg_order_value
FROM {{GOLD_DB}}.obt_orders
GROUP BY payment_method
ORDER BY net_revenue DESC
