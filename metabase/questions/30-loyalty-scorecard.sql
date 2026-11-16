-- name: Loyalty tier scorecard
-- dashboard: Retention & Returns
-- display: table
-- width: full
-- description: Orders, revenue, AOV, engagement and return rate side by side per tier. Each fact is tagged and folded in with UNION ALL, so orders are never fanned out by event count.
SELECT
    loyalty_tier,
    uniqExactIf(user_id, source = 'order')                                  AS customers,
    uniqExactIf(order_id, source = 'order')                                 AS orders,
    round(sumIf(amount, source = 'order'), 2)                               AS net_revenue,
    round(sumIf(amount, source = 'order')
          / nullIf(uniqExactIf(order_id, source = 'order'), 0), 2)          AS avg_order_value,
    round(uniqExactIf(order_id, source = 'order')
          / nullIf(uniqExactIf(user_id, source = 'order'), 0), 2)           AS orders_per_customer,
    countIf(source = 'event')                                               AS events,
    round(countIf(source = 'event')
          / nullIf(uniqExactIf(user_id, source = 'order'), 0), 1)           AS events_per_customer,
    round(100 * sumIf(amount, source = 'return')
          / nullIf(sumIf(amount, source = 'order'), 0), 2)                  AS return_rate_pct
FROM
(
    SELECT
        loyalty_tier,
        user_id,
        order_id,
        toFloat64(total_order_value) AS amount,
        'order'                      AS source
    FROM {{GOLD_DB}}.obt_orders
    UNION ALL
    SELECT
        loyalty_tier,
        user_id,
        '',
        toFloat64(0),
        'event'
    FROM {{GOLD_DB}}.obt_clickstream
    UNION ALL
    SELECT
        loyalty_tier,
        user_id,
        '',
        toFloat64(refund_amount),
        'return'
    FROM {{GOLD_DB}}.obt_returns
)
GROUP BY loyalty_tier
ORDER BY net_revenue DESC
