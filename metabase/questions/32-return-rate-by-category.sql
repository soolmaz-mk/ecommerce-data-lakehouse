-- name: Return rate by category
-- dashboard: Retention & Returns
-- display: bar
-- width: half
-- description: BLOCKED UPSTREAM -- one "Unknown" bar, because products.category is NULL in 100% of source messages. Rate is by refund AMOUNT, not unit count.
SELECT
    category,
    round(sum(sale_value), 2)                                        AS net_sales,
    round(sum(refund_value), 2)                                      AS refunds,
    round(100 * sum(refund_value) / nullIf(sum(sale_value), 0), 2)   AS return_rate_pct
FROM
(
    SELECT
        category_top            AS category,
        toFloat64(total_amount) AS sale_value,
        toFloat64(0)            AS refund_value
    FROM {{GOLD_DB}}.obt_sales
    UNION ALL
    SELECT
        category_top,
        toFloat64(0),
        toFloat64(refund_amount)
    FROM {{GOLD_DB}}.obt_returns
)
GROUP BY category
ORDER BY return_rate_pct DESC
