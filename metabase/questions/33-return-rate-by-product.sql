-- name: Worst return rates by product
-- dashboard: Retention & Returns
-- display: table
-- width: full
-- description: Product-level return rate. Trustworthy only for the ~45% of returns whose order_item resolves to a product; unresolved ones carry the -1 sentinel and are excluded.
SELECT
    product_id,
    max(product_name)                                              AS product_name,
    round(sum(sale_value), 2)                                      AS net_sales,
    round(sum(refund_value), 2)                                    AS refunds,
    sum(return_row)                                                AS returns,
    round(100 * sum(refund_value) / nullIf(sum(sale_value), 0), 2) AS return_rate_pct
FROM
(
    SELECT
        product_id,
        product_name,
        toFloat64(total_amount) AS sale_value,
        toFloat64(0)            AS refund_value,
        toUInt64(0)             AS return_row
    FROM {{GOLD_DB}}.obt_sales
    WHERE product_id NOT IN ('', 'Unknown')
    UNION ALL
    SELECT
        product_id,
        product_name,
        toFloat64(0),
        toFloat64(refund_amount),
        toUInt64(1)
    FROM {{GOLD_DB}}.obt_returns
    WHERE product_id NOT IN ('', 'Unknown')
)
GROUP BY product_id
HAVING sum(sale_value) > 0 AND sum(return_row) > 0
ORDER BY return_rate_pct DESC
LIMIT 25
