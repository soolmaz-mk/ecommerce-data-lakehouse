-- name: Revenue summary
-- dashboard: Revenue & Discounts
-- display: table
-- width: full
-- height: 4
-- description: Gross, discount, net and refunds in one row. Shipping is absent from the source data, so there is no shipping column.
SELECT
    round(sumIf(gross, source = 'sale'), 2)                                                AS gross_sales,
    round(sumIf(discount, source = 'sale'), 2)                                             AS discount_given,
    round(100 * sumIf(discount, source = 'sale') / nullIf(sumIf(gross, source = 'sale'), 0), 2) AS discount_pct,
    round(sumIf(net, source = 'sale'), 2)                                                  AS net_sales,
    round(sumIf(refund, source = 'return'), 2)                                             AS refunded,
    round(sumIf(net, source = 'sale') - sumIf(refund, source = 'return'), 2)               AS effective_revenue
FROM
(
    SELECT
        toFloat64(gross_sales_amount) AS gross,
        toFloat64(discount_amount)    AS discount,
        toFloat64(total_amount)       AS net,
        toFloat64(0)                  AS refund,
        'sale'                        AS source
    FROM {{GOLD_DB}}.obt_sales
    UNION ALL
    SELECT
        toFloat64(0),
        toFloat64(0),
        toFloat64(0),
        toFloat64(refund_amount),
        'return'
    FROM {{GOLD_DB}}.obt_returns
)
