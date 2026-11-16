-- name: Discount rate over time
-- dashboard: Revenue & Discounts
-- display: line
-- width: half
-- description: What share of gross sales is being given away as discount, by day.
SELECT
    order_date                                                               AS day,
    round(100 * sum(discount_amount) / nullIf(sum(gross_sales_amount), 0), 2) AS discount_rate_pct,
    round(sum(discount_amount), 2)                                           AS discount_given
FROM {{GOLD_DB}}.obt_sales
WHERE order_date IS NOT NULL
GROUP BY day
ORDER BY day
