-- name: Gross vs net revenue by month
-- dashboard: Revenue & Discounts
-- display: bar
-- width: half
-- description: Monthly gross sales, discount given and net sales at line grain.
SELECT
    toStartOfMonth(order_date)        AS month,
    round(sum(gross_sales_amount), 2) AS gross_sales,
    round(sum(discount_amount), 2)    AS discount,
    round(sum(total_amount), 2)       AS net_sales
FROM {{GOLD_DB}}.obt_sales
WHERE order_date IS NOT NULL
GROUP BY month
ORDER BY month
