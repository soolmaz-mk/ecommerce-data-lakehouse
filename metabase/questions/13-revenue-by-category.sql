-- name: Revenue and discount by category
-- dashboard: Revenue & Discounts
-- display: bar
-- width: half
-- description: BLOCKED UPSTREAM -- returns a single "Unknown" bar. products.category is NULL in 100% of source messages.
SELECT
    category_top                      AS category,
    round(sum(gross_sales_amount), 2) AS gross_sales,
    round(sum(discount_amount), 2)    AS discount,
    round(sum(total_amount), 2)       AS net_sales,
    count()                           AS line_items
FROM {{GOLD_DB}}.obt_sales
GROUP BY category
ORDER BY net_sales DESC
