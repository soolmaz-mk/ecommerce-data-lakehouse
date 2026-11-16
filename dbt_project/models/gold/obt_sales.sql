{{ config(
    order_by=['order_date_key']
) }}

-- Flat query structure to prevent ClickHouse SQL Analyzer resolution bugs 
-- when reading from external tables (e.g., Iceberg)
SELECT
    s.order_item_id AS sale_id,
    s.order_id,
    s.quantity,
    s.unit_price AS price,
    s.gross_sales_amount,
    s.discount_amount,
    s.net_sales_amount AS total_amount,
    s.order_date_key,
    d.full_date AS order_date,
    d.year,
    d.month_name,
    d.day_name,
    u.user_id,
    u.username,
    u.city,
    u.province,
    u.loyalty_tier,
    p.product_id,
    p.product_name,
    p.category_top,
    p.category_sub,
    p.category_leaf

FROM {{ source('silver', 'fact_sales') }} AS s
LEFT JOIN {{ source('silver', 'dim_user') }} AS u 
    ON s.user_key = u.user_key
LEFT JOIN {{ source('silver', 'dim_product') }} AS p 
    ON s.product_key = p.product_key
LEFT JOIN {{ source('silver', 'dim_date') }} AS d 
    ON s.order_date_key = d.date_key