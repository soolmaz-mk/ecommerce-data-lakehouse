{{ config(
    order_by=['order_date_key']
) }}

-- Direct joins on source tables to avoid CTE alias scoping issues
SELECT
    o.order_id,
    o.order_discount_amount AS discount_amount,
    o.order_net_amount AS total_order_value,
    o.order_date_key,
    d.full_date AS order_date,
    d.year,
    d.month_name,
    s.order_status,
    p.payment_method,
    u.user_id,
    u.city,
    u.province,
    u.loyalty_tier

FROM {{ source('silver', 'fact_orders') }} AS o
LEFT JOIN {{ source('silver', 'dim_user') }} AS u 
    ON o.user_key = u.user_key
LEFT JOIN {{ source('silver', 'dim_order_status') }} AS s 
    ON o.order_status_key = s.order_status_key
LEFT JOIN {{ source('silver', 'dim_payment_method') }} AS p 
    ON o.payment_method_key = p.payment_method_key
LEFT JOIN {{ source('silver', 'dim_date') }} AS d 
    ON o.order_date_key = d.date_key