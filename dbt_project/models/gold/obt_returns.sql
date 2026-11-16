{{ config(
    order_by=['return_date_key']
) }}

-- Fully flattened structure for seamless Iceberg table integration
SELECT
    r.return_refund_id AS return_id,
    r.order_item_id AS sale_id,
    r.refund_amount,
    r.return_lag_days,
    r.return_date_key,
    d.full_date AS return_date,
    d.year,
    d.month,
    rr.return_reason,
    p.product_id,
    p.product_name,
    p.category_top,
    u.user_id,
    u.loyalty_tier

FROM {{ source('silver', 'fact_returns') }} AS r
LEFT JOIN {{ source('silver', 'dim_user') }} AS u 
    ON r.user_key = u.user_key
LEFT JOIN {{ source('silver', 'dim_product') }} AS p 
    ON r.product_key = p.product_key
LEFT JOIN {{ source('silver', 'dim_return_reason') }} AS rr 
    ON r.return_reason_key = rr.return_reason_key
LEFT JOIN {{ source('silver', 'dim_date') }} AS d 
    ON r.return_date_key = d.date_key