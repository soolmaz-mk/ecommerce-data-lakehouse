{{ config(
    order_by=['event_date_key']
) }}

-- Eliminating WITH clauses to ensure correct alias mapping in ClickHouse
SELECT
    c.event_key AS event_id,
    c.session_id,
    c.duration_sec,
    c.cart_value,
    c.event_date_key,
    d.full_date AS event_date,
    d.day_name,
    d.is_weekend,
    e.event_type,
    dev.device,
    u.user_id,
    u.loyalty_tier,
    p.product_id,
    p.product_name,
    p.category_top

FROM {{ source('silver', 'fact_clickstream') }} AS c
LEFT JOIN {{ source('silver', 'dim_user') }} AS u
    ON c.user_key = u.user_key
LEFT JOIN {{ source('silver', 'dim_product') }} AS p
    ON c.product_key = p.product_key
LEFT JOIN {{ source('silver', 'dim_event_type') }} AS e 
    ON c.event_type_key = e.event_type_key
LEFT JOIN {{ source('silver', 'dim_device') }} AS dev 
    ON c.device_key = dev.device_key
LEFT JOIN {{ source('silver', 'dim_date') }} AS d 
    ON c.event_date_key = d.date_key