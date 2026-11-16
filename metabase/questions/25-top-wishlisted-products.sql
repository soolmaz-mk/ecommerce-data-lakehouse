-- name: Most wishlisted products and whether they convert
-- dashboard: Customer Journey
-- display: table
-- width: full
-- description: High wishlist volume with low conversion is the signal worth acting on -- demand exists but something stops the purchase.
SELECT
    product_id,
    max(product_name)                                                                 AS product_name,
    countIf(wishlisted = 1)                                                           AS wishlisted_by_users,
    countIf(wishlisted = 1 AND purchased = 1)                                         AS purchased_by_users,
    round(100 * countIf(wishlisted = 1 AND purchased = 1)
          / nullIf(countIf(wishlisted = 1), 0), 2)                                    AS conversion_pct
FROM
(
    SELECT
        user_id,
        product_id,
        max(product_name) AS product_name,
        max(is_wishlist)  AS wishlisted,
        max(is_sale)      AS purchased
    FROM
    (
        SELECT user_id, product_id, product_name, toUInt8(1) AS is_wishlist, toUInt8(0) AS is_sale
        FROM {{GOLD_DB}}.obt_clickstream
        WHERE event_type = 'wishlist_add'
          AND product_id NOT IN ('', 'Unknown')
          AND user_id NOT IN ('', 'Unknown')
        UNION ALL
        SELECT user_id, product_id, product_name, toUInt8(0), toUInt8(1)
        FROM {{GOLD_DB}}.obt_sales
        WHERE product_id NOT IN ('', 'Unknown')
          AND user_id NOT IN ('', 'Unknown')
    )
    GROUP BY user_id, product_id
)
GROUP BY product_id
HAVING countIf(wishlisted = 1) >= 3
ORDER BY wishlisted_by_users DESC
LIMIT 25
