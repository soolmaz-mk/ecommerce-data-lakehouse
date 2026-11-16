-- name: Wishlist to purchase conversion
-- dashboard: Customer Journey
-- display: table
-- width: full
-- height: 4
-- description: Of the (user, product) pairs added to a wishlist, how many were later bought by that same user. Counted per pair, not per event.
SELECT
    countIf(wishlisted = 1)                                                           AS wishlisted_pairs,
    countIf(wishlisted = 1 AND purchased = 1)                                         AS purchased_pairs,
    round(100 * countIf(wishlisted = 1 AND purchased = 1)
          / nullIf(countIf(wishlisted = 1), 0), 2)                                    AS conversion_pct
FROM
(
    SELECT
        user_id,
        product_id,
        max(is_wishlist) AS wishlisted,
        max(is_sale)     AS purchased
    FROM
    (
        SELECT user_id, product_id, toUInt8(1) AS is_wishlist, toUInt8(0) AS is_sale
        FROM {{GOLD_DB}}.obt_clickstream
        WHERE event_type = 'wishlist_add'
          AND product_id NOT IN ('', 'Unknown')
          AND user_id NOT IN ('', 'Unknown')
        UNION ALL
        SELECT user_id, product_id, toUInt8(0), toUInt8(1)
        FROM {{GOLD_DB}}.obt_sales
        WHERE product_id NOT IN ('', 'Unknown')
          AND user_id NOT IN ('', 'Unknown')
    )
    GROUP BY user_id, product_id
)
