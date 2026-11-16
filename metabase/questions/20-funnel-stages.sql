-- name: Purchase funnel by stage
-- dashboard: Customer Journey
-- display: funnel
-- width: half
-- description: Distinct sessions and users reaching each stage, ordered by the real funnel sequence.
-- viz: {"funnel.dimension": "stage", "funnel.metric": "sessions"}
SELECT
    stage_no,
    stage,
    sessions,
    users
FROM (
    SELECT
        multiIf(
            event_type = 'page_view',       1,
            event_type = 'product_search',  2,
            event_type = 'add_to_cart',     3,
            event_type = 'checkout_start',  4,
            event_type = 'payment_attempt', 5,
            event_type = 'order_complete',  6,
            99)                   AS stage_no,
        event_type                AS stage,
        uniqExact(session_id)     AS sessions,
        uniqExact(user_id)        AS users
    FROM {{GOLD_DB}}.obt_clickstream
    WHERE event_type IN (
        'page_view', 'product_search', 'add_to_cart',
        'checkout_start', 'payment_attempt', 'order_complete')
    GROUP BY stage_no, stage
)
ORDER BY stage_no
