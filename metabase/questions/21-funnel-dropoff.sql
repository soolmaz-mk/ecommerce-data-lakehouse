-- name: Funnel drop-off between stages
-- dashboard: Customer Journey
-- display: table
-- width: half
-- description: Where users actually leave -- conversion against the entry stage and against the previous stage.
SELECT
    stage_no,
    stage,
    sessions,
    round(100 * sessions / nullIf(first_value(sessions) OVER (
        ORDER BY stage_no ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING), 0), 2) AS pct_of_entry,
    round(100 * sessions / nullIf(lagInFrame(sessions, 1) OVER (
        ORDER BY stage_no ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), 0), 2)                 AS pct_of_previous
FROM
(
    SELECT
        multiIf(
            event_type = 'page_view',       1,
            event_type = 'product_search',  2,
            event_type = 'add_to_cart',     3,
            event_type = 'checkout_start',  4,
            event_type = 'payment_attempt', 5,
            event_type = 'order_complete',  6,
            99)               AS stage_no,
        event_type            AS stage,
        uniqExact(session_id) AS sessions
    FROM {{GOLD_DB}}.obt_clickstream
    WHERE event_type IN (
        'page_view', 'product_search', 'add_to_cart',
        'checkout_start', 'payment_attempt', 'order_complete')
    GROUP BY stage_no, stage
)
ORDER BY stage_no
