-- name: Session conversion rate by loyalty tier
-- dashboard: Customer Journey
-- display: bar
-- width: half
-- description: Share of sessions that reached order_complete, per tier.
SELECT
    loyalty_tier,
    uniqExact(session_id)                                        AS sessions,
    uniqExactIf(session_id, event_type = 'order_complete')       AS converted_sessions,
    round(100 * uniqExactIf(session_id, event_type = 'order_complete')
              / nullIf(uniqExact(session_id), 0), 2)             AS conversion_pct
FROM {{GOLD_DB}}.obt_clickstream
GROUP BY loyalty_tier
ORDER BY conversion_pct DESC
