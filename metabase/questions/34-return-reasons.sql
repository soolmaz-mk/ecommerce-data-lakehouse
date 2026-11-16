-- name: Refunds by return reason
-- dashboard: Retention & Returns
-- display: row
-- width: half
-- description: Which reasons cost the most money, not just which occur most often.
SELECT
    return_reason,
    count()                        AS returns,
    round(sum(refund_amount), 2)   AS refunds,
    round(avg(refund_amount), 2)   AS avg_refund
FROM {{GOLD_DB}}.obt_returns
GROUP BY return_reason
ORDER BY refunds DESC
