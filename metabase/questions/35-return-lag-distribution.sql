-- name: How long after the order do returns arrive
-- dashboard: Retention & Returns
-- display: bar
-- width: half
-- description: Return lag in days, bucketed. Negative buckets are upstream future-dating, not a modelling error.
SELECT
    multiIf(
        return_lag_days < 0,   '00 future-dated (upstream bug)',
        return_lag_days <= 1,  '01 same day / next day',
        return_lag_days <= 7,  '02 within a week',
        return_lag_days <= 14, '03 1-2 weeks',
        return_lag_days <= 30, '04 2-4 weeks',
        '05 over a month')             AS lag_bucket,
    count()                            AS returns,
    round(sum(refund_amount), 2)       AS refunds
FROM {{GOLD_DB}}.obt_returns
WHERE return_lag_days IS NOT NULL
GROUP BY lag_bucket
ORDER BY lag_bucket
