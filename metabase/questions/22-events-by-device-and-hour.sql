-- name: Engagement by device and weekday
-- dashboard: Customer Journey
-- display: bar
-- width: half
-- description: Event volume and median session dwell time split by device.
SELECT
    device,
    day_name,
    count()                                  AS events,
    uniqExact(session_id)                    AS sessions,
    round(medianOrNull(duration_sec), 1)     AS median_duration_sec
FROM {{GOLD_DB}}.obt_clickstream
GROUP BY device, day_name
ORDER BY events DESC
