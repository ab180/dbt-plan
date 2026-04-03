-- Pattern: CTE chain with explicit final SELECT columns
-- Expected: ["app_id", "event_date", "device_id", "total_revenue", "event_count", "is_active", "day_n"]

WITH source_data AS (
    SELECT
        event_timestamp,
        received_timestamp,
        app_id,
        device_id
    FROM raw_events
    WHERE received_timestamp > '2024-01-01'
),

user_daily_agg AS (
    SELECT
        app_id,
        CAST(event_timestamp AS DATE) AS event_date,
        device_id,
        SUM(revenue) AS total_revenue,
        COUNT(*) AS event_count,
        1 AS is_active
    FROM source_data
    GROUP BY 1, 2, 3
)

SELECT
    app_id,
    event_date,
    device_id,
    total_revenue,
    event_count,
    is_active,
    CASE
        WHEN install_date IS NOT NULL
        THEN DATEDIFF('day', install_date, event_date)
        ELSE NULL
    END AS day_n
FROM user_daily_agg
