-- Pattern: CTE chain ending in SELECT *
-- Expected: ["*"]

WITH base AS (
    SELECT *
    FROM raw_events
    WHERE event_date >= '2024-01-01'
),

enriched AS (
    SELECT
        base.*,
        dim.country
    FROM base
    INNER JOIN dim_device AS dim
        ON base.device_id = dim.device_id
)

SELECT * FROM enriched
