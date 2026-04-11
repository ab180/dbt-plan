-- Window functions: common in dbt incremental models
SELECT
    user_id,
    event_date,
    event_type,
    ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY event_date DESC) AS row_num,
    LAG(event_date) OVER (PARTITION BY user_id ORDER BY event_date) AS prev_event_date,
    SUM(revenue) OVER (PARTITION BY user_id) AS lifetime_revenue
FROM events
QUALIFY row_num = 1
