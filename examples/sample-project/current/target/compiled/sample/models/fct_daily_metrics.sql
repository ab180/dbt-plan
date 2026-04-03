SELECT
    app_id,
    event_date,
    COUNT(*) AS event_count,
    COUNT(DISTINCT device_id) AS unique_devices,
    SUM(revenue) AS total_revenue
FROM int_unified
GROUP BY 1, 2
