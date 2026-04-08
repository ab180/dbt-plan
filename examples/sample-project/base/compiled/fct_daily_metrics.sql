SELECT
    app_id,
    event_date,
    data__device,
    COUNT(*) AS event_count,
    COUNT(DISTINCT device_id) AS unique_devices
FROM int_unified
GROUP BY 1, 2, 3
