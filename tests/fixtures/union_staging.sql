-- UNION ALL staging pattern: common for multi-source models
SELECT
    'ios' AS platform,
    device_id,
    event_name,
    event_timestamp
FROM raw_ios_events

UNION ALL

SELECT
    'android' AS platform,
    device_id,
    event_name,
    event_timestamp
FROM raw_android_events
