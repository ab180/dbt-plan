

SELECT
    event_id,
    app_id,
    device_uuid,
    event_date,
    'unknown' AS source
FROM "memory"."main"."stg_events"