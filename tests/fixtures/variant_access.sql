-- Pattern: Snowflake VARIANT column access + QUALIFY
-- Expected: ["write_date", "data__device__airbridgegenerateddeviceuuid", "data__device__deviceuuid", "data__device__country", "app_id", "event_date"]

SELECT
    e.write_date,
    e.data__device:airbridgeGeneratedDeviceUUID::STRING AS data__device__airbridgegenerateddeviceuuid,
    e.data__device:deviceUUID::STRING AS data__device__deviceuuid,
    e.data__device:country::STRING AS data__device__country,
    e.app_id,
    e.event_date
FROM all_events AS e
QUALIFY ROW_NUMBER() OVER (PARTITION BY log_uuid ORDER BY received_timestamp DESC) = 1
