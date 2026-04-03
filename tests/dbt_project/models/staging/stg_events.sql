{{ config(materialized='view') }}

SELECT
    1 AS event_id,
    'app_001' AS app_id,
    '2024-01-01' AS event_date,
    'device_abc' AS device_id
