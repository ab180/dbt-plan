{{ config(
    materialized='incremental',
    on_schema_change='sync_all_columns'
) }}

SELECT
    event_id,
    app_id,
    device_id,
    event_date
FROM {{ ref('stg_events') }}
