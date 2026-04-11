-- Multi-CTE chain: very common in dbt models
WITH source AS (
    SELECT
        id,
        created_at,
        raw_data
    FROM raw_events
),

renamed AS (
    SELECT
        id AS event_id,
        created_at AS event_timestamp,
        raw_data:user_id::STRING AS user_id,
        raw_data:device_type::STRING AS device_type
    FROM source
),

final AS (
    SELECT
        event_id,
        event_timestamp,
        user_id,
        device_type,
        DATE_TRUNC('day', event_timestamp) AS event_date
    FROM renamed
)

SELECT * FROM final
