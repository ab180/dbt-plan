{{ config(materialized='table') }}

SELECT
    app_id,
    'App Name' AS app_name
FROM {{ ref('stg_events') }}
GROUP BY 1
