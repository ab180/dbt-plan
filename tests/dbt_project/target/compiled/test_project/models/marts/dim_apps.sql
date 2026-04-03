

SELECT
    app_id,
    'App Name' AS app_name
FROM "memory"."main"."stg_events"
GROUP BY 1