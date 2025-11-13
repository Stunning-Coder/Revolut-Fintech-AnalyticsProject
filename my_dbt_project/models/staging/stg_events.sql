-- models/staging/stg_events.sql

SELECT
    event_id,
    user_id,
    event_name,
    event_timestamp,
    platform,
    device_type
FROM
    {{ source('stg_data', 'events') }} 
ORDER BY user_id, event_timestamp
