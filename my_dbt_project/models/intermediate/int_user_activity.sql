-- models/intermediate/int_user_activity.sql

{{ config(materialized='table') }}

WITH unified_events AS (
    -- Signup as event
    SELECT 
        user_id,
        signup_date AS event_timestamp,
        'signup' AS event_name,
        NULL AS platform,
        NULL AS device_type
    FROM {{ ref('stg_users') }}

    UNION ALL

    -- Real events  
    SELECT 
        user_id,
        event_timestamp,
        event_name,
        platform,
        device_type
    FROM {{ ref('stg_events') }}
),
daily_events AS (
	SELECT
		user_id,
		event_timestamp,
		DATE(event_timestamp) AS event_date,
		event_name,
		platform,
		device_type
	FROM unified_events
)
SELECT
	user_id,
	event_date,
	COUNT(*) total_events,
	MIN(event_timestamp) first_event_ts,
	MAX(event_timestamp) last_event_ts,
	COUNT(CASE WHEN platform='android' THEN 1 END) as android_events,
	COUNT(CASE WHEN platform='ios' THEN 1 END) as ios_events,
	COUNT(CASE WHEN platform='web' THEN 1 END) as web_events,
	COUNT(CASE WHEN device_type='mobile' THEN 1 END) as mobile_events,
	COUNT(CASE WHEN device_type='desktop' THEN 1 END) as desktop_events,
	CASE WHEN event_date = MIN(event_date) OVER(PARTITION BY user_id) 
		THEN TRUE ELSE FALSE END AS is_first_active_day
FROM
	daily_events
GROUP BY
	user_id, event_date
