-- models/staging/stg_users.sql

SELECT
    user_id,
    signup_date,
    country,
    acquisition_channel,
    signup_date::DATE AS signup_day
FROM
    {{ source('stg_data', 'users') }}
