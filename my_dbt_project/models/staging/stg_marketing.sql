-- models/staging/stg_marketing.sql


SELECT
    date,
    channel,
    amount_spent AS spend
FROM
    {{ source('stg_data', 'marketing_spend') }}
