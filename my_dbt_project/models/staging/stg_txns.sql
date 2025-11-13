-- models/staging/stg_txns.sql


SELECT
    transaction_id AS txn_id,
    user_id,
    amount,
    fee,
    type AS txn_type,
    currency,
    timestamp AS transaction_timestamp
FROM
    {{ source('stg_data', 'transactions') }}
