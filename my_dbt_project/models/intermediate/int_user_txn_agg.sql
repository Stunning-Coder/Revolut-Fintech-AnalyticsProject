-- models/intermediate/int_user_txn_agg.sql
{{ config(materialized='table') }}

select
	su.user_id,
	count(st.user_id) as total_txn_count,
	round(sum(coalesce(amount::numeric,0)),2) as gmv,
	round(sum(coalesce(fee::numeric,0)),2) as total_fees,
	count(case when txn_type = 'deposit' then 1 end) as deposit_txn_count,
	count(case when txn_type = 'withdrawal' then 1 end) as withdrawal_txn_count,
	count(case when txn_type = 'transfer' then 1 end) as transfer_txn_count,
	count(case when txn_type = 'merchant_payment' then 1 end) as merchant_payment_txn_count,
	round(avg(amount::numeric),2) as avg_txn_amount,
	min(transaction_timestamp) as first_transaction_ts,
	max(transaction_timestamp) as last_transaction_ts
from {{ ref('stg_users') }} su
left join {{ ref('stg_txns') }} st 
	using(user_id)
group by su.user_id
