-- models/marts/finance/revenue_summary.sql

{{ config(materialized='table') }}

{% set data_cutoff = '2024-12-31' %}

select
	usr.user_id,
	usr.country,
	usr.acquisition_channel,
	usr.signup_date,
	txn.gmv as gmv_per_user,
	txn.total_fees as revenue_per_user,
	txn.total_txn_count as txn_per_user,
	txn.total_fees / nullif(gmv,0) as take_rate,
	txn.gmv / nullif(txn.total_txn_count,0) as avg_gmv_per_txn,
	extract(day from (txn.last_transaction_ts) - 
(txn.first_transaction_ts)) as days_between_first_last_txn,
	case when txn.last_transaction_ts > (to_date('{{ data_cutoff 
}}','YYYY-MM-DD') - interval '30 day')then TRUE else FALSE end as 
is_active_customer
from {{ ref('user_funnel') }} usr
join {{ ref('int_user_txn_agg') }} txn 
	using(user_id)
