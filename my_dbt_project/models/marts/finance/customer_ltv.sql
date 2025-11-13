-- models/marts/finance/customer_ltv.sql
{{ config(materialized='table') }}


with ltv_30 as(
	select
		user_id,
		sum(fee) as ltv_30d
	from {{ ref('stg_txns') }} tx
	join {{ ref('revenue_summary') }} rs using(user_id)
	where tx.transaction_timestamp::date between rs.signup_date and 
rs.signup_date + interval '30 day'
	group by 1
),
ltv_90 as(
	select
		user_id,
		sum(fee) as ltv_90d
	from {{ ref('stg_txns') }} tx
	join {{ ref('revenue_summary') }} rs using(user_id)
	where tx.transaction_timestamp::date between rs.signup_date and 
rs.signup_date + interval '90 day'
	group by 1
)
select
	rs.user_id,
	rs.country,
	rs.acquisition_channel,
	rs.signup_date,
	rs.gmv_per_user,
	rs.revenue_per_user,
	rs.days_between_first_last_txn,
	round(coalesce(l3.ltv_30d,0)::numeric,2) as ltv_30d,
	round(coalesce(l9.ltv_90d,0)::numeric,2) as ltv_90d,
	rs.revenue_per_user as ltv_total
from
	{{ ref('revenue_summary') }} rs
left join
	ltv_30 l3 using(user_id)
left join
	ltv_90 l9 using(user_id)
