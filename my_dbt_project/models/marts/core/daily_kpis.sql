-- models/marts/core/daily_kpis.sql
{{ config(materialized='table') }}

with calendar_date as(
	select cast(generate_series('2024-01-01'::date, 
'2024-12-31'::date, '1 day') AS date) AS calendar_day
),
user_signups as (
	select 
		date_trunc('day', signup_date)::date as signup_date,
		count(distinct user_id) as new_signups
	from
		{{ ref('stg_users') }}
	group by 1
),
daily_kyc as(
	select
		date_trunc('day',kyc_start_ts)::date as kyc_start_date,
		count(case when kyc_start_ts::date is not null then 1 end) 
as new_kyc_starts
	from
		{{ ref('user_funnel') }}
	group by 1
),
daily_activations as(
	select
		date_trunc('day',activation_ts)::date as activation_date,
		count(case when activation_ts::date is not null then 1 
end) as new_activation
	from
		{{ ref('user_funnel') }}
	group by 1	
),
daily_activity as(
	select 
		date_trunc('day',event_date)::date as event_date,
		count(distinct user_id) as dau,
		sum(coalesce(android_events, 0)) as active_users_android,
		sum(coalesce(ios_events,0)) as active_users_ios,
		sum(coalesce(web_events,0)) as active_users_web
	from
		{{ ref('int_user_activity') }}
	group by 1
),
daily_transactions as(
	select
		date_trunc('day', transaction_timestamp)::date as 
txn_date,
		sum(amount) as gmv,
		sum(fee) as fees
	from
		{{ ref('stg_txns') }}
	group by 1
),
daily_marketing as(
	select 
		date,
		round(sum(coalesce(spend,0)::numeric),2) as 
marketing_spend
	from 
		{{ ref('int_marketing_daily') }}
	group by 1
)

select
	calendar_day,
	coalesce(us.new_signups, 0) as new_signups,
	coalesce(dk.new_kyc_starts, 0) as new_kyc_starts,
	coalesce(da_act.new_activation, 0) as new_activations,
	round(coalesce(dt.gmv, 0)::numeric, 2) as gmv,
	round(coalesce(dt.fees, 0)::numeric, 2) as fees,
	coalesce(dm.marketing_spend, 0) as marketing_spend,
	coalesce(da.dau, 0) as dau,
	da.active_users_android,
	da.active_users_ios,
	da.active_users_web,
	round((da_act.new_activation / 
nullif(us.new_signups,0))::numeric,2) as conversion_rate,
	round((dt.gmv / nullif(da.dau, 0))::numeric,2) as 
gmv_per_active_user
from calendar_date ca
left join user_signups us
	on ca.calendar_day = us.signup_date
left join daily_activity da
	on ca.calendar_day = da.event_date
left join daily_kyc dk
	on ca.calendar_day = dk.kyc_start_date
left join daily_activations da_act
	on ca.calendar_day = da_act.activation_date
left join daily_transactions dt
	on ca.calendar_day = dt.txn_date
left join daily_marketing dm
	on ca.calendar_day = dm.date
