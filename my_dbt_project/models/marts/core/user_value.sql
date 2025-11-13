-- models/marts/user_value.sql

{{ config(materialized='table') }}

with user_activity_agg as (
  select
    user_id,
    count(distinct event_date) as active_days,
    min(first_event_ts) as first_event_ts,
    max(last_event_ts) as last_event_ts,
    sum(total_events) as total_events
  from {{ ref('int_user_activity') }}
  group by user_id
)
select 
	ua.user_id,
	uf.country,
	uf.acquisition_channel,
	uf.signup_date,
	uf.activated_flag,
	uf.funnel_stage,
	ita.total_txn_count,
	ita.gmv,
	ita.total_fees,
	ita.avg_txn_amount,
	ita.first_transaction_ts,
	ita.last_transaction_ts,
	ua.total_events,
	ua.first_event_ts,
	ua.last_event_ts,
	ua.active_days,
	extract(day from(coalesce(ua.last_event_ts, now()) - uf.signup_date)) as lifetime_days,
	round((nullif(ita.gmv,0) / ua.active_days),2) as gmv_per_active_days,
	round((ita.total_fees / NULLIF(ita.total_txn_count, 0)),2) as avg_fee_per_txn
from user_activity_agg ua
left join {{ ref('user_funnel') }} uf using(user_id)
left join {{ ref('int_user_txn_agg') }} ita
	using(user_id)
group by
	ua.user_id,
	uf.country,
	uf.acquisition_channel,
	uf.signup_date,
	uf.activated_flag,
	uf.funnel_stage,
	ita.total_txn_count,
	ita.gmv,
	ita.total_fees,
	ita.avg_txn_amount,
	ita.first_transaction_ts,
	ita.last_transaction_ts,
	ua.total_events,
	ua.first_event_ts,
	ua.last_event_ts,
	ua.active_days
