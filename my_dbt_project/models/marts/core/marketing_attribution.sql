-- models/marts/core/marketing_attribution.sql
	{{ config(materialized='table') }}
	
with channel_aggregation as (
	select
		acquisition_channel,
		date_trunc('day', usr.signup_ts)::date as signup_date,
		count(case when usr.signup_ts is not null then 1 end) as 
signups_per_channel,
		count (case when usr.kyc_complete_ts is not null then 1 
end) as kyc_per_channel,
		count (case when usr.activation_ts is not null then 1 end) 
as activations_per_channel
	from {{ ref('user_funnel') }} usr
	group by 1,2
)
select 
	mkt.date,
	mkt.channel,
	nullif(ca.signups_per_channel,0) as signups_per_channel,
	nullif(ca.kyc_per_channel,0) as kyc_per_channel,
	nullif(ca.activations_per_channel,0) as activations_per_channel,
	mkt.spend as mkt_spend,
	case when ca.signups_per_channel > 0 then round((mkt.spend / 
ca.signups_per_channel)::numeric,2) end as CAC,
	case when ca.activations_per_channel > 0 then round((spend / 
activations_per_channel)::numeric,2) end as CPA
from {{ ref('int_marketing_daily') }} mkt 
left join channel_aggregation ca on mkt.channel = ca.acquisition_channel 
and mkt.date = ca.signup_date
group by mkt.date, mkt.channel, signups_per_channel,kyc_per_channel, 
activations_per_channel, mkt.spend
order by mkt.date, mkt.channel
