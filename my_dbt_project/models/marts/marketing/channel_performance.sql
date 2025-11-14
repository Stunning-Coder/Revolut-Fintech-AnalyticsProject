-- models/marts/marketing/channel_performance.sql
	{{ config(materialized='table') }}


with marketing_agg as (	
	select 
		channel,
		date_trunc('month',date) as month,
		sum(signups_per_channel) as signups_per_channel,
		sum(activations_per_channel) activations_per_channel,
		sum(mkt_spend) as mkt_spend,
		sum(mkt_spend) / sum(signups_per_channel) as avg_cac,
		sum(mkt_spend) / sum(activations_per_channel) as avg_cpa
	from {{ ref('marketing_attribution') }}
	group by 1,2
),
revenue_agg as(
	select
		acquisition_channel,
		date_trunc('month',signup_date) as month,
		sum(revenue_per_user) as total_revenue
	from {{ ref('revenue_summary') }}
	group by 1,2
	
)
select
	ma.channel,
	ma.month,
	ma.signups_per_channel,
	ma.activations_per_channel,
	round(ma.mkt_spend::numeric,2) as mkt_spend,
	round(ma.avg_cac::numeric,2) as avg_cac,
	round(ma.avg_cpa::numeric,2) as avg_cpa,
	ra.total_revenue,
	round((ra.total_revenue) / nullif(ma.mkt_spend::numeric,0),3) as roi,
	round((ma.activations_per_channel) / nullif(ma.signups_per_channel,0),3) as 
conversion_rate,
	round(((ra.total_revenue)- (ma.mkt_spend::numeric)) / 
nullif(ra.total_revenue,0),3) as profit_margin,
	case
		when round((ra.total_revenue) / nullif(ma.mkt_spend::numeric,0),3) > 
1.5 then 'High ROI'
		when round((ra.total_revenue) / nullif(ma.mkt_spend::numeric,0),3) >= 
0.5 then 'Near Breakeven'
		else 'Negative ROI' end as roi_tier
from marketing_agg ma
left join revenue_agg ra
on ma.month = ra.month and ma.channel = ra.acquisition_channel
order by 1,2
