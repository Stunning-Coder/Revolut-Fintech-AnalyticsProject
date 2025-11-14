-- models/marts/marketing/roi_efficiency.sql
	
	{{ config(materialized='table') }}

with ltv_avg_month as (
	select
		acquisition_channel,
		date_trunc('month', signup_date) as signup_month,
		avg(ltv_30d) as avg_ltv_30d,
		avg(ltv_90d) as avg_ltv_90d,
		avg(ltv_total) as avg_ltv_total
	from
		{{ ref('customer_ltv') }}
	group by 1,2
),
marketing_agg as(
	select
		date_trunc('month', date) as mkt_month,
		channel,
		sum(signups_per_channel) as agg_signups_per_channel,
		sum(mkt_spend) as agg_mkt_spend,
		(sum(mkt_spend) / nullif(sum(signups_per_channel),0)) as agg_cac
	from
		{{ ref('marketing_attribution') }}
	group by 1,2
)
select
	lm.acquisition_channel,
	lm.signup_month,
	round(lm.avg_ltv_30d::numeric,2) as avg_ltv_30d,
	round(lm.avg_ltv_90d::numeric,2) as avg_ltv_90d,
	round(avg_ltv_total::numeric,2) as avg_ltv_total,
	round(ma.agg_cac::numeric,2) as CAC,
	round((lm.avg_ltv_30d / ma.agg_cac)::numeric,3) as payback_ratio_30d,
	round((lm.avg_ltv_90d / ma.agg_cac)::numeric,3) as payback_ratio_90d,
	round((lm.avg_ltv_total / ma.agg_cac)::numeric,3) as payback_ratio,
	case
		when lm.avg_ltv_90d >= ma.agg_cac then 'Achieved'
		 else ' Not Achieved' end as payback_status_90d,
	case
		when (lm.avg_ltv_total / ma.agg_cac) >= 1.5 then 'High ROI'
		when (lm.avg_ltv_total / ma.agg_cac) <= 0.8 then  'Near Breakeven'
		else 'Underperforming' end as roi_tier
from ltv_avg_month lm
join marketing_agg ma
	on lm.acquisition_channel = ma.channel and lm.signup_month = ma.mkt_month
