-- models/marts/finance/profitability_dashboard.sql
 {{ config(materialized='table') }}

with calendar_month as(
	select generate_series('2024-01-01'::date, '2025-01-31'::date, 
interval '1 month') as cal_month
),
income_gen as (
	select
		rt.cal_month,
		ma.channel as acq_channel,
		sum(ma.mkt_spend) as spend,
		sum(rt.total_revenue) as revenue
	from {{ ref('revenue_trends') }} rt
	left join {{ ref('marketing_attribution') }} ma
	on rt.cal_month = ma.date and rt.acquisition_channel = ma.channel
	group by 1,2
	order by 1,2
)
select
	cm.cal_month as month,
	coalesce(ig.acq_channel,'n/a') acq_channel,
	round(coalesce(ig.spend::numeric,0),2) mkt_spend,
	round(coalesce(ig.revenue::numeric,0),2) curr_mon_revenue,
	lag(nullif(ig.revenue,0)) over (partition by ig.acq_channel order 
by cm.cal_month) as prev_month_revenue,
	round(coalesce((ig.revenue - ig.spend)::numeric,0),2) net_profit,
	round(coalesce(ig.revenue / ig.spend,0)::numeric,3) roi,
	round(coalesce((ig.revenue - ig.spend)/ig.revenue, 0)::numeric,3) 
as profit_margin,
	case
		when (ig.revenue / ig.spend) > 1.5 then TRUE else FALSE 
end as high_roi_flag,
	round((coalesce(ig.revenue::numeric,0) - lag(nullif(ig.revenue,0)) 
over (partition by ig.acq_channel order by cm.cal_month)) / 
nullif(lag(ig.revenue) over (partition by ig.acq_channel order by 
cm.cal_month),0),3) as revenue_growth_rate
from 
	calendar_month cm
left join
	income_gen ig
		on cm.cal_month = ig.cal_month
