-- models/marts/finance/revenue_trends.sql
{{ config(materialized='table') }}

with calendar_month as(
	select generate_series('2024-01-01'::date, '2025-01-31'::date, 
interval '1 month') as cal_month
),
revenue_agg as( 
	select
		date_trunc('month',signup_date) as signup_month,
		acquisition_channel,
		sum(gmv_per_user) as total_gmv,
		sum(revenue_per_user) as total_revenue,
		avg(take_rate) as avg_take_rate_per_period,
		count(user_id) as active_contributing_customers,
		avg(days_between_first_last_txn) as avg_engagement_span
	from
		{{ ref ('revenue_summary') }}
	group by 1,2
	order by signup_month
)
select 
	cm.cal_month,
	coalesce(ra.acquisition_channel,'n/a') as acquisition_channel,
	coalesce(ra.total_gmv,0) as total_gmw,
	coalesce(ra.total_revenue,0) as total_revenue,
	coalesce(round(ra.avg_take_rate_per_period, 3),0) as 
avg_take_rate_per_period,
	coalesce(ra.active_contributing_customers,0) as 
active_contributing_customers,
	coalesce(round(ra.avg_engagement_span,3),0) as 
avg_engagement_span,
	coalesce(lag(total_gmv,0) over(partition by ra.acquisition_channel 
order by cal_month),0) as month_over_month_growth_gmv,
	coalesce(lag(total_revenue,0) over(partition by 
ra.acquisition_channel order by cal_month),0) as 
month_over_month_growth_revenue
from calendar_month cm
left join revenue_agg ra on cm.cal_month = ra.signup_month
