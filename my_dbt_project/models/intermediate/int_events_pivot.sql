-- models/intermediate/int_events_pivot.sql

{{ config(materialized='table') }}

select 
	u.user_id,
	u.signup_date as signup_ts,
	min(case when e.event_name = 'kyc_start' then e.event_timestamp end) as kyc_start_ts,
	min(case when e.event_name = 'kyc_complete' then e.event_timestamp end) as kyc_complete_ts,
	min(case when e.event_name = 'add_money_click' then 
e.event_timestamp end) as add_money_click_ts,
	min(case when e.event_name = 'activation' then e.event_timestamp end) as activation_ts
from {{ ref('stg_users') }} u
left join {{ ref('stg_events') }} e using(user_id)
group by 1,2
