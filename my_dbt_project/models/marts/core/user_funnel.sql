-- models/marts/user_funnel.sql

{{ config(materialized='table') }}

select
	su.user_id,
	iep.signup_ts,
	date(iep.signup_ts) as signup_date,
	su.country,
	su.acquisition_channel,
	iep.kyc_start_ts,
	iep.kyc_complete_ts,
	iep.add_money_click_ts,
	iep.activation_ts,
	iep.kyc_start_ts is not null as kyc_started_flag,
	iep.kyc_complete_ts is not null as kyc_completed_flag,
	iep.add_money_click_ts is not null as add_money_clicked_flag,
	iep.activation_ts is not null as activated_flag,
	case when iep.kyc_start_ts is not null and iep.signup_ts is not null then extract(day from(iep.kyc_start_ts - iep.signup_ts)) end as time_to_kyc_start_days,
	case when iep.kyc_complete_ts is not null and iep.kyc_start_ts is not null then extract(day from(iep.kyc_complete_ts - iep.kyc_start_ts)) end as 
time_to_kyc_complete_days,
	case when iep.add_money_click_ts is not null and iep.kyc_complete_ts is not null then extract(day from(iep.add_money_click_ts - iep.kyc_complete_ts)) end as 
time_to_add_money_days,
	case when iep.activation_ts is not null and iep.add_money_click_ts is not null then extract(day from(iep.activation_ts - iep.add_money_click_ts)) end as 
time_to_activation_days,
	case
  when iep.activation_ts IS NOT NULL THEN 'activated'
  when iep.add_money_click_ts IS NOT NULL THEN 'add_money_stage'
  when iep.kyc_complete_ts IS NOT NULL THEN 'kyc_completed'
  when iep.kyc_start_ts IS NOT NULL THEN 'kyc_started'
  else 'signed_up_only'
end as funnel_stage
from {{ ref('stg_users') }} su
left join {{ ref('int_events_pivot') }} iep
using(user_id)
