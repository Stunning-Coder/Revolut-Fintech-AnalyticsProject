-- models/intermediate/int_marketing_daily.sql
{{ config(materialized='table') }}

select 
	date,
	channel,
	spend,
	case when extract(dow from date) in (0,6) then true else false end as is_weekend,
	extract(week from date) as week,
	extract(month from date) as month,
	extract(quarter from date) as quarter,
	extract(year from date) as year
from {{ ref('stg_marketing') }}
