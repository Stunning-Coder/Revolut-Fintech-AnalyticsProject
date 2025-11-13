-- models/marts/core/cohorts.sql

{{ config(materialized='table') }}


with user_cohorts as (
    select
        user_id,
        signup_date::date as cohort_date,
        date_trunc('week', signup_date) as cohort_week,
        date_trunc('month', signup_date) as cohort_month
    from {{ ref('stg_users') }}
),
activity_event as (
    select
        user_id,
        event_date
    from {{ ref('int_user_activity') }}
),
cohort_activity as (
    select
        uc.user_id,
        uc.cohort_date,
        (ae.event_date - uc.cohort_date) as days_since_signup
    from user_cohorts uc
    left join activity_event ae using(user_id)
),
cohort_sizes as (
    select
        cohort_date,
        count(distinct user_id) as cohort_size
    from user_cohorts
    group by cohort_date
),
retention_by_day as (
    select
        uc.cohort_date,
        ca.days_since_signup,
        count(distinct ca.user_id) as active_users
    from user_cohorts uc
    left join cohort_activity ca using(user_id)
    group by 1, 2
)
select
    rbd.cohort_date,
    rbd.days_since_signup,
    rbd.active_users,
    cs.cohort_size,
    round(rbd.active_users::numeric / nullif(cs.cohort_size, 0), 3) as 
retention_rate
from retention_by_day rbd
left join cohort_sizes cs using(cohort_date)
order by cohort_date, days_since_signup
