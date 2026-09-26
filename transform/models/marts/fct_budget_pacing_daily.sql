{{
    config(
        partition_by={'field': 'date_day', 'data_type': 'date'} if target.type == 'bigquery' else none,
        cluster_by=['customer_id', 'budget_id'] if target.type == 'bigquery' else none,
    )
}}

{#-
    Month-to-date spend against month-to-date budget for every budget, per day.
    Pacing is measured per budget, not per campaign, because a shared budget caps the
    combined spend of all its campaigns.
-#}

with budget_days as (
    select * from {{ ref('int_google_ads__budget_daily') }}
),

month_to_date as (
    select
        *,
        sum(spend) over month_so_far as mtd_spend,
        sum(daily_budget) over month_so_far as mtd_budget,
        count(*) over month_so_far as days_elapsed,
        {{ dbt.datediff('month_start', dbt.last_day('month_start', 'month'), 'day') }} + 1 as days_in_month
    from budget_days
    window month_so_far as (
        partition by customer_id, budget_id, month_start
        order by date_day
        rows between unbounded preceding and current row
    )
)

select
    date_day,
    customer_id,
    budget_id,
    budget_name,
    budget_period,
    budget_is_shared,
    daily_budget,
    budget_snapshot_date,
    campaigns_with_delivery,
    spend,
    mtd_spend,
    mtd_budget,
    days_elapsed,
    days_in_month,
    daily_budget * days_in_month as month_budget,
    round(mtd_spend * days_in_month / days_elapsed, 2) as projected_month_spend,
    round(mtd_spend / nullif(mtd_budget, 0), 4) as pacing_ratio,
    case
        when budget_period != 'DAILY' or coalesce(daily_budget, 0) = 0 then 'NOT_APPLICABLE'
        when mtd_spend < {{ var('pacing_under_threshold') }} * mtd_budget then 'UNDER'
        when mtd_spend > {{ var('pacing_over_threshold') }} * mtd_budget then 'OVER'
        else 'ON_TRACK'
    end as pacing_status
from month_to_date
