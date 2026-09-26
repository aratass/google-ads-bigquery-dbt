{#-
    One row per budget per calendar day, for each month in which the budget delivered,
    from the first to the last day loaded for the account.

    GAQL returns an attribute's current value only, so the budget in force on a day is
    taken from the latest snapshot on or before that day. Days older than the first
    snapshot use the earliest snapshot, the best information available.
-#}

with campaign_days as (
    select customer_id, campaign_id, date_day, cost
    from {{ ref('stg_google_ads__campaign_daily') }}
),

snapshots as (
    select * from {{ ref('stg_google_ads__campaign_budget_snapshots') }}
),

loaded_days as (
    select
        customer_id,
        min(date_day) as first_loaded_day,
        max(date_day) as last_loaded_day
    from {{ ref('stg_google_ads__account_daily') }}
    group by customer_id
),

-- The budget each campaign belonged to on each day it delivered.
campaign_budget_candidates as (
    select
        campaign_days.customer_id,
        campaign_days.date_day,
        campaign_days.cost,
        snapshots.budget_id,
        row_number() over (
            partition by campaign_days.customer_id, campaign_days.campaign_id, campaign_days.date_day
            order by
                case when snapshots.snapshot_date <= campaign_days.date_day then 0 else 1 end,
                case when snapshots.snapshot_date <= campaign_days.date_day then snapshots.snapshot_date end desc,
                snapshots.snapshot_date
        ) as snapshot_rank
    from campaign_days
    inner join snapshots
        on snapshots.customer_id = campaign_days.customer_id
        and snapshots.campaign_id = campaign_days.campaign_id
),

budget_spend as (
    select
        customer_id,
        budget_id,
        date_day,
        sum(cost) as spend,
        count(*) as campaigns_with_delivery
    from campaign_budget_candidates
    where snapshot_rank = 1
    group by customer_id, budget_id, date_day
),

budget_months as (
    select distinct
        customer_id,
        budget_id,
        cast({{ dbt.date_trunc('month', 'date_day') }} as date) as month_start
    from budget_spend
),

day_offsets as (
    {%- for offset in range(31) %}
    select {{ offset }} as day_offset{% if not loop.last %} union all{% endif %}
    {%- endfor %}
),

calendar as (
    select
        budget_months.customer_id,
        budget_months.budget_id,
        budget_months.month_start,
        cast({{ dbt.dateadd('day', 'day_offsets.day_offset', 'budget_months.month_start') }} as date) as date_day,
        loaded_days.first_loaded_day,
        loaded_days.last_loaded_day
    from budget_months
    cross join day_offsets
    inner join loaded_days
        on loaded_days.customer_id = budget_months.customer_id
),

spine as (
    select customer_id, budget_id, month_start, date_day
    from calendar
    where date_day <= cast({{ dbt.last_day('month_start', 'month') }} as date)
        and date_day between first_loaded_day and last_loaded_day
),

budget_snapshots as (
    select distinct
        customer_id,
        budget_id,
        snapshot_date,
        budget_name,
        budget_period,
        budget_amount,
        budget_is_shared
    from snapshots
),

budget_candidates as (
    select
        spine.customer_id,
        spine.budget_id,
        spine.month_start,
        spine.date_day,
        budget_snapshots.snapshot_date as budget_snapshot_date,
        budget_snapshots.budget_name,
        budget_snapshots.budget_period,
        budget_snapshots.budget_amount,
        budget_snapshots.budget_is_shared,
        row_number() over (
            partition by spine.customer_id, spine.budget_id, spine.date_day
            order by
                case when budget_snapshots.snapshot_date <= spine.date_day then 0 else 1 end,
                case when budget_snapshots.snapshot_date <= spine.date_day then budget_snapshots.snapshot_date end desc,
                budget_snapshots.snapshot_date
        ) as snapshot_rank
    from spine
    inner join budget_snapshots
        on budget_snapshots.customer_id = spine.customer_id
        and budget_snapshots.budget_id = spine.budget_id
)

select
    budget_candidates.customer_id,
    budget_candidates.budget_id,
    budget_candidates.date_day,
    budget_candidates.month_start,
    budget_candidates.budget_name,
    budget_candidates.budget_period,
    budget_candidates.budget_is_shared,
    budget_candidates.budget_amount as daily_budget,
    budget_candidates.budget_snapshot_date,
    coalesce(budget_spend.spend, 0) as spend,
    coalesce(budget_spend.campaigns_with_delivery, 0) as campaigns_with_delivery
from budget_candidates
left join budget_spend
    on budget_spend.customer_id = budget_candidates.customer_id
    and budget_spend.budget_id = budget_candidates.budget_id
    and budget_spend.date_day = budget_candidates.date_day
where budget_candidates.snapshot_rank = 1
