{#-
    Campaign-level spend must add up to account-level spend, within
    `spend_reconciliation_tolerance` (0.5%), for every account and day.

    Both sides come from separate GAQL queries, so this catches campaigns lost on the way:
    a status filter that drops removed campaigns, a failed or partial load, a bad join in
    the mart. A day present on only one side fails too.
-#}

{%- set tolerance = var('spend_reconciliation_tolerance') -%}

with account_spend as (
    select customer_id, date_day, cost as account_cost
    from {{ ref('stg_google_ads__account_daily') }}
),

campaign_spend as (
    select customer_id, date_day, sum(cost) as campaign_cost
    from {{ ref('fct_campaign_performance_daily') }}
    group by customer_id, date_day
),

compared as (
    select
        coalesce(account_spend.customer_id, campaign_spend.customer_id) as customer_id,
        coalesce(account_spend.date_day, campaign_spend.date_day) as date_day,
        coalesce(account_spend.account_cost, 0) as account_cost,
        coalesce(campaign_spend.campaign_cost, 0) as campaign_cost
    from account_spend
    full outer join campaign_spend
        on campaign_spend.customer_id = account_spend.customer_id
        and campaign_spend.date_day = account_spend.date_day
)

select
    customer_id,
    date_day,
    account_cost,
    campaign_cost,
    campaign_cost - account_cost as difference
from compared
where abs(campaign_cost - account_cost) > {{ tolerance }} * abs(account_cost)
