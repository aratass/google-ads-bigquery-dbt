{#-
    Every unit of campaign spend must land on exactly one budget in the pacing mart.
    Fails if the budget lookup drops a campaign (no snapshot) or double counts one.
-#}

with campaign_spend as (
    select customer_id, date_day, sum(cost) as campaign_cost
    from {{ ref('fct_campaign_performance_daily') }}
    group by customer_id, date_day
),

paced_spend as (
    select customer_id, date_day, sum(spend) as paced_cost
    from {{ ref('fct_budget_pacing_daily') }}
    group by customer_id, date_day
)

select
    campaign_spend.customer_id,
    campaign_spend.date_day,
    campaign_spend.campaign_cost,
    paced_spend.paced_cost
from campaign_spend
left join paced_spend
    on paced_spend.customer_id = campaign_spend.customer_id
    and paced_spend.date_day = campaign_spend.date_day
where paced_spend.paced_cost is null
    or abs(campaign_spend.campaign_cost - paced_spend.paced_cost) > 0.01
